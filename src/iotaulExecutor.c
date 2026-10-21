//
// FILE            iotaulExecutor.c
//
// Copyright 2026 Seamware
// SPDX-License-Identifier: Apache-2.0
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
// The IoT Agent UltraLight's commands, as Service Executions - see header.
//
// Threads: serviceExecute runs on a broker thread and never blocks - an MQTT command is published (the
// publish is queued by libmosquitto), an HTTP command is queued for the HTTP worker, a pull command is
// stored. Answers arrive on libmosquitto's thread (cmdexe), on the worker (the POST's response) and on the
// southbound server's thread (a device's getCmd=1, and a command's answer in its measure); all report through
// brokerP->serviceUpdateIn - never from inside serviceExecute, on the broker's thread.
//
#include <pthread.h>                                  // pthread_*
#include <stdbool.h>                                  // bool
#include <stdio.h>                                    // snprintf
#include <stdlib.h>                                   // malloc, free, atoi
#include <string.h>                                   // strlen, strncmp, strchr, strdup, memcpy
#include <strings.h>                                  // strcasecmp, strncasecmp

#include <mosquitto.h>                                // mosquitto_*

#include "corLog/corLog.h"                            // COR_E, COR_W, COR_V
#include "corAlloc/CorAlloc.h"                        // CorAlloc
#include "corAlloc/corAllocBufferInit.h"              // corAllocBufferInit
#include "corAlloc/corAllocBufferReset.h"             // corAllocBufferReset
#include "corAlloc/corAlloc.h"                        // corAlloc
#include "corJson/CorJson.h"                          // CorJson
#include "corJson/corJsonCreate.h"                    // corJsonCreate
#include "corJson/corJsonParse.h"                     // corJsonParse
#include "corJson/corJsonRender.h"                    // corJsonFastRender
#include "corJson/corJsonRenderSize.h"                // corJsonFastRenderSize
#include "corTree/CorNode.h"                          // CorNode
#include "corTree/corTreeBuilder.h"                   // corTreeObject, corTreeString, corTreeChildAdd
#include "corTree/corTreeLookup.h"                    // corTreeLookup
#include "corRest/corRestClient.h"                    // corRestClient*

#include "corBridge/BridgeBroker.h"                   // BridgeBroker, BRIDGE_*

#include "iotaulSouth.h"                            // iotaulSouthOpen
#include "iotaulMeasures.h"                         // iotaulMeasure
#include "iotaulExecutor.h"                         // Own interface



#define SCHEME_MQTT   "iota-ul+mqtt://"
#define SCHEME_HTTP   "iota-ul+http://"
#define SCHEME_HTTPS  "iota-ul+https://"
#define SCHEME_POLL   "iota-ul+poll://"



// -----------------------------------------------------------------------------
//
// Pending - a command waiting for the device's answer, oldest first per (apikey, device, cmd): an MQTT
// command published, or a pull command - stored until the device asks (getCmd=1), then delivered
//
typedef struct Pending
{
  char             executionId[128];
  char             apikey[128];
  char             deviceId[256];
  char             cmd[128];
  bool             pull;                              // iota-ul+poll://
  bool             delivered;                         // a pull command handed to the device
  char*            payload;                           // a pull command's {"<cmd>": <value>}, malloc'd
  struct Pending*  next;
} Pending;

//
// HttpJob - a command to POST, for the worker
//
typedef struct HttpJob
{
  char*            url;                               // http[s]://..., without the cmd parameter
  char*            body;
  char*            cmd;
  char             executionId[128];
  struct HttpJob*  next;
} HttpJob;



static const BridgeBroker*  brokerP     = NULL;
static struct mosquitto*    mosq        = NULL;
static bool                 connected   = false;
static char                 serverDesc[300];

static pthread_mutex_t      pendingMutex = PTHREAD_MUTEX_INITIALIZER;
static Pending*             pendingHead  = NULL;

static pthread_mutex_t      jobMutex     = PTHREAD_MUTEX_INITIALIZER;
static pthread_cond_t       jobCond      = PTHREAD_COND_INITIALIZER;
static HttpJob*             jobHead      = NULL;
static HttpJob*             jobTail      = NULL;
static bool                 workerStop   = false;
static bool                 workerUp     = false;
static pthread_t            worker;
static bool                 southUp      = false;



// -----------------------------------------------------------------------------
//
// report - serviceUpdateIn, if the broker has it
//
static void reportProgress(const char* executionId, const char* status, const char* progressJson, const char* outputJson, const char* errorJson)
{
  if ((brokerP == NULL) || (brokerP->abiVersion < 11) || (brokerP->serviceUpdateIn == NULL))
    return;

  int r = brokerP->serviceUpdateIn("iotaul", executionId, status, progressJson, outputJson, errorJson);

  if ((r != BRIDGE_OK) && (r != BRIDGE_NOT_FOUND))
    COR_W("iotaul: the broker did not take the report on '%s' (%d)", executionId, r);
}

static void report(const char* executionId, const char* status, const char* outputJson, const char* errorJson)
{
  reportProgress(executionId, status, NULL, outputJson, errorJson);
}



// -----------------------------------------------------------------------------
//
// pendingFree -
//
static void pendingFree(Pending* p)
{
  free(p->payload);
  free(p);
}



// -----------------------------------------------------------------------------
//
// failed - an execution failed, with a ProblemDetails saying why
//
static void failed(const char* executionId, int status, const char* title, const char* detail)
{
  char      buf[2048];
  CorAlloc  ka;
  char      kaBuf[4096];

  corAllocBufferInit(&ka, kaBuf, sizeof(kaBuf), 4096, NULL, "iotaul problem");

  CorNode* pdP = corTreeObject(&ka, NULL);

  corTreeChildAdd(pdP, corTreeString(&ka, "type",   "https://uri.etsi.org/ngsi-ld/errors/InternalError"));
  corTreeChildAdd(pdP, corTreeString(&ka, "title",  title));
  corTreeChildAdd(pdP, corTreeInteger(&ka, "status", status));
  corTreeChildAdd(pdP, corTreeString(&ka, "detail", detail));

  if (corJsonFastRenderSize(pdP) < (int) sizeof(buf))
  {
    corJsonFastRender(pdP, buf);
    report(executionId, "failed", NULL, buf);
  }
  else
    report(executionId, "failed", NULL, NULL);

  corAllocBufferReset(&ka, false);
}



// -----------------------------------------------------------------------------
//
// render - a tree as JSON text, malloc'd
//
static char* render(CorNode* nodeP)
{
  int   size = corJsonFastRenderSize(nodeP) + 1;
  char* text = malloc(size);

  if (text != NULL)
    corJsonFastRender(nodeP, text);

  return text;
}



// -----------------------------------------------------------------------------
//
// valueText - a value as UltraLight carries it: a string as it is, anything else as its JSON text; malloc'd
//
static char* valueText(CorNode* nodeP)
{
  if (nodeP->type == CorString)
    return strdup(nodeP->value.s);

  const char* name = nodeP->name;

  nodeP->name = NULL;
  char* text = render(nodeP);
  nodeP->name = (char*) name;

  return text;
}



// -----------------------------------------------------------------------------
//
// commandPayload - <deviceId>@<cmd>|<input.value>, as iotagent-ul makes it (ulParser.createCommandPayload);
// an object value - or the input without id and type, when it has no "value" - as <deviceId>@<cmd>|<key>=
// <value>|... (each value as valueText makes it). malloc'd; NULL: the input is not a JSON object
//
static char* commandPayload(const char* deviceId, const char* cmd, const char* inputJson)
{
  CorAlloc  ka;
  CorJson   cj;
  char      kaBuf[16 * 1024];
  char*     copy = strdup((inputJson != NULL) ? inputJson : "{}");
  char*     out  = NULL;

  if (copy == NULL)
    return NULL;

  corAllocBufferInit(&ka, kaBuf, sizeof(kaBuf), 16 * 1024, NULL, "iotaul input");
  corJsonCreate(&cj, &ka);

  CorNode* inP = corJsonParse(&cj, copy);

  if ((inP != NULL) && (inP->type == CorObject))
  {
    CorNode* valueP = corTreeLookup(inP, "value");

    if (valueP == NULL)
    {
      CorNode* idP   = corTreeLookup(inP, "id");
      CorNode* typeP = corTreeLookup(inP, "type");

      if (idP   != NULL) corTreeChildRemove(inP, idP);
      if (typeP != NULL) corTreeChildRemove(inP, typeP);
      valueP = inP;
    }

    int size = strlen(deviceId) + strlen(cmd) + 2;

    if ((out = malloc(size)) != NULL)
      snprintf(out, size, "%s@%s", deviceId, cmd);

    CorNode* firstP = (valueP->type == CorObject) ? valueP->value.head : valueP;

    for (CorNode* pairP = firstP; (pairP != NULL) && (out != NULL); pairP = (valueP->type == CorObject) ? pairP->next : NULL)
    {
      char* text = valueText(pairP);

      if (text == NULL)
      {
        free(out);
        out = NULL;
        break;
      }

      const char* key = (valueP->type == CorObject) ? pairP->name : NULL;
      int         len = strlen(out) + ((key != NULL) ? strlen(key) + 1 : 0) + strlen(text) + 2;
      char*       grown = realloc(out, len);

      if (grown != NULL)
      {
        out = grown;
        if (key != NULL)
          snprintf(&out[strlen(out)], len - strlen(out), "|%s=%s", key, text);
        else
          snprintf(&out[strlen(out)], len - strlen(out), "|%s", text);
      }
      else
      {
        free(out);
        out = NULL;
      }

      free(text);
    }
  }

  corAllocBufferReset(&ka, false);
  free(copy);
  return out;
}



// -----------------------------------------------------------------------------
//
// segment - the next '/'-separated part of s into buf; NULL at the end
//
static const char* segment(const char* s, char* buf, int size)
{
  int n = 0;

  while ((*s != 0) && (*s != '/') && (*s != '?'))
  {
    if (n < size - 1)
      buf[n++] = *s;
    s++;
  }
  buf[n] = 0;

  return (n == 0) ? NULL : s;
}



// -----------------------------------------------------------------------------
//
// queryParam - the value of 'name' in a query string (after '?'), into buf; false: not there
//
static bool queryParam(const char* query, const char* name, char* buf, int size)
{
  int nameLen = strlen(name);

  for (const char* p = query; (p != NULL) && (*p != 0); )
  {
    if ((strncmp(p, name, nameLen) == 0) && (p[nameLen] == '='))
    {
      const char* v   = &p[nameLen + 1];
      const char* end = strchr(v, '&');
      int         len = (end != NULL) ? (int) (end - v) : (int) strlen(v);

      if (len >= size)
        len = size - 1;
      memcpy(buf, v, len);
      buf[len] = 0;
      return true;
    }

    p = strchr(p, '&');
    p = (p != NULL) ? p + 1 : NULL;
  }

  return false;
}



// -----------------------------------------------------------------------------
//
// deviceParse - <apikey>/<deviceId>/<cmd> after the scheme, into pP; what follows it (NUL or '?'), NULL:
// not that
//
static const char* deviceParse(const char* url, const char* scheme, Pending* pP)
{
  const char* s = &url[strlen(scheme)];

  memset(pP, 0, sizeof(Pending));

  if (((s = segment(s, pP->apikey, sizeof(pP->apikey))) == NULL) || (*s++ != '/') ||
      ((s = segment(s, pP->deviceId, sizeof(pP->deviceId))) == NULL) || (*s++ != '/') ||
      ((s = segment(s, pP->cmd, sizeof(pP->cmd))) == NULL) || ((*s != 0) && (*s != '?')))
  {
    COR_W("iotaul: '%s' - not %s<apikey>/<deviceId>/<command>", url, scheme);
    return NULL;
  }

  return s;
}



// -----------------------------------------------------------------------------
//
// pendingAppend - the newest of its (apikey, device, cmd)
//
static void pendingAppend(Pending* pendingP)
{
  pthread_mutex_lock(&pendingMutex);
  Pending** lastPP = &pendingHead;
  while (*lastPP != NULL)
    lastPP = &(*lastPP)->next;
  *lastPP = pendingP;
  pthread_mutex_unlock(&pendingMutex);
}



// -----------------------------------------------------------------------------
//
// mqttExecute - iota-ul+mqtt://<apikey>/<deviceId>/<cmd>[?qos=..&retain=..]
//
static int mqttExecute(const char* url, const char* executionId, const char* inputJson)
{
  Pending     p;
  const char* s = deviceParse(url, SCHEME_MQTT, &p);

  if (s == NULL)
    return BRIDGE_BAD_INPUT;

  char qosBuf[8]    = "0";
  char retainBuf[8] = "false";

  if (*s == '?')
  {
    queryParam(&s[1], "qos",    qosBuf,    sizeof(qosBuf));
    queryParam(&s[1], "retain", retainBuf, sizeof(retainBuf));
  }

  if (mosq == NULL)
  {
    COR_W("iotaul: '%s' - no MQTT server configured (\"server\": { \"uri\": ... })", url);
    return BRIDGE_ERR;
  }

  char* payload = commandPayload(p.deviceId, p.cmd, inputJson);

  if (payload == NULL)
    return BRIDGE_BAD_INPUT;

  char topic[600];

  snprintf(topic, sizeof(topic), "/%s/%s/cmd", p.apikey, p.deviceId);
  snprintf(p.executionId, sizeof(p.executionId), "%s", executionId);

  //
  // Pending BEFORE the publish: the device may answer before mosquitto_publish returns
  //
  Pending* pendingP = malloc(sizeof(Pending));

  if (pendingP == NULL)
  {
    free(payload);
    return BRIDGE_ERR;
  }

  *pendingP = p;
  pendingAppend(pendingP);

  int rc = mosquitto_publish(mosq, NULL, topic, strlen(payload), payload, atoi(qosBuf), strcmp(retainBuf, "true") == 0);

  free(payload);

  if (rc != MOSQ_ERR_SUCCESS)
  {
    pthread_mutex_lock(&pendingMutex);
    for (Pending** pp = &pendingHead; *pp != NULL; pp = &(*pp)->next)
    {
      if (*pp == pendingP)
      {
        *pp = pendingP->next;
        break;
      }
    }
    pthread_mutex_unlock(&pendingMutex);
    pendingFree(pendingP);

    COR_W("iotaul: command '%s' to %s: %s", p.cmd, topic, mosquitto_strerror(rc));
    return BRIDGE_ERR;
  }

  //
  // No report from here: this is the broker's thread, inside its serviceExecute call - the broker has stored
  // the execution as executing already; the outcome comes on libmosquitto's thread (cmdexe)
  //
  COR_V("iotaul: execution '%s': published on %s", executionId, topic);
  return BRIDGE_OK;
}



// -----------------------------------------------------------------------------
//
// pollExecute - iota-ul+poll://<apikey>/<deviceId>/<cmd> - stored until the device asks for its commands
// (getCmd=1 on the southbound port)
//
static int pollExecute(const char* url, const char* executionId, const char* inputJson)
{
  Pending p;

  if (deviceParse(url, SCHEME_POLL, &p) == NULL)
    return BRIDGE_BAD_INPUT;

  if (southUp == false)
  {
    COR_W("iotaul: '%s' - no southbound port configured (\"southbound\": { \"port\": ... }): a device cannot ask for it", url);
    return BRIDGE_ERR;
  }

  Pending* pendingP = malloc(sizeof(Pending));

  if (pendingP == NULL)
    return BRIDGE_ERR;

  *pendingP = p;
  pendingP->pull    = true;
  pendingP->payload = commandPayload(p.deviceId, p.cmd, inputJson);
  snprintf(pendingP->executionId, sizeof(pendingP->executionId), "%s", executionId);

  if (pendingP->payload == NULL)
  {
    free(pendingP);
    return BRIDGE_BAD_INPUT;
  }

  pendingAppend(pendingP);

  COR_V("iotaul: execution '%s': stored for %s/%s", executionId, p.apikey, p.deviceId);
  return BRIDGE_OK;
}



// -----------------------------------------------------------------------------
//
// httpExecute - iota-ul+http[s]://<host>[:<port>]/<path>?cmd=<cmd>&device=<deviceId>[&...] - queued for the
// worker. The device id is in the payload (<deviceId>@<cmd>|...): iotagent-ul has it from the device's
// provisioning, a Service Registration says it here
//
static int httpExecute(const char* url, const char* executionId, const char* inputJson)
{
  const char* rest  = strstr(url, "://") + 3;
  bool        tls   = (strncmp(url, SCHEME_HTTPS, strlen(SCHEME_HTTPS)) == 0);
  const char* query = strchr(rest, '?');
  char        cmd[128];
  char        deviceId[256];

  if ((query == NULL) || (queryParam(&query[1], "cmd", cmd, sizeof(cmd)) == false) || (cmd[0] == 0) ||
      (queryParam(&query[1], "device", deviceId, sizeof(deviceId)) == false) || (deviceId[0] == 0))
  {
    COR_W("iotaul: '%s' - the command or the device is missing (?cmd=<command>&device=<deviceId>)", url);
    return BRIDGE_BAD_INPUT;
  }

  //
  // The device's URL: the scheme without "iota-ul+", and the query without cmd= and device=
  //
  int   size   = strlen(url) + 16;
  char* devUrl = malloc(size);

  if (devUrl == NULL)
    return BRIDGE_ERR;

  int at = snprintf(devUrl, size, "%s://%.*s", (tls == true) ? "https" : "http", (int) (query - rest), rest);
  char sep = '?';

  for (const char* p = &query[1]; (p != NULL) && (*p != 0); )
  {
    const char* end = strchr(p, '&');
    int         len = (end != NULL) ? (int) (end - p) : (int) strlen(p);

    if ((strncmp(p, "cmd=", 4) != 0) && (strncmp(p, "device=", 7) != 0))
    {
      at += snprintf(&devUrl[at], size - at, "%c%.*s", sep, len, p);
      sep = '&';
    }

    p = (end != NULL) ? end + 1 : NULL;
  }

  char*    payload = commandPayload(deviceId, cmd, inputJson);
  HttpJob* jobP    = calloc(1, sizeof(HttpJob));

  if ((payload == NULL) || (jobP == NULL))
  {
    free(devUrl);
    free(payload);
    free(jobP);
    return (payload == NULL) ? BRIDGE_BAD_INPUT : BRIDGE_ERR;
  }

  jobP->url  = devUrl;
  jobP->body = payload;
  jobP->cmd  = strdup(cmd);
  snprintf(jobP->executionId, sizeof(jobP->executionId), "%s", executionId);

  pthread_mutex_lock(&jobMutex);
  if (jobTail != NULL)
    jobTail->next = jobP;
  else
    jobHead = jobP;
  jobTail = jobP;
  pthread_cond_signal(&jobCond);
  pthread_mutex_unlock(&jobMutex);

  return BRIDGE_OK;
}



// -----------------------------------------------------------------------------
//
// jsonString - s as a JSON string, malloc'd
//
static char* jsonString(const char* s)
{
  CorAlloc ka;
  char     kaBuf[1024];

  corAllocBufferInit(&ka, kaBuf, sizeof(kaBuf), 16 * 1024, NULL, "iotaul string");

  char* out = render(corTreeString(&ka, NULL, s));

  corAllocBufferReset(&ka, false);
  return out;
}



// -----------------------------------------------------------------------------
//
// jsonOrString - a field as iotagent-ul reads a result: JSON when it is JSON, else the text as a string; the
// JSON text, malloc'd
//
static char* jsonOrString(const char* field)
{
  CorAlloc  ka;
  CorJson   cj;
  char      kaBuf[4 * 1024];
  char*     copy = strdup(field);
  char*     out  = NULL;

  if (copy == NULL)
    return NULL;

  corAllocBufferInit(&ka, kaBuf, sizeof(kaBuf), 16 * 1024, NULL, "iotaul result");
  corJsonCreate(&cj, &ka);

  CorNode* nodeP = (field[0] != 0) ? corJsonParse(&cj, copy) : NULL;

  out = (nodeP != NULL) ? render(nodeP) : jsonString(field);

  corAllocBufferReset(&ka, false);
  free(copy);
  return out;
}



// -----------------------------------------------------------------------------
//
// ulResult - <deviceId>@<cmd>|<result>, as iotagent-ul reads a command's answer (ulParser.result): the device
// and the command into deviceId / cmd (when not NULL), the result into *outputP, malloc'd - its JSON when it is
// JSON, else a string (raw: the text as it is). Not that form: false, and *outputP is the whole text
//
static bool ulResult(const char* text, int len, char* deviceId, int devSize, char* cmd, int cmdSize, char** outputP, bool raw)
{
  char* copy = malloc(len + 1);

  *outputP = NULL;

  if (copy == NULL)
    return false;

  memcpy(copy, text, len);
  copy[len] = 0;

  char* bar = strchr(copy, '|');
  char* at  = strchr(copy, '@');

  if ((at == NULL) || ((bar != NULL) && (at > bar)))
  {
    *outputP = (raw == true) ? strdup(copy) : jsonString(copy);
    free(copy);
    return false;
  }

  *at = 0;

  char* cmdStart = at + 1;
  char* cmdEnd   = cmdStart + strcspn(cmdStart, "@|");
  char  saved    = *cmdEnd;

  *cmdEnd = 0;
  if (deviceId != NULL)
    snprintf(deviceId, devSize, "%s", copy);
  if (cmd != NULL)
    snprintf(cmd, cmdSize, "%s", cmdStart);
  *cmdEnd = saved;

  if (bar != NULL)
  {
    char* result = bar + 1;
    char* end    = strchr(result, '|');               // the second field only, as iotagent-ul takes it

    if (end != NULL)
      *end = 0;

    *outputP = (raw == true) ? strdup(result) : jsonOrString(result);
  }

  free(copy);
  return true;
}



// -----------------------------------------------------------------------------
//
// httpAnswer - a device's 200 response: <deviceId>@<cmd>|<result> as iotagent-ul reads it - the result is the
// execution's output; another body is the output as a string
//
static void httpAnswer(HttpJob* jobP, CorRestClientResponse* respP)
{
  char* output = NULL;

  if ((respP->body != NULL) && (respP->bodyLen > 0))
    ulResult(respP->body, respP->bodyLen, NULL, 0, NULL, 0, &output, false);

  report(jobP->executionId, "completed", output, NULL);
  free(output);
}



// -----------------------------------------------------------------------------
//
// httpWorker - POSTs the queued commands, one at a time
//
static void* httpWorker(void* arg)
{
  (void) arg;

  while (true)
  {
    pthread_mutex_lock(&jobMutex);
    while ((jobHead == NULL) && (workerStop == false))
      pthread_cond_wait(&jobCond, &jobMutex);

    if (workerStop == true)
    {
      pthread_mutex_unlock(&jobMutex);
      return NULL;
    }

    HttpJob* jobP = jobHead;
    jobHead = jobP->next;
    if (jobHead == NULL)
      jobTail = NULL;
    pthread_mutex_unlock(&jobMutex);

    CorRestClientRequest  req;
    CorRestClientResponse resp;

    corRestClientRequestInit(&req, CorVerbPost, jobP->url, NULL);
    corRestClientRequestHeader(&req, "Content-Type", "text/plain");
    corRestClientRequestBody(&req, jobP->body, strlen(jobP->body));
    corRestClientRequestTimeout(&req, 5000, 10000);

    int rc = corRestClientSend(&req, &resp);

    COR_V("iotaul: execution '%s': POST %s - %d", jobP->executionId, jobP->url, resp.statusCode);

    if ((rc == CORR_OK) && (resp.statusCode == 200))
      httpAnswer(jobP, &resp);
    else
    {
      char detail[512];

      if ((rc != CORR_OK) || (resp.statusCode <= 0))
        snprintf(detail, sizeof(detail), "the command could not be sent to %s: %s", jobP->url, (resp.errorDetail[0] != 0) ? resp.errorDetail : "no answer");
      else
      {
        //
        // iotagent-ul's error names the result of the answer, when it is <deviceId>@<cmd>|<result>, else its body
        //
        char* result = NULL;

        if ((resp.body != NULL) && (resp.bodyLen > 0))
          ulResult(resp.body, resp.bodyLen, NULL, 0, NULL, 0, &result, true);

        snprintf(detail, sizeof(detail), "the device answered %d%s%s", resp.statusCode, (result != NULL) ? ": " : "", (result != NULL) ? result : "");
        free(result);
      }

      failed(jobP->executionId, 502, "Device Refused", detail);
    }

    corRestClientResponseCleanup(&resp);
    corRestClientRequestCleanup(&req);

    free(jobP->url);
    free(jobP->body);
    free(jobP->cmd);
    free(jobP);
  }
}



// -----------------------------------------------------------------------------
//
// complete - the oldest execution of the command on the device, completed with the output (JSON text)
//
static void complete(const char* apikey, const char* deviceId, const char* cmd, const char* outputJson)
{
  Pending* foundP = NULL;

  pthread_mutex_lock(&pendingMutex);
  for (Pending** pp = &pendingHead; *pp != NULL; pp = &(*pp)->next)
  {
    Pending* p = *pp;

    if ((strcmp(p->apikey, apikey) == 0) && (strcmp(p->deviceId, deviceId) == 0) && (strcmp(p->cmd, cmd) == 0))
    {
      *pp    = p->next;
      foundP = p;
      break;
    }
  }
  pthread_mutex_unlock(&pendingMutex);

  if (foundP == NULL)
  {
    COR_W("iotaul: the answer to '%s' from %s/%s - no execution waits for it", cmd, apikey, deviceId);
    return;
  }

  report(foundP->executionId, "completed", outputJson, NULL);
  pendingFree(foundP);
}



// -----------------------------------------------------------------------------
//
// cmdexe - <deviceId>@<cmd>|<result> from a device on .../cmdexe: the oldest execution of that command on the
// device (the topic's) completed, its output the result - JSON when it is JSON, else a string
//
static void cmdexe(const char* apikey, const char* deviceId, const char* payload, int len)
{
  char  cmd[128];
  char* output = NULL;

  if (ulResult(payload, len, NULL, 0, cmd, sizeof(cmd), &output, false) == false)
  {
    COR_W("iotaul: cmdexe from %s/%s is not <deviceId>@<command>|<result> - dropped", apikey, deviceId);
    free(output);
    return;
  }

  complete(apikey, deviceId, cmd, output);
  free(output);
}



// -----------------------------------------------------------------------------
//
// iotaulCommandResult - a measure group <deviceId>@<cmd>|...: as iotagent-ul reads a command in a measure
// (ulParser.command) - one field without '=' is the result, as text; key=value fields an object of texts
//
void iotaulCommandResult(const char* apikey, const char* deviceId, const char* group, int len)
{
  CorAlloc ka;
  char     kaBuf[4 * 1024];
  char*    copy = malloc(len + 1);

  if (copy == NULL)
    return;

  memcpy(copy, group, len);
  copy[len] = 0;

  corAllocBufferInit(&ka, kaBuf, sizeof(kaBuf), 16 * 1024, NULL, "iotaul command group");

  char* bar = strchr(copy, '|');
  char* at  = strchr(copy, '@');
  char* cmd = (at != NULL) ? at + 1 : NULL;

  if (cmd == NULL)
  {
    corAllocBufferReset(&ka, false);
    free(copy);
    return;
  }

  if (bar != NULL)
    *bar = 0;

  char* cmdEnd = strchr(cmd, '@');

  if (cmdEnd != NULL)
    *cmdEnd = 0;

  CorNode* outP = NULL;

  if ((bar != NULL) && (strchr(bar + 1, '|') == NULL) && (strchr(bar + 1, '=') == NULL))
    outP = corTreeString(&ka, NULL, bar + 1);
  else
  {
    outP = corTreeObject(&ka, NULL);

    for (char* field = (bar != NULL) ? bar + 1 : NULL; (field != NULL) && (outP != NULL); )
    {
      char* next = strchr(field, '|');
      char* eq;

      if (next != NULL)
        *next++ = 0;

      if (((eq = strchr(field, '=')) == NULL) || (strchr(eq + 1, '=') != NULL))
      {
        COR_W("iotaul: the answer to '%s' from %s/%s - '%s' is not <key>=<value>: dropped", cmd, apikey, deviceId, field);
        outP = NULL;
        break;
      }

      *eq = 0;
      corTreeChildAdd(outP, corTreeString(&ka, field, eq + 1));
      field = next;
    }
  }

  char* output = (outP != NULL) ? render(outP) : NULL;

  if (output != NULL)
    complete(apikey, deviceId, cmd, output);

  free(output);
  corAllocBufferReset(&ka, false);
  free(copy);
}



// -----------------------------------------------------------------------------
//
// iotaulPullDeliver - the device's pull commands not yet delivered, joined as iotagent-ul joins them:
// <deviceId>@<cmd>|<value>|<deviceId>@<cmd2>|<value> - each as it was stored, in the order stored; "": none.
// Each is delivered from here on: its execution still executing, its progress { "delivered": true }. malloc'd.
//
char* iotaulPullDeliver(const char* apikey, const char* deviceId)
{
  int   size      = 1;
  int   delivered = 0;

  pthread_mutex_lock(&pendingMutex);

  for (Pending* p = pendingHead; p != NULL; p = p->next)
  {
    if ((p->pull == true) && (p->delivered == false) && (strcmp(p->apikey, apikey) == 0) && (strcmp(p->deviceId, deviceId) == 0))
    {
      size += strlen(p->payload) + 1;
      delivered++;
    }
  }

  char*      out = malloc(size);
  char     (*idV)[128] = (delivered > 0) ? malloc(delivered * 128) : NULL;
  int        ids = 0;

  if ((out != NULL) && ((delivered == 0) || (idV != NULL)))
  {
    int at = 0;

    out[0] = 0;

    for (Pending* p = pendingHead; p != NULL; p = p->next)
    {
      if ((p->pull == true) && (p->delivered == false) && (strcmp(p->apikey, apikey) == 0) && (strcmp(p->deviceId, deviceId) == 0))
      {
        at += snprintf(&out[at], size - at, "%s%s", (at > 0) ? "|" : "", p->payload);
        p->delivered = true;
        snprintf(idV[ids++], 128, "%s", p->executionId);
      }
    }
  }
  else
  {
    free(out);
    out = NULL;
  }

  pthread_mutex_unlock(&pendingMutex);

  //
  // Reported after the lock: the ids copied, as the answer may end an execution meanwhile
  //
  for (int ix = 0; ix < ids; ix++)
    reportProgress(idV[ix], "executing", "{\"delivered\":true}", NULL, NULL);

  free(idV);
  return out;
}



// -----------------------------------------------------------------------------
//
// onMessage - libmosquitto's thread: [/][ul/]<apikey>/<deviceId>/cmdexe, a command's answer, and
// [/][ul/]<apikey>/<deviceId>/attrs[/<attribute>], a measure
//
// ul/<a>/<b>/attrs is the "ul" level's measure, not a single measure of the device <b>/attrs: iotagent-ul
// reads it so too.
//
static void onMessage(struct mosquitto* m, void* userdata, const struct mosquitto_message* msgP)
{
  (void) m;
  (void) userdata;

  const char* s = msgP->topic;
  char        level[5][256];
  int         levels = 0;

  if (*s == '/')
    s++;

  while ((*s != 0) && (levels < 5))
  {
    const char* next = segment(s, level[levels], sizeof(level[0]));

    if (next == NULL)
      return;
    levels++;
    s = (*next == '/') ? next + 1 : next;
  }

  if (*s != 0)
    return;

  int first = ((levels >= 4) && (strcasecmp(level[0], "ul") == 0) && ((levels == 5) || (strcmp(level[3], "cmdexe") == 0) || (strcmp(level[3], "attrs") == 0))) ? 1 : 0;
  int rest  = levels - first;                         // <apikey>/<deviceId>/<what>[/<attribute>]

  if ((rest == 3) && (strcmp(level[first + 2], "cmdexe") == 0))
    cmdexe(level[first], level[first + 1], (const char*) msgP->payload, msgP->payloadlen);
  else if (((rest == 3) || (rest == 4)) && (strcmp(level[first + 2], "attrs") == 0))
  {
    const char* name    = NULL;
    char        message[512];
    const char* attrName = (rest == 4) ? level[first + 3] : NULL;
    int         status  = iotaulMeasure(level[first], level[first + 1], (const char*) msgP->payload, msgP->payloadlen,
                                        attrName, NULL, &name, message, sizeof(message));

    if (status != 200)
      COR_W("iotaul: measure on '%s' dropped - %s: %s", msgP->topic, name, message);
  }
}



// -----------------------------------------------------------------------------
//
// onConnect / onDisconnect - libmosquitto's thread; it reconnects by itself
//
static void onConnect(struct mosquitto* m, void* userdata, int rc)
{
  (void) userdata;

  if (rc != 0)
  {
    COR_W("iotaul: MQTT server %s refused the connection (%d)", serverDesc, rc);
    return;
  }

  connected = true;

  //
  // The topics iotagent-ul takes a command's answer on - with and without the "ul" level
  //
  mosquitto_subscribe(m, NULL, "/+/+/cmdexe",    1);
  mosquitto_subscribe(m, NULL, "/ul/+/+/cmdexe", 1);
  mosquitto_subscribe(m, NULL, "ul/+/+/cmdexe",  1);

  //
  // ... and its measures: all at once, and one attribute at a time
  //
  mosquitto_subscribe(m, NULL, "/+/+/attrs",      1);
  mosquitto_subscribe(m, NULL, "/+/+/attrs/+",    1);
  mosquitto_subscribe(m, NULL, "/ul/+/+/attrs",   1);
  mosquitto_subscribe(m, NULL, "/ul/+/+/attrs/+", 1);
  mosquitto_subscribe(m, NULL, "ul/+/+/attrs",    1);
  mosquitto_subscribe(m, NULL, "ul/+/+/attrs/+",  1);

  COR_V("iotaul: connected to %s", serverDesc);
}

static void onDisconnect(struct mosquitto* m, void* userdata, int rc)
{
  (void) m;
  (void) userdata;

  connected = false;

  if (rc != 0)
    COR_W("iotaul: lost the MQTT server %s - reconnecting", serverDesc);
}



// -----------------------------------------------------------------------------
//
// mqttOpen - mqtt://host[:port]
//
static int mqttOpen(const char* uri)
{
  char        host[256];
  int         port = 1883;
  const char* s    = uri;

  if (strncmp(s, "mqtt://", 7) != 0)
  {
    COR_E("iotaul: server.uri '%s' - only mqtt://host[:port] for now", uri);
    return BRIDGE_ERR;
  }
  s += 7;

  const char* colon = strchr(s, ':');
  const char* slash = strchr(s, '/');
  int         len   = (colon != NULL) ? (int) (colon - s) : (slash != NULL) ? (int) (slash - s) : (int) strlen(s);

  if ((len <= 0) || (len >= (int) sizeof(host)))
  {
    COR_E("iotaul: server.uri '%s' - no host", uri);
    return BRIDGE_ERR;
  }
  memcpy(host, s, len);
  host[len] = 0;

  if (colon != NULL)
    port = atoi(&colon[1]);

  snprintf(serverDesc, sizeof(serverDesc), "%s:%d", host, port);

  mosquitto_lib_init();
  mosq = mosquitto_new(NULL, true, NULL);
  if (mosq == NULL)
    return BRIDGE_ERR;

  mosquitto_connect_callback_set(mosq, onConnect);
  mosquitto_disconnect_callback_set(mosq, onDisconnect);
  mosquitto_message_callback_set(mosq, onMessage);
  mosquitto_reconnect_delay_set(mosq, 1, 30, true);

  int rc = mosquitto_connect_async(mosq, host, port, 30);

  if ((rc != MOSQ_ERR_SUCCESS) && (rc != MOSQ_ERR_ERRNO))   // ERRNO: not up yet - the loop retries
  {
    COR_E("iotaul: cannot connect to %s: %s", serverDesc, mosquitto_strerror(rc));
    return BRIDGE_ERR;
  }

  if (mosquitto_loop_start(mosq) != MOSQ_ERR_SUCCESS)
  {
    COR_E("iotaul: cannot start libmosquitto's thread");
    return BRIDGE_ERR;
  }

  return BRIDGE_OK;
}



// -----------------------------------------------------------------------------
//
// iotaulExecutorOpen -
//
int iotaulExecutorOpen(CorNode* serverP, CorNode* southP, const BridgeBroker* _brokerP)
{
  brokerP = _brokerP;

  CorNode* uriP  = (serverP != NULL) ? corTreeLookup(serverP, "uri")  : NULL;
  CorNode* portP = (southP  != NULL) ? corTreeLookup(southP,  "port") : NULL;

  if ((uriP != NULL) && (uriP->type == CorString) && (mqttOpen(uriP->value.s) != BRIDGE_OK))
    return BRIDGE_ERR;

  if (portP != NULL)
  {
    if ((portP->type != CorInt) || (portP->value.i <= 0) || (portP->value.i > 65535))
    {
      COR_E("iotaul: southbound.port - a TCP port, 1-65535");
      return BRIDGE_ERR;
    }

    if (iotaulSouthOpen((unsigned short) portP->value.i, brokerP) != BRIDGE_OK)
      return BRIDGE_ERR;
    southUp = true;
  }

  workerStop = false;
  if (pthread_create(&worker, NULL, httpWorker, NULL) != 0)
  {
    COR_E("iotaul: cannot start the HTTP worker");
    return BRIDGE_ERR;
  }
  workerUp = true;

  return BRIDGE_OK;
}



// -----------------------------------------------------------------------------
//
// iotaulExecutorClose -
//
void iotaulExecutorClose(void)
{
  if (workerUp == true)
  {
    pthread_mutex_lock(&jobMutex);
    workerStop = true;
    pthread_cond_signal(&jobCond);
    pthread_mutex_unlock(&jobMutex);
    pthread_join(worker, NULL);
    workerUp = false;
  }

  if (mosq != NULL)
  {
    mosquitto_disconnect(mosq);
    mosquitto_loop_stop(mosq, true);
    mosquitto_destroy(mosq);
    mosquitto_lib_cleanup();
    mosq = NULL;
  }

  pthread_mutex_lock(&pendingMutex);
  while (pendingHead != NULL)
  {
    Pending* p = pendingHead;
    pendingHead = p->next;
    pendingFree(p);
  }
  pthread_mutex_unlock(&pendingMutex);

  brokerP = NULL;
}



// -----------------------------------------------------------------------------
//
// iotaulServiceExecute - BridgeDriver.serviceExecute
//
int iotaulServiceExecute(const char* url, const char* executionId, const char* inputJson)
{
  if (strncmp(url, SCHEME_MQTT, strlen(SCHEME_MQTT)) == 0)
    return mqttExecute(url, executionId, inputJson);

  if (strncmp(url, SCHEME_POLL, strlen(SCHEME_POLL)) == 0)
    return pollExecute(url, executionId, inputJson);

  if ((strncmp(url, SCHEME_HTTP, strlen(SCHEME_HTTP)) == 0) || (strncmp(url, SCHEME_HTTPS, strlen(SCHEME_HTTPS)) == 0))
    return httpExecute(url, executionId, inputJson);

  return BRIDGE_NOT_FOUND;
}



// -----------------------------------------------------------------------------
//
// iotaulServiceCancel - BridgeDriver.serviceCancel: a pull command the device has not asked for yet is
// withdrawn; a command handed to a device cannot be taken back
//
int iotaulServiceCancel(const char* url, const char* executionId)
{
  (void) url;

  Pending* foundP    = NULL;
  bool     withdrawn = false;

  pthread_mutex_lock(&pendingMutex);
  for (Pending** pp = &pendingHead; *pp != NULL; pp = &(*pp)->next)
  {
    if (strcmp((*pp)->executionId, executionId) == 0)
    {
      foundP = *pp;

      if ((foundP->pull == true) && (foundP->delivered == false))
      {
        *pp       = foundP->next;
        withdrawn = true;
      }
      break;
    }
  }
  pthread_mutex_unlock(&pendingMutex);

  if (withdrawn == true)
  {
    pendingFree(foundP);
    return BRIDGE_OK;
  }

  return (foundP != NULL) ? BRIDGE_UNSUPPORTED : BRIDGE_NOT_FOUND;
}



// -----------------------------------------------------------------------------
//
// iotaulExecutorVersion -
//
const char* iotaulExecutorVersion(void)
{
  static char version[64];
  int         major, minor, revision;

  mosquitto_lib_version(&major, &minor, &revision);
  snprintf(version, sizeof(version), "libmosquitto %d.%d.%d", major, minor, revision);

  return version;
}
