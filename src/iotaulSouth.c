//
// FILE            iotaulSouth.c
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
// The southbound port - see header. The broker serves it (BridgeBroker.httpListen, ABI 16); here only the
// routes: what iotagent-ul's HTTP binding answers (lib/bindings/HTTPBinding.js), its errors
// {"name", "message"} included.
//
#include <stdbool.h>                                  // bool
#include <stdio.h>                                    // snprintf
#include <stdlib.h>                                   // malloc, realloc, free
#include <string.h>                                   // strlen, strcmp, strncmp, memcpy
#include <strings.h>                                  // strncasecmp

#include "corLog/corLog.h"                            // COR_E, COR_W, COR_V

#include "corBridge/BridgeBroker.h"                   // BridgeBroker, BridgeHttpRequest, BRIDGE_*

#include "iotaulExecutor.h"                         // iotaulPullDeliver
#include "iotaulMeasures.h"                         // iotaulMeasure
#include "iotaulSouth.h"                            // Own interface



// -----------------------------------------------------------------------------
//
// reply - a status and a JSON body, for the broker to send
//
static int reply(char** bodyP, int status, const char* json)
{
  *bodyP = strdup(json);
  return (*bodyP != NULL) ? status : 500;
}



// -----------------------------------------------------------------------------
//
// error - iotagent-ul's error body: {"name": "<NAME>", "message": "<message>"}
//
static int error(char** bodyP, int status, const char* name, const char* message)
{
  char body[2048];

  snprintf(body, sizeof(body), "{\"name\":\"%s\",\"message\":\"%s\"}", name, message);
  return reply(bodyP, status, body);
}



// -----------------------------------------------------------------------------
//
// jsonEscape - a message as a JSON string's content: it names what a device sent
//
static const char* jsonEscape(const char* in, char* out, int size)
{
  int at = 0;

  for (; (*in != 0) && (at < size - 7); in++)
  {
    unsigned char c = (unsigned char) *in;

    if ((c == '"') || (c == '\\'))
    {
      out[at++] = '\\';
      out[at++] = c;
    }
    else if (c < 0x20)
      at += snprintf(&out[at], size - at, "\\u%04x", c);
    else
      out[at++] = c;
  }

  out[at] = 0;
  return out;
}



// -----------------------------------------------------------------------------
//
// serve - a request on the southbound port (BridgeHttpHandler): GET and POST /iot/d, as iotagent-ul's
//
static int serve(const BridgeHttpRequest* reqP, char** bodyP, const char** contentTypeP)
{
  const char* method = reqP->method;
  bool        get    = (strcmp(method, "GET")  == 0);
  bool        post   = (strcmp(method, "POST") == 0);

  *contentTypeP = "application/json";

  if ((strcmp(reqP->path, "/iot/d") != 0) || ((get == false) && (post == false)))
    return error(bodyP, 404, "NOT_FOUND", "no such resource");

  if (reqP->bodyTooBig != 0)
    return error(bodyP, 413, "PAYLOAD_TOO_LARGE", "request entity too large");

  const char* apikey   = reqP->param(reqP, "k");
  const char* deviceId = reqP->param(reqP, "i");
  const char* getCmd   = reqP->param(reqP, "getCmd");
  const char* d        = reqP->param(reqP, "d");
  bool        pull     = ((getCmd != NULL) && (strcmp(getCmd, "1") == 0));

  //
  // The mandatory parameters, as iotagent-ul lists the missing ones - a GET carries its measure in d=,
  // unless it only asks for its commands
  //
  char missing[128] = "";
  int  at           = 0;

  if ((apikey == NULL) || (apikey[0] == 0))
    at += snprintf(&missing[at], sizeof(missing) - at, "%s\\\"API Key\\\"", (at > 0) ? "," : "");
  if ((deviceId == NULL) || (deviceId[0] == 0))
    at += snprintf(&missing[at], sizeof(missing) - at, "%s\\\"Device Id\\\"", (at > 0) ? "," : "");
  if ((get == true) && (d == NULL) && (pull == false))
    at += snprintf(&missing[at], sizeof(missing) - at, "%s\\\"Payload\\\"", (at > 0) ? "," : "");

  if (at > 0)
  {
    char message[256];

    snprintf(message, sizeof(message), "Some of the mandatory params weren't found in the request: [%s]", missing);
    return error(bodyP, 400, "MANDATORY_PARAMS_NOT_FOUND", message);
  }

  //
  // A POST is text/plain - so taken when it says no type
  //
  const char* type = reqP->header(reqP, "Content-Type");

  if ((post == true) && (type != NULL) && (strncasecmp(type, "text/plain", 10) != 0))
    return error(bodyP, 400, "UNSUPPORTED_TYPE", "The request content didn't have the expected type [text/plain ]");

  //
  // The measure: a POST's body, else d= - and a first group <deviceId>@<cmd>|... is a command's answer
  //
  const char* body = ((post == true) && (reqP->bodyLen > 0)) ? reqP->body : d;
  int         len  = ((post == true) && (reqP->bodyLen > 0)) ? reqP->bodyLen : ((d != NULL) ? (int) strlen(d) : 0);

  if ((body != NULL) && (len > 0))
  {
    const char* name = NULL;
    char        message[512];
    char        escaped[1024];
    int         status = iotaulMeasure(apikey, deviceId, body, len, NULL, reqP->param(reqP, "t"), &name, message, sizeof(message));

    if (status != 200)
    {
      COR_W("iotaul: a measure from %s/%s refused - %s: %s", apikey, deviceId, name, message);
      return error(bodyP, status, name, jsonEscape(message, escaped, sizeof(escaped)));
    }
  }

  *contentTypeP = "text/plain";

  if (pull == false)
    return reply(bodyP, 200, "");

  char* commands = iotaulPullDeliver(apikey, deviceId);

  if (commands == NULL)
  {
    *contentTypeP = "application/json";
    return error(bodyP, 500, "INTERNAL_ERROR", "out of memory");
  }

  COR_V("iotaul: %s/%s asked for its commands: %s", apikey, deviceId, commands);

  *bodyP = commands;                                  // malloc'd - the broker frees it
  return 200;
}



// -----------------------------------------------------------------------------
//
// iotaulSouthOpen -
//
int iotaulSouthOpen(unsigned short port, const BridgeBroker* brokerP)
{
  if ((brokerP->abiVersion < 16) || (brokerP->httpListen == NULL))
  {
    COR_E("iotaul: the southbound port %u needs a broker that serves it (bridge ABI 16, httpListen)", port);
    return BRIDGE_ERR;
  }

  if (brokerP->httpListen("iotaul", port, serve) != BRIDGE_OK)
  {
    COR_E("iotaul: cannot listen on the southbound port %u", port);
    return BRIDGE_ERR;
  }

  COR_V("iotaul: the southbound port %u", port);
  return BRIDGE_OK;
}
