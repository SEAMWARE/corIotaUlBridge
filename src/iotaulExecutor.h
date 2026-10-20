#ifndef IOTAUL_EXECUTOR_H_
#define IOTAUL_EXECUTOR_H_

//
// FILE            iotaulExecutor.h
//
// Copyright 2026 Seamware
// SPDX-License-Identifier: Apache-2.0
//
// The IoT Agent UltraLight's commands, as Service Executions - coraine doc/iot-agent-executor.md.
//
// A Service Registration whose endpoint is one of
//
//   iota-ul+mqtt://<apikey>/<deviceId>/<cmd>[?qos=0|1|2][&retain=true]
//   iota-ul+http://<host>[:<port>]/<path>?cmd=<cmd>&device=<deviceId>       (iota-ul+https too)
//   iota-ul+poll://<apikey>/<deviceId>/<cmd>
//
// is executed here: the device gets <deviceId>@<cmd>|<value> - what iotagent-ul sends it; an object value as
// <deviceId>@<cmd>|<key>=<value>|... - on MQTT /<apikey>/<deviceId>/cmd, as the text/plain body of a POST to
// its endpoint, or in the answer to its own GET /iot/d?k=<apikey>&i=<deviceId>&getCmd=1 (poll). Its answer
// completes the execution: <deviceId>@<cmd>|<result> on [/][ul/]<apikey>/<deviceId>/cmdexe, the POST's 200
// response, or a measure group of its own on the southbound port (POST /iot/d).
//
#include "corTree/CorNode.h"                          // CorNode
#include "corBridge/BridgeBroker.h"                   // BridgeBroker



// -----------------------------------------------------------------------------
//
// iotaulExecutorOpen - the MQTT connection ("server": { "uri": "mqtt://host[:port]" }, optional), the
// southbound port ("southbound": { "port": 7896 }, optional) and the HTTP worker; BRIDGE_OK or BRIDGE_ERR
//
extern int iotaulExecutorOpen(CorNode* serverP, CorNode* southP, const BridgeBroker* brokerP);



// -----------------------------------------------------------------------------
//
// iotaulExecutorClose - no thread of the plugin calls the broker after it
//
extern void iotaulExecutorClose(void);



// -----------------------------------------------------------------------------
//
// iotaulServiceExecute / iotaulServiceCancel - BridgeDriver.serviceExecute / serviceCancel
//
extern int iotaulServiceExecute(const char* url, const char* executionId, const char* inputJson);
extern int iotaulServiceCancel(const char* url, const char* executionId);



// -----------------------------------------------------------------------------
//
// iotaulPullDeliver - a device's getCmd=1: its pull commands not yet delivered, as iotagent-ul joins them -
// <deviceId>@<cmd>|<value>|<deviceId>@<cmd2>|<value> - malloc'd ("": none; NULL: out of memory); delivered
// from here on
//
extern char* iotaulPullDeliver(const char* apikey, const char* deviceId);



// -----------------------------------------------------------------------------
//
// iotaulCommandResult - a command's answer in a measure (the payload's first group has '@'):
// <deviceId>@<cmd>|<result> completes the oldest execution of that command on that device - its output the
// result as text; <deviceId>@<cmd>|<key>=<value>|... an object of the pairs (texts)
//
extern void iotaulCommandResult(const char* apikey, const char* deviceId, const char* group, int len);



// -----------------------------------------------------------------------------
//
// iotaulExecutorVersion - "libmosquitto x.y.z"
//
extern const char* iotaulExecutorVersion(void);

#endif  // IOTAUL_EXECUTOR_H_
