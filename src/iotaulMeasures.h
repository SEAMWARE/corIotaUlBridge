#ifndef IOTAUL_MEASURES_H_
#define IOTAUL_MEASURES_H_

//
// FILE            iotaulMeasures.h
//
// Copyright 2026 Seamware
// SPDX-License-Identifier: Apache-2.0
//
// The IoT Agent UltraLight's measures, as DataChannel records (coraine doc/iot-agent-measures.md): a device's
// measure becomes a record - its keys, plus @deviceId and @apikey - handed to the broker on the device's
// DataChannel (<apikey>/<deviceId>), else its group's (<apikey>). The broker's mapping makes the entity.
//
#include <stdbool.h>                                  // bool
#include <stdint.h>                                   // int64_t

#include "corBridge/BridgeBroker.h"                   // BridgeBroker



// -----------------------------------------------------------------------------
//
// iotaulMeasuresOpen / Close -
//
extern void iotaulMeasuresOpen(const BridgeBroker* brokerP);
extern void iotaulMeasuresClose(void);



// -----------------------------------------------------------------------------
//
// iotaulMeasureEndpointAdd / Del - a DataChannel's endpoint: <apikey> (a group) or <apikey>/<deviceId>
//
extern int iotaulMeasureEndpointAdd(const char* endpoint);
extern int iotaulMeasureEndpointDel(const char* endpoint);



// -----------------------------------------------------------------------------
//
// iotaulMeasure - one measure from a device, on any transport
//
// @param body       the measure: UltraLight 2.0 - <key>|<value>|..., after a timestamp (an even number of
//                   bars), groups split by '#' (a multi-measure); a first group <deviceId>@<cmd>|... is a
//                   command's answer (iotaulCommandResult). With attrName: the value of the one attribute
// @param attrName   NULL, or the attribute of a single measure (MQTT .../attrs/<name>)
// @param timeParam  the HTTP 't' parameter (ISO 8601), NULL when none - every group's time
// @param nameP      OUT, on anything but 200: iotagent-ul's error name
// @param messageP   OUT, on anything but 200: the message
//
// @return 200, 400 (PARSE_ERROR: a group is not a measure), 404 (no DataChannel - no group: DEVICE_GROUP_NOT_FOUND, a
//         group's devices but not this one: DEVICE_NOT_FOUND)
//
extern int iotaulMeasure
(
  const char*   apikey,
  const char*   deviceId,
  const char*   body,
  int           len,
  const char*   attrName,
  const char*   timeParam,
  const char**  nameP,
  char*         message,
  int           messageSize
);

#endif  // IOTAUL_MEASURES_H_
