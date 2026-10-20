#ifndef IOTAUL_SOUTH_H_
#define IOTAUL_SOUTH_H_

//
// FILE            iotaulSouth.h
//
// Copyright 2026 Seamware
// SPDX-License-Identifier: Apache-2.0
//
// The southbound port - where a device is the client, as iotagent-ul's (7896): served by the broker for
// the plugin (BridgeBroker.httpListen, bridge ABI 16), plain HTTP, no NGSI-LD and no authentication (a
// device names itself with k=<apikey>&i=<deviceId>, as it does to iotagent-ul).
//
//   POST /iot/d?k=<apikey>&i=<deviceId>[&t=<time>][&getCmd=1]   a measure, text/plain (iotaulMeasures.h) - or a
//                                                               command's answer, <deviceId>@<cmd>|<result>
//   GET  /iot/d?k=<apikey>&i=<deviceId>&d=<measure>[&t=][&getCmd=1]   the same, in d=
//
// getCmd=1: the answer is the device's pull commands, <deviceId>@<cmd>|<value>|..., text/plain; else empty.
//
#include "corBridge/BridgeBroker.h"                   // BridgeBroker



// -----------------------------------------------------------------------------
//
// iotaulSouthOpen - ask the broker to serve 'port'; BRIDGE_OK or BRIDGE_ERR. No close: the broker stops
// serving it before the plugin's close()
//
extern int iotaulSouthOpen(unsigned short port, const BridgeBroker* brokerP);

#endif  // IOTAUL_SOUTH_H_
