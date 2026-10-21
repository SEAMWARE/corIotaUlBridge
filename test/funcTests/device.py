#!/usr/bin/env python3
#
# FILE            device.py
#
# Copyright 2026 Seamware
# SPDX-License-Identifier: Apache-2.0
#
# device.py <mqttPort> <httpPort> <logFile> - a device as iotagent-ul talks to it:
#
#   MQTT  a command on /<apikey>/<deviceId>/cmd, <deviceId>@<cmd>|<value> - answered on
#         /<apikey>/<deviceId>/cmdexe with <deviceId>@<cmd>|<value> done; a device of apikey 'silent' never answers
#   HTTP  a command POSTed to the device's endpoint (text/plain) - answered 200 <deviceId>@<cmd>|<value> done;
#         a path under /fail answers 500 <deviceId>@<cmd>|broken
#
# Every command received is one line of the log: MQTT <topic> <payload> / HTTP <path> <body>.
#
import sys
import threading
from http.server import BaseHTTPRequestHandler, HTTPServer

import paho.mqtt.client as mqtt

mqttPort = int(sys.argv[1])
httpPort = int(sys.argv[2])
logFile  = sys.argv[3]
lock     = threading.Lock()


def log(line):
    with lock:
        with open(logFile, "a") as f:
            f.write(line + "\n")


def answer(body, result=None):
    head, _, value = body.partition("|")             # <deviceId>@<cmd>, <value>[|...]
    return "%s|%s" % (head, result if result is not None else value + " done")


def onConnect(client, userdata, flags, rc, properties=None):
    client.subscribe("/+/+/cmd", qos=1)
    log("READY")


def onMessage(client, userdata, msg):
    payload = msg.payload.decode()
    log("MQTT %s %s" % (msg.topic, payload))
    level = msg.topic.split("/")             # '', apikey, deviceId, 'cmd'
    if level[1] != "silent":
        client.publish("/%s/%s/cmdexe" % (level[1], level[2]), answer(payload), qos=1)


class Handler(BaseHTTPRequestHandler):
    def do_POST(self):
        body = self.rfile.read(int(self.headers.get("Content-Length", 0))).decode()
        log("HTTP %s %s %s" % (self.path, self.headers.get("Content-Type"), body))
        fail = self.path.startswith("/fail")
        out  = answer(body, "broken" if fail else None).encode()
        self.send_response(500 if fail else 200)
        self.send_header("Content-Type", "text/plain")
        self.send_header("Content-Length", str(len(out)))
        self.end_headers()
        self.wfile.write(out)

    def log_message(self, *args):
        pass


client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
client.on_connect = onConnect
client.on_message = onMessage
client.connect("localhost", mqttPort)
client.loop_start()

HTTPServer(("localhost", httpPort), Handler).serve_forever()
