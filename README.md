# corIotaUlBridge

**Prototype - not released.** The FIWARE IoT Agent for UltraLight 2.0 in the coraine NGSI-LD broker: its
**commands** as **Service Executions**, its **measures** as **DataChannel records** - `iotaul.so`, a bridge
plugin loaded with `--bridges iotaul`. The designs are coraine's `doc/iot-agent-executor.md` and
`doc/iot-agent-measures.md`; the payloads are iotagent-ul's.

## Commands

A device command is a Service Registration whose endpoint says how to reach the device; invoking the
service sends the device what iotagent-ul sends it, and the device's answer completes the execution.

| Endpoint | The device gets | Its answer |
|---|---|---|
| `iota-ul+mqtt://<apikey>/<deviceId>/<cmd>[?qos=0\|1\|2][&retain=true]` | `<deviceId>@<cmd>\|<value>` on `/<apikey>/<deviceId>/cmd` | `<deviceId>@<cmd>\|<result>` on `[/][ul/]<apikey>/<deviceId>/cmdexe` |
| `iota-ul+http[s]://<host>[:<port>]/<path>?cmd=<cmd>&device=<deviceId>` | `POST` of `<deviceId>@<cmd>\|<value>`, `text/plain`, to `http[s]://<host>/<path>` | the response, 200: `<deviceId>@<cmd>\|<result>` |
| `iota-ul+poll://<apikey>/<deviceId>/<cmd>` | nothing until it asks, on the southbound port: `GET /iot/d?k=<apikey>&i=<deviceId>&getCmd=1` answers `<deviceId>@<cmd>\|<value>\|<deviceId>@<cmd2>\|<value>` | a measure of its own: `POST /iot/d?k=<apikey>&i=<deviceId>` `<deviceId>@<cmd>\|<result>` |

- `<value>` is the execution input's `value` - the whole input without `id` and `type` when it has none:
  a string as it is, any other value as its JSON. An object is `<deviceId>@<cmd>|<key>=<value>|...`.
- The device id is in the payload: an HTTP endpoint names it (`device=`), as iotagent-ul has it from the
  device's provisioning.
- The execution ends `completed`, its `executionOutput` the device's `<result>` - the second field only,
  JSON when it is JSON, else a string (`ulParser.result`). An answer in a measure (the first group has
  `@`) is read as iotagent-ul reads it: one field without `=` the result as text, `<key>=<value>` fields an
  object of texts. `failed` when the device answers anything but 200 (HTTP) - the error names the result -
  or not at all within the registration's `executionTimeout`.
- A pull command the device has asked for is still `executing`, with `executionProgress`
  `{"delivered": true}`. All the device's commands are handed over in one answer, in the order stored.
- A pull command the device has not asked for yet can be cancelled; a command handed to a device cannot (409).

## Measures

A device's measure is handed to the broker as a record on a **DataChannel** - the device's own,
`<apikey>/<deviceId>`, else its group's, `<apikey>`. The DataChannel's mapping (coraine
`doc/bridge-channels.md`, DataChannels) makes the entities; the plugin maps nothing.

| From the device | |
|---|---|
| `POST /iot/d?k=<apikey>&i=<deviceId>[&t=<time>]` | the body, `text/plain` (also when it says no type): UltraLight 2.0 |
| `GET /iot/d?k=<apikey>&i=<deviceId>&d=<measure>[&t=<time>]` | `d`: UltraLight 2.0 |
| MQTT `[/][ul/]<apikey>/<deviceId>/attrs` | UltraLight 2.0 |
| MQTT `[/][ul/]<apikey>/<deviceId>/attrs/<name>` | one attribute: JSON when it is JSON, else the text |

- **UltraLight 2.0**: groups split by `#`; a group is `<key>|<value>|...`, after a timestamp when it has an
  even number of bars (`2026-10-20T21:00:00Z|t|21`). Each value is JSON when it is JSON (`21.5`, `true`),
  else a string.
- **The record**: the group's keys, `id` and `type` renamed `measure_id` and `measure_type` (as
  iotagent-ul does), plus `@deviceId` and `@apikey`.
- **Its time** (every attribute's `observedAt`): the group's timestamp when it is ISO 8601, else the
  arrival time; `t` is every group's time, over its own.
- **Groups** are one record each, in ascending time order.
- **Errors**, as iotagent-ul's: 404 `DEVICE_GROUP_NOT_FOUND` when the apikey has no DataChannel,
  `DEVICE_NOT_FOUND` when it has devices' DataChannels but not this device's; 400 `PARSE_ERROR` when a
  group is not a measure (an odd number of fields, an empty one) - the payload is refused whole; 400
  `UNSUPPORTED_TYPE` for a POST that is not `text/plain`; 400 `MANDATORY_PARAMS_NOT_FOUND`. Over MQTT a
  refused measure is logged and dropped.
- **Only DataChannels**: a broker of bridge ABI 15 says which channel type an endpoint is, and a Channel
  or an AssetChannel is refused (dormant, "carries DataChannels only"). An older broker cannot say: every
  channel is taken as a DataChannel.

```json
{ "iotaul": {
    "southbound": { "port": 7896 },
    "ngsild": {
      "otherColumns": "property",
      "dataChannels": {
        "th-key": { "entities": [ { "type": "Thermometer", "id": "urn:ngsi-ld:Thermometer:{@deviceId}",
                                    "attributes": { "temperature": { "column": "t", "as": "number", "unitCode": "CEL" } } } ] } } } } }
```

## Configuration

In the `--bridgeConfig` file: the MQTT server, and the southbound port - where a device is the client, as
iotagent-ul's 7896. Without the server no MQTT, without the port no HTTP:

```json
{ "iotaul": { "server": { "uri": "mqtt://mosquitto:1883" }, "southbound": { "port": 7896 } } }
```

The southbound port is served by the broker for the plugin (bridge ABI 16, `httpListen` - a broker
without it does not start when a port is configured), plain HTTP on every address, with no
NGSI-LD and no authentication: a device names itself with `k` and `i`, as it does to iotagent-ul. Its
errors are iotagent-ul's, `{"name": ..., "message": ...}`; its answers `text/plain`.

## Not built

The drop-in provisioning API (`/iot/groups`, `/iot/devices` - a DataChannel comes from the
configuration file), the configuration topic (`.../configuration/commands`), AMQP, lazy attributes, JEXL
expressions, a command's `payloadType`, explicitAttrs, MQTT TLS and credentials.

## Build and test

    make && make test          # libmosquitto-dev; the tests run mosquitto and python3-paho-mqtt

`test/funcTests/device.py` plays the devices, over a private mosquitto on port 11885.
