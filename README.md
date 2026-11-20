# IoT Data Collector

Config-driven Linux collector written in Go. The initial runnable version supports Modbus RTU, Modbus TCP, HTTP JSON polling, and raw TCP request/response where the response is a JSON object. Successful samples are pushed to a WebSocket endpoint and written to InfluxDB v2 using the v2 API.

> MQTT, CoAP, and OPC UA are not implemented yet. Raw TCP is device-specific: this initial implementation expects JSON responses and does not provide a generic binary-frame decoder. Please do not enable unsupported protocols; the service logs an explicit error for them.

## Requirements

- Go 1.22+
- InfluxDB 2.x (tested against the v2 API; configure an organization, bucket, and token)

## Configure and run

```sh
cp config.example.yaml config.yaml
# Edit device addresses/registers and provide a token:
export INFLUXDB_TOKEN='your-token'
go mod tidy
go run ./cmd/collector ./config.yaml
```

The WebSocket endpoint defaults to `ws://localhost:8080/ws`. It emits JSON events of the form:

```json
{"device_id":"meter-rtu-01","protocol":"modbus-rtu","timestamp":"2026-01-01T00:00:00Z","values":{"temperature":21.5}}
```

Influx measurement is `telemetry`; `device_id` and `protocol` are tags and each configured point name is a field. Modbus addresses are zero-based. For Modbus points, configure a function (`holding_register`, `input_register`, `coil`, `discrete_input`), data type, quantity and byte order according to the device manual. `scale` defaults to 1 when set to zero; applied value is `raw * scale + offset`.

## Build a single Linux binary

```sh
mkdir -p dist
CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -trimpath -ldflags='-s -w' -o dist/iot-collector ./cmd/collector
# For ARM64, use GOARCH=arm64 instead.
```

Install the binary at `/usr/local/bin/iot-collector`, place configuration at `/etc/iot-collector/config.yaml`, and set the Influx token using an environment variable or an environment file managed by systemd. For RTU serial access, grant the service account permission to access the configured serial device.

## systemd

A sample unit is in `deploy/iot-collector.service`. Logs go to journald; inspect them with `journalctl -u iot-collector -f`.
