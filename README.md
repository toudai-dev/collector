# Toudai Collector

The OpenTelemetry Collector distribution for [Toudai](https://github.com/toudai-dev/toudai),
a ClickHouse-backed telemetry service. It bundles stock Collector components with an
owned exporter that writes OTLP logs and traces into ClickHouse.

This project is under active development.

```sh
make build   # bin/toudai-collector
make vet
make test
```

## Pins

`go.mod` pins the `otelcol-contrib` components at v0.161.0, the Collector core modules at
v1.67.0, and `clickhouse-go/v2` at v2.47.0 (Go 1.26).

The distribution still includes the stock `clickhouseexporter` and `transformprocessor`,
which were used for stock-path probes; they can be dropped once nothing needs them.

## Exporter

The `toudai` exporter (package `toudaiexporter`) validates a whole export request, writes
the canonical payload row, then the serving row. Queue, retry, and timeout come from
`exporterhelper`. Serving rows use the `tenant_id` and `timestamp` columns and carry a
`routing_epoch` stamp, configurable with `routing_epoch` (default `1`).

The exporter relies on a gateway pipeline that injects the trusted `toudai.workspace_id`,
`toudai.dataset`, and `toudai.scope_source` resource attributes before it.

`testdata/schema/` vendors the ClickHouse DDL from `toudai-dev/toudai`; a test fails when the
exporter's INSERT columns drift from it. Each file's header names the source commit.

## License

GPL-3.0. See [LICENSE](LICENSE).
