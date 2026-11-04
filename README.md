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
`exporterhelper`. One deployment serves one customer (Decision 0002), so rows carry no tenant
or workspace column: serving rows are keyed by `dataset`, `timestamp`, and `event_id` and carry a
`routing_epoch` stamp, configurable with `routing_epoch` (default `1`). The ClickHouse database is
configuration, `database` (default `toudai`), and must be the one the DDL was applied to.

The exporter relies on a gateway pipeline that sets the `toudai.dataset` resource attribute and a
`toudai.event_id` on every record before it (see Gateway). It writes the assigned `event_id` as
is; a missing dataset or a missing or non-UUIDv7 ID fails the whole export as a permanent error.
Any other `toudai.*` attribute is dropped, never stored. Nothing is deduplicated (Decision 0001
D5).

`testdata/schema/` vendors the ClickHouse DDL verbatim from `toudai-dev/toudai` at
`df95068ebccf9e93fcc221088f3566b068ef729c` (PR #27); a test fails when the exporter's INSERT
columns drift from it. The DDL names its database as the `{database:Identifier}` query parameter,
so apply it with `clickhouse-client --param_database=<name>` (or the source repository's
`clickhouse/apply-schema.sh`), using the same name as the exporter's `database`.

## Gateway

`config/gateway.example.yaml` is the product gateway pipeline: OTLP in, `toudaiauth` and
`toudaigateway` ahead of any batching, the `toudai` exporter out.

- **`toudaiauth` extension** validates an ingestion key from the `X-Toudai-Key` header against a
  static key file (`config/keys.example.yaml`). A key maps to permissions, never to a customer or
  workspace (Decision 0002 D3): write permission and an optional list of datasets. A key without
  a `datasets` field may write any dataset; an explicit empty `datasets: []` is refused at start.
  A missing, unknown, repeated, or read-only key, or a blank or repeated `X-Toudai-Dataset`
  header, is a 401 before any pipeline runs. The key is held as a digest, removed from receiver
  metadata, and never appears in auth data, errors, or exported data. Rotation and revocation are
  file edits plus a restart.
- **`toudaigateway` processor** resolves the request's dataset: the `X-Toudai-Dataset` header, else
  the key's only dataset, else `default_dataset` (empty by default, `TOUDAI_DEFAULT_DATASET` in the
  example). A dataset outside the key's list is a 403; no resolvable dataset or an invalid name is
  a 400. It sets `toudai.dataset` on every resource. A producer `toudai.dataset` on the resource is
  accepted only when it equals the resolved dataset; any other `toudai.*` resource attribute
  (including the retired `toudai.workspace_id` and `toudai.scope_source`) and any producer
  `toudai.event_id` on a record is a 400. It assigns every log record and span a random UUIDv7
  `event_id` before batching, so a producer retry or an identical event always gets a new one.

Dataset resolution is write-side only; reads name their dataset explicitly in the query service.
`main_test.go` runs the example through the real receivers over HTTP and gRPC.

## License

GPL-3.0. See [LICENSE](LICENSE).
