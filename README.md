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
`exporterhelper`. Rows carry no tenant column: one deployment database holds one customer's
data, and `dataset` is the only scope a row has (Decision 0002). Serving rows use the
`timestamp` column and carry a `routing_epoch` stamp, configurable with `routing_epoch`
(default `1`).

The database is deployment configuration: `database` (default `toudai`; the example gateway
reads `TOUDAI_CLICKHOUSE_DATABASE`) must name the database the DDL was applied to.

The exporter relies on a gateway pipeline that sets the `toudai.dataset` resource attribute and
a `toudai.event_id` on every record before it (see Gateway). A resource without a dataset fails
the whole export as a permanent error. It writes the assigned `event_id` as is; a missing or
non-UUIDv7 ID also fails the whole export. Nothing is deduplicated (Decision 0001 D5).

`testdata/schema/` vendors the ClickHouse DDL from `toudai-dev/toudai`; a test fails when the
exporter's INSERT columns drift from it. Each file's header names the source commit. The DDL
names the database as the `{database:Identifier}` query parameter, so apply it with the same
name the exporter uses, for example
`clickhouse-client --param_database="${TOUDAI_CLICKHOUSE_DATABASE:-toudai}" --multiquery < file.sql`
for each file in order (upstream `clickhouse/apply-schema.sh` does exactly this).

## Gateway

`config/gateway.example.yaml` is the product gateway pipeline: OTLP in, `toudaiauth` and
`toudaigateway` ahead of any batching, the `toudai` exporter out. There is no workspace or tenant:
one deployment serves one customer, and accounts, workspaces, and the mapping of keys to customers
belong to a private control plane (Decision 0002).

- **`toudaiauth` extension** validates an ingestion key from the `X-Toudai-Key` header against a
  static key file (`config/keys.example.yaml`) that maps each key to its write permission and an
  optional list of allowed datasets; a key without a list may write to any dataset. A missing,
  unknown, or read-only key is a 401 before any pipeline runs. The key is held as a digest,
  removed from receiver metadata, and never appears in auth data, errors, or exported data.
  Rotation and revocation are file edits plus a restart.
- **`toudaigateway` processor** resolves and sets `toudai.dataset`: the `X-Toudai-Dataset`
  header, else the key's only dataset, else the configured `default_dataset` (none by default).
  A dataset outside the key's list is a 403, whichever source it came from; an unresolved or
  invalid dataset name is a 400. A producer `toudai.dataset` on the resource is accepted only when
  it equals the resolved dataset (403 when outside the key's list, else 400); any other
  `toudai.*` resource attribute is a 400. It assigns every log record and span a random UUIDv7
  `event_id`, so a producer retry or an identical event always gets a new one; a record or span
  that already carries `toudai.event_id` is a 400 for the whole request (Decision 0002 D3).

Dataset resolution is write-side only. The query service takes the dataset explicitly from each
query and checks it against the read key's optional list.

`main_test.go` runs the example through the real receivers over HTTP and gRPC.

## License

GPL-3.0. See [LICENSE](LICENSE).
