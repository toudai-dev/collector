-- Vendored from github.com/toudai-dev/toudai at df95068ebccf9e93fcc221088f3566b068ef729c
-- (PR #27), clickhouse/schema/001-database.sql. Identical to the source. Apply it with
-- --param_database=<name> (the exporter's `database`, default `toudai`), and keep
-- the exporter's INSERT columns in sync with it.
-- Decision 0002 D1: the database is deployment configuration, not part of the
-- schema. Every file names it as the `database` query parameter; apply them with
-- `clickhouse/apply-schema.sh` (default `toudai`) or pass
-- `--param_database=<name>` to clickhouse-client yourself.
CREATE DATABASE IF NOT EXISTS {database:Identifier};
