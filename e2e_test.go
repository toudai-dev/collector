package main

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"

	"go.opentelemetry.io/collector/otelcol"
)

// The end-to-end test runs the example gateway with the real toudai exporter
// against a ClickHouse server in Docker. It is skipped without a Docker daemon
// unless TOUDAI_REQUIRE_CLICKHOUSE is set, which turns that into a
// failure. TOUDAI_E2E_CLICKHOUSE_IMAGE overrides the image.
const (
	e2eImage    = "clickhouse/clickhouse-server:26.8.2.7"
	e2eUser     = "toudai"
	e2ePassword = "toudai-test"
	// A database other than the default, so a component that ignores the
	// configured name writes to, or reads from, the wrong place and fails.
	e2eDatabase = "toudai_e2e_custom"
)

type clickHouseContainer struct {
	name       string
	nativeAddr string
}

// query runs one statement with clickhouse-client inside the container and
// returns its TSV output.
func (c *clickHouseContainer) query(t *testing.T, sql string, args ...string) string {
	t.Helper()
	cmd := exec.Command("docker", append([]string{"exec", c.name, "clickhouse-client",
		"--user", e2eUser, "--password", e2ePassword, "--query", sql}, args...)...)
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("query %q: %v: %s", sql, err, out)
	}
	return strings.TrimRight(string(out), "\n")
}

// applySchema applies the vendored DDL the way the source repository's
// apply-schema.sh does: every file in order, with the database as the
// `database` query parameter (Decision 0002 D1).
func (c *clickHouseContainer) applySchema(t *testing.T, database string) {
	t.Helper()
	files, err := filepath.Glob(filepath.Join("testdata", "schema", "*.sql"))
	if err != nil || len(files) != 4 {
		t.Fatalf("vendored DDL files = %v (%v), want 4", files, err)
	}
	sort.Strings(files)
	for _, f := range files {
		ddl, err := os.ReadFile(f)
		if err != nil {
			t.Fatal(err)
		}
		cmd := exec.Command("docker", "exec", "-i", c.name, "clickhouse-client",
			"--user", e2eUser, "--password", e2ePassword,
			"--param_database="+database, "--multiquery")
		cmd.Stdin = bytes.NewReader(ddl)
		if out, err := cmd.CombinedOutput(); err != nil {
			t.Fatalf("apply %s: %v: %s", f, err, out)
		}
	}
}

func startClickHouse(t *testing.T) *clickHouseContainer {
	t.Helper()
	if exec.Command("docker", "info").Run() != nil {
		if os.Getenv("TOUDAI_REQUIRE_CLICKHOUSE") != "" {
			t.Fatal("TOUDAI_REQUIRE_CLICKHOUSE is set but no Docker daemon answers")
		}
		t.Skip("no Docker daemon: skipping the ClickHouse end-to-end test")
	}
	image := os.Getenv("TOUDAI_E2E_CLICKHOUSE_IMAGE")
	if image == "" {
		image = e2eImage
	}
	template, err := os.ReadFile(filepath.Join("testdata", "e2e", "storage-test.xml"))
	if err != nil {
		t.Fatal(err)
	}
	storage := filepath.Join(t.TempDir(), "storage-test.xml")
	if err := os.WriteFile(storage, bytes.ReplaceAll(template, []byte("@DATA_DIR@"), []byte("/var/lib/clickhouse")), 0o644); err != nil {
		t.Fatal(err)
	}
	run := exec.Command("docker", "run", "--detach", "--publish", "127.0.0.1::9000",
		"--volume", storage+":/etc/clickhouse-server/config.d/toudai-storage-test.xml:ro",
		"--env", "CLICKHOUSE_USER="+e2eUser, "--env", "CLICKHOUSE_PASSWORD="+e2ePassword,
		"--ulimit", "nofile=262144:262144", image)
	// Only stdout is the container ID: on a machine without the image cached,
	// docker writes its pull progress to stderr.
	var stderr bytes.Buffer
	run.Stderr = &stderr
	out, err := run.Output()
	if err != nil {
		t.Fatalf("docker run %s: %v: %s", image, err, stderr.String())
	}
	c := &clickHouseContainer{name: strings.TrimSpace(string(out))}
	t.Cleanup(func() { _ = exec.Command("docker", "rm", "--force", "--volumes", c.name).Run() })
	port, err := exec.Command("docker", "port", c.name, "9000/tcp").CombinedOutput()
	if err != nil {
		t.Fatalf("docker port %s: %v: %s", c.name, err, port)
	}
	c.nativeAddr = strings.TrimSpace(strings.SplitN(string(port), "\n", 2)[0])
	for deadline := time.Now().Add(60 * time.Second); ; time.Sleep(250 * time.Millisecond) {
		cmd := exec.Command("docker", "exec", c.name, "clickhouse-client",
			"--user", e2eUser, "--password", e2ePassword, "--query", "SELECT 1")
		if out, err := cmd.Output(); err == nil && strings.TrimSpace(string(out)) == "1" {
			return c
		}
		if time.Now().After(deadline) {
			t.Fatal("ClickHouse did not become ready")
		}
	}
}

// runExporterGateway runs the example gateway config unchanged except for
// telemetry: logs go to a file at debug level so the test can scan them for
// credentials, and the exporter logs one summary line per batch.
func runExporterGateway(t *testing.T, ch *clickHouseContainer, defaultDataset string) (baseURL, logFile string) {
	t.Helper()
	dir := t.TempDir()
	logFile = filepath.Join(dir, "collector.log")
	override := filepath.Join(dir, "override.yaml")
	if err := os.WriteFile(override, []byte(`
exporters:
  toudai:
    log_batch_summaries: true
    sending_queue:
      batch:
        flush_timeout: 100ms
service:
  telemetry:
    metrics:
      level: none
    logs:
      level: debug
      output_paths: [`+logFile+`]
      error_output_paths: [`+logFile+`]
`), 0o600); err != nil {
		t.Fatal(err)
	}
	httpAddr, grpcAddr := freeAddr(t), freeAddr(t)
	t.Setenv("TOUDAI_OTLP_HTTP_ENDPOINT", httpAddr)
	t.Setenv("TOUDAI_OTLP_GRPC_ENDPOINT", grpcAddr)
	t.Setenv("TOUDAI_KEYS_FILE", exampleKeys)
	t.Setenv("TOUDAI_DEFAULT_DATASET", defaultDataset)
	t.Setenv("TOUDAI_CLICKHOUSE_ENDPOINT", ch.nativeAddr)
	t.Setenv("TOUDAI_CLICKHOUSE_DATABASE", e2eDatabase)
	t.Setenv("TOUDAI_CLICKHOUSE_USERNAME", e2eUser)
	t.Setenv("TOUDAI_CLICKHOUSE_PASSWORD", e2ePassword)

	set := collectorSettings()
	set.DisableGracefulShutdown = true
	set.ConfigProviderSettings.ResolverSettings.URIs = []string{"file:" + exampleConfig, "file:" + override}
	col, err := otelcol.NewCollector(set)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- col.Run(ctx) }()
	t.Cleanup(func() {
		cancel()
		select {
		case <-done:
		case <-time.After(10 * time.Second):
			t.Error("collector did not shut down")
		}
	})
	for deadline := time.Now().Add(20 * time.Second); col.GetState() != otelcol.StateRunning; time.Sleep(20 * time.Millisecond) {
		select {
		case err := <-done:
			t.Fatalf("collector exited during startup: %v", err)
		default:
		}
		if time.Now().After(deadline) {
			t.Fatal("collector did not start")
		}
	}
	return "http://" + httpAddr, logFile
}

// Through the example gateway into ClickHouse: the DDL applied to a custom
// database, dataset precedence and key restrictions as stored rows, rejected
// requests writing nothing, and no tenant, workspace, or credential stored.
func TestEndToEndClickHouse(t *testing.T) {
	ch := startClickHouse(t)
	ch.applySchema(t, e2eDatabase)
	base, logFile := runExporterGateway(t, ch, "fallback")

	logs := func(body, resource, record string) string {
		return `{"resourceLogs":[{"resource":{"attributes":[{"key":"service.name","value":{"stringValue":"svc"}}` + resource + `]},
 "scopeLogs":[{"logRecords":[{"timeUnixNano":"1788246000123456789","body":{"stringValue":"` + body + `"},"attributes":[` + record + `]}]}]}]}`
	}
	tests := []struct {
		body       string
		headers    map[string]string
		resource   string
		record     string
		wantStatus int
		wantDS     string // stored dataset; "" when nothing may be stored
	}{
		{"key-only-dataset", map[string]string{"X-Toudai-Key": keyProd}, "", "", 200, "prod"},
		{"header", map[string]string{"X-Toudai-Key": keyTwo, "X-Toudai-Dataset": "staging"}, "", "", 200, "staging"},
		{"default-unrestricted", map[string]string{"X-Toudai-Key": keyAny}, "", "", 200, "fallback"},
		{"header-unrestricted", map[string]string{"X-Toudai-Key": keyAny, "X-Toudai-Dataset": "custom"}, "", "", 200, "custom"},
		{"matching-resource", map[string]string{"X-Toudai-Key": keyProd}, `,{"key":"toudai.dataset","value":{"stringValue":"prod"}}`, "", 200, "prod"},
		{"REJECT-outside-list", map[string]string{"X-Toudai-Key": keyProd, "X-Toudai-Dataset": "staging"}, "", "", 403, ""},
		{"REJECT-default-outside-list", map[string]string{"X-Toudai-Key": keyTwo}, "", "", 403, ""},
		{"REJECT-mismatch", map[string]string{"X-Toudai-Key": keyTwo, "X-Toudai-Dataset": "prod"}, `,{"key":"toudai.dataset","value":{"stringValue":"staging"}}`, "", 400, ""},
		{"REJECT-workspace", map[string]string{"X-Toudai-Key": keyProd}, `,{"key":"toudai.workspace_id","value":{"stringValue":"ws-victim"}}`, "", 400, ""},
		{"REJECT-event-id", map[string]string{"X-Toudai-Key": keyProd}, "", `{"key":"toudai.event_id","value":{"stringValue":"00000000-0000-7000-8000-000000000000"}}`, 400, ""},
		{"REJECT-readonly", map[string]string{"X-Toudai-Key": keyReadonly}, "", "", 401, ""},
	}
	want := map[string]string{}
	for _, tc := range tests {
		status, resp := post(t, base+"/v1/logs", logs(tc.body, tc.resource, tc.record), tc.headers)
		if status != tc.wantStatus {
			t.Fatalf("%s: status = %d (%s), want %d", tc.body, status, resp, tc.wantStatus)
		}
		if strings.Contains(resp, "tk_example") {
			t.Fatalf("%s: response echoes a key: %s", tc.body, resp)
		}
		if tc.wantDS != "" {
			want[tc.body] = tc.wantDS
		}
	}
	span := `{"resourceSpans":[{"scopeSpans":[{"spans":[{"traceId":"0102030405060708090a0b0c0d0e0f10","spanId":"0102030405060708","name":"span-header","startTimeUnixNano":"1788246000123456789","endTimeUnixNano":"1788246000223456789"}]}]}]}`
	if status, resp := post(t, base+"/v1/traces", span, map[string]string{"X-Toudai-Key": keyTwo, "X-Toudai-Dataset": "prod"}); status != 200 {
		t.Fatalf("span: status = %d (%s)", status, resp)
	}

	db := e2eDatabase
	var got string
	for deadline := time.Now().Add(30 * time.Second); ; time.Sleep(200 * time.Millisecond) {
		got = ch.query(t, "SELECT count() FROM "+db+".serving_logs")
		if got == fmt.Sprint(len(want)) || time.Now().After(deadline) {
			break
		}
	}
	if got != fmt.Sprint(len(want)) {
		t.Fatalf("serving_logs rows = %s, want %d", got, len(want))
	}
	// Each accepted body landed once, in its resolved dataset, with a UUIDv7.
	rows := ch.query(t, "SELECT body, dataset, toUUIDOrZero(event_id) != toUUID('00000000-0000-0000-0000-000000000000') AND substring(event_id, 15, 1) = '7' FROM "+db+".serving_logs ORDER BY body")
	var wantRows []string
	for body, ds := range want {
		wantRows = append(wantRows, body+"\t"+ds+"\t1")
	}
	sort.Strings(wantRows)
	if rows != strings.Join(wantRows, "\n") {
		t.Fatalf("serving_logs (body, dataset, uuidv7) =\n%s\nwant\n%s", rows, strings.Join(wantRows, "\n"))
	}
	for deadline := time.Now().Add(30 * time.Second); ; time.Sleep(200 * time.Millisecond) {
		if got = ch.query(t, "SELECT dataset, span_name FROM "+db+".serving_spans"); got != "" || time.Now().After(deadline) {
			break
		}
	}
	if got != "prod\tspan-header" {
		t.Fatalf("serving_spans = %q, want one prod span", got)
	}
	// Every serving row has exactly one canonical row with the same identity and
	// payload hash, and the payload identity is (dataset, event_id) only.
	for _, table := range []string{"serving_logs", "serving_spans"} {
		orphans := ch.query(t, "SELECT count() FROM "+db+"."+table+" s LEFT ANTI JOIN "+db+".canonical_events c ON s.dataset = c.dataset AND s.event_id = c.event_id AND s.payload_sha256 = c.payload_sha256")
		if orphans != "0" {
			t.Fatalf("%s rows without a matching canonical row: %s", table, orphans)
		}
	}
	if got := ch.query(t, "SELECT count() FROM "+db+".canonical_events"); got != fmt.Sprint(len(want)+1) {
		t.Fatalf("canonical rows = %s, want %d", got, len(want)+1)
	}
	if got := ch.query(t, "SELECT count() FROM "+db+".canonical_events WHERE JSONExtractKeys(payload, 'identity') != ['dataset', 'event_id'] OR JSONExtractString(payload, 'identity', 'dataset') != dataset OR JSONExtractString(payload, 'identity', 'event_id') != event_id"); got != "0" {
		t.Fatalf("%s canonical payloads have an identity other than (dataset, event_id)", got)
	}
	// The schema the exporter wrote is the single-tenant one, in the configured
	// database only.
	if got := ch.query(t, "SELECT count() FROM system.columns WHERE database = '"+db+"' AND name = 'tenant_id'"); got != "0" {
		t.Fatalf("tenant_id columns in %s: %s", db, got)
	}
	if got := ch.query(t, "SELECT name, sorting_key FROM system.tables WHERE database = '"+db+"' ORDER BY name"); got != "canonical_events\tdataset, event_id\nserving_logs\tdataset, timestamp, event_id\nserving_spans\tdataset, timestamp, event_id" {
		t.Fatalf("tables and sort keys =\n%s", got)
	}
	if got := ch.query(t, "SELECT count() FROM system.databases WHERE name = 'toudai'"); got != "0" {
		t.Fatal("the default toudai database exists: something ignored the configured database")
	}
	// Nothing rejected, retired, or credential-bearing is stored anywhere, and
	// no key reached the collector's logs.
	dump := ch.query(t, "SELECT * FROM "+db+".serving_logs FORMAT TSV") + ch.query(t, "SELECT * FROM "+db+".serving_spans FORMAT TSV") + ch.query(t, "SELECT * FROM "+db+".canonical_events FORMAT TSV")
	for _, forbidden := range []string{"tk_example", "REJECT", "workspace", "scope_source", "tenant_id", "toudai.event_id", "00000000-0000-7000-8000-000000000000"} {
		if strings.Contains(dump, forbidden) {
			t.Fatalf("stored rows contain %q", forbidden)
		}
	}
	collectorLog, err := os.ReadFile(logFile)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Contains(collectorLog, []byte("toudai exporter batch")) {
		t.Fatal("collector log has no batch summary; the credential scan would prove nothing")
	}
	if bytes.Contains(collectorLog, []byte("tk_example")) {
		t.Fatal("an ingestion key reached the collector log")
	}
}
