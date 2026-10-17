package main

import (
	"bytes"
	"context"
	"fmt"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"go.opentelemetry.io/collector/otelcol"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/plog/plogotlp"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

const (
	exampleConfig = "config/gateway.example.yaml"
	exampleKeys   = "config/keys.example.yaml"

	// Synthetic keys from config/keys.example.yaml.
	keyWSAProd     = "tk_example_ws_a_prod_0123456789abcdef"
	keyWSAMulti    = "tk_example_ws_a_multi_0123456789abcdef"
	keyWSBProd     = "tk_example_ws_b_prod_0123456789abcdef"
	keyWSAReadonly = "tk_example_ws_a_readonly_0123456789ab"
	wsA            = "11111111-1111-4111-8111-111111111111"
	wsB            = "22222222-2222-4222-8222-222222222222"
)

func freeAddr(t *testing.T) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = l.Close() }()
	return l.Addr().String()
}

func TestExampleGatewayConfigIsValid(t *testing.T) {
	set := collectorSettings()
	set.ConfigProviderSettings.ResolverSettings.URIs = []string{"file:" + exampleConfig}
	col, err := otelcol.NewCollector(set)
	if err != nil {
		t.Fatal(err)
	}
	if err := col.DryRun(context.Background()); err != nil {
		t.Fatalf("example gateway config does not validate: %v", err)
	}
}

// gateway runs the distribution in-process with the example gateway config,
// swapping only the destination (the toudai exporter needs ClickHouse) for a
// file exporter. Everything between the receiver and the exporter is the
// example config as shipped.
func gateway(t *testing.T) (baseURL, grpcAddr, outFile string) {
	t.Helper()
	dir := t.TempDir()
	outFile = filepath.Join(dir, "out.jsonl")
	override := filepath.Join(dir, "override.yaml")
	if err := os.WriteFile(override, []byte(`
exporters:
  file/out:
    path: `+outFile+`
    flush_interval: 10ms
service:
  telemetry:
    metrics:
      level: none
    logs:
      level: error
  pipelines:
    logs:
      exporters: [file/out]
    traces:
      exporters: [file/out]
`), 0o600); err != nil {
		t.Fatal(err)
	}
	httpAddr, grpcAddr := freeAddr(t), freeAddr(t)
	t.Setenv("TOUDAI_OTLP_HTTP_ENDPOINT", httpAddr)
	t.Setenv("TOUDAI_OTLP_GRPC_ENDPOINT", grpcAddr)
	t.Setenv("TOUDAI_KEYS_FILE", exampleKeys)

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
	deadline := time.Now().Add(20 * time.Second)
	for col.GetState() != otelcol.StateRunning {
		select {
		case err := <-done:
			t.Fatalf("collector exited during startup: %v", err)
		default:
		}
		if time.Now().After(deadline) {
			t.Fatal("collector did not start")
		}
		time.Sleep(20 * time.Millisecond)
	}
	return "http://" + httpAddr, grpcAddr, outFile
}

const logBody = `{"resourceLogs":[{"resource":{"attributes":[{"key":"service.name","value":{"stringValue":"svc"}}%s]},
 "scopeLogs":[{"logRecords":[{"timeUnixNano":"1788246000123456789","body":{"stringValue":"same body"},
 "attributes":[%s]}]}]}]}`

func post(t *testing.T, url string, body string, headers map[string]string) (int, string) {
	t.Helper()
	req, err := http.NewRequest(http.MethodPost, url, strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Content-Type", "application/json")
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	var buf bytes.Buffer
	_, _ = buf.ReadFrom(resp.Body)
	return resp.StatusCode, buf.String()
}

// exported reads back every record the file exporter wrote: its resource
// attributes and its own attributes, plus the raw file for credential scans.
type exportedRecord struct {
	resource map[string]string
	record   map[string]string
}

func readExported(t *testing.T, path string, want int) ([]exportedRecord, string) {
	t.Helper()
	var raw []byte
	var out []exportedRecord
	for deadline := time.Now().Add(5 * time.Second); ; time.Sleep(20 * time.Millisecond) {
		raw, _ = os.ReadFile(path)
		out = out[:0]
		for _, line := range bytes.Split(raw, []byte("\n")) {
			if len(bytes.TrimSpace(line)) == 0 {
				continue
			}
			switch {
			case bytes.Contains(line, []byte(`"resourceLogs"`)):
				ld, err := (&plog.JSONUnmarshaler{}).UnmarshalLogs(line)
				if err != nil {
					continue
				}
				for i := 0; i < ld.ResourceLogs().Len(); i++ {
					rl := ld.ResourceLogs().At(i)
					for j := 0; j < rl.ScopeLogs().Len(); j++ {
						recs := rl.ScopeLogs().At(j).LogRecords()
						for k := 0; k < recs.Len(); k++ {
							out = append(out, exportedRecord{asStrings(rl.Resource().Attributes().AsRaw()), asStrings(recs.At(k).Attributes().AsRaw())})
						}
					}
				}
			case bytes.Contains(line, []byte(`"resourceSpans"`)):
				td, err := (&ptrace.JSONUnmarshaler{}).UnmarshalTraces(line)
				if err != nil {
					continue
				}
				for i := 0; i < td.ResourceSpans().Len(); i++ {
					rs := td.ResourceSpans().At(i)
					for j := 0; j < rs.ScopeSpans().Len(); j++ {
						spans := rs.ScopeSpans().At(j).Spans()
						for k := 0; k < spans.Len(); k++ {
							out = append(out, exportedRecord{asStrings(rs.Resource().Attributes().AsRaw()), asStrings(spans.At(k).Attributes().AsRaw())})
						}
					}
				}
			}
		}
		if len(out) >= want || time.Now().After(deadline) {
			return out, string(raw)
		}
	}
}

func asStrings(m map[string]any) map[string]string {
	out := make(map[string]string, len(m))
	for k, v := range m {
		out[k] = fmt.Sprint(v)
	}
	return out
}

// Through the real receiver, auth extension, and gateway processor from the
// example config: what is accepted carries key-derived scope and fresh IDs, and
// everything else is refused with the right status and writes nothing.
func TestGatewayIngestion(t *testing.T) {
	base, _, outFile := gateway(t)
	tests := []struct {
		name       string
		headers    map[string]string
		resource   string // extra resource attributes (JSON list items, leading comma)
		record     string
		wantStatus int
		wantWS     string
		wantDS     string
	}{
		{"accepted, single-dataset key", map[string]string{"X-Toudai-Key": keyWSAProd}, "", "", 200, wsA, "prod"},
		{"accepted, other workspace", map[string]string{"X-Toudai-Key": keyWSBProd}, "", "", 200, wsB, "prod"},
		{"accepted, multi-dataset key names its dataset", map[string]string{"X-Toudai-Key": keyWSAMulti, "X-Toudai-Dataset": "staging"}, "", "", 200, wsA, "staging"},
		{"unknown dataset", map[string]string{"X-Toudai-Key": keyWSAMulti, "X-Toudai-Dataset": "nope"}, "", "", 403, "", ""},
		{"dataset belonging only to another key", map[string]string{"X-Toudai-Key": keyWSBProd, "X-Toudai-Dataset": "staging"}, "", "", 403, "", ""},
		{"multi-dataset key without a dataset", map[string]string{"X-Toudai-Key": keyWSAMulti}, "", "", 400, "", ""},
		{"override attempt: workspace on resource", map[string]string{"X-Toudai-Key": keyWSAProd}, `,{"key":"toudai.workspace_id","value":{"stringValue":"` + wsB + `"}}`, "", 400, "", ""},
		{"override attempt: dataset on resource", map[string]string{"X-Toudai-Key": keyWSAProd}, `,{"key":"toudai.dataset","value":{"stringValue":"staging"}}`, "", 400, "", ""},
		{"override attempt: forged scope marker", map[string]string{"X-Toudai-Key": keyWSAProd}, `,{"key":"toudai.scope_source","value":{"stringValue":"gateway"}}`, "", 400, "", ""},
		{"missing key", nil, "", "", 401, "", ""},
		{"unknown key", map[string]string{"X-Toudai-Key": "tk_example_not_a_real_key_000"}, "", "", 401, "", ""},
		{"read-only key", map[string]string{"X-Toudai-Key": keyWSAReadonly}, "", "", 401, "", ""},
		{"authorization header is not an ingestion key", map[string]string{"Authorization": "Bearer " + keyWSAProd}, "", "", 401, "", ""},
	}
	accepted := 0
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			status, body := post(t, base+"/v1/logs", fmt.Sprintf(logBody, tc.resource, tc.record), tc.headers)
			if status != tc.wantStatus {
				t.Fatalf("status = %d (%s), want %d", status, body, tc.wantStatus)
			}
			// Error bodies go back to the producer; none may echo a credential.
			for _, k := range tc.headers {
				if strings.Contains(body, strings.TrimPrefix(k, "Bearer ")) {
					t.Fatalf("response %q echoes a supplied credential", body)
				}
			}
			if status != 200 {
				return
			}
			accepted++
			got, _ := readExported(t, outFile, accepted)
			rec := got[accepted-1]
			if rec.resource["toudai.workspace_id"] != tc.wantWS || rec.resource["toudai.dataset"] != tc.wantDS || rec.resource["toudai.scope_source"] != "gateway" {
				t.Fatalf("resource = %v, want workspace %s dataset %s", rec.resource, tc.wantWS, tc.wantDS)
			}
			if id, err := uuid.Parse(rec.record["toudai.event_id"]); err != nil || id.Version() != 7 {
				t.Fatalf("event_id = %q, want a UUIDv7 (%v)", rec.record["toudai.event_id"], err)
			}
		})
	}
	// Rejected requests must not have reached the exporter, and no credential may
	// have been written anywhere an exporter could store it.
	// Accepted requests reach the file exporter synchronously and rejected ones
	// never leave the processor, so the file is complete once the responses are in.
	got, raw := readExported(t, outFile, 0)
	if len(got) != accepted {
		t.Fatalf("exporter received %d records, want only the %d accepted", len(got), accepted)
	}
	if strings.Contains(raw, "tk_example") {
		t.Fatal("an ingestion key reached exported data")
	}
}

// A producer retry of an identical request is a new request and gets new IDs;
// two identical records inside one request do too. Nothing is deduplicated.
func TestGatewayAssignsDistinctIDsToIdenticalEvents(t *testing.T) {
	base, _, outFile := gateway(t)
	record := `{"timeUnixNano":"1","body":{"stringValue":"same"}}`
	body := `{"resourceLogs":[{"scopeLogs":[{"logRecords":[` + record + `,` + record + `]}]}]}`
	hdr := map[string]string{"X-Toudai-Key": keyWSAProd}
	for i := 0; i < 2; i++ {
		if status, resp := post(t, base+"/v1/logs", body, hdr); status != 200 {
			t.Fatalf("attempt %d: status = %d (%s)", i+1, status, resp)
		}
	}
	got, _ := readExported(t, outFile, 4)
	if len(got) != 4 {
		t.Fatalf("exported %d records, want 4 (no deduplication)", len(got))
	}
	seen := map[string]bool{}
	for _, r := range got {
		id := r.record["toudai.event_id"]
		if seen[id] {
			t.Fatalf("event_id %s assigned to two records", id)
		}
		seen[id] = true
	}
}

func TestGatewayTraces(t *testing.T) {
	base, _, outFile := gateway(t)
	body := `{"resourceSpans":[{"scopeSpans":[{"spans":[{"traceId":"0102030405060708090a0b0c0d0e0f10","spanId":"0102030405060708","name":"s","startTimeUnixNano":"1","endTimeUnixNano":"2"}]}]}]}`
	if status, resp := post(t, base+"/v1/traces", body, map[string]string{"X-Toudai-Key": keyWSBProd}); status != 200 {
		t.Fatalf("status = %d (%s)", status, resp)
	}
	if status, _ := post(t, base+"/v1/traces", body, nil); status != 401 {
		t.Fatalf("keyless status = %d, want 401", status)
	}
	got, _ := readExported(t, outFile, 1)
	if len(got) != 1 || got[0].resource["toudai.workspace_id"] != wsB || got[0].record["toudai.event_id"] == "" {
		t.Fatalf("exported = %+v, want one span scoped to %s with an event_id", got, wsB)
	}
}

// The example config enables gRPC with the same authenticator; gRPC lowercases
// metadata keys, so this exercises the extension's case-insensitive lookup.
func TestGatewayGRPC(t *testing.T) {
	_, addr, outFile := gateway(t)
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	logs := plogotlp.NewGRPCClient(conn)
	req := plogotlp.NewExportRequestFromLogs(func() plog.Logs {
		ld := plog.NewLogs()
		ld.ResourceLogs().AppendEmpty().ScopeLogs().AppendEmpty().LogRecords().AppendEmpty().Body().SetStr("b")
		return ld
	}())
	tests := []struct {
		name     string
		md       metadata.MD
		wantCode codes.Code
	}{
		{"accepted", metadata.Pairs("x-toudai-key", keyWSAProd), codes.OK},
		{"missing key", metadata.MD{}, codes.Unauthenticated},
		{"unknown key", metadata.Pairs("x-toudai-key", "tk_example_not_a_real_key_000"), codes.Unauthenticated},
		{"unknown dataset", metadata.Pairs("x-toudai-key", keyWSAProd, "x-toudai-dataset", "nope"), codes.PermissionDenied},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(metadata.NewOutgoingContext(context.Background(), tc.md), 5*time.Second)
			defer cancel()
			_, err := logs.Export(ctx, req)
			if got := status.Code(err); got != tc.wantCode {
				t.Fatalf("code = %v (%v), want %v", got, err, tc.wantCode)
			}
		})
	}
	got, _ := readExported(t, outFile, 1)
	if len(got) != 1 || got[0].resource["toudai.workspace_id"] != wsA {
		t.Fatalf("exported = %+v, want one record scoped to %s", got, wsA)
	}
}
