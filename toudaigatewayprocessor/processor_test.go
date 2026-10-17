package toudaigatewayprocessor

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/google/uuid"
	"go.opentelemetry.io/collector/client"
	"go.opentelemetry.io/collector/consumer/consumererror"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/toudai-dev/collector/internal/reserved"
	"github.com/toudai-dev/collector/toudaiauthextension"
)

// testAuth mirrors the auth data the extension publishes.
type testAuth map[string]any

func (a testAuth) GetAttribute(n string) any { return a[n] }
func (a testAuth) GetAttributeNames() []string {
	var out []string
	for k := range a {
		out = append(out, k)
	}
	return out
}

func authCtx(workspace string, datasets []string, requested string, write bool) context.Context {
	a := testAuth{
		toudaiauthextension.AttrWorkspaceID: workspace,
		toudaiauthextension.AttrDatasets:    datasets,
		toudaiauthextension.AttrWrite:       write,
	}
	if requested != "" {
		a[toudaiauthextension.AttrRequestedDataset] = requested
	}
	return client.NewContext(context.Background(), client.Info{Auth: a})
}

func oneLog(mutateResource func(plog.ResourceLogs), mutateRecord func(plog.LogRecord)) plog.Logs {
	ld := plog.NewLogs()
	rl := ld.ResourceLogs().AppendEmpty()
	rl.Resource().Attributes().PutStr("service.name", "svc")
	lr := rl.ScopeLogs().AppendEmpty().LogRecords().AppendEmpty()
	lr.Body().SetStr("same body")
	if mutateResource != nil {
		mutateResource(rl)
	}
	if mutateRecord != nil {
		mutateRecord(lr)
	}
	return ld
}

func TestProcessLogsScope(t *testing.T) {
	tests := []struct {
		name        string
		ctx         context.Context
		wantErr     error
		wantWS      string
		wantDataset string
	}{
		{"single dataset key defaults to its dataset", authCtx("ws-a", []string{"prod"}, "", true), nil, "ws-a", "prod"},
		{"requested dataset inside scope", authCtx("ws-a", []string{"prod", "staging"}, "staging", true), nil, "ws-a", "staging"},
		{"requested dataset equal to the only dataset", authCtx("ws-a", []string{"prod"}, "prod", true), nil, "ws-a", "prod"},
		{"multi-dataset key must choose", authCtx("ws-a", []string{"prod", "staging"}, "", true), errDatasetRequired, "", ""},
		{"unknown dataset", authCtx("ws-a", []string{"prod"}, "nope", true), errUnknownDataset, "", ""},
		{"dataset of another workspace", authCtx("ws-a", []string{"prod"}, "ws-b-only", true), errUnknownDataset, "", ""},
		{"dataset match is exact", authCtx("ws-a", []string{"prod"}, "Prod", true), errUnknownDataset, "", ""},
		{"no write permission", authCtx("ws-a", []string{"prod"}, "", false), errNoWrite, "", ""},
		{"no authenticated identity", context.Background(), errNoIdentity, "", ""},
		{"identity without workspace", authCtx("", []string{"prod"}, "", true), errNoIdentity, "", ""},
	}
	g := newGateway()
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ld := oneLog(nil, nil)
			out, err := g.processLogs(tc.ctx, ld)
			if tc.wantErr != nil {
				if !errors.Is(err, tc.wantErr) || !consumererror.IsPermanent(err) {
					t.Fatalf("error = %v, want permanent %v", err, tc.wantErr)
				}
				// A rejected request must carry no trusted attributes forward.
				res := out.ResourceLogs().At(0).Resource().Attributes()
				for _, k := range []string{reserved.WorkspaceAttr, reserved.DatasetAttr, reserved.ScopeSourceAttr} {
					if _, ok := res.Get(k); ok {
						t.Fatalf("rejected request was stamped with %s", k)
					}
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			res := out.ResourceLogs().At(0).Resource().Attributes()
			want := map[string]string{
				reserved.WorkspaceAttr:   tc.wantWS,
				reserved.DatasetAttr:     tc.wantDataset,
				reserved.ScopeSourceAttr: reserved.ScopeSourceValue,
				"service.name":           "svc",
			}
			for k, v := range want {
				if got, ok := res.Get(k); !ok || got.Str() != v {
					t.Fatalf("resource %s = %v (present=%v), want %q", k, got.AsString(), ok, v)
				}
			}
		})
	}
}

func TestProcessLogsRejectsReservedResourceAttributes(t *testing.T) {
	for _, key := range []string{
		reserved.WorkspaceAttr, reserved.DatasetAttr, reserved.ScopeSourceAttr, reserved.EventIDAttr, "toudai.anything_else",
	} {
		t.Run(key, func(t *testing.T) {
			ld := oneLog(func(rl plog.ResourceLogs) {
				rl.Resource().Attributes().PutStr(key, "99999999-9999-4999-8999-999999999999")
			}, nil)
			_, err := newGateway().processLogs(authCtx("ws-a", []string{"prod"}, "", true), ld)
			if !errors.Is(err, errReservedResource) || !consumererror.IsPermanent(err) {
				t.Fatalf("error = %v, want permanent %v", err, errReservedResource)
			}
			if strings.Contains(err.Error(), "9999") {
				t.Fatalf("error %q reflects the producer value", err)
			}
		})
	}
}

// One forged resource rejects the whole request, even when it is not the first
// resource, and nothing is stamped on the others.
func TestProcessLogsForgedResourceRejectsWholeRequest(t *testing.T) {
	ld := oneLog(nil, nil)
	rl := ld.ResourceLogs().AppendEmpty()
	rl.Resource().Attributes().PutStr(reserved.WorkspaceAttr, "ws-victim")
	rl.ScopeLogs().AppendEmpty().LogRecords().AppendEmpty()
	out, err := newGateway().processLogs(authCtx("ws-a", []string{"prod"}, "", true), ld)
	if !errors.Is(err, errReservedResource) {
		t.Fatalf("error = %v, want %v", err, errReservedResource)
	}
	first := out.ResourceLogs().At(0)
	if _, ok := first.Resource().Attributes().Get(reserved.WorkspaceAttr); ok {
		t.Fatal("a rejected request left its other resources stamped")
	}
	if _, ok := first.ScopeLogs().At(0).LogRecords().At(0).Attributes().Get(reserved.EventIDAttr); ok {
		t.Fatal("a rejected request left records with assigned event IDs")
	}
}

func TestProcessLogsAssignsUniqueUUIDv7PerRecord(t *testing.T) {
	ld := plog.NewLogs()
	// Three resources, two scopes, identical content everywhere: only a random
	// ID can tell the records apart.
	for i := 0; i < 3; i++ {
		rl := ld.ResourceLogs().AppendEmpty()
		for j := 0; j < 2; j++ {
			recs := rl.ScopeLogs().AppendEmpty().LogRecords()
			for k := 0; k < 2; k++ {
				recs.AppendEmpty().Body().SetStr("same body")
			}
		}
	}
	out, err := newGateway().processLogs(authCtx("ws-a", []string{"prod"}, "", true), ld)
	if err != nil {
		t.Fatal(err)
	}
	seen := map[string]bool{}
	var last string
	for i := 0; i < out.ResourceLogs().Len(); i++ {
		for j := 0; j < out.ResourceLogs().At(i).ScopeLogs().Len(); j++ {
			recs := out.ResourceLogs().At(i).ScopeLogs().At(j).LogRecords()
			for k := 0; k < recs.Len(); k++ {
				v, ok := recs.At(k).Attributes().Get(reserved.EventIDAttr)
				if !ok {
					t.Fatalf("record %d/%d/%d has no event ID", i, j, k)
				}
				id, err := uuid.Parse(v.Str())
				if err != nil || id.Version() != 7 || id.String() != v.Str() {
					t.Fatalf("event ID %q is not a canonical UUIDv7 (%v)", v.Str(), err)
				}
				if seen[v.Str()] {
					t.Fatalf("event ID %s assigned twice", v.Str())
				}
				seen[v.Str()] = true
				// Lexical order of v7 text is generation order; assigned in
				// request order so IDs are non-decreasing.
				if v.Str() <= last {
					t.Fatalf("event ID %s does not sort after %s", v.Str(), last)
				}
				last = v.Str()
			}
		}
	}
	if len(seen) != 12 {
		t.Fatalf("assigned %d IDs, want 12", len(seen))
	}
}

// A producer-supplied record event ID is replaced rather than trusted, including
// when the wire form carries the key twice, which a plain upsert would leave
// behind the assigned one. The request is built from JSON because pcommon's API
// cannot express duplicate keys.
func TestProcessLogsReplacesProducerEventID(t *testing.T) {
	const forged = "00000000-0000-7000-8000-000000000000"
	body := `{"resourceLogs":[{"scopeLogs":[{"logRecords":[{"body":{"stringValue":"b"},"attributes":[
		{"key":"toudai.event_id","value":{"stringValue":"` + forged + `"}},
		{"key":"keep","value":{"stringValue":"me"}},
		{"key":"toudai.event_id","value":{"stringValue":"` + forged + `"}},
		{"key":"toudai.workspace_id","value":{"stringValue":"ws-victim"}}]}]}]}]}`
	ld, err := (&plog.JSONUnmarshaler{}).UnmarshalLogs([]byte(body))
	if err != nil {
		t.Fatal(err)
	}
	attrs := ld.ResourceLogs().At(0).ScopeLogs().At(0).LogRecords().At(0).Attributes()
	if attrs.Len() != 4 {
		t.Fatalf("fixture has %d attributes, want the duplicate preserved (4)", attrs.Len())
	}
	out, err := newGateway().processLogs(authCtx("ws-a", []string{"prod"}, "", true), ld)
	if err != nil {
		t.Fatal(err)
	}
	attrs = out.ResourceLogs().At(0).ScopeLogs().At(0).LogRecords().At(0).Attributes()
	var ids []string
	attrs.Range(func(k string, v pcommon.Value) bool {
		if k == reserved.EventIDAttr {
			ids = append(ids, v.Str())
		}
		return true
	})
	if len(ids) != 1 || ids[0] == forged {
		t.Fatalf("event IDs = %v, want exactly one fresh gateway ID", ids)
	}
	if v, _ := attrs.Get("keep"); v.Str() != "me" {
		t.Fatal("unrelated record attributes must be preserved")
	}
}

func TestProcessLogsFailsWhenIDGenerationFails(t *testing.T) {
	g := newGateway()
	boom := errors.New("entropy exhausted")
	g.newEventID = func() (string, error) { return "", boom }
	_, err := g.processLogs(authCtx("ws-a", []string{"prod"}, "", true), oneLog(nil, nil))
	// Not permanent: the producer's request was fine, so the receiver may retry.
	if !errors.Is(err, boom) || consumererror.IsPermanent(err) {
		t.Fatalf("error = %v, want a retryable wrapped %v", err, boom)
	}
}

func TestProcessTraces(t *testing.T) {
	build := func(forgeResource bool) ptrace.Traces {
		td := ptrace.NewTraces()
		rs := td.ResourceSpans().AppendEmpty()
		if forgeResource {
			rs.Resource().Attributes().PutStr(reserved.WorkspaceAttr, "ws-victim")
		}
		spans := rs.ScopeSpans().AppendEmpty().Spans()
		spans.AppendEmpty().SetName("same")
		spans.AppendEmpty().SetName("same")
		return td
	}
	tests := []struct {
		name    string
		ctx     context.Context
		forge   bool
		wantErr error
	}{
		{"accepted", authCtx("ws-a", []string{"prod"}, "", true), false, nil},
		{"unknown dataset", authCtx("ws-a", []string{"prod"}, "nope", true), false, errUnknownDataset},
		{"forged workspace", authCtx("ws-a", []string{"prod"}, "", true), true, errReservedResource},
		{"no identity", context.Background(), false, errNoIdentity},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			out, err := newGateway().processTraces(tc.ctx, build(tc.forge))
			if tc.wantErr != nil {
				if !errors.Is(err, tc.wantErr) || !consumererror.IsPermanent(err) {
					t.Fatalf("error = %v, want permanent %v", err, tc.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			res := out.ResourceSpans().At(0).Resource().Attributes()
			if v, _ := res.Get(reserved.WorkspaceAttr); v.Str() != "ws-a" {
				t.Fatalf("workspace = %q, want ws-a", v.Str())
			}
			if v, _ := res.Get(reserved.DatasetAttr); v.Str() != "prod" {
				t.Fatalf("dataset = %q, want prod", v.Str())
			}
			spans := out.ResourceSpans().At(0).ScopeSpans().At(0).Spans()
			a, _ := spans.At(0).Attributes().Get(reserved.EventIDAttr)
			b, _ := spans.At(1).Attributes().Get(reserved.EventIDAttr)
			if a.Str() == "" || a.Str() == b.Str() {
				t.Fatalf("span event IDs %q and %q must be assigned and distinct", a.Str(), b.Str())
			}
		})
	}
}

// OTLP receivers turn an error into the producer-visible status; a bare
// permanent error would surface as an internal server error.
func TestRejectionsCarryClientStatuses(t *testing.T) {
	tests := []struct {
		err  error
		want codes.Code
	}{
		{errUnknownDataset, codes.PermissionDenied},
		{errDatasetRequired, codes.InvalidArgument},
		{errReservedResource, codes.InvalidArgument},
		{errNoWrite, codes.PermissionDenied},
		{errNoIdentity, codes.Internal},
	}
	for _, tc := range tests {
		t.Run(tc.want.String()+"/"+tc.err.Error()[:12], func(t *testing.T) {
			if got := status.Code(tc.err); got != tc.want {
				t.Fatalf("status = %v, want %v", got, tc.want)
			}
		})
	}
}
