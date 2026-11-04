package toudaigatewayprocessor

import (
	"context"
	"errors"
	"reflect"
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

func authCtx(datasets []string, requested string, write bool) context.Context {
	a := testAuth{
		toudaiauthextension.AttrDatasets: datasets,
		toudaiauthextension.AttrWrite:    write,
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

// Decision 0002 D3: the dataset is the header, else the key's only dataset,
// else the configured default, and from any source it must be in the key's
// list. A producer resource toudai.dataset must agree with the result.
func TestProcessLogsScope(t *testing.T) {
	all := []string(nil) // a key with no dataset list permits every dataset
	dsOnResource := func(v string) func(plog.ResourceLogs) {
		return func(rl plog.ResourceLogs) { rl.Resource().Attributes().PutStr(reserved.DatasetAttr, v) }
	}
	tests := []struct {
		name        string
		def         string // configured default_dataset
		ctx         context.Context
		resource    func(plog.ResourceLogs)
		wantErr     error
		wantDataset string
	}{
		{"single dataset key defaults to its dataset", "", authCtx([]string{"prod"}, "", true), nil, nil, "prod"},
		{"key's only dataset wins over the default", "staging", authCtx([]string{"prod"}, "", true), nil, nil, "prod"},
		{"header wins over the key's only dataset", "", authCtx([]string{"prod"}, "prod", true), nil, nil, "prod"},
		{"header inside the key's list", "", authCtx([]string{"prod", "staging"}, "staging", true), nil, nil, "staging"},
		{"header wins over the default", "prod", authCtx([]string{"prod", "staging"}, "staging", true), nil, nil, "staging"},
		{"multi-dataset key falls back to the default", "staging", authCtx([]string{"prod", "staging"}, "", true), nil, nil, "staging"},
		{"unrestricted key falls back to the default", "prod", authCtx(all, "", true), nil, nil, "prod"},
		{"unrestricted key accepts any valid header", "", authCtx(all, "anything-1", true), nil, nil, "anything-1"},
		{"multi-dataset key without header or default", "", authCtx([]string{"prod", "staging"}, "", true), nil, errDatasetRequired, ""},
		{"unrestricted key without header or default", "", authCtx(all, "", true), nil, errDatasetRequired, ""},
		{"default outside the key's list", "other", authCtx([]string{"prod", "staging"}, "", true), nil, errUnknownDataset, ""},
		{"header outside the key's list", "", authCtx([]string{"prod"}, "nope", true), nil, errUnknownDataset, ""},
		{"dataset match is exact", "", authCtx([]string{"prod"}, "Prod", true), nil, errUnknownDataset, ""},
		{"invalid header name, unrestricted key", "", authCtx(all, "a b", true), nil, errInvalidDataset, ""},
		{"resource dataset equal to the resolved one", "", authCtx([]string{"prod"}, "", true), dsOnResource("prod"), nil, "prod"},
		{"resource dataset equal to the header", "", authCtx(all, "x1", true), dsOnResource("x1"), nil, "x1"},
		{"resource dataset outside the key's list", "", authCtx([]string{"prod"}, "", true), dsOnResource("staging"), errUnknownDataset, ""},
		{"resource dataset permitted but not resolved", "", authCtx([]string{"prod", "staging"}, "prod", true), dsOnResource("staging"), errDatasetMismatch, ""},
		{"resource dataset differs, unrestricted key", "prod", authCtx(all, "", true), dsOnResource("staging"), errDatasetMismatch, ""},
		{"resource dataset is not a string", "", authCtx([]string{"prod"}, "", true), func(rl plog.ResourceLogs) {
			rl.Resource().Attributes().PutInt(reserved.DatasetAttr, 1)
		}, errDatasetMismatch, ""},
		{"no write permission", "", authCtx([]string{"prod"}, "", false), nil, errNoWrite, ""},
		{"no authenticated identity", "", context.Background(), nil, errNoIdentity, ""},
		{"auth data from another authenticator", "", client.NewContext(context.Background(), client.Info{Auth: testAuth{"subject": "x"}}), nil, errNoIdentity, ""},
		{"write attribute without a dataset list", "", client.NewContext(context.Background(), client.Info{Auth: testAuth{
			toudaiauthextension.AttrWrite: true, toudaiauthextension.AttrRequestedDataset: "prod",
		}}), nil, errNoIdentity, ""},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ld := oneLog(tc.resource, nil)
			before := ld.ResourceLogs().At(0).Resource().Attributes().AsRaw()
			out, err := newGateway(&Config{DefaultDataset: tc.def}).processLogs(tc.ctx, ld)
			if tc.wantErr != nil {
				if !errors.Is(err, tc.wantErr) || !consumererror.IsPermanent(err) {
					t.Fatalf("error = %v, want permanent %v", err, tc.wantErr)
				}
				// A rejected request is not stamped: no event ID, and the
				// resource is exactly what the producer sent.
				if _, ok := out.ResourceLogs().At(0).ScopeLogs().At(0).LogRecords().At(0).Attributes().Get(reserved.EventIDAttr); ok {
					t.Fatal("rejected request was assigned an event ID")
				}
				if got := out.ResourceLogs().At(0).Resource().Attributes().AsRaw(); !reflect.DeepEqual(got, before) {
					t.Fatalf("rejected request's resource = %v, want it unchanged %v", got, before)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			res := out.ResourceLogs().At(0).Resource().Attributes()
			// The dataset is the only attribute the gateway adds to a resource.
			want := map[string]any{reserved.DatasetAttr: tc.wantDataset, "service.name": "svc"}
			if got := res.AsRaw(); !reflect.DeepEqual(got, want) {
				t.Fatalf("resource = %v, want %v", got, want)
			}
		})
	}
}

func TestProcessLogsRejectsReservedResourceAttributes(t *testing.T) {
	for _, key := range []string{
		// The retired workspace and scope-marker attributes are now ordinary
		// reserved-namespace keys and get the same rejection as any other.
		"toudai.workspace_id", "toudai.scope_source", reserved.EventIDAttr, "toudai.anything_else",
	} {
		t.Run(key, func(t *testing.T) {
			ld := oneLog(func(rl plog.ResourceLogs) {
				rl.Resource().Attributes().PutStr(key, "99999999-9999-4999-8999-999999999999")
			}, nil)
			_, err := newGateway(&Config{}).processLogs(authCtx([]string{"prod"}, "", true), ld)
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
	rl.Resource().Attributes().PutStr("toudai.anything_else", "x")
	rl.ScopeLogs().AppendEmpty().LogRecords().AppendEmpty()
	out, err := newGateway(&Config{}).processLogs(authCtx([]string{"prod"}, "", true), ld)
	if !errors.Is(err, errReservedResource) {
		t.Fatalf("error = %v, want %v", err, errReservedResource)
	}
	first := out.ResourceLogs().At(0)
	if _, ok := first.Resource().Attributes().Get(reserved.DatasetAttr); ok {
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
	out, err := newGateway(&Config{}).processLogs(authCtx([]string{"prod"}, "", true), ld)
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
		{"key":"toudai.anything_else","value":{"stringValue":"x"}}]}]}]}]}`
	ld, err := (&plog.JSONUnmarshaler{}).UnmarshalLogs([]byte(body))
	if err != nil {
		t.Fatal(err)
	}
	attrs := ld.ResourceLogs().At(0).ScopeLogs().At(0).LogRecords().At(0).Attributes()
	if attrs.Len() != 4 {
		t.Fatalf("fixture has %d attributes, want the duplicate preserved (4)", attrs.Len())
	}
	out, err := newGateway(&Config{}).processLogs(authCtx([]string{"prod"}, "", true), ld)
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

// Every resource and every copy of a duplicated OTLP key is checked: a matching
// value may appear twice and collapses to one, while any conflicting copy or
// resource rejects the whole request before anything is stamped. Built from
// JSON because pcommon's API cannot express duplicate keys.
func TestProcessLogsChecksEveryResourceDataset(t *testing.T) {
	resource := func(values ...string) string {
		var attrs []string
		for _, v := range values {
			attrs = append(attrs, `{"key":"toudai.dataset","value":{"stringValue":"`+v+`"}}`)
		}
		return `{"resource":{"attributes":[` + strings.Join(attrs, ",") + `]},"scopeLogs":[{"logRecords":[{"body":{"stringValue":"b"}}]}]}`
	}
	tests := []struct {
		name      string
		resources []string
		wantErr   error
	}{
		{"duplicate matching copies", []string{resource("prod", "prod")}, nil},
		{"duplicate conflicting copies", []string{resource("prod", "staging")}, errDatasetMismatch},
		{"second resource mismatched", []string{resource(), resource("staging")}, errDatasetMismatch},
		{"second resource outside the key's list", []string{resource(), resource("other")}, errUnknownDataset},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ld, err := (&plog.JSONUnmarshaler{}).UnmarshalLogs([]byte(`{"resourceLogs":[` + strings.Join(tc.resources, ",") + `]}`))
			if err != nil {
				t.Fatal(err)
			}
			out, err := newGateway(&Config{}).processLogs(authCtx([]string{"prod", "staging"}, "prod", true), ld)
			if !errors.Is(err, tc.wantErr) {
				t.Fatalf("error = %v, want %v", err, tc.wantErr)
			}
			for i := 0; i < out.ResourceLogs().Len(); i++ {
				var got []string
				out.ResourceLogs().At(i).Resource().Attributes().Range(func(k string, v pcommon.Value) bool {
					if k == reserved.DatasetAttr {
						got = append(got, v.Str())
					}
					return true
				})
				switch {
				case tc.wantErr == nil && (len(got) != 1 || got[0] != "prod"):
					t.Fatalf("resource %d datasets = %v, want exactly [prod]", i, got)
				case tc.wantErr != nil && i == 0 && len(tc.resources) > 1 && len(got) != 0:
					t.Fatal("a rejected request left its first resource stamped")
				}
			}
		})
	}
}

func TestConfigValidate(t *testing.T) {
	for _, tc := range []struct {
		def     string
		wantErr bool
	}{{"", false}, {"prod", false}, {"a b", true}, {"-lead", true}} {
		if err := (&Config{DefaultDataset: tc.def}).Validate(); (err != nil) != tc.wantErr {
			t.Errorf("Validate(%q) = %v, wantErr %v", tc.def, err, tc.wantErr)
		}
	}
}

func TestProcessLogsFailsWhenIDGenerationFails(t *testing.T) {
	g := newGateway(&Config{})
	boom := errors.New("entropy exhausted")
	g.newEventID = func() (string, error) { return "", boom }
	_, err := g.processLogs(authCtx([]string{"prod"}, "", true), oneLog(nil, nil))
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
			rs.Resource().Attributes().PutStr("toudai.anything_else", "x")
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
		{"accepted", authCtx([]string{"prod"}, "", true), false, nil},
		{"unknown dataset", authCtx([]string{"prod"}, "nope", true), false, errUnknownDataset},
		{"reserved resource attribute", authCtx([]string{"prod"}, "", true), true, errReservedResource},
		{"no identity", context.Background(), false, errNoIdentity},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			out, err := newGateway(&Config{}).processTraces(tc.ctx, build(tc.forge))
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
		{errInvalidDataset, codes.InvalidArgument},
		{errDatasetMismatch, codes.InvalidArgument},
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
