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

// authCtx builds the auth data of a key; nil datasets is an unrestricted key.
func authCtx(datasets []string, requested string, write bool) context.Context {
	a := testAuth{
		toudaiauthextension.AttrAuthenticator: toudaiauthextension.AuthenticatorName,
		toudaiauthextension.AttrDatasets:      datasets,
		toudaiauthextension.AttrWrite:         write,
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

func newTestGateway(defaultDataset string) *gateway {
	return newGateway(&Config{DefaultDataset: defaultDataset})
}

// Decision 0002 D3: the dataset header, else the key's single allowed dataset,
// else the configured default; a key's dataset list bounds all three.
func TestProcessLogsResolvesDataset(t *testing.T) {
	two := []string{"prod", "staging"}
	tests := []struct {
		name           string
		ctx            context.Context
		defaultDataset string
		wantErr        error
		wantDataset    string
	}{
		// Precedence.
		{"header equal to the key's only dataset", authCtx([]string{"prod"}, "prod", true), "staging", nil, "prod"},
		{"header wins over the default", authCtx(two, "staging", true), "prod", nil, "staging"},
		{"key's only dataset wins over the default", authCtx([]string{"staging"}, "", true), "prod", nil, "staging"},
		{"default when the key lists several", authCtx(two, "", true), "prod", nil, "prod"},
		{"default for an unrestricted key", authCtx(nil, "", true), "prod", nil, "prod"},
		{"header for an unrestricted key", authCtx(nil, "anything", true), "prod", nil, "anything"},
		// Nothing names a dataset.
		{"several datasets and no default", authCtx(two, "", true), "", errDatasetRequired, ""},
		{"unrestricted key and no default", authCtx(nil, "", true), "", errDatasetRequired, ""},
		// The key's list bounds every source.
		{"header outside the list", authCtx(two, "nope", true), "prod", errUnknownDataset, ""},
		{"header outside a single-dataset list", authCtx([]string{"prod"}, "staging", true), "", errUnknownDataset, ""},
		{"default outside the list", authCtx(two, "", true), "other", errUnknownDataset, ""},
		{"dataset match is exact", authCtx([]string{"prod"}, "Prod", true), "", errUnknownDataset, ""},
		// The header is producer input that becomes a stored value.
		{"invalid header for an unrestricted key", authCtx(nil, "a b", true), "", errInvalidDataset, ""},
		// Identity.
		{"no write permission", authCtx([]string{"prod"}, "", false), "", errNoWrite, ""},
		{"no authenticated identity", context.Background(), "prod", errNoIdentity, ""},
		{"auth data from another authenticator", client.NewContext(context.Background(), client.Info{Auth: testAuth{"sub": "x"}}), "prod", errNoIdentity, ""},
		// Matching attribute names are not enough without toudaiauth's marker.
		{"look-alike auth data", client.NewContext(context.Background(), client.Info{Auth: testAuth{toudaiauthextension.AttrWrite: true}}), "prod", errNoIdentity, ""},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			out, err := newTestGateway(tc.defaultDataset).processLogs(tc.ctx, oneLog(nil, nil))
			res := out.ResourceLogs().At(0).Resource().Attributes()
			if tc.wantErr != nil {
				if !errors.Is(err, tc.wantErr) || !consumererror.IsPermanent(err) {
					t.Fatalf("error = %v, want permanent %v", err, tc.wantErr)
				}
				// A rejected request must carry no gateway attributes forward.
				if _, ok := res.Get(reserved.DatasetAttr); ok {
					t.Fatal("rejected request was stamped with a dataset")
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if got, _ := res.Get(reserved.DatasetAttr); got.Str() != tc.wantDataset {
				t.Fatalf("dataset = %q, want %q", got.Str(), tc.wantDataset)
			}
			if got, _ := res.Get("service.name"); got.Str() != "svc" {
				t.Fatal("unrelated resource attributes must be preserved")
			}
		})
	}
}

// A producer toudai.dataset on the resource is accepted only when it equals the
// resolved dataset; every other reserved resource key, including the retired
// workspace and scope-source attributes, fails the request.
func TestProcessLogsResourceReservedAttributes(t *testing.T) {
	tests := []struct {
		name    string
		set     func(pcommon.Map)
		wantErr error
	}{
		{"matching dataset", func(m pcommon.Map) { m.PutStr(reserved.DatasetAttr, "prod") }, nil},
		{"mismatching dataset", func(m pcommon.Map) { m.PutStr(reserved.DatasetAttr, "staging") }, errDatasetMismatch},
		{"dataset differing in case", func(m pcommon.Map) { m.PutStr(reserved.DatasetAttr, "Prod") }, errDatasetMismatch},
		{"dataset that is not a string", func(m pcommon.Map) { m.PutInt(reserved.DatasetAttr, 1) }, errDatasetMismatch},
		{"retired workspace", func(m pcommon.Map) { m.PutStr("toudai.workspace_id", "99999999-9999-4999-8999-999999999999") }, errReservedResource},
		{"retired scope marker", func(m pcommon.Map) { m.PutStr("toudai.scope_source", "gateway") }, errReservedResource},
		{"event ID on the resource", func(m pcommon.Map) { m.PutStr(reserved.EventIDAttr, "99999999-9999-7999-8999-999999999999") }, errReservedResource},
		{"other reserved key", func(m pcommon.Map) { m.PutStr("toudai.anything_else", "9999") }, errReservedResource},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ld := oneLog(func(rl plog.ResourceLogs) { tc.set(rl.Resource().Attributes()) }, nil)
			out, err := newTestGateway("").processLogs(authCtx([]string{"prod", "staging"}, "prod", true), ld)
			if tc.wantErr == nil {
				if err != nil {
					t.Fatal(err)
				}
				if got, _ := out.ResourceLogs().At(0).Resource().Attributes().Get(reserved.DatasetAttr); got.Str() != "prod" {
					t.Fatalf("dataset = %q, want prod", got.Str())
				}
				return
			}
			if !errors.Is(err, tc.wantErr) || !consumererror.IsPermanent(err) {
				t.Fatalf("error = %v, want permanent %v", err, tc.wantErr)
			}
			if strings.Contains(err.Error(), "9999") || strings.Contains(err.Error(), "staging") {
				t.Fatalf("error %q reflects the producer value", err)
			}
		})
	}
}

// A wire resource can carry toudai.dataset twice; both copies must match, and
// exactly one survives, because the exporter rejects duplicate resource keys.
func TestProcessLogsDuplicateResourceDataset(t *testing.T) {
	build := func(second string) plog.Logs {
		body := `{"resourceLogs":[{"resource":{"attributes":[
			{"key":"toudai.dataset","value":{"stringValue":"prod"}},
			{"key":"toudai.dataset","value":{"stringValue":"` + second + `"}}]},
			"scopeLogs":[{"logRecords":[{"body":{"stringValue":"b"}}]}]}]}`
		ld, err := (&plog.JSONUnmarshaler{}).UnmarshalLogs([]byte(body))
		if err != nil {
			t.Fatal(err)
		}
		return ld
	}
	ctx := authCtx([]string{"prod", "staging"}, "prod", true)
	if _, err := newTestGateway("").processLogs(ctx, build("staging")); !errors.Is(err, errDatasetMismatch) {
		t.Fatalf("error = %v, want %v for a mismatching duplicate", err, errDatasetMismatch)
	}
	out, err := newTestGateway("").processLogs(ctx, build("prod"))
	if err != nil {
		t.Fatal(err)
	}
	var n int
	out.ResourceLogs().At(0).Resource().Attributes().Range(func(k string, _ pcommon.Value) bool {
		if k == reserved.DatasetAttr {
			n++
		}
		return true
	})
	if n != 1 {
		t.Fatalf("toudai.dataset appears %d times, want 1", n)
	}
}

// One forged resource rejects the whole request, even when it is not the first
// resource, and nothing is stamped on the others.
func TestProcessLogsForgedResourceRejectsWholeRequest(t *testing.T) {
	ld := oneLog(nil, nil)
	rl := ld.ResourceLogs().AppendEmpty()
	rl.Resource().Attributes().PutStr(reserved.DatasetAttr, "staging")
	rl.ScopeLogs().AppendEmpty().LogRecords().AppendEmpty()
	out, err := newTestGateway("").processLogs(authCtx([]string{"prod", "staging"}, "prod", true), ld)
	if !errors.Is(err, errDatasetMismatch) {
		t.Fatalf("error = %v, want %v", err, errDatasetMismatch)
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
	out, err := newTestGateway("").processLogs(authCtx([]string{"prod"}, "", true), ld)
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

// A producer-supplied record event ID is rejected, not replaced (Decision 0002
// D3), including when it is not the first record and when it is not a string,
// and no record of the request is stamped.
func TestProcessLogsRejectsProducerEventID(t *testing.T) {
	tests := []struct {
		name string
		set  func(pcommon.Map)
	}{
		{"uuidv7 string", func(m pcommon.Map) { m.PutStr(reserved.EventIDAttr, "00000000-0000-7000-8000-000000000000") }},
		{"empty string", func(m pcommon.Map) { m.PutStr(reserved.EventIDAttr, "") }},
		{"not a string", func(m pcommon.Map) { m.PutInt(reserved.EventIDAttr, 7) }},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ld := oneLog(nil, nil)
			recs := ld.ResourceLogs().At(0).ScopeLogs().At(0).LogRecords()
			tc.set(recs.AppendEmpty().Attributes())
			out, err := newTestGateway("").processLogs(authCtx([]string{"prod"}, "", true), ld)
			if !errors.Is(err, errProducerEventID) || !consumererror.IsPermanent(err) {
				t.Fatalf("error = %v, want permanent %v", err, errProducerEventID)
			}
			if strings.Contains(err.Error(), "00000000") {
				t.Fatalf("error %q reflects the producer value", err)
			}
			first := out.ResourceLogs().At(0).ScopeLogs().At(0).LogRecords().At(0)
			if _, ok := first.Attributes().Get(reserved.EventIDAttr); ok {
				t.Fatal("a rejected request left a record with an assigned event ID")
			}
		})
	}
}

func TestProcessLogsFailsWhenIDGenerationFails(t *testing.T) {
	g := newTestGateway("")
	boom := errors.New("entropy exhausted")
	g.newEventID = func() (string, error) { return "", boom }
	_, err := g.processLogs(authCtx([]string{"prod"}, "", true), oneLog(nil, nil))
	// Not permanent: the producer's request was fine, so the receiver may retry.
	if !errors.Is(err, boom) || consumererror.IsPermanent(err) {
		t.Fatalf("error = %v, want a retryable wrapped %v", err, boom)
	}
}

func TestProcessTraces(t *testing.T) {
	build := func(forgeResource, forgeEventID bool) ptrace.Traces {
		td := ptrace.NewTraces()
		rs := td.ResourceSpans().AppendEmpty()
		if forgeResource {
			rs.Resource().Attributes().PutStr(reserved.DatasetAttr, "staging")
		}
		spans := rs.ScopeSpans().AppendEmpty().Spans()
		spans.AppendEmpty().SetName("same")
		last := spans.AppendEmpty()
		last.SetName("same")
		if forgeEventID {
			last.Attributes().PutStr(reserved.EventIDAttr, "00000000-0000-7000-8000-000000000000")
		}
		return td
	}
	tests := []struct {
		name    string
		ctx     context.Context
		forge   bool
		forgeID bool
		wantErr error
	}{
		{"accepted", authCtx([]string{"prod"}, "", true), false, false, nil},
		{"unknown dataset", authCtx([]string{"prod"}, "nope", true), false, false, errUnknownDataset},
		{"mismatching resource dataset", authCtx([]string{"prod"}, "", true), true, false, errDatasetMismatch},
		{"producer span event ID", authCtx([]string{"prod"}, "", true), false, true, errProducerEventID},
		{"no identity", context.Background(), false, false, errNoIdentity},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			out, err := newTestGateway("").processTraces(tc.ctx, build(tc.forge, tc.forgeID))
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
		{errProducerEventID, codes.InvalidArgument},
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

func TestConfigValidate(t *testing.T) {
	tests := []struct {
		name    string
		cfg     Config
		wantErr bool
	}{
		{"no default", Config{}, false},
		{"valid default", Config{DefaultDataset: "prod"}, false},
		{"invalid default", Config{DefaultDataset: "a b"}, true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if err := tc.cfg.Validate(); (err != nil) != tc.wantErr {
				t.Fatalf("Validate() = %v, wantErr %v", err, tc.wantErr)
			}
		})
	}
}
