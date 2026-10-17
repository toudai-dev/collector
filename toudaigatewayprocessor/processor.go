package toudaigatewayprocessor

import (
	"context"
	"fmt"
	"slices"

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

// Every rejection is permanent: retrying the same request cannot change the
// result. It also carries a gRPC status, because the OTLP receivers map a bare
// permanent error to HTTP 500 / Internal (measured against the pinned receiver),
// which would tell a producer its own bad request was a server fault. Messages
// never include a producer value, so nothing a producer sent can be reflected
// back or logged.
var (
	errNoIdentity       = reject(codes.Internal, "request has no authenticated ingestion key: the receiver must use the toudaiauth authenticator")
	errNoWrite          = reject(codes.PermissionDenied, "ingestion key lacks write permission")
	errDatasetRequired  = reject(codes.InvalidArgument, "dataset is required: the key is scoped to more than one dataset")
	errUnknownDataset   = reject(codes.PermissionDenied, "dataset is not permitted for this ingestion key")
	errReservedResource = reject(codes.InvalidArgument, "resource attribute in the reserved toudai. namespace is not accepted from producers")
)

// rejection is a permanent error that maps to a specific client-visible status.
type rejection struct {
	code codes.Code
	msg  string
}

func reject(code codes.Code, msg string) error {
	return consumererror.NewPermanent(&rejection{code: code, msg: msg})
}

func (r *rejection) Error() string              { return r.msg }
func (r *rejection) GRPCStatus() *status.Status { return status.New(r.code, r.msg) }

// scope is the trusted scope for one request.
type scope struct {
	workspaceID string
	dataset     string
}

type gateway struct {
	// newEventID is replaceable so tests can inject a failing or fixed source.
	newEventID func() (string, error)
}

func newGateway() *gateway {
	return &gateway{newEventID: func() (string, error) {
		// UUIDv7 is time-ordered, so IDs sort by gateway receipt time, and it is
		// random rather than content-derived: two identical events, or a producer
		// retry, always get distinct IDs. google/uuid keeps v7 IDs monotonic
		// within a process.
		id, err := uuid.NewV7()
		if err != nil {
			return "", err
		}
		return id.String(), nil
	}}
}

// requestScope resolves the scope the authenticated key grants for this request.
func requestScope(ctx context.Context) (scope, error) {
	auth := client.FromContext(ctx).Auth
	if auth == nil {
		return scope{}, errNoIdentity
	}
	workspace, _ := auth.GetAttribute(toudaiauthextension.AttrWorkspaceID).(string)
	if workspace == "" {
		return scope{}, errNoIdentity
	}
	// The authenticator already refuses keys without write permission; checking
	// again keeps a misconfigured authenticator from granting it.
	if write, _ := auth.GetAttribute(toudaiauthextension.AttrWrite).(bool); !write {
		return scope{}, errNoWrite
	}
	datasets, _ := auth.GetAttribute(toudaiauthextension.AttrDatasets).([]string)
	requested, _ := auth.GetAttribute(toudaiauthextension.AttrRequestedDataset).(string)
	switch {
	case requested == "" && len(datasets) == 1:
		requested = datasets[0]
	case requested == "":
		return scope{}, errDatasetRequired
	case !slices.Contains(datasets, requested):
		// Covers datasets that do not exist and datasets that exist for another
		// workspace alike; the producer cannot tell them apart.
		return scope{}, errUnknownDataset
	}
	return scope{workspaceID: workspace, dataset: requested}, nil
}

// checkResource rejects a resource that tries to set gateway-owned scope.
// Overwriting would also be safe; rejecting makes a forgery attempt visible to
// the producer and to operators instead of silently succeeding.
func checkResource(attrs pcommon.Map) error {
	var found bool
	attrs.Range(func(k string, _ pcommon.Value) bool {
		found = reserved.Is(k)
		return !found
	})
	if found {
		return errReservedResource
	}
	return nil
}

func (g *gateway) stampResource(attrs pcommon.Map, sc scope) {
	attrs.PutStr(reserved.WorkspaceAttr, sc.workspaceID)
	attrs.PutStr(reserved.DatasetAttr, sc.dataset)
	// Pipeline-composition marker the exporter requires; not authentication.
	attrs.PutStr(reserved.ScopeSourceAttr, reserved.ScopeSourceValue)
}

// stampEvent assigns the record's event_id, replacing anything the producer sent
// under the same key.
func (g *gateway) stampEvent(attrs pcommon.Map) error {
	id, err := g.newEventID()
	if err != nil {
		return fmt.Errorf("assign event_id: %w", err)
	}
	// RemoveIf rather than a plain upsert: an OTLP map can carry duplicate keys,
	// and an upsert would leave a producer-supplied copy behind the assigned one.
	attrs.RemoveIf(func(k string, _ pcommon.Value) bool { return k == reserved.EventIDAttr })
	attrs.PutStr(reserved.EventIDAttr, id)
	return nil
}

func (g *gateway) processLogs(ctx context.Context, ld plog.Logs) (plog.Logs, error) {
	sc, err := requestScope(ctx)
	if err != nil {
		return ld, err
	}
	// Validate every resource before mutating any, so a rejected request is
	// never half-stamped.
	rls := ld.ResourceLogs()
	for i := 0; i < rls.Len(); i++ {
		if err := checkResource(rls.At(i).Resource().Attributes()); err != nil {
			return ld, err
		}
	}
	for i := 0; i < rls.Len(); i++ {
		rl := rls.At(i)
		g.stampResource(rl.Resource().Attributes(), sc)
		for j := 0; j < rl.ScopeLogs().Len(); j++ {
			recs := rl.ScopeLogs().At(j).LogRecords()
			for k := 0; k < recs.Len(); k++ {
				if err := g.stampEvent(recs.At(k).Attributes()); err != nil {
					return ld, err
				}
			}
		}
	}
	return ld, nil
}

func (g *gateway) processTraces(ctx context.Context, td ptrace.Traces) (ptrace.Traces, error) {
	sc, err := requestScope(ctx)
	if err != nil {
		return td, err
	}
	rss := td.ResourceSpans()
	for i := 0; i < rss.Len(); i++ {
		if err := checkResource(rss.At(i).Resource().Attributes()); err != nil {
			return td, err
		}
	}
	for i := 0; i < rss.Len(); i++ {
		rs := rss.At(i)
		g.stampResource(rs.Resource().Attributes(), sc)
		for j := 0; j < rs.ScopeSpans().Len(); j++ {
			spans := rs.ScopeSpans().At(j).Spans()
			for k := 0; k < spans.Len(); k++ {
				if err := g.stampEvent(spans.At(k).Attributes()); err != nil {
					return td, err
				}
			}
		}
	}
	return td, nil
}
