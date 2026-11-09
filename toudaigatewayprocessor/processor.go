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
	errDatasetRequired  = reject(codes.InvalidArgument, "dataset is required: the key does not name a single dataset and no default dataset is configured")
	errInvalidDataset   = reject(codes.InvalidArgument, "dataset is not a valid dataset name")
	errUnknownDataset   = reject(codes.PermissionDenied, "dataset is not permitted for this ingestion key")
	errDatasetMismatch  = reject(codes.InvalidArgument, "resource toudai.dataset does not match the request's dataset")
	errReservedResource = reject(codes.InvalidArgument, "resource attribute in the reserved toudai. namespace is not accepted from producers")
	errProducerEventID  = reject(codes.InvalidArgument, "record attribute toudai.event_id is assigned by the gateway and is not accepted from producers")
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

// scope is the trusted scope for one request: the resolved dataset and the
// key's dataset list (empty when the key permits every dataset).
type scope struct {
	dataset  string
	datasets []string
}

func (sc scope) permits(dataset string) bool {
	return len(sc.datasets) == 0 || slices.Contains(sc.datasets, dataset)
}

type gateway struct {
	defaultDataset string
	// newEventID is replaceable so tests can inject a failing or fixed source.
	newEventID func() (string, error)
}

func newGateway(cfg *Config) *gateway {
	return &gateway{defaultDataset: cfg.DefaultDataset, newEventID: func() (string, error) {
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

// requestScope resolves the request's dataset (Decision 0002 D3): the dataset
// header, else the key's only dataset, else the configured default. Whichever
// source wins, a dataset outside the key's list is refused.
func (g *gateway) requestScope(ctx context.Context) (scope, error) {
	auth := client.FromContext(ctx).Auth
	if auth == nil {
		return scope{}, errNoIdentity
	}
	// Auth data from another authenticator has no write attribute at all; that
	// is a pipeline misconfiguration, not a producer without permission.
	write, ok := auth.GetAttribute(toudaiauthextension.AttrWrite).(bool)
	if !ok {
		return scope{}, errNoIdentity
	}
	// The authenticator already refuses keys without write permission; checking
	// again keeps a misconfigured authenticator from granting it.
	if !write {
		return scope{}, errNoWrite
	}
	// The extension always publishes a typed list, nil when the key permits
	// every dataset. Its absence means another authenticator, and reading that
	// as "every dataset" would fail open.
	datasets, ok := auth.GetAttribute(toudaiauthextension.AttrDatasets).([]string)
	if !ok {
		return scope{}, errNoIdentity
	}
	sc := scope{datasets: datasets}
	sc.dataset, _ = auth.GetAttribute(toudaiauthextension.AttrRequestedDataset).(string)
	switch {
	case sc.dataset != "":
	case len(datasets) == 1:
		sc.dataset = datasets[0]
	case g.defaultDataset != "":
		sc.dataset = g.defaultDataset
	default:
		return scope{}, errDatasetRequired
	}
	// The header is producer input and a key without a list accepts any name,
	// so the name itself must be checked before it becomes a stored value.
	if !reserved.ValidDataset(sc.dataset) {
		return scope{}, errInvalidDataset
	}
	if !sc.permits(sc.dataset) {
		return scope{}, errUnknownDataset
	}
	return sc, nil
}

// checkResource rejects a resource that tries to set gateway-owned values. A
// producer toudai.dataset is accepted only when it equals the resolved dataset
// (Decision 0002 D3); one outside the key's list is refused like a header
// would be. Every other toudai.* key is rejected. Overwriting would also be
// safe; rejecting makes a conflicting or forged value visible to the producer
// and to operators instead of silently succeeding.
func checkResource(attrs pcommon.Map, sc scope) error {
	var err error
	attrs.Range(func(k string, v pcommon.Value) bool {
		switch {
		case !reserved.Is(k):
		case k != reserved.DatasetAttr:
			err = errReservedResource
		case v.Type() != pcommon.ValueTypeStr:
			err = errDatasetMismatch
		case !sc.permits(v.Str()):
			err = errUnknownDataset
		case v.Str() != sc.dataset:
			err = errDatasetMismatch
		}
		return err == nil
	})
	return err
}

func (g *gateway) stampResource(attrs pcommon.Map, sc scope) {
	// RemoveIf rather than a plain upsert: an OTLP map can carry duplicate keys,
	// and a matching producer value may appear more than once on the wire.
	attrs.RemoveIf(func(k string, _ pcommon.Value) bool { return k == reserved.DatasetAttr })
	attrs.PutStr(reserved.DatasetAttr, sc.dataset)
}

// checkEvent rejects a record or span that carries its own event_id. Identity
// is the gateway's alone, and Decision 0002 D3 rejects a producer value rather
// than overwriting it, so a producer relying on its own IDs finds out instead
// of silently losing them. Other toudai.* record keys are left for the
// exporter to drop and count.
func checkEvent(attrs pcommon.Map) error {
	var found bool
	attrs.Range(func(k string, _ pcommon.Value) bool {
		found = k == reserved.EventIDAttr
		return !found
	})
	if found {
		return errProducerEventID
	}
	return nil
}

// stampEvent assigns the record's event_id. checkEvent has already refused any
// producer copy, so a plain put cannot leave a duplicate behind.
func (g *gateway) stampEvent(attrs pcommon.Map) error {
	id, err := g.newEventID()
	if err != nil {
		return fmt.Errorf("assign event_id: %w", err)
	}
	attrs.PutStr(reserved.EventIDAttr, id)
	return nil
}

func (g *gateway) processLogs(ctx context.Context, ld plog.Logs) (plog.Logs, error) {
	sc, err := g.requestScope(ctx)
	if err != nil {
		return ld, err
	}
	// Validate every resource and record before mutating any, so a rejected
	// request is never half-stamped. Resources go first so a dataset the key
	// does not permit is a 403 wherever it sits in the request.
	rls := ld.ResourceLogs()
	for i := 0; i < rls.Len(); i++ {
		if err := checkResource(rls.At(i).Resource().Attributes(), sc); err != nil {
			return ld, err
		}
	}
	for i := 0; i < rls.Len(); i++ {
		rl := rls.At(i)
		for j := 0; j < rl.ScopeLogs().Len(); j++ {
			recs := rl.ScopeLogs().At(j).LogRecords()
			for k := 0; k < recs.Len(); k++ {
				if err := checkEvent(recs.At(k).Attributes()); err != nil {
					return ld, err
				}
			}
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
	sc, err := g.requestScope(ctx)
	if err != nil {
		return td, err
	}
	rss := td.ResourceSpans()
	for i := 0; i < rss.Len(); i++ {
		if err := checkResource(rss.At(i).Resource().Attributes(), sc); err != nil {
			return td, err
		}
	}
	for i := 0; i < rss.Len(); i++ {
		rs := rss.At(i)
		for j := 0; j < rs.ScopeSpans().Len(); j++ {
			spans := rs.ScopeSpans().At(j).Spans()
			for k := 0; k < spans.Len(); k++ {
				if err := checkEvent(spans.At(k).Attributes()); err != nil {
					return td, err
				}
			}
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
