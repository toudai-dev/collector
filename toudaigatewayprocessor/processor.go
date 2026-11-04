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
	errDatasetRequired  = reject(codes.InvalidArgument, "dataset is required: name one with the dataset header")
	errInvalidDataset   = reject(codes.InvalidArgument, "dataset is not a valid dataset name")
	errUnknownDataset   = reject(codes.PermissionDenied, "dataset is not permitted for this ingestion key")
	errDatasetMismatch  = reject(codes.InvalidArgument, "resource attribute toudai.dataset does not match the request's dataset")
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

// requestDataset resolves the dataset of this request (Decision 0002 D3): the
// dataset header, else the key's only dataset, else the configured default. A
// key with a dataset list may write only inside it, whichever source named the
// dataset.
func (g *gateway) requestDataset(ctx context.Context) (string, error) {
	auth := client.FromContext(ctx).Auth
	if auth == nil {
		return "", errNoIdentity
	}
	// Auth data from any other authenticator is a pipeline misconfiguration,
	// not a producer error. Reading its attributes as permissions could grant an
	// unrestricted key, so only toudaiauth's marked data is accepted.
	if auth.GetAttribute(toudaiauthextension.AttrAuthenticator) != toudaiauthextension.AuthenticatorName {
		return "", errNoIdentity
	}
	write, _ := auth.GetAttribute(toudaiauthextension.AttrWrite).(bool)
	// The authenticator already refuses keys without write permission; checking
	// again keeps a misconfigured authenticator from granting it.
	if !write {
		return "", errNoWrite
	}
	datasets, _ := auth.GetAttribute(toudaiauthextension.AttrDatasets).([]string)
	dataset, _ := auth.GetAttribute(toudaiauthextension.AttrRequestedDataset).(string)
	switch {
	case dataset != "":
	case len(datasets) == 1:
		dataset = datasets[0]
	case g.defaultDataset != "":
		dataset = g.defaultDataset
	default:
		return "", errDatasetRequired
	}
	// The header is producer input and becomes a stored column value.
	if !reserved.ValidDataset(dataset) {
		return "", errInvalidDataset
	}
	if datasets != nil && !slices.Contains(datasets, dataset) {
		return "", errUnknownDataset
	}
	return dataset, nil
}

// checkResource rejects a resource that sets a gateway-owned attribute. The one
// exception is a producer toudai.dataset equal to the resolved dataset, which
// Decision 0002 D3 accepts; any other value, or any other reserved key
// (including the retired toudai.workspace_id and toudai.scope_source), fails the
// request. Overwriting would also be safe; rejecting makes a mismatch visible to
// the producer and to operators instead of silently succeeding.
func checkResource(attrs pcommon.Map, dataset string) error {
	var err error
	attrs.Range(func(k string, v pcommon.Value) bool {
		switch {
		case k == reserved.DatasetAttr:
			if v.Type() != pcommon.ValueTypeStr || v.Str() != dataset {
				err = errDatasetMismatch
			}
		case reserved.Is(k):
			err = errReservedResource
		}
		return err == nil
	})
	return err
}

func (g *gateway) stampResource(attrs pcommon.Map, dataset string) {
	// RemoveIf rather than a plain upsert: an OTLP map can carry duplicate keys,
	// and the exporter rejects a resource with a duplicate key.
	attrs.RemoveIf(func(k string, _ pcommon.Value) bool { return k == reserved.DatasetAttr })
	attrs.PutStr(reserved.DatasetAttr, dataset)
}

// checkEvent rejects a record that carries its own event_id. Identity is the
// gateway's alone (Decision 0001 D5, kept by Decision 0002 D3); a producer value
// is refused rather than overwritten so a producer relying on its own IDs finds
// out instead of silently losing them.
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
	dataset, err := g.requestDataset(ctx)
	if err != nil {
		return ld, err
	}
	// Validate every resource and record before mutating any, so a rejected
	// request is never half-stamped.
	rls := ld.ResourceLogs()
	for i := 0; i < rls.Len(); i++ {
		rl := rls.At(i)
		if err := checkResource(rl.Resource().Attributes(), dataset); err != nil {
			return ld, err
		}
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
		g.stampResource(rl.Resource().Attributes(), dataset)
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
	dataset, err := g.requestDataset(ctx)
	if err != nil {
		return td, err
	}
	rss := td.ResourceSpans()
	for i := 0; i < rss.Len(); i++ {
		rs := rss.At(i)
		if err := checkResource(rs.Resource().Attributes(), dataset); err != nil {
			return td, err
		}
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
		g.stampResource(rs.Resource().Attributes(), dataset)
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
