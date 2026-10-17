package toudaiauthextension

import (
	"context"
	"errors"
	"strings"

	"go.opentelemetry.io/collector/client"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/extension"
	"go.opentelemetry.io/collector/extension/extensionauth"
	"google.golang.org/grpc/metadata"
)

// Auth data attribute names, read by the gateway processor through
// client.Info.Auth. The ingestion key is intentionally not among them.
const (
	AttrWorkspaceID      = "workspace_id"
	AttrDatasets         = "datasets"
	AttrWrite            = "write"
	AttrRequestedDataset = "requested_dataset"
)

// Errors are fixed strings: HTTP receivers return them to the client verbatim,
// and none may carry the supplied key.
var (
	errMissingKey     = errors.New("missing ingestion key")
	errInvalidKey     = errors.New("invalid ingestion key")
	errMultipleKeys   = errors.New("multiple ingestion keys supplied")
	errNoWrite        = errors.New("ingestion key lacks write permission")
	errMultipleScopes = errors.New("multiple dataset headers supplied")
)

type authExtension struct {
	cfg   *Config
	store keyStore
}

var _ extensionauth.Server = (*authExtension)(nil)

func newExtension(cfg *Config) (*authExtension, error) {
	store, err := loadKeyStore(cfg.KeysFile)
	if err != nil {
		return nil, err
	}
	return &authExtension{cfg: cfg, store: store}, nil
}

func (e *authExtension) Start(context.Context, component.Host) error { return nil }
func (e *authExtension) Shutdown(context.Context) error              { return nil }

// Authenticate resolves the key header to a scope and returns a context whose
// client.Info carries it. A key that is unknown, or lacks write permission,
// fails the request before any pipeline sees data.
func (e *authExtension) Authenticate(ctx context.Context, headers map[string][]string) (context.Context, error) {
	keys := headerValues(headers, e.cfg.KeyHeader)
	switch len(keys) {
	case 0:
		return ctx, errMissingKey
	case 1:
	default:
		return ctx, errMultipleKeys
	}
	key := strings.TrimSpace(keys[0])
	if key == "" {
		return ctx, errMissingKey
	}
	sc, ok := e.store.lookup(key)
	if !ok {
		return ctx, errInvalidKey
	}
	if !sc.write {
		return ctx, errNoWrite
	}
	requested := headerValues(headers, e.cfg.DatasetHeader)
	if len(requested) > 1 {
		return ctx, errMultipleScopes
	}
	data := authData{
		AttrWorkspaceID: sc.workspaceID,
		AttrDatasets:    append([]string(nil), sc.datasets...),
		AttrWrite:       sc.write,
	}
	if len(requested) == 1 {
		data[AttrRequestedDataset] = strings.TrimSpace(requested[0])
	}

	info := client.FromContext(ctx)
	info.Auth = data
	info.Metadata = withoutHeader(info.Metadata, e.cfg.KeyHeader)
	return client.NewContext(withoutIncomingGRPCHeader(ctx, e.cfg.KeyHeader), info), nil
}

// withoutIncomingGRPCHeader removes the key from the raw gRPC metadata too. The
// gRPC receiver authenticates with the request context as is and hands the
// returned context to the handler, so the key stays recoverable with
// metadata.FromIncomingContext (even with include_metadata off) unless it is
// replaced here. HTTP headers are not carried in the context.
func withoutIncomingGRPCHeader(ctx context.Context, name string) context.Context {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return ctx
	}
	// FromIncomingContext returns a private copy, so deleting leaves the
	// receiver's own metadata untouched.
	md.Delete(name)
	return metadata.NewIncomingContext(ctx, md)
}

// withoutHeader drops the key header from receiver metadata (present when the
// receiver sets include_metadata) so no downstream component, such as a
// processor copying metadata into attributes, can store the credential.
func withoutHeader(md client.Metadata, name string) client.Metadata {
	kept := map[string][]string{}
	for k := range md.Keys() {
		if !strings.EqualFold(k, name) {
			kept[k] = md.Get(k)
		}
	}
	return client.NewMetadata(kept)
}

// headerValues does a case-insensitive lookup: HTTP receivers canonicalize
// header names, gRPC lowercases metadata keys.
func headerValues(headers map[string][]string, name string) []string {
	var out []string
	for k, v := range headers {
		if strings.EqualFold(k, name) {
			out = append(out, v...)
		}
	}
	return out
}

// authData is the client.AuthData the extension publishes.
type authData map[string]any

func (a authData) GetAttribute(name string) any {
	v := a[name]
	// Callers must not be able to mutate the shared scope.
	if ds, ok := v.([]string); ok {
		return append([]string(nil), ds...)
	}
	return v
}

func (a authData) GetAttributeNames() []string {
	names := make([]string, 0, len(a))
	for k := range a {
		names = append(names, k)
	}
	return names
}

var _ client.AuthData = authData(nil)

func createExtension(_ context.Context, _ extension.Settings, cfg component.Config) (extension.Extension, error) {
	return newExtension(cfg.(*Config))
}
