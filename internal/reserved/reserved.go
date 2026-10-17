// Package reserved holds the gateway-owned attribute names shared by the gateway
// components (auth extension, gateway processor) and the exporter. The gateway
// processor is the only writer of these keys; the exporter is the only reader.
package reserved

import "strings"

const (
	// WorkspaceAttr, DatasetAttr, and ScopeSourceAttr are resource attributes
	// injected by the gateway from the ingestion key's scope.
	WorkspaceAttr   = "toudai.workspace_id"
	DatasetAttr     = "toudai.dataset"
	ScopeSourceAttr = "toudai.scope_source"
	// ScopeSourceValue marks a request that went through the gateway processor.
	ScopeSourceValue = "gateway"

	// EventIDAttr is a log-record or span attribute holding the gateway-assigned
	// event_id (Decision 0001 D5). The exporter consumes it and never stores it
	// as an attribute.
	EventIDAttr = "toudai.event_id"

	// Prefix is the namespace producers may not use.
	Prefix = "toudai."
)

// Is reports whether an attribute key is in the gateway-owned namespace.
func Is(key string) bool { return strings.HasPrefix(key, Prefix) }
