// Package reserved holds the gateway-owned attribute names shared by the gateway
// components (auth extension, gateway processor) and the exporter. The gateway
// processor is the only writer of these keys; the exporter is the only reader.
package reserved

import (
	"regexp"
	"strings"
)

const (
	// DatasetAttr is the resource attribute holding the dataset the gateway
	// resolved for the request (Decision 0002 D2/D3). It is the only reserved
	// resource attribute on the wire contract.
	DatasetAttr = "toudai.dataset"

	// EventIDAttr is a log-record or span attribute holding the gateway-assigned
	// event_id (Decision 0001 D5). The exporter consumes it and never stores it
	// as an attribute.
	EventIDAttr = "toudai.event_id"

	// Prefix is the namespace producers may not use, apart from a
	// toudai.dataset resource attribute that matches the resolved dataset.
	Prefix = "toudai."
)

// Is reports whether an attribute key is in the gateway-owned namespace.
func Is(key string) bool { return strings.HasPrefix(key, Prefix) }

// datasetRE bounds dataset names to a conservative charset so they are safe in
// logs, attributes, and ClickHouse string columns. A dataset can come from a
// request header, so the bound applies to producer input, not only to the key
// file and configuration.
var datasetRE = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$`)

// ValidDataset reports whether name is an acceptable dataset name.
func ValidDataset(name string) bool { return datasetRE.MatchString(name) }
