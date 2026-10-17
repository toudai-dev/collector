// Package toudaigatewayprocessor turns the scope authenticated by
// toudaiauthextension into the trusted attributes the toudai exporter requires,
// and assigns every record its event_id. It must run before any component that
// batches or reorders data: scope is per request, and a batch can mix requests.
package toudaigatewayprocessor

// Config has no options: the scope comes from the authenticated key, never from
// processor configuration. That is the point of replacing the static resource
// processor.
type Config struct{}
