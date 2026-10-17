// Package toudaiauthextension authenticates producers by an ingestion key sent
// in a request header and publishes the key's trusted scope (workspace, allowed
// datasets, write permission) as client auth data for the gateway processor.
//
// It deliberately owns no routing or attribute injection: it answers "who is
// this key", and toudaigatewayprocessor turns that answer into scope on the
// data. The key itself is never placed in auth data, errors, or metadata.
package toudaiauthextension

import (
	"errors"
	"net/textproto"
)

// Config configures the extension.
type Config struct {
	// KeysFile is the static key file: ingestion key to workspace, datasets, and
	// write permission. It is read once at start; rotation is a file change plus
	// restart until dynamic resolution exists.
	KeysFile string `mapstructure:"keys_file"`
	// KeyHeader is the request header carrying the ingestion key.
	KeyHeader string `mapstructure:"key_header"`
	// DatasetHeader optionally names the dataset a request writes to. Without it
	// a key scoped to exactly one dataset writes there.
	DatasetHeader string `mapstructure:"dataset_header"`
}

const (
	defaultKeyHeader     = "X-Toudai-Key"
	defaultDatasetHeader = "X-Toudai-Dataset"
)

func defaultConfig() *Config {
	return &Config{KeyHeader: defaultKeyHeader, DatasetHeader: defaultDatasetHeader}
}

func (c *Config) Validate() error {
	if c.KeysFile == "" {
		return errors.New("keys_file is required")
	}
	for name, h := range map[string]string{"key_header": c.KeyHeader, "dataset_header": c.DatasetHeader} {
		if h == "" {
			return errors.New(name + " is required")
		}
	}
	// One header cannot carry both values; a shared name would send the key into
	// the dataset field.
	if textproto.CanonicalMIMEHeaderKey(c.KeyHeader) == textproto.CanonicalMIMEHeaderKey(c.DatasetHeader) {
		return errors.New("key_header and dataset_header must differ")
	}
	return nil
}
