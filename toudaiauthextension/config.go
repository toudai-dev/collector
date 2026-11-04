// Package toudaiauthextension authenticates producers by an ingestion key sent
// in a request header and publishes what the key permits (write permission and
// an optional dataset list) as client auth data for the gateway processor.
//
// A key maps to permissions, never to a customer or workspace: one deployment
// serves one customer (Decision 0002 D3). The extension owns no attribute
// injection; toudaigatewayprocessor resolves the request's dataset from these
// permissions. The key itself is never placed in auth data, errors, or metadata.
package toudaiauthextension

import (
	"errors"
	"net/textproto"
)

// Config configures the extension.
type Config struct {
	// KeysFile is the static key file: ingestion key to write permission and an
	// optional dataset list. It is read once at start; rotation is a file change plus
	// restart until dynamic resolution exists.
	KeysFile string `mapstructure:"keys_file"`
	// KeyHeader is the request header carrying the ingestion key.
	KeyHeader string `mapstructure:"key_header"`
	// DatasetHeader optionally names the dataset a request writes to. Without it
	// the gateway processor falls back to the key's only dataset, then to its
	// configured default.
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
