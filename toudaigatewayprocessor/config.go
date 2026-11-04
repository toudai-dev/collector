// Package toudaigatewayprocessor resolves the dataset of each request from the
// permissions toudaiauthextension authenticated, sets it as the toudai.dataset
// resource attribute the toudai exporter requires, and assigns every record its
// event_id. It must run before any component that batches or reorders data: the
// dataset is per request, and a batch can mix requests.
package toudaigatewayprocessor

import (
	"errors"

	"github.com/toudai-dev/collector/internal/reserved"
)

// Config configures the processor.
type Config struct {
	// DefaultDataset is the dataset a request writes to when it names none with
	// the dataset header and its key is not restricted to exactly one dataset
	// (Decision 0002 D3). Empty means there is no default, so such a request is
	// refused. The key's dataset list still applies to the default.
	DefaultDataset string `mapstructure:"default_dataset"`
}

func (c *Config) Validate() error {
	if c.DefaultDataset != "" && !reserved.ValidDataset(c.DefaultDataset) {
		return errors.New("default_dataset is not a valid dataset name")
	}
	return nil
}
