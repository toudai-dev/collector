// Package toudaigatewayprocessor turns the permissions authenticated by
// toudaiauthextension into the trusted toudai.dataset attribute the toudai
// exporter requires, and assigns every record its event_id. It must run before any component that
// batches or reorders data: scope is per request, and a batch can mix requests.
package toudaigatewayprocessor

import (
	"errors"

	"github.com/toudai-dev/collector/internal/reserved"
)

// Config configures the processor.
type Config struct {
	// DefaultDataset is the dataset of a request that names none, when the key
	// does not permit exactly one (Decision 0002 D3). It is still checked
	// against the key's dataset list. Empty means no default: such a request
	// is refused.
	DefaultDataset string `mapstructure:"default_dataset"`
}

func (c *Config) Validate() error {
	if c.DefaultDataset != "" && !reserved.ValidDataset(c.DefaultDataset) {
		return errors.New("default_dataset is not a valid dataset name")
	}
	return nil
}
