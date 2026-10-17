package toudaiauthextension

import (
	"bytes"
	"crypto/sha256"
	"errors"
	"fmt"
	"os"
	"regexp"
	"sort"

	"go.yaml.in/yaml/v3"
)

// minKeyLength rejects keys short enough to be guessed; generated keys are far
// longer.
const minKeyLength = 16

// nameRE bounds workspace and dataset identifiers to a conservative charset so
// they are safe in logs, attributes, and ClickHouse string columns.
var nameRE = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$`)

type keyFile struct {
	Keys map[string]keyEntry `yaml:"keys"`
}

type keyEntry struct {
	WorkspaceID string   `yaml:"workspace_id"`
	Datasets    []string `yaml:"datasets"`
	Write       bool     `yaml:"write"`
}

// scope is the trusted scope a key grants.
type scope struct {
	workspaceID string
	datasets    []string
	write       bool
}

// keyStore maps the SHA-256 of an ingestion key to its scope. Keying on the
// digest keeps raw keys out of long-lived memory and makes lookups independent
// of how many leading bytes a guess shares with a real key.
type keyStore map[[sha256.Size]byte]scope

func (s keyStore) lookup(key string) (scope, bool) {
	sc, ok := s[sha256.Sum256([]byte(key))]
	return sc, ok
}

func loadKeyStore(path string) (keyStore, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read keys_file: %w", err)
	}
	return parseKeyStore(raw)
}

// parseKeyStore never echoes a key in an error: the file is the credential
// store, and errors reach logs.
func parseKeyStore(raw []byte) (keyStore, error) {
	var kf keyFile
	dec := yaml.NewDecoder(bytes.NewReader(raw))
	// Unknown fields would silently drop a misspelled permission, so they fail.
	dec.KnownFields(true)
	if err := dec.Decode(&kf); err != nil {
		return nil, yamlError(err)
	}
	if len(kf.Keys) == 0 {
		return nil, errors.New("keys_file defines no keys")
	}
	// Report entries in a stable order so a bad file fails the same way each run.
	keys := make([]string, 0, len(kf.Keys))
	for k := range kf.Keys {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	store := make(keyStore, len(keys))
	for i, k := range keys {
		e := kf.Keys[k]
		// Entries are identified by position, never by key text.
		if len(k) < minKeyLength {
			return nil, fmt.Errorf("keys_file entry %d: key is shorter than %d characters", i+1, minKeyLength)
		}
		if !nameRE.MatchString(e.WorkspaceID) {
			return nil, fmt.Errorf("keys_file entry %d: workspace_id %q is not a valid identifier", i+1, e.WorkspaceID)
		}
		if len(e.Datasets) == 0 {
			return nil, fmt.Errorf("keys_file entry %d: datasets must list at least one dataset", i+1)
		}
		seen := make(map[string]bool, len(e.Datasets))
		for _, d := range e.Datasets {
			if !nameRE.MatchString(d) {
				return nil, fmt.Errorf("keys_file entry %d: dataset %q is not a valid identifier", i+1, d)
			}
			if seen[d] {
				return nil, fmt.Errorf("keys_file entry %d: dataset %q is listed twice", i+1, d)
			}
			seen[d] = true
		}
		store[sha256.Sum256([]byte(k))] = scope{
			workspaceID: e.WorkspaceID,
			datasets:    append([]string(nil), e.Datasets...),
			write:       e.Write,
		}
	}
	return store, nil
}

var yamlLineRE = regexp.MustCompile(`line (\d+)`)

// yamlError reduces a decoder error to its line number. The decoder echoes
// offending text (a duplicate mapping key is quoted verbatim), and in this file
// that text can be a credential.
func yamlError(err error) error {
	if m := yamlLineRE.FindStringSubmatch(err.Error()); m != nil {
		return fmt.Errorf("parse keys_file: invalid content at line %s", m[1])
	}
	return errors.New("parse keys_file: invalid YAML")
}
