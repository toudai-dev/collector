package toudaiauthextension

import (
	"context"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"go.opentelemetry.io/collector/client"
	"google.golang.org/grpc/metadata"
)

const (
	keyA     = "tk_test_workspace_a_0123456789"
	keyB     = "tk_test_workspace_b_0123456789"
	keyRO    = "tk_test_read_only_0123456789"
	wsA      = "11111111-1111-4111-8111-111111111111"
	wsB      = "22222222-2222-4222-8222-222222222222"
	keysYAML = `
keys:
  ` + keyA + `:
    workspace_id: ` + wsA + `
    datasets: [prod, staging]
    write: true
  ` + keyB + `:
    workspace_id: ` + wsB + `
    datasets: [prod]
    write: true
  ` + keyRO + `:
    workspace_id: ` + wsA + `
    datasets: [prod]
    write: false
`
)

func newTestExtension(t *testing.T) *authExtension {
	t.Helper()
	path := filepath.Join(t.TempDir(), "keys.yaml")
	if err := os.WriteFile(path, []byte(keysYAML), 0o600); err != nil {
		t.Fatal(err)
	}
	ext, err := newExtension(&Config{KeysFile: path, KeyHeader: "X-Toudai-Key", DatasetHeader: "X-Toudai-Dataset"})
	if err != nil {
		t.Fatal(err)
	}
	return ext
}

func TestAuthenticate(t *testing.T) {
	tests := []struct {
		name        string
		headers     map[string][]string
		wantErr     error
		wantWS      string
		wantDataset string
	}{
		{"accepted", map[string][]string{"X-Toudai-Key": {keyA}}, nil, wsA, ""},
		{"header names are case-insensitive", map[string][]string{"x-toudai-key": {keyB}, "X-TOUDAI-DATASET": {"prod"}}, nil, wsB, "prod"},
		{"surrounding whitespace is trimmed", map[string][]string{"X-Toudai-Key": {"  " + keyA + " "}}, nil, wsA, ""},
		{"requested dataset is passed through unchecked", map[string][]string{"X-Toudai-Key": {keyA}, "X-Toudai-Dataset": {"nope"}}, nil, wsA, "nope"},
		{"missing key", map[string][]string{"X-Toudai-Dataset": {"prod"}}, errMissingKey, "", ""},
		{"no headers", nil, errMissingKey, "", ""},
		{"empty key", map[string][]string{"X-Toudai-Key": {"  "}}, errMissingKey, "", ""},
		{"unknown key", map[string][]string{"X-Toudai-Key": {"tk_test_not_a_real_key_000"}}, errInvalidKey, "", ""},
		{"key is a prefix of a real key", map[string][]string{"X-Toudai-Key": {keyA[:len(keyA)-1]}}, errInvalidKey, "", ""},
		{"authorization header is not the key", map[string][]string{"Authorization": {"Bearer " + keyA}}, errMissingKey, "", ""},
		{"read-only key", map[string][]string{"X-Toudai-Key": {keyRO}}, errNoWrite, "", ""},
		{"two key headers", map[string][]string{"X-Toudai-Key": {keyA, keyB}}, errMultipleKeys, "", ""},
		{"two dataset headers", map[string][]string{"X-Toudai-Key": {keyA}, "X-Toudai-Dataset": {"prod", "staging"}}, errMultipleScopes, "", ""},
	}
	ext := newTestExtension(t)
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctx, err := ext.Authenticate(context.Background(), tc.headers)
			if err != tc.wantErr {
				t.Fatalf("error = %v, want %v", err, tc.wantErr)
			}
			if err != nil {
				for _, v := range tc.headers {
					for _, s := range v {
						if k := strings.TrimSpace(s); k != "" && strings.Contains(err.Error(), k) {
							t.Fatalf("error %q reflects a supplied header value", err)
						}
					}
				}
				if client.FromContext(ctx).Auth != nil {
					t.Fatal("a failed authentication must not attach auth data")
				}
				return
			}
			auth := client.FromContext(ctx).Auth
			if got := auth.GetAttribute(AttrWorkspaceID); got != tc.wantWS {
				t.Fatalf("workspace = %v, want %s", got, tc.wantWS)
			}
			if got, _ := auth.GetAttribute(AttrRequestedDataset).(string); got != tc.wantDataset {
				t.Fatalf("requested dataset = %q, want %q", got, tc.wantDataset)
			}
			if auth.GetAttribute(AttrWrite) != true {
				t.Fatal("write permission missing from auth data")
			}
			// The credential must be unreachable through the auth data.
			for _, name := range auth.GetAttributeNames() {
				if strings.Contains(strings.Join(flatten(auth.GetAttribute(name)), ","), "tk_test") {
					t.Fatalf("auth attribute %q exposes the ingestion key", name)
				}
			}
		})
	}
}

func flatten(v any) []string {
	switch x := v.(type) {
	case string:
		return []string{x}
	case []string:
		return x
	}
	return nil
}

// The key must be unreachable after authentication through every API a
// downstream component could use: the Collector's client metadata (populated
// only with include_metadata) and, for gRPC, the raw incoming metadata (always
// populated). Other headers survive, and the caller's context is untouched.
func TestAuthenticateStripsKeyFromMetadata(t *testing.T) {
	ext := newTestExtension(t)
	tests := []struct {
		name            string
		includeMetadata bool
		grpc            bool
	}{
		{"http without include_metadata", false, false},
		{"http with include_metadata", true, false},
		{"grpc without include_metadata", false, true},
		{"grpc with include_metadata", true, true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			base := context.Background()
			if tc.includeMetadata {
				base = client.NewContext(base, client.Info{Metadata: client.NewMetadata(map[string][]string{
					"X-Toudai-Key":     {keyA},
					"X-Toudai-Dataset": {"prod"},
					"User-Agent":       {"sdk/1"},
				})})
			}
			if tc.grpc {
				base = metadata.NewIncomingContext(base, metadata.Pairs("x-toudai-key", keyA, "user-agent", "sdk/1"))
			}
			ctx, err := ext.Authenticate(base, map[string][]string{"X-Toudai-Key": {keyA}})
			if err != nil {
				t.Fatal(err)
			}
			md := client.FromContext(ctx).Metadata
			if got := md.Get("x-toudai-key"); len(got) != 0 {
				t.Fatalf("client metadata still carries the key: %v", got)
			}
			if tc.includeMetadata {
				if got := md.Get("user-agent"); !slices.Equal(got, []string{"sdk/1"}) {
					t.Fatalf("unrelated client metadata = %v, want it preserved", got)
				}
			}
			if tc.grpc {
				in, ok := metadata.FromIncomingContext(ctx)
				if !ok {
					t.Fatal("incoming gRPC metadata disappeared")
				}
				if got := in.Get("x-toudai-key"); len(got) != 0 {
					t.Fatalf("incoming gRPC metadata still carries the key: %v", got)
				}
				if got := in.Get("user-agent"); !slices.Equal(got, []string{"sdk/1"}) {
					t.Fatalf("unrelated gRPC metadata = %v, want it preserved", got)
				}
				// The receiver's own map must not have been mutated.
				orig, _ := metadata.FromIncomingContext(base)
				if got := orig.Get("x-toudai-key"); len(got) == 0 {
					t.Fatal("authentication mutated the incoming context's metadata")
				}
			}
			if tc.includeMetadata {
				if got := client.FromContext(base).Metadata.Get("x-toudai-key"); len(got) == 0 {
					t.Fatal("authentication mutated the incoming client metadata")
				}
			}
		})
	}
}

func TestAuthDataDatasetsAreCopies(t *testing.T) {
	ext := newTestExtension(t)
	ctx, err := ext.Authenticate(context.Background(), map[string][]string{"X-Toudai-Key": {keyA}})
	if err != nil {
		t.Fatal(err)
	}
	auth := client.FromContext(ctx).Auth
	auth.GetAttribute(AttrDatasets).([]string)[0] = "tampered"
	if got := auth.GetAttribute(AttrDatasets).([]string); got[0] != "prod" {
		t.Fatalf("datasets = %v, a caller mutated shared scope", got)
	}
}

func TestParseKeyStore(t *testing.T) {
	const ok = `
keys:
  tk_test_valid_key_0123456789:
    workspace_id: ws-1
    datasets: [a]
    write: true
`
	tests := []struct {
		name    string
		file    string
		wantErr string
	}{
		{"valid", ok, ""},
		{"two keys for one workspace", ok + `
  tk_test_second_key_0123456789:
    workspace_id: ws-1
    datasets: [a, b]
    write: true
`, ""},
		{"no keys", "keys: {}\n", "defines no keys"},
		{"short key", strings.Replace(ok, "tk_test_valid_key_0123456789", "short", 1), "shorter than"},
		{"missing workspace", strings.Replace(ok, "workspace_id: ws-1", "workspace_id: ''", 1), "workspace_id"},
		{"workspace with spaces", strings.Replace(ok, "ws-1", "ws 1", 1), "workspace_id"},
		{"no datasets", strings.Replace(ok, "datasets: [a]", "datasets: []", 1), "at least one dataset"},
		{"bad dataset", strings.Replace(ok, "[a]", "['a b']", 1), "not a valid identifier"},
		{"duplicate dataset", strings.Replace(ok, "[a]", "[a, a]", 1), "listed twice"},
		{"misspelled field", strings.Replace(ok, "write:", "writes:", 1), "invalid content at line"},
		{"duplicate key", ok + strings.TrimPrefix(ok, "\nkeys:\n"), "invalid content at line"},
		{"not yaml", "keys: [", "parse keys_file"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := parseKeyStore([]byte(tc.file))
			switch {
			case tc.wantErr == "" && err != nil:
				t.Fatalf("unexpected error: %v", err)
			case tc.wantErr != "" && (err == nil || !strings.Contains(err.Error(), tc.wantErr)):
				t.Fatalf("error = %v, want it to contain %q", err, tc.wantErr)
			}
			// The key file is a credential store; no error may echo a key.
			if err != nil && strings.Contains(err.Error(), "tk_test_") {
				t.Fatalf("error %q echoes a key", err)
			}
		})
	}
}

func TestConfigValidate(t *testing.T) {
	tests := []struct {
		name    string
		cfg     Config
		wantErr bool
	}{
		{"defaults plus file", Config{KeysFile: "k", KeyHeader: "X-Toudai-Key", DatasetHeader: "X-Toudai-Dataset"}, false},
		{"no file", Config{KeyHeader: "X-Toudai-Key", DatasetHeader: "X-Toudai-Dataset"}, true},
		{"no key header", Config{KeysFile: "k", DatasetHeader: "X-Toudai-Dataset"}, true},
		{"same header for both", Config{KeysFile: "k", KeyHeader: "x-toudai-key", DatasetHeader: "X-Toudai-Key"}, true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if err := tc.cfg.Validate(); (err != nil) != tc.wantErr {
				t.Fatalf("Validate() = %v, wantErr %v", err, tc.wantErr)
			}
		})
	}
}
