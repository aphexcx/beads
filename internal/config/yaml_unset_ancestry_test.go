package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

// Regression coverage for gf-c7n finding 2j (the gf-xid vs-integrated gate's
// Medium at internal/config/yaml_config.go:1019, upstream's nested matcher
// under finding 2e's patch): the matcher allowed an unmatched intermediate
// parent, so with block YAML holding sync.backup.remote BEFORE sync.remote,
// `bd config unset sync.remote` commented out the backup's remote, reset its
// matching state, and left the requested setting live. The result still
// parsed, so the fence behind 2e accepted it. A key is now unset only when
// its complete parent path — every open mapping above it, by indentation —
// equals the requested path's parents.

func TestCommentOutYamlKeyMatchesTheFullParentPath(t *testing.T) {
	tests := []struct {
		name     string
		content  string
		key      string
		expected string
	}{
		{
			// The gate's fixture: a deeper same-named key under an
			// intermediate parent, before the requested one.
			name:     "deeper same-named key before the requested one",
			content:  "sync:\n  backup:\n    remote: backup-origin\n  remote: origin\nother: 1\n",
			key:      "sync.remote",
			expected: "sync:\n  backup:\n    remote: backup-origin\n  # remote: origin\nother: 1",
		},
		{
			name:     "deeper same-named key after the requested one",
			content:  "sync:\n  remote: origin\n  backup:\n    remote: backup-origin\nother: 1\n",
			key:      "sync.remote",
			expected: "sync:\n  # remote: origin\n  backup:\n    remote: backup-origin\nother: 1",
		},
		{
			// The intermediate parent is the one asked for: only its child
			// goes, the sibling at the parent's level stays.
			name:     "the nested key itself",
			content:  "sync:\n  backup:\n    remote: backup-origin\n  remote: origin\nother: 1\n",
			key:      "sync.backup.remote",
			expected: "sync:\n  backup:\n    # remote: backup-origin\n  remote: origin\nother: 1",
		},
		{
			// A same-named leaf under another top-level section is not the
			// requested key, whichever comes first.
			name:     "same leaf under another section first",
			content:  "backup:\n  remote: backup-origin\nsync:\n  remote: origin\n",
			key:      "sync.remote",
			expected: "backup:\n  remote: backup-origin\nsync:\n  # remote: origin",
		},
		{
			// The requested path is absent: a key whose parents only share a
			// suffix of the path is left alone, and nothing is written.
			name:     "only a deeper same-named key exists",
			content:  "sync:\n  backup:\n    remote: backup-origin\nother: 1\n",
			key:      "sync.remote",
			expected: "sync:\n  backup:\n    remote: backup-origin\nother: 1",
		},
		{
			// Three levels: the full ancestry, not just the nearest parent.
			name:     "three levels with a decoy at two",
			content:  "a:\n  c: decoy\n  b:\n    c: target\n",
			key:      "a.b.c",
			expected: "a:\n  c: decoy\n  b:\n    # c: target",
		},
		{
			// A block value under the deeper decoy stays; the requested
			// key's own block value goes with it (finding 2e's rule).
			name:     "block values on both",
			content:  "sync:\n  backup:\n    remote:\n      url: b\n  remote:\n    url: o\nother: 1\n",
			key:      "sync.remote",
			expected: "sync:\n  backup:\n    remote:\n      url: b\n  # remote:\n  #   url: o\nother: 1",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := commentOutYamlKey(tt.content, tt.key)
			if got != tt.expected {
				t.Errorf("commentOutYamlKey(%q) =\n%s\nwant:\n%s", tt.key, got, tt.expected)
			}
			var node yaml.Node
			if err := yaml.Unmarshal([]byte(got), &node); err != nil {
				t.Errorf("the result does not parse: %v\n%s", err, got)
			}
		})
	}
}

// TestUnsetYamlConfigRemovesOnlyTheKeyOnTheRequestedPath runs the gate's
// command shape end to end over a written config.yaml: sync.backup.remote
// before sync.remote, unset sync.remote — the backup's remote still reads
// back, the requested setting does not.
func TestUnsetYamlConfigRemovesOnlyTheKeyOnTheRequestedPath(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "config.yaml")
	seed := "sync:\n    backup:\n        remote: backup-origin\n    remote: origin\ndolt:\n    port: 3307\n"
	if err := os.WriteFile(path, []byte(seed), 0o600); err != nil {
		t.Fatalf("seed: %v", err)
	}
	t.Setenv("BEADS_DIR", dir)
	if err := UnsetYamlConfig("sync.remote"); err != nil {
		t.Fatalf("UnsetYamlConfig: %v", err)
	}
	body, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	var parsed map[string]any
	if err := yaml.Unmarshal(body, &parsed); err != nil {
		t.Fatalf("config.yaml no longer parses after the unset: %v\n%s", err, body)
	}
	if got := GetStringFromDir(dir, "sync.remote"); got != "" {
		t.Errorf("sync.remote still reads back as %q (the requested setting was left live)\n%s", got, body)
	}
	if got := GetStringFromDir(dir, "sync.backup.remote"); got != "backup-origin" {
		t.Errorf("sync.backup.remote = %q, want backup-origin (the wrong setting was removed)\n%s", got, body)
	}
	if got := GetStringFromDir(dir, "dolt.port"); got != "3307" {
		t.Errorf("the next section's dolt.port = %q, want 3307\n%s", got, body)
	}
	if !strings.Contains(string(body), "    # remote: origin") || strings.Contains(string(body), "# remote: backup-origin") {
		t.Errorf("the requested key alone should be commented out:\n%s", body)
	}
}
