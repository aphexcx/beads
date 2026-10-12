package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

// Regression coverage for gf-pu2 finding 2e (the gf-jr0 vs-integrated gate's
// Major at internal/config/yaml_config.go:944; upstream fixed the same
// matcher in 29aba4f8b, #5760): unsetting a key whose value is a nested
// collection commented out only the key's line and left its contents
// indented under a comment — `bd config unset federation.allowed-remote-patterns`
// corrupted config.yaml when a block list followed a sibling
// federation.remote setting, the command reported success, and the next
// configuration load failed. The whole value subtree now goes with the key,
// and the result is parsed before anything is written.

func TestCommentOutYamlKeyTakesTheWholeBlockValue(t *testing.T) {
	tests := []struct {
		name     string
		content  string
		key      string
		expected string
	}{
		{
			// The gate's example: a block list under a nested key, after a
			// sibling scalar.
			name:     "nested block list after a sibling",
			content:  "federation:\n  remote: origin\n  allowed-remote-patterns:\n    - file:///*\n    - git+https://*\nother: 1\n",
			key:      "federation.allowed-remote-patterns",
			expected: "federation:\n  remote: origin\n  # allowed-remote-patterns:\n  #   - file:///*\n  #   - git+https://*\nother: 1",
		},
		{
			name:     "nested block mapping",
			content:  "dolt:\n  server:\n    host: 127.0.0.1\n    port: 3307\n  mode: server\n",
			key:      "dolt.server",
			expected: "dolt:\n  # server:\n  #   host: 127.0.0.1\n  #   port: 3307\n  mode: server",
		},
		{
			// A list whose items sit at the key's own indentation is the
			// key's value too.
			name:     "flat key with a list at its own indentation",
			content:  "types.custom:\n- step\n- gate\nother: 1\n",
			key:      "types.custom",
			expected: "# types.custom:\n# - step\n# - gate\nother: 1",
		},
		{
			name:     "flat key with an indented block",
			content:  "sync.settings:\n  branch: beads-sync\n  remote: origin\nother: 1\n",
			key:      "sync.settings",
			expected: "# sync.settings:\n#   branch: beads-sync\n#   remote: origin\nother: 1",
		},
		{
			// A block scalar's body is the key's value.
			name:     "nested block scalar",
			content:  "meta:\n  notes: |\n    keep this\n    and this\n  other: 1\n",
			key:      "meta.notes",
			expected: "meta:\n  # notes: |\n  #   keep this\n  #   and this\n  other: 1",
		},
		{
			// A blank line inside the block stays blank; a dedent ends it.
			name:     "blank line inside the block, dedent after it",
			content:  "a:\n  b:\n    x: 1\n\n    y: 2\n  c: 3\nd: 4\n",
			key:      "a.b",
			expected: "a:\n  # b:\n  #   x: 1\n\n  #   y: 2\n  c: 3\nd: 4",
		},
		{
			// A scalar value is still one line, as before.
			name:     "nested scalar is one line",
			content:  "federation:\n  remote: origin\n  identity: citadel\n",
			key:      "federation.remote",
			expected: "federation:\n  # remote: origin\n  identity: citadel",
		},
		{
			// A trailing comment on the key line does not make a scalar a block.
			name:     "scalar with a trailing comment is one line",
			content:  "a:\n  b: 1 # note\n  c: 2\n",
			key:      "a.b",
			expected: "a:\n  # b: 1 # note\n  c: 2",
		},
		{
			// Only a comment after the colon: the value is the block below.
			name:     "comment-only key line opens a block",
			content:  "a:\n  b: # list\n    - x\n  c: 2\n",
			key:      "a.b",
			expected: "a:\n  # b: # list\n  #   - x\n  c: 2",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := commentOutYamlKey(tt.content, tt.key)
			if got != tt.expected {
				t.Errorf("commentOutYamlKey() =\n%q\nwant:\n%q", got, tt.expected)
			}
			var node yaml.Node
			if err := yaml.Unmarshal([]byte(got), &node); err != nil {
				t.Errorf("the result does not parse: %v\n%s", err, got)
			}
		})
	}
}

// TestUnsetYamlConfigLeavesAParseableFileAfterABlockValue runs the gate's
// command shape end to end: the unset of a nested block list after a sibling
// scalar leaves config.yaml parseable, the key reads back unset, and the
// sibling survives.
func TestUnsetYamlConfigLeavesAParseableFileAfterABlockValue(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "config.yaml")
	seed := "federation:\n    remote: origin\n    allowed-remote-patterns:\n        - file:///*\n        - git+https://*\ndolt:\n    port: 3307\n"
	if err := os.WriteFile(path, []byte(seed), 0o600); err != nil {
		t.Fatalf("seed: %v", err)
	}
	t.Setenv("BEADS_DIR", dir)
	if err := UnsetYamlConfig("federation.allowed-remote-patterns"); err != nil {
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
	if got := GetStringFromDir(dir, "federation.allowed-remote-patterns"); got != "" {
		t.Errorf("federation.allowed-remote-patterns still reads back as %q\n%s", got, body)
	}
	if got := GetStringFromDir(dir, "federation.remote"); got != "origin" {
		t.Errorf("the sibling federation.remote = %q, want origin\n%s", got, body)
	}
	if got := GetStringFromDir(dir, "dolt.port"); got != "3307" {
		t.Errorf("the next section's dolt.port = %q, want 3307\n%s", got, body)
	}
	if !strings.Contains(string(body), "    # allowed-remote-patterns:") || !strings.Contains(string(body), "    #     - file:///*") {
		t.Errorf("the key and its list are not preserved as comments:\n%s", body)
	}
}

// TestUnsetYamlConfigRefusesToWriteAnUnparseableResult is the fence behind
// the fix: an unset whose result would not parse is an error and the file is
// left as it was.
func TestUnsetYamlConfigRefusesToWriteAnUnparseableResult(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "config.yaml")
	// Already broken YAML: no unset may report success over it, and it must
	// not be rewritten.
	seed := "a: [unclosed\nbackup.enabled: true\n"
	if err := os.WriteFile(path, []byte(seed), 0o600); err != nil {
		t.Fatalf("seed: %v", err)
	}
	t.Setenv("BEADS_DIR", dir)
	err := UnsetYamlConfig("backup.enabled")
	if err == nil || !strings.Contains(err.Error(), "unparseable") {
		t.Fatalf("UnsetYamlConfig over a result that does not parse = %v, want the refusal", err)
	}
	body, _ := os.ReadFile(path)
	if string(body) != seed {
		t.Fatalf("the file was rewritten despite the refusal:\n%s", body)
	}
}
