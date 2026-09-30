package config

import (
	"os"
	"path/filepath"
	"testing"
)

func TestWorkspaceYamlHasPrefix(t *testing.T) {
	cases := []struct {
		name string
		yaml string
		want bool
	}{
		{"flat dotted key", "issue_prefix: hw\nfederation.prefix_home.hw: citadel\n", true},
		{"nested key", "federation:\n  prefix_home:\n    hw: citadel\n", true},
		{"commented out is absent", "# federation.prefix_home.hw: citadel\n", false},
		{"other federation key only", "federation.remote: dolthub://org/beads\n", false},
		{"empty file", "", false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			if err := os.WriteFile(filepath.Join(dir, "config.yaml"), []byte(tc.yaml), 0o600); err != nil {
				t.Fatal(err)
			}
			if got := WorkspaceYamlHasPrefix(dir, "federation.prefix_home."); got != tc.want {
				t.Fatalf("WorkspaceYamlHasPrefix = %v, want %v for:\n%s", got, tc.want, tc.yaml)
			}
		})
	}
	if WorkspaceYamlHasPrefix(filepath.Join(t.TempDir(), "missing"), "federation.prefix_home.") {
		t.Fatal("a workspace without config.yaml must not be scoped")
	}
	if WorkspaceYamlHasPrefix("", "federation.prefix_home.") {
		t.Fatal("an empty workspace must not be scoped")
	}
}
