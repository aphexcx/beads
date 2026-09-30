package config

import (
	"os"
	"path/filepath"
	"testing"
)

func writeWorkspaceYaml(t *testing.T, dir, name, content string) {
	t.Helper()
	if err := os.WriteFile(filepath.Join(dir, name), []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
}

func TestWorkspaceEffectiveYamlHasPrefix(t *testing.T) {
	const prefix = "federation.prefix_home."
	cases := []struct {
		name  string
		main  string
		local string
		want  bool
	}{
		{"flat dotted key", "issue_prefix: hw\nfederation.prefix_home.hw: citadel\n", "", true},
		{"nested key", "federation:\n  prefix_home:\n    hw: citadel\n", "", true},
		{"commented out is absent", "# federation.prefix_home.hw: citadel\n", "", false},
		{"other federation key only", "federation.remote: dolthub://org/beads\n", "", false},
		{"empty file", "", "", false},
		{"declared in config.local.yaml alone", "issue_prefix: hw\n", "federation.prefix_home.hw: citadel\n", true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			writeWorkspaceYaml(t, dir, "config.yaml", tc.main)
			if tc.local != "" {
				writeWorkspaceYaml(t, dir, "config.local.yaml", tc.local)
			}
			if got := WorkspaceEffectiveYamlHasPrefix(dir, prefix); got != tc.want {
				t.Fatalf("WorkspaceEffectiveYamlHasPrefix = %v, want %v for:\n%s---\n%s", got, tc.want, tc.main, tc.local)
			}
		})
	}
	if WorkspaceEffectiveYamlHasPrefix(filepath.Join(t.TempDir(), "missing"), prefix) {
		t.Fatal("a workspace without config files must not be scoped")
	}
	if WorkspaceEffectiveYamlHasPrefix("", prefix) {
		t.Fatal("an empty workspace must not be scoped")
	}
}

func TestWorkspaceEffectiveYamlValueLocalOverrides(t *testing.T) {
	dir := t.TempDir()
	writeWorkspaceYaml(t, dir, "config.yaml", "federation.prefix_home.hw: citadel\nfederation.prefix_home.gp: citadel\n")
	writeWorkspaceYaml(t, dir, "config.local.yaml", "federation:\n  prefix_home:\n    hw: jadegate\n")
	if got, ok := WorkspaceEffectiveYamlValue(dir, "federation.prefix_home.hw"); !ok || got != "jadegate" {
		t.Fatalf("hw home = %q, %v; want the config.local.yaml override jadegate", got, ok)
	}
	if got, ok := WorkspaceEffectiveYamlValue(dir, "federation.prefix_home.gp"); !ok || got != "citadel" {
		t.Fatalf("gp home = %q, %v; want config.yaml's citadel", got, ok)
	}
	if _, ok := WorkspaceEffectiveYamlValue(dir, "federation.prefix_home.zz"); ok {
		t.Fatal("an undeclared prefix must not resolve")
	}
}

// TestNodeIDWithoutInitialize pins the fallback a process that never called
// Initialize takes: the environment first, then the user-level files with
// Initialize's precedence (documented over native over legacy).
func TestNodeIDWithoutInitialize(t *testing.T) {
	saved := v
	v = nil
	t.Cleanup(func() { v = saved })
	home := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv("XDG_CONFIG_HOME", filepath.Join(home, ".config"))
	t.Setenv("BEADS_NODE_ID", "")
	t.Setenv("BD_NODE_ID", "")
	candidates := currentUserConfigYamlCandidates()
	for _, path := range []string{candidates.legacy, candidates.native, candidates.documented} {
		if path == "" {
			t.Fatalf("candidate path unresolved: %+v", candidates)
		}
	}
	write := func(path, node string) {
		if err := os.MkdirAll(filepath.Dir(path), 0o750); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, []byte("node_id: "+node+"\n"), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	if got := NodeID(); got != "" {
		t.Fatalf("NodeID with nothing set = %q, want empty", got)
	}
	write(candidates.legacy, "legacy-node")
	if got := NodeID(); got != "legacy-node" {
		t.Fatalf("NodeID from the legacy file = %q, want legacy-node", got)
	}
	if candidates.native != candidates.documented {
		write(candidates.native, "native-node")
		if got := NodeID(); got != "native-node" {
			t.Fatalf("NodeID with native over legacy = %q, want native-node", got)
		}
	}
	write(candidates.documented, "documented-node")
	if got := NodeID(); got != "documented-node" {
		t.Fatalf("NodeID with documented over the rest = %q, want documented-node", got)
	}
	t.Setenv("BD_NODE_ID", "env-node")
	if got := NodeID(); got != "env-node" {
		t.Fatalf("NodeID with the environment set = %q, want env-node", got)
	}
}
