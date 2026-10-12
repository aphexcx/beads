package config

import (
	"maps"
	"os"
	"path/filepath"
	"testing"
)

// writeStoreYaml writes one config file of a test store; "-" writes nothing.
func writeStoreYaml(t *testing.T, dir, name, content string) {
	t.Helper()
	if content == "-" {
		return
	}
	if err := os.WriteFile(filepath.Join(dir, name), []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
}

// TestReadWorkspaceFederation covers the two outcomes that are not errors: a
// store that declares no prefix home (the legacy wake) and a store that
// declares one in any yaml form, with config.local.yaml over config.yaml.
func TestReadWorkspaceFederation(t *testing.T) {
	cases := []struct {
		name  string
		main  string
		local string
		want  map[string]string
	}{
		{"no files", "-", "-", nil},
		{"no federation key", "issue_prefix: hw\n", "-", nil},
		{"commented out is absent", "# federation.prefix_home.hw: citadel\n", "-", nil},
		{"other federation key only", "federation.remote: dolthub://org/beads\n", "-", nil},
		{"flat", "federation.prefix_home.hw: citadel\n", "-", map[string]string{"hw": "citadel"}},
		{"nested", "federation:\n  prefix_home:\n    hw: citadel\n", "-", map[string]string{"hw": "citadel"}},
		{"mixed, flat tail under a nested head", "federation:\n  prefix_home.hw: citadel\n", "-", map[string]string{"hw": "citadel"}},
		{"mixed, nested tail under a flat head", "federation.prefix_home:\n  hw: citadel\n", "-", map[string]string{"hw": "citadel"}},
		{"multi-part prefix", "federation.prefix_home.beads-vscode: laptop\n", "-", map[string]string{"beads-vscode": "laptop"}},
		{"declared in config.local.yaml alone", "issue_prefix: hw\n", "federation.prefix_home.hw: citadel\n", map[string]string{"hw": "citadel"}},
		{"config.local.yaml wins per key",
			"federation.prefix_home.hw: citadel\nfederation.prefix_home.gp: citadel\n",
			"federation:\n  prefix_home:\n    hw: jadegate\n",
			map[string]string{"hw": "jadegate", "gp": "citadel"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			writeStoreYaml(t, dir, "config.yaml", tc.main)
			writeStoreYaml(t, dir, "config.local.yaml", tc.local)
			got, err := ReadWorkspaceFederation(dir)
			if err != nil {
				t.Fatalf("ReadWorkspaceFederation: %v", err)
			}
			if len(got.PrefixHomes) != len(tc.want) || !maps.Equal(got.PrefixHomes, tc.want) {
				t.Fatalf("PrefixHomes = %v, want %v", got.PrefixHomes, tc.want)
			}
		})
	}
}

// TestReadWorkspaceFederationFailsClosed covers the third outcome: a store
// whose config cannot be read, parsed or used is an error, never "not
// federated" — the wake skips its sweep instead of waking the rows unscoped.
func TestReadWorkspaceFederationFailsClosed(t *testing.T) {
	cases := []struct{ name, main, local string }{
		{"malformed config.yaml", "federation.prefix_home.hw: [citadel\n", "-"},
		{"malformed config.local.yaml", "federation.prefix_home.hw: citadel\n", "federation: [unclosed\n"},
		{"null home", "federation.prefix_home.hw:\n", "-"},
		{"list home", "federation.prefix_home.hw: [citadel, jadegate]\n", "-"},
		{"scalar parent", "federation.prefix_home: citadel\n", "-"},
		{"empty mapping home", "federation.prefix_home.hw: {}\n", "-"},
		{"empty mapping parent", "federation.prefix_home: {}\n", "-"},
		{"mapping where a node belongs", "federation.prefix_home.hw:\n  node: citadel\n", "-"},
		{"one prefix declared by two yaml paths of one file",
			"federation.prefix_home:\n  hw: citadel\nfederation:\n  prefix_home:\n    hw: jadegate\n", "-"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			writeStoreYaml(t, dir, "config.yaml", tc.main)
			writeStoreYaml(t, dir, "config.local.yaml", tc.local)
			if got, err := ReadWorkspaceFederation(dir); err == nil {
				t.Fatalf("ReadWorkspaceFederation = %+v with no error, want an error", got)
			}
		})
	}
	t.Run("unreadable config.yaml", func(t *testing.T) {
		dir := t.TempDir()
		if err := os.Mkdir(filepath.Join(dir, "config.yaml"), 0o750); err != nil {
			t.Fatal(err)
		}
		if got, err := ReadWorkspaceFederation(dir); err == nil {
			t.Fatalf("ReadWorkspaceFederation = %+v with no error, want a read error", got)
		}
	})
}

// isolateUserConfig points HOME at an empty directory and clears the node
// variables, returning the user-level config candidates under it.
func isolateUserConfig(t *testing.T) userConfigYamlCandidates {
	t.Helper()
	home := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv("XDG_CONFIG_HOME", filepath.Join(home, ".config"))
	t.Setenv("BEADS_NODE_ID", "")
	t.Setenv("BD_NODE_ID", "")
	return currentUserConfigYamlCandidates()
}

func writeNodeID(t *testing.T, path, node string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o750); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte("node_id: "+node+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
}

// TestWorkspaceFederationNodeID pins the wake's own node resolution: the
// user files (legacy, native, documented), then the store's files (local
// over main), then the environment, each over the one before.
func TestWorkspaceFederationNodeID(t *testing.T) {
	candidates := isolateUserConfig(t)
	store := t.TempDir()
	const federated = "federation.prefix_home.hw: citadel\n"
	writeStoreYaml(t, store, "config.yaml", federated)
	node := func() string {
		t.Helper()
		fed, err := ReadWorkspaceFederation(store)
		if err != nil {
			t.Fatalf("ReadWorkspaceFederation: %v", err)
		}
		return fed.NodeID
	}
	expect := func(step, want string) {
		t.Helper()
		if got := node(); got != want {
			t.Fatalf("%s: NodeID = %q, want %q", step, got, want)
		}
	}
	expect("nothing set", "")
	writeNodeID(t, candidates.legacy, "legacy-node")
	expect("legacy user file", "legacy-node")
	if candidates.native != candidates.documented {
		writeNodeID(t, candidates.native, "native-node")
		expect("native over legacy", "native-node")
	}
	writeNodeID(t, candidates.documented, "documented-node")
	expect("documented over the other user files", "documented-node")
	writeStoreYaml(t, store, "config.yaml", federated+"node_id: store-node\n")
	expect("the store's config.yaml over the user files", "store-node")
	writeStoreYaml(t, store, "config.local.yaml", "node_id: local-node\n")
	expect("config.local.yaml over config.yaml", "local-node")
	t.Setenv("BD_NODE_ID", "bd-env-node")
	expect("BD_NODE_ID over the files", "bd-env-node")
	t.Setenv("BEADS_NODE_ID", "beads-env-node")
	expect("BEADS_NODE_ID first", "beads-env-node")
}

// TestNodeIDUninitializedIsUnchanged pins that this change leaves
// config.NodeID alone: in a process that never called Initialize it still
// answers "", whatever the environment and the user files say, so the lease
// reclaim guard behind it behaves exactly as before.
func TestNodeIDUninitializedIsUnchanged(t *testing.T) {
	saved := v
	v = nil
	t.Cleanup(func() { v = saved })
	candidates := isolateUserConfig(t)
	writeNodeID(t, candidates.documented, "documented-node")
	t.Setenv("BEADS_NODE_ID", "env-node")
	if got := NodeID(); got != "" {
		t.Fatalf("NodeID in an uninitialized process = %q, want the base's empty answer", got)
	}
}
