package doctor

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/steveyegge/beads/internal/config"
	"github.com/steveyegge/beads/internal/configfile"
)

// TestForkLineageStoreHasServer pins the predicate behind the fork-lineage
// server fallback: it agrees with the storage-mode predicate SharedStore used
// (an empty dolt_mode on a localhost host is embedded and has no server to
// ask), and it also honors shared-server intent from config.yaml, which the
// storage-mode predicate reads only from the environment (codex round 2,
// gp-0i4o: a YAML shared-server store with no local database directory
// otherwise lost the diagnostic).
func TestForkLineageStoreHasServer(t *testing.T) {
	// No inherited server-mode signal: the cases below set their own.
	t.Setenv("BEADS_DOLT_SERVER_MODE", "")
	t.Setenv("BEADS_DOLT_SHARED_SERVER", "")
	t.Setenv("BEADS_DOLT_SERVER_HOST", "")
	t.Setenv("BEADS_DOLT_HOST", "")
	// No user-global config.yaml (dolt.port, dolt.shared-server) may leak in.
	home := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv("XDG_CONFIG_HOME", filepath.Join(home, "xdg"))
	t.Setenv("BEADS_TEST_IGNORE_REPO_CONFIG", "1")

	cases := []struct {
		name string
		cfg  configfile.Config
		yaml string
		env  map[string]string
		want bool
	}{
		{
			name: "embedded default: empty dolt_mode, localhost, no shared-server intent",
			cfg:  configfile.Config{Backend: configfile.BackendDolt, DoltDatabase: "beads_test"},
			want: false,
		},
		{
			name: "metadata.json dolt_mode=server",
			cfg:  configfile.Config{Backend: configfile.BackendDolt, DoltDatabase: "beads_test", DoltMode: configfile.DoltModeServer},
			want: true,
		},
		{
			name: "BEADS_DOLT_SHARED_SERVER=1 (the storage predicate's own shared-server leg)",
			cfg:  configfile.Config{Backend: configfile.BackendDolt, DoltDatabase: "beads_test"},
			env:  map[string]string{"BEADS_DOLT_SHARED_SERVER": "1"},
			want: true,
		},
		{
			name: "config.yaml dolt.shared-server: true (read by bd's open path, not by the storage predicate)",
			cfg:  configfile.Config{Backend: configfile.BackendDolt, DoltDatabase: "beads_test"},
			yaml: "dolt:\n  shared-server: true\n",
			want: true,
		},
		{
			name: "config.yaml dolt.mode: server",
			cfg:  configfile.Config{Backend: configfile.BackendDolt, DoltDatabase: "beads_test"},
			yaml: "dolt:\n  mode: server\n",
			want: true,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			beadsDir := filepath.Join(t.TempDir(), ".beads")
			if err := os.MkdirAll(beadsDir, 0o755); err != nil {
				t.Fatal(err)
			}
			cfg := tc.cfg
			if err := cfg.Save(beadsDir); err != nil {
				t.Fatalf("save metadata.json: %v", err)
			}
			if tc.yaml != "" {
				if err := os.WriteFile(filepath.Join(beadsDir, "config.yaml"), []byte(tc.yaml), 0o644); err != nil {
					t.Fatalf("write config.yaml: %v", err)
				}
			}
			for k, v := range tc.env {
				t.Setenv(k, v)
			}
			// config.yaml is read through the global viper state, the way
			// bd's own open path reads dolt.shared-server.
			t.Setenv("BEADS_DIR", beadsDir)
			config.ResetForTesting()
			if err := config.Initialize(); err != nil {
				t.Fatalf("config.Initialize: %v", err)
			}
			t.Cleanup(config.ResetForTesting)

			if got := forkLineageStoreHasServer(beadsDir); got != tc.want {
				t.Fatalf("forkLineageStoreHasServer = %v, want %v", got, tc.want)
			}
		})
	}
}
