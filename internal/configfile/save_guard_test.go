package configfile

import (
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

// The guards under test protect an existing metadata.json from being
// rewritten by non-hermetic test runs and from silent server→embedded mode
// flips (bd-9oh).

func writeMetadata(t *testing.T, beadsDir, content string) {
	t.Helper()
	if err := os.MkdirAll(beadsDir, 0o750); err != nil {
		t.Fatalf("MkdirAll: %v", err)
	}
	if err := os.WriteFile(filepath.Join(beadsDir, ConfigFileName), []byte(content), 0o600); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}
}

// hermeticEnv marks the test as an isolated run so the test-context tripwire
// does not fire; individual tests then exercise one guard at a time.
func hermeticEnv(t *testing.T) {
	t.Helper()
	t.Setenv("BEADS_TEST_IGNORE_REPO_CONFIG", "1")
	t.Setenv(EnvAllowEmbeddedOverServer, "")
}

// testModeEnv simulates a TestMain-marked test run (BEADS_TEST_MODE set)
// that has NOT declared hermetic isolation — the tripwire's target.
func testModeEnv(t *testing.T) {
	t.Helper()
	t.Setenv("BEADS_TEST_MODE", "1")
	t.Setenv("BEADS_TEST_IGNORE_REPO_CONFIG", "")
}

func TestSaveFreshCreateAllowedWithoutHermeticEnv(t *testing.T) {
	// No pre-existing metadata.json: Save must succeed even from a test
	// context that has not declared hermetic isolation.
	testModeEnv(t)
	beadsDir := t.TempDir()

	cfg := DefaultConfig()
	if err := cfg.Save(beadsDir); err != nil {
		t.Fatalf("Save() on fresh dir failed: %v", err)
	}
}

func TestSaveTestTripwireRefusesRewriteOfExistingMetadata(t *testing.T) {
	testModeEnv(t)
	beadsDir := t.TempDir()
	writeMetadata(t, beadsDir, `{"database":"dolt","backend":"dolt","dolt_mode":"server","dolt_database":"bd"}`)

	cfg := DefaultConfig()
	cfg.Backend = BackendDolt
	cfg.DoltMode = DoltModeServer
	err := cfg.Save(beadsDir)
	if err == nil {
		t.Fatal("Save() rewrote an existing metadata.json from a test context without BEADS_TEST_IGNORE_REPO_CONFIG")
	}
	if !strings.Contains(err.Error(), "test context") {
		t.Errorf("error should explain the test-context refusal, got: %v", err)
	}

	// The tripwire also applies to SaveAllowingModeFlip: sanctioned mode
	// flips do not exempt tests from hermetic isolation.
	if err := cfg.SaveAllowingModeFlip(beadsDir); err == nil {
		t.Fatal("SaveAllowingModeFlip() bypassed the test-context tripwire")
	}
}

func TestSaveTestTripwireAllowsHermeticRuns(t *testing.T) {
	t.Setenv("BEADS_TEST_MODE", "1")
	hermeticEnv(t)
	beadsDir := t.TempDir()
	writeMetadata(t, beadsDir, `{"database":"dolt","backend":"dolt","dolt_mode":"server","dolt_database":"bd"}`)

	cfg, err := Load(beadsDir)
	if err != nil || cfg == nil {
		t.Fatalf("Load: cfg=%v err=%v", cfg, err)
	}
	cfg.DoltDatabase = "bd_renamed"
	if err := cfg.Save(beadsDir); err != nil {
		t.Fatalf("Save() under hermetic env failed: %v", err)
	}
}

func TestSaveRefusesServerToEmbeddedFlip(t *testing.T) {
	hermeticEnv(t)

	for _, existingMode := range []string{DoltModeServer, DoltModeProxiedServer} {
		t.Run(existingMode, func(t *testing.T) {
			beadsDir := t.TempDir()
			writeMetadata(t, beadsDir,
				`{"database":"dolt","backend":"dolt","dolt_mode":"`+existingMode+`","dolt_database":"bd"}`)

			for _, nextMode := range []string{DoltModeEmbedded, ""} {
				cfg := DefaultConfig()
				cfg.Backend = BackendDolt
				cfg.DoltMode = nextMode
				err := cfg.Save(beadsDir)
				if err == nil {
					t.Fatalf("Save() flipped dolt_mode %q → %q without an explicit override", existingMode, nextMode)
				}
				if !strings.Contains(err.Error(), EnvAllowEmbeddedOverServer) {
					t.Errorf("flip refusal should mention the override env var, got: %v", err)
				}
			}

			// The file must be untouched after the refusals.
			loaded, err := Load(beadsDir)
			if err != nil || loaded == nil {
				t.Fatalf("Load after refusal: cfg=%v err=%v", loaded, err)
			}
			if loaded.DoltMode != existingMode {
				t.Errorf("dolt_mode changed despite refusal: %q", loaded.DoltMode)
			}
		})
	}
}

func TestSaveAllowsServerToServerRewrite(t *testing.T) {
	hermeticEnv(t)
	beadsDir := t.TempDir()
	writeMetadata(t, beadsDir, `{"database":"dolt","backend":"dolt","dolt_mode":"server","dolt_database":"bd"}`)

	cfg, err := Load(beadsDir)
	if err != nil || cfg == nil {
		t.Fatalf("Load: cfg=%v err=%v", cfg, err)
	}
	cfg.DoltServerPort = 13307
	if err := cfg.Save(beadsDir); err != nil {
		t.Fatalf("Save() preserving server mode failed: %v", err)
	}
}

func TestSaveAllowsEmbeddedToServerUpgrade(t *testing.T) {
	hermeticEnv(t)
	beadsDir := t.TempDir()
	writeMetadata(t, beadsDir, `{"database":"dolt","backend":"dolt","dolt_mode":"embedded","dolt_database":"bd"}`)

	cfg, err := Load(beadsDir)
	if err != nil || cfg == nil {
		t.Fatalf("Load: cfg=%v err=%v", cfg, err)
	}
	cfg.DoltMode = DoltModeServer
	if err := cfg.Save(beadsDir); err != nil {
		t.Fatalf("Save() upgrading embedded→server failed: %v", err)
	}
}

func TestSaveFlipOverrides(t *testing.T) {
	t.Run("EnvOverride", func(t *testing.T) {
		hermeticEnv(t)
		t.Setenv(EnvAllowEmbeddedOverServer, "1")
		beadsDir := t.TempDir()
		writeMetadata(t, beadsDir, `{"database":"dolt","backend":"dolt","dolt_mode":"server","dolt_database":"bd"}`)

		cfg := DefaultConfig()
		cfg.Backend = BackendDolt
		cfg.DoltMode = DoltModeEmbedded
		if err := cfg.Save(beadsDir); err != nil {
			t.Fatalf("Save() with %s=1 failed: %v", EnvAllowEmbeddedOverServer, err)
		}
	})

	t.Run("SaveAllowingModeFlip", func(t *testing.T) {
		hermeticEnv(t)
		beadsDir := t.TempDir()
		writeMetadata(t, beadsDir, `{"database":"dolt","backend":"dolt","dolt_mode":"server","dolt_database":"bd"}`)

		cfg := DefaultConfig()
		cfg.Backend = BackendDolt
		cfg.DoltMode = DoltModeEmbedded
		if err := cfg.SaveAllowingModeFlip(beadsDir); err != nil {
			t.Fatalf("SaveAllowingModeFlip() failed: %v", err)
		}
		loaded, err := Load(beadsDir)
		if err != nil || loaded == nil {
			t.Fatalf("Load: cfg=%v err=%v", loaded, err)
		}
		if loaded.DoltMode != DoltModeEmbedded {
			t.Errorf("dolt_mode = %q, want %q after sanctioned flip", loaded.DoltMode, DoltModeEmbedded)
		}
	})
}

func TestSaveCorruptExistingMetadataAllowsRepair(t *testing.T) {
	hermeticEnv(t)
	beadsDir := t.TempDir()
	writeMetadata(t, beadsDir, `{not json`)

	cfg := DefaultConfig()
	cfg.Backend = BackendDolt
	cfg.DoltMode = DoltModeEmbedded
	if err := cfg.Save(beadsDir); err != nil {
		t.Fatalf("Save() over corrupt metadata.json should allow repair, got: %v", err)
	}
}

func TestSaveUnreadableExistingMetadataRefuses(t *testing.T) {
	// An existing-but-unreadable metadata.json must refuse the rewrite: the
	// guard cannot tell whether the write would flip a server-mode store
	// (codex review of bd-9oh).
	if runtime.GOOS == "windows" {
		t.Skip("permission-bit semantics differ on Windows")
	}
	if os.Getuid() == 0 {
		t.Skip("running as root bypasses file permissions")
	}
	hermeticEnv(t)
	beadsDir := t.TempDir()
	writeMetadata(t, beadsDir, `{"database":"dolt","backend":"dolt","dolt_mode":"server","dolt_database":"bd"}`)
	configPath := filepath.Join(beadsDir, ConfigFileName)
	if err := os.Chmod(configPath, 0o000); err != nil {
		t.Fatalf("Chmod: %v", err)
	}
	t.Cleanup(func() { _ = os.Chmod(configPath, 0o600) })

	cfg := DefaultConfig()
	cfg.Backend = BackendDolt
	cfg.DoltMode = DoltModeServer
	err := cfg.Save(beadsDir)
	if err == nil {
		t.Fatal("Save() over unreadable existing metadata.json must refuse")
	}
	if !strings.Contains(err.Error(), "cannot read existing file") {
		t.Errorf("error should explain the unreadable-file refusal, got: %v", err)
	}
}
