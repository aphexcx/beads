//go:build cgo

package main

import (
	"bytes"
	"encoding/json"
	"os"
	"strings"
	"testing"
)

func TestVersionCommand(t *testing.T) {
	// Save original stdout
	oldStdout := os.Stdout
	defer func() { os.Stdout = oldStdout }()

	t.Run("plain text version output", func(t *testing.T) {
		// Create a pipe to capture output
		r, w, err := os.Pipe()
		if err != nil {
			t.Fatalf("Failed to create pipe: %v", err)
		}
		os.Stdout = w
		jsonOutput = false

		// Run version command
		if err := versionCmd.RunE(versionCmd, []string{}); err != nil {
			t.Fatalf("versionCmd.RunE: %v", err)
		}

		// Close writer and read output
		w.Close()
		var buf bytes.Buffer
		buf.ReadFrom(r)
		output := buf.String()

		// Verify output contains version info
		if !strings.Contains(output, "bd version") {
			t.Errorf("Expected output to contain 'bd version', got: %s", output)
		}
		if !strings.Contains(output, Version) {
			t.Errorf("Expected output to contain version %s, got: %s", Version, output)
		}
	})

	t.Run("json version output", func(t *testing.T) {
		// Create a pipe to capture output
		r, w, err := os.Pipe()
		if err != nil {
			t.Fatalf("Failed to create pipe: %v", err)
		}
		os.Stdout = w
		jsonOutput = true

		// Run version command
		if err := versionCmd.RunE(versionCmd, []string{}); err != nil {
			t.Fatalf("versionCmd.RunE: %v", err)
		}

		// Close writer and read output
		w.Close()
		var buf bytes.Buffer
		buf.ReadFrom(r)
		output := buf.String()

		// Parse JSON output
		var result map[string]interface{}
		if err := json.Unmarshal([]byte(output), &result); err != nil {
			t.Fatalf("Failed to parse JSON output: %v", err)
		}

		// Verify JSON contains version and build
		if result["version"] != Version {
			t.Errorf("Expected version %s, got %s", Version, result["version"])
		}
		if result["build"] == "" {
			t.Error("Expected build field to be non-empty")
		}
		// cgo field removed — server-only operation, no CGO bifurcation
		if _, ok := result["cgo"]; ok {
			t.Error("cgo field should no longer be present in version output")
		}
	})

	// Restore default
	jsonOutput = false
}

func TestResolveCommitHash(t *testing.T) {
	// Save original Commit value
	origCommit := Commit
	defer func() { Commit = origCommit }()

	t.Run("returns ldflag value when set", func(t *testing.T) {
		testCommit := "abc123def456"
		Commit = testCommit
		result := resolveCommitHash()
		if result != testCommit {
			t.Errorf("Expected %q, got %q", testCommit, result)
		}
	})

	t.Run("returns empty string when not set", func(t *testing.T) {
		Commit = ""
		result := resolveCommitHash()
		// Result could be from git or empty - just verify it doesn't panic
		if result == "" || len(result) >= 7 {
			// Either empty or looks like a git hash
			return
		}
		t.Errorf("Unexpected result format: %q", result)
	})
}

func TestResolveBranch(t *testing.T) {
	// Save original Branch value
	origBranch := Branch
	defer func() { Branch = origBranch }()

	t.Run("returns ldflag value when set", func(t *testing.T) {
		testBranch := "main"
		Branch = testBranch
		result := resolveBranch()
		if result != testBranch {
			t.Errorf("Expected %q, got %q", testBranch, result)
		}
	})

	t.Run("returns empty string or git branch when not set", func(t *testing.T) {
		Branch = ""
		result := resolveBranch()
		// Result could be from git or empty - just verify it doesn't panic
		if result == "" || result == "main" || strings.Contains(result, "detached") {
			return
		}
		t.Logf("Got branch: %q", result)
	})
}

func TestVersionOutputWithCommitAndBranch(t *testing.T) {
	// Save original values
	oldStdout := os.Stdout
	origCommit := Commit
	origBranch := Branch
	defer func() {
		os.Stdout = oldStdout
		Commit = origCommit
		Branch = origBranch
	}()

	t.Run("text output includes commit and branch when available", func(t *testing.T) {
		Commit = "7e709405b38c472d8cbc996c7cd26df7e3b438d0"
		Branch = "main"

		r, w, err := os.Pipe()
		if err != nil {
			t.Fatalf("Failed to create pipe: %v", err)
		}
		os.Stdout = w
		jsonOutput = false

		if err := versionCmd.RunE(versionCmd, []string{}); err != nil {
			t.Fatalf("versionCmd.RunE: %v", err)
		}

		w.Close()
		var buf bytes.Buffer
		buf.ReadFrom(r)
		output := buf.String()

		// Should contain both branch and commit
		if !strings.Contains(output, "main@") {
			t.Errorf("Expected output to contain 'main@', got: %s", output)
		}
		if !strings.Contains(output, "7e70940") { // first 7 chars of commit
			t.Errorf("Expected output to contain commit hash, got: %s", output)
		}
	})

	t.Run("json output includes commit and branch when available", func(t *testing.T) {
		Commit = "7e709405b38c472d8cbc996c7cd26df7e3b438d0"
		Branch = "main"

		r, w, err := os.Pipe()
		if err != nil {
			t.Fatalf("Failed to create pipe: %v", err)
		}
		os.Stdout = w
		jsonOutput = true

		if err := versionCmd.RunE(versionCmd, []string{}); err != nil {
			t.Fatalf("versionCmd.RunE: %v", err)
		}

		w.Close()
		var buf bytes.Buffer
		buf.ReadFrom(r)
		output := buf.String()

		var result map[string]interface{}
		if err := json.Unmarshal([]byte(output), &result); err != nil {
			t.Fatalf("Failed to parse JSON output: %v", err)
		}

		if result["commit"] != Commit {
			t.Errorf("Expected commit %q, got %q", Commit, result["commit"])
		}
		if result["branch"] != Branch {
			t.Errorf("Expected branch %q, got %q", Branch, result["branch"])
		}
	})
}

func TestCommitFromModuleVersion(t *testing.T) {
	tests := []struct {
		name    string
		version string
		want    string
	}{
		{
			name:    "base pseudo-version (no parent tag)",
			version: "v0.0.0-20260702201530-193a87707245",
			want:    "193a87707245",
		},
		{
			name:    "pre-release pseudo-version",
			version: "v1.1.0-rc.2.0.20260702201530-193a87707245",
			want:    "193a87707245",
		},
		{
			name:    "post-release pseudo-version",
			version: "v1.1.1-0.20260702201530-abcdef012345",
			want:    "abcdef012345",
		},
		{
			name:    "pseudo-version with incompatible suffix",
			version: "v2.0.0-20180207000608-abcdef123456+incompatible",
			want:    "abcdef123456",
		},
		{
			name:    "exact release tag has no commit",
			version: "v1.1.0",
			want:    "",
		},
		{
			name:    "plain pre-release tag is not a pseudo-version",
			version: "v1.1.0-rc.1",
			want:    "",
		},
		{
			name:    "devel placeholder from local builds",
			version: "(devel)",
			want:    "",
		},
		{
			name:    "empty version",
			version: "",
			want:    "",
		},
		{
			name:    "pre-release ending in 12 hex chars without timestamp",
			version: "v1.0.0-alpha.abcdef012345",
			want:    "",
		},
		{
			name:    "last segment not 12 hex chars",
			version: "v0.0.0-20260702201530-xyz",
			want:    "",
		},
		{
			name:    "missing v prefix is not a module version",
			version: "foo-20260702201530-abcdef012345",
			want:    "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := commitFromModuleVersion(tt.version); got != tt.want {
				t.Errorf("commitFromModuleVersion(%q) = %q, want %q", tt.version, got, tt.want)
			}
		})
	}
}

func TestVersionJSONPayload(t *testing.T) {
	t.Run("commit key present even when unknown", func(t *testing.T) {
		result := versionJSONPayload("", "")
		commit, ok := result["commit"]
		if !ok {
			t.Fatal("expected 'commit' key to be present when commit is unknown")
		}
		if commit != "" {
			t.Errorf("expected empty commit, got %v", commit)
		}
		if _, ok := result["branch"]; ok {
			t.Error("expected no 'branch' key when branch is empty")
		}
	})

	t.Run("known commit and branch pass through", func(t *testing.T) {
		result := versionJSONPayload("193a87707245", "main")
		if result["commit"] != "193a87707245" {
			t.Errorf("expected commit 193a87707245, got %v", result["commit"])
		}
		if result["branch"] != "main" {
			t.Errorf("expected branch main, got %v", result["branch"])
		}
		if result["version"] != Version {
			t.Errorf("expected version %s, got %v", Version, result["version"])
		}
	})
}

func TestVersionJSONCommitAlwaysPresent(t *testing.T) {
	// Integration-level check through RunE + outputJSON. The deterministic
	// unknown-commit schema guarantee is pinned by TestVersionJSONPayload;
	// here the resolved commit may be non-empty depending on the test
	// binary's build info, so only key presence is asserted.
	oldStdout := os.Stdout
	origCommit := Commit
	defer func() {
		os.Stdout = oldStdout
		Commit = origCommit
		jsonOutput = false
	}()

	Commit = ""

	r, w, err := os.Pipe()
	if err != nil {
		t.Fatalf("Failed to create pipe: %v", err)
	}
	os.Stdout = w
	jsonOutput = true

	if err := versionCmd.RunE(versionCmd, []string{}); err != nil {
		t.Fatalf("versionCmd.RunE: %v", err)
	}

	w.Close()
	var buf bytes.Buffer
	buf.ReadFrom(r)

	var result map[string]interface{}
	if err := json.Unmarshal(buf.Bytes(), &result); err != nil {
		t.Fatalf("Failed to parse JSON output: %v", err)
	}

	commit, ok := result["commit"]
	if !ok {
		t.Fatalf("Expected 'commit' key to always be present in JSON output, got: %s", buf.String())
	}
	if _, isString := commit.(string); !isString {
		t.Errorf("Expected 'commit' to be a string, got %T", commit)
	}
}

func TestVersionFlag(t *testing.T) {
	// Reset global state for test isolation
	ensureCleanGlobalState(t)

	// Ensure cleanup after running cobra commands
	t.Cleanup(func() {
		resetCommandContext()
	})

	// Save original stdout
	oldStdout := os.Stdout
	defer func() { os.Stdout = oldStdout }()

	t.Run("--version flag", func(t *testing.T) {
		// Create a pipe to capture output
		r, w, err := os.Pipe()
		if err != nil {
			t.Fatalf("Failed to create pipe: %v", err)
		}
		os.Stdout = w

		// Set version flag and run root command
		rootCmd.SetArgs([]string{"--version"})
		rootCmd.Execute()

		// Close writer and read output
		w.Close()
		var buf bytes.Buffer
		buf.ReadFrom(r)
		output := buf.String()

		// Verify output contains version info
		if !strings.Contains(output, "bd version") {
			t.Errorf("Expected output to contain 'bd version', got: %s", output)
		}
		if !strings.Contains(output, Version) {
			t.Errorf("Expected output to contain version %s, got: %s", Version, output)
		}

		// Reset args
		rootCmd.SetArgs(nil)
	})

	t.Run("-v shorthand", func(t *testing.T) {
		// Create a pipe to capture output
		r, w, err := os.Pipe()
		if err != nil {
			t.Fatalf("Failed to create pipe: %v", err)
		}
		os.Stdout = w

		// Set version flag and run root command
		rootCmd.SetArgs([]string{"-v"})
		rootCmd.Execute()

		// Close writer and read output
		w.Close()
		var buf bytes.Buffer
		buf.ReadFrom(r)
		output := buf.String()

		// Verify output contains version info
		if !strings.Contains(output, "bd version") {
			t.Errorf("Expected output to contain 'bd version', got: %s", output)
		}
		if !strings.Contains(output, Version) {
			t.Errorf("Expected output to contain version %s, got: %s", Version, output)
		}

		// Reset args
		rootCmd.SetArgs(nil)
	})
}
