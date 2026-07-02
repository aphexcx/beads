package main

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

// TestRunPrepareCommitMsgHook_TrailerBlockContiguous reproduces the
// houmanoids_www/jack report (hq-wisp-9zjiv): when a commit message already
// contains other trailers (Codex-Reviewed-By, Co-Authored-By, etc.), the
// hook's appended Executed-By: must end up in the SAME contiguous trailer
// block, not separated by a blank line. `git interpret-trailers --parse`
// only recognizes the final contiguous block, so a blank-line separation
// invalidates every preceding trailer for any CI gate that checks via
// `--parse` (Codex Review Gate, Signed-off-by enforcement, etc.).
func TestRunPrepareCommitMsgHook_TrailerBlockContiguous(t *testing.T) {
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git not available")
	}

	tmp := t.TempDir()
	msgFile := filepath.Join(tmp, "COMMIT_EDITMSG")
	original := `fix(linear): some change

Body paragraph explaining what changed.

Codex-Reviewed-By: codex-rescue (clean: no blockers)
Co-Authored-By: Claude Opus 4.7 <noreply@anthropic.com>
`
	if err := os.WriteFile(msgFile, []byte(original), 0600); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}

	t.Setenv("BD_ACTOR", "houmanoids_www/crew/jack")

	if rc := runPrepareCommitMsgHook([]string{msgFile}); rc != 0 {
		t.Fatalf("hook returned %d, want 0", rc)
	}

	out, err := exec.Command("git", "interpret-trailers", "--parse", msgFile).Output()
	if err != nil {
		t.Fatalf("git interpret-trailers --parse: %v", err)
	}
	parsed := string(out)

	// All three trailers must surface — a blank-line separation would have
	// caused git to recognize ONLY the last (Executed-By:) and treat the
	// other two as body text.
	wantTrailers := []string{
		"Codex-Reviewed-By: codex-rescue",
		"Co-Authored-By: Claude Opus 4.7",
		"Executed-By: houmanoids_www/crew/jack",
	}
	for _, want := range wantTrailers {
		if !strings.Contains(parsed, want) {
			t.Errorf("interpret-trailers --parse missing %q\nactual output:\n%s", want, parsed)
		}
	}
}

// TestRunPrepareCommitMsgHook_NoExistingTrailers verifies the hook still
// works when the commit has no prior trailers — Executed-By becomes the
// first (and only) trailer.
func TestRunPrepareCommitMsgHook_NoExistingTrailers(t *testing.T) {
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git not available")
	}

	tmp := t.TempDir()
	msgFile := filepath.Join(tmp, "COMMIT_EDITMSG")
	if err := os.WriteFile(msgFile, []byte("fix: simple commit\n"), 0600); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}

	t.Setenv("BD_ACTOR", "test/agent")

	if rc := runPrepareCommitMsgHook([]string{msgFile}); rc != 0 {
		t.Fatalf("hook returned %d, want 0", rc)
	}

	out, err := exec.Command("git", "interpret-trailers", "--parse", msgFile).Output()
	if err != nil {
		t.Fatalf("git interpret-trailers --parse: %v", err)
	}
	if !strings.Contains(string(out), "Executed-By: test/agent") {
		t.Errorf("Executed-By trailer not found in parsed output:\n%s", string(out))
	}
}

// TestRunPrepareCommitMsgHook_AlreadyPresent verifies the dup-guard via
// `git interpret-trailers --if-exists=doNothing`: re-running the hook on a
// message that already has Executed-By must NOT add a duplicate trailer.
// We assert via --parse rather than byte equality because git
// interpret-trailers may canonicalize trailing whitespace.
func TestRunPrepareCommitMsgHook_AlreadyPresent(t *testing.T) {
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git not available")
	}

	tmp := t.TempDir()
	msgFile := filepath.Join(tmp, "COMMIT_EDITMSG")
	original := "fix: x\n\nExecuted-By: test/agent\n"
	if err := os.WriteFile(msgFile, []byte(original), 0600); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}

	t.Setenv("BD_ACTOR", "test/agent")

	if rc := runPrepareCommitMsgHook([]string{msgFile}); rc != 0 {
		t.Fatalf("hook returned %d, want 0", rc)
	}

	out, err := exec.Command("git", "interpret-trailers", "--parse", msgFile).Output()
	if err != nil {
		t.Fatalf("interpret-trailers --parse: %v", err)
	}
	count := strings.Count(string(out), "Executed-By:")
	if count != 1 {
		t.Errorf("Executed-By: appears %d times, want exactly 1\nparsed output:\n%s", count, string(out))
	}
}

// TestRunPrepareCommitMsgHook_RejectsNewlineInActor verifies that a
// BD_ACTOR containing \r or \n is refused (defense against
// trailer-injection via env).
func TestRunPrepareCommitMsgHook_RejectsNewlineInActor(t *testing.T) {
	tmp := t.TempDir()
	msgFile := filepath.Join(tmp, "COMMIT_EDITMSG")
	original := "fix: x\n"
	if err := os.WriteFile(msgFile, []byte(original), 0600); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}

	t.Setenv("BD_ACTOR", "test\nFake-Trailer: injected")

	if rc := runPrepareCommitMsgHook([]string{msgFile}); rc != 0 {
		t.Fatalf("hook returned %d, want 0", rc)
	}

	got, err := os.ReadFile(msgFile)
	if err != nil {
		t.Fatalf("ReadFile: %v", err)
	}
	if string(got) != original {
		t.Errorf("hook modified message despite invalid actor:\nbefore: %q\nafter:  %q", original, string(got))
	}
}

// TestRunPrepareCommitMsgHook_TrailingNewline verifies the message ends with
// a newline after the hook runs (matches git convention).
func TestRunPrepareCommitMsgHook_TrailingNewline(t *testing.T) {
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git not available")
	}

	tmp := t.TempDir()
	msgFile := filepath.Join(tmp, "COMMIT_EDITMSG")
	if err := os.WriteFile(msgFile, []byte("fix: x\n"), 0600); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}

	t.Setenv("BD_ACTOR", "test/agent")

	if rc := runPrepareCommitMsgHook([]string{msgFile}); rc != 0 {
		t.Fatalf("hook returned %d, want 0", rc)
	}

	got, err := os.ReadFile(msgFile)
	if err != nil {
		t.Fatalf("ReadFile: %v", err)
	}
	if !strings.HasSuffix(string(got), "\n") {
		t.Errorf("message should end with newline, got %q", string(got))
	}
}
