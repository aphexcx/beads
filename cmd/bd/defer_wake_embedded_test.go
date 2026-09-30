//go:build cgo

package main

import (
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

// showDeferState returns (status, defer_until) for an issue via bd show --json.
func showDeferState(t *testing.T, bd, dir, id string) (string, interface{}) {
	t.Helper()
	cmd := exec.Command(bd, "show", id, "--json")
	cmd.Dir = dir
	cmd.Env = bdEnv(dir)
	stdout, stderr, err := runCommandBuffers(t, cmd)
	if err != nil {
		t.Fatalf("bd show %s --json failed: %v\nstdout:\n%s\nstderr:\n%s", id, err, stdout.String(), stderr.String())
	}
	s := strings.TrimSpace(stdout.String())
	start := strings.IndexAny(s, "[{")
	if start < 0 {
		t.Fatalf("no JSON in show output: %s", s)
	}
	var m map[string]interface{}
	if s[start] == '[' {
		var arr []map[string]interface{}
		if err := json.Unmarshal([]byte(s[start:]), &arr); err != nil {
			t.Fatalf("parse show JSON array: %v\n%s", err, s)
		}
		if len(arr) == 0 {
			t.Fatalf("empty JSON array in show output")
		}
		m = arr[0]
	} else {
		if err := json.Unmarshal([]byte(s[start:]), &m); err != nil {
			t.Fatalf("parse show JSON: %v\n%s", err, s)
		}
	}
	status, _ := m["status"].(string)
	return status, m["defer_until"]
}

// wakeReadyIDs runs bd ready --json (plus extra args) and returns the listed ids.
func wakeReadyIDs(t *testing.T, bd, dir string, args ...string) map[string]bool {
	t.Helper()
	ids, _ := wakeReadyIDsStderr(t, bd, dir, args...)
	return ids
}

// wakeReadyIDsStderr is wakeReadyIDs plus the run's stderr, where the wake
// sweep reports the rows its owner scope declined.
func wakeReadyIDsStderr(t *testing.T, bd, dir string, args ...string) (map[string]bool, string) {
	t.Helper()
	fullArgs := append([]string{"ready", "--json"}, args...)
	cmd := exec.Command(bd, fullArgs...)
	cmd.Dir = dir
	cmd.Env = bdEnv(dir)
	stdout, stderr, err := runCommandBuffers(t, cmd)
	if err != nil {
		t.Fatalf("bd ready --json failed: %v\nstdout:\n%s\nstderr:\n%s", err, stdout.String(), stderr.String())
	}
	s := strings.TrimSpace(stdout.String())
	start := strings.Index(s, "[")
	if start < 0 {
		return map[string]bool{}, stderr.String()
	}
	var arr []map[string]interface{}
	if err := json.Unmarshal([]byte(s[start:]), &arr); err != nil {
		t.Fatalf("parse ready JSON: %v\n%s", err, s)
	}
	ids := map[string]bool{}
	for _, row := range arr {
		if id, ok := row["id"].(string); ok {
			ids[id] = true
		}
	}
	return ids, stderr.String()
}

// TestEmbeddedDeferAutoWake documents the defer contract: a DATED defer is a
// snooze — once defer_until passes, the next ready-front read returns the
// issue to open (status open, defer_until cleared, same shape bd undefer
// writes). A DATELESS defer is the indefinite icebox and never auto-wakes.
func TestEmbeddedDeferAutoWake(t *testing.T) {
	if os.Getenv("BEADS_TEST_EMBEDDED_DOLT") != "1" {
		t.Skip("set BEADS_TEST_EMBEDDED_DOLT=1 to run embedded dolt integration tests")
	}
	t.Parallel()

	bd := buildEmbeddedBD(t)
	dir, _, _ := bdInit(t, bd, "--prefix", "dw")

	t.Run("expired_dated_defer_wakes_on_ready", func(t *testing.T) {
		issue := bdCreate(t, bd, dir, "Expired snooze", "--type", "task")
		// defer.go warns about a past date but proceeds: status=deferred with
		// an already-expired defer_until — exactly the tomb state a defer that
		// expired while nobody was looking leaves behind.
		bdDefer(t, bd, dir, issue.ID, "--until", "2020-01-01")
		status, _ := showDeferState(t, bd, dir, issue.ID)
		if status != "deferred" {
			t.Fatalf("precondition: expected status=deferred, got %q", status)
		}

		ids := wakeReadyIDs(t, bd, dir)
		if !ids[issue.ID] {
			t.Errorf("expected %s in bd ready after its defer date passed", issue.ID)
		}
		status, deferUntil := showDeferState(t, bd, dir, issue.ID)
		if status != "open" {
			t.Errorf("expected status=open after wake, got %q", status)
		}
		if deferUntil != nil {
			t.Errorf("expected defer_until cleared after wake (undefer's shape), got %v", deferUntil)
		}
	})

	t.Run("dateless_defer_never_wakes", func(t *testing.T) {
		issue := bdCreate(t, bd, dir, "Indefinite icebox", "--type", "task")
		bdDefer(t, bd, dir, issue.ID)

		ids := wakeReadyIDs(t, bd, dir)
		if ids[issue.ID] {
			t.Errorf("dateless defer %s must not appear in bd ready", issue.ID)
		}
		status, _ := showDeferState(t, bd, dir, issue.ID)
		if status != "deferred" {
			t.Errorf("dateless defer must stay deferred, got %q", status)
		}
	})

	t.Run("future_dated_defer_stays_hidden", func(t *testing.T) {
		issue := bdCreate(t, bd, dir, "Future snooze", "--type", "task")
		bdDefer(t, bd, dir, issue.ID, "--until", "+8760h")

		ids := wakeReadyIDs(t, bd, dir)
		if ids[issue.ID] {
			t.Errorf("future defer %s must not appear in bd ready", issue.ID)
		}
		status, _ := showDeferState(t, bd, dir, issue.ID)
		if status != "deferred" {
			t.Errorf("future defer must stay deferred, got %q", status)
		}
	})

	t.Run("expired_dated_defer_wakes_wisp", func(t *testing.T) {
		// Wisps ride the same defer/wake contract through dolt_ignored tables,
		// which take a different persistence path (SQL commit, no version
		// commit) — the seam where the uow stack once rolled wisp wakes back.
		issue := bdCreate(t, bd, dir, "Wisp snooze", "--type", "task", "--ephemeral", "--wisp-type", "heartbeat")
		bdDefer(t, bd, dir, issue.ID, "--until", "2020-01-01")
		status, _ := showDeferState(t, bd, dir, issue.ID)
		if status != "deferred" {
			t.Fatalf("precondition: expected status=deferred, got %q", status)
		}

		_ = wakeReadyIDs(t, bd, dir) // trigger the sweep

		status, deferUntil := showDeferState(t, bd, dir, issue.ID)
		if status != "open" {
			t.Errorf("expected wisp status=open after wake, got %q", status)
		}
		if deferUntil != nil {
			t.Errorf("expected wisp defer_until cleared after wake, got %v", deferUntil)
		}
	})

	t.Run("expired_dated_defer_claimable", func(t *testing.T) {
		issue := bdCreate(t, bd, dir, "Claimable after snooze", "--type", "task", "--labels", "wake-claim-test")
		bdDefer(t, bd, dir, issue.ID, "--until", "2020-01-01")

		// The claim path wakes before selecting, so the freshly-expired bead
		// is claimable without any listing having run first.
		cmd := exec.Command(bd, "ready", "--claim", "--json", "-l", "wake-claim-test")
		cmd.Dir = dir
		cmd.Env = bdEnv(dir)
		stdout, stderr, err := runCommandBuffers(t, cmd)
		if err != nil {
			t.Fatalf("bd ready --claim failed: %v\nstdout:\n%s\nstderr:\n%s", err, stdout.String(), stderr.String())
		}
		if !strings.Contains(stdout.String(), issue.ID) {
			t.Errorf("expected claim to win %s, got:\n%s", issue.ID, stdout.String())
		}
		status, _ := showDeferState(t, bd, dir, issue.ID)
		if status != "in_progress" {
			t.Errorf("expected status=in_progress after claim, got %q", status)
		}
	})
}

// federatedDeferStores is the topology pc_7778c4ee1b4e was filed against: two
// stores of the same prefix over one dolt hub, each a different node. The
// citadel store creates and defers the row, commits and pushes; the jadegate
// store clones the hub. Both declare the same federation.prefix_home.hw, the
// way a git-tracked .beads/config.yaml carries one answer to every clone.
type federatedDeferStores struct {
	citadelDir, jadegateDir, issueID string
}

func setupFederatedDeferStores(t *testing.T, bd, prefixHome string, createArgs ...string) federatedDeferStores {
	t.Helper()
	hubURL := "file://" + filepath.Join(t.TempDir(), "hub")

	citadelDir, _, _ := bdInit(t, bd, "--prefix", "hw", "--skip-hooks", "--skip-agents")
	bdCommand(t, bd, citadelDir, "config", "set", "node_id", "citadel")
	bdCommand(t, bd, citadelDir, "config", "set", "federation.prefix_home.hw", prefixHome)
	issue := bdCreate(t, bd, citadelDir, append([]string{"Federated snooze", "--type", "task"}, createArgs...)...)
	bdDefer(t, bd, citadelDir, issue.ID, "--until", "2020-01-01")
	bdDolt(t, bd, citadelDir, "commit")
	bdDolt(t, bd, citadelDir, "remote", "add", "origin", hubURL)
	bdDolt(t, bd, citadelDir, "push", "--force")

	jadegateDir := t.TempDir()
	initGitRepoAt(t, jadegateDir)
	cmd := exec.Command(bd, "init", "--quiet", "--prefix", "hw", "--remote", hubURL, "--skip-hooks", "--skip-agents")
	cmd.Dir = jadegateDir
	cmd.Env = bdEnv(jadegateDir)
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("bd init --remote %s failed: %v\n%s", hubURL, err, out)
	}
	bdCommand(t, bd, jadegateDir, "config", "set", "node_id", "jadegate")
	bdCommand(t, bd, jadegateDir, "config", "set", "federation.prefix_home.hw", prefixHome)

	for _, dir := range []string{citadelDir, jadegateDir} {
		if status, _ := showDeferState(t, bd, dir, issue.ID); status != "deferred" {
			t.Fatalf("precondition: %s in %s: status=%q, want deferred", issue.ID, dir, status)
		}
	}
	return federatedDeferStores{citadelDir: citadelDir, jadegateDir: jadegateDir, issueID: issue.ID}
}

// TestEmbeddedDeferAutoWakeOwnerScoped is the 9/28 sighting of pc_7778c4ee1b4e
// as a test: both stores hold the same citadel-owned dated defer past its date.
// Only the node the owner:<node> label names wakes it; the peer reports the row
// skipped and writes nothing, so its next pull is a fast-forward, not the
// 25-conflict rollback the incident ended in.
func TestEmbeddedDeferAutoWakeOwnerScoped(t *testing.T) {
	if os.Getenv("BEADS_TEST_EMBEDDED_DOLT") != "1" {
		t.Skip("set BEADS_TEST_EMBEDDED_DOLT=1 to run embedded dolt integration tests")
	}
	t.Parallel()

	bd := buildEmbeddedBD(t)
	f := setupFederatedDeferStores(t, bd, "citadel", "--labels", "owner:citadel")

	// The peer sweeps first. The row is citadel's: untouched, and said so.
	ids, stderr := wakeReadyIDsStderr(t, bd, f.jadegateDir)
	if ids[f.issueID] {
		t.Errorf("jadegate store woke %s, which owner:citadel reserves for the citadel node", f.issueID)
	}
	if !strings.Contains(stderr, "defer-wake: skipped 1 ") || !strings.Contains(stderr, `"citadel"`) {
		t.Errorf("jadegate store did not report the skipped row on stderr:\n%s", stderr)
	}
	status, deferUntil := showDeferState(t, bd, f.jadegateDir, f.issueID)
	if status != "deferred" || deferUntil == nil {
		t.Errorf("jadegate store changed the row: status=%q defer_until=%v, want deferred with its date", status, deferUntil)
	}

	// The owner sweeps: the row wakes, nothing is reported skipped.
	ids, stderr = wakeReadyIDsStderr(t, bd, f.citadelDir)
	if !ids[f.issueID] {
		t.Errorf("citadel store did not list %s in bd ready after its defer date passed", f.issueID)
	}
	if strings.Contains(stderr, "defer-wake: skipped") {
		t.Errorf("citadel store reported a skip on its own row:\n%s", stderr)
	}
	status, deferUntil = showDeferState(t, bd, f.citadelDir, f.issueID)
	if status != "open" || deferUntil != nil {
		t.Errorf("citadel store after wake: status=%q defer_until=%v, want open with defer_until cleared", status, deferUntil)
	}

	// One node wrote the wake, so the hub round-trip is a clean fast-forward.
	bdDolt(t, bd, f.citadelDir, "push")
	bdDolt(t, bd, f.jadegateDir, "pull")
	status, deferUntil = showDeferState(t, bd, f.jadegateDir, f.issueID)
	if status != "open" || deferUntil != nil {
		t.Errorf("jadegate store after pull: status=%q defer_until=%v, want the owner's wake", status, deferUntil)
	}
}

// TestEmbeddedDeferAutoWakeUnownedRowHomePrefix covers the row with no
// owner:<node> label: it belongs to the prefix's declared home, here the PEER
// (federation.prefix_home.hw = jadegate). The creating store skips it and the
// home node wakes it.
func TestEmbeddedDeferAutoWakeUnownedRowHomePrefix(t *testing.T) {
	if os.Getenv("BEADS_TEST_EMBEDDED_DOLT") != "1" {
		t.Skip("set BEADS_TEST_EMBEDDED_DOLT=1 to run embedded dolt integration tests")
	}
	t.Parallel()

	bd := buildEmbeddedBD(t)
	f := setupFederatedDeferStores(t, bd, "jadegate")

	// citadel created the row but is not hw's home: skipped, untouched.
	ids, stderr := wakeReadyIDsStderr(t, bd, f.citadelDir)
	if ids[f.issueID] {
		t.Errorf("citadel store woke unlabelled %s, whose prefix home is jadegate", f.issueID)
	}
	if !strings.Contains(stderr, "defer-wake: skipped 1 ") || !strings.Contains(stderr, `"jadegate"`) {
		t.Errorf("citadel store did not report the skipped row on stderr:\n%s", stderr)
	}
	status, deferUntil := showDeferState(t, bd, f.citadelDir, f.issueID)
	if status != "deferred" || deferUntil == nil {
		t.Errorf("citadel store changed the row: status=%q defer_until=%v, want deferred with its date", status, deferUntil)
	}

	// jadegate is hw's declared home: it wakes the row.
	ids, stderr = wakeReadyIDsStderr(t, bd, f.jadegateDir)
	if !ids[f.issueID] {
		t.Errorf("jadegate store did not list %s in bd ready after its defer date passed", f.issueID)
	}
	if strings.Contains(stderr, "defer-wake: skipped") {
		t.Errorf("jadegate store reported a skip on a row it is home for:\n%s", stderr)
	}
	status, deferUntil = showDeferState(t, bd, f.jadegateDir, f.issueID)
	if status != "open" || deferUntil != nil {
		t.Errorf("jadegate store after wake: status=%q defer_until=%v, want open with defer_until cleared", status, deferUntil)
	}

	bdDolt(t, bd, f.jadegateDir, "push")
	bdDolt(t, bd, f.citadelDir, "pull")
	status, deferUntil = showDeferState(t, bd, f.citadelDir, f.issueID)
	if status != "open" || deferUntil != nil {
		t.Errorf("citadel store after pull: status=%q defer_until=%v, want the home node's wake", status, deferUntil)
	}

	// A wisp never federates (dolt_ignored), so the owner scope must leave it
	// alone: citadel is not hw's home, yet its own expired wisp still wakes.
	wisp := bdCreate(t, bd, f.citadelDir, "Local wisp snooze", "--type", "task", "--ephemeral", "--wisp-type", "heartbeat")
	bdDefer(t, bd, f.citadelDir, wisp.ID, "--until", "2020-01-01")
	_, stderr = wakeReadyIDsStderr(t, bd, f.citadelDir)
	if strings.Contains(stderr, "defer-wake: skipped") {
		t.Errorf("citadel store reported its own wisp skipped:\n%s", stderr)
	}
	status, deferUntil = showDeferState(t, bd, f.citadelDir, wisp.ID)
	if status != "open" || deferUntil != nil {
		t.Errorf("citadel store left its local wisp %s deferred: status=%q defer_until=%v, want open", wisp.ID, status, deferUntil)
	}
}
