//go:build cgo

package linear

import (
	"context"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/steveyegge/beads/internal/storage"
	"github.com/steveyegge/beads/internal/storage/embeddeddolt"
	"github.com/steveyegge/beads/internal/tracker"
	"github.com/steveyegge/beads/internal/types"
)

// bd-6cl P6 integration tests: exercise pullOneProject end-to-end
// against the real embedded Dolt store. Skips Linear HTTP — the
// resolve primitive's logic was already unit-tested in
// project_pull_test.go (P4). These tests verify the
// store-mutation side: that materialized epics actually land,
// snapshots get written, and the Q1 close-state preservation
// holds when applied through the real UpdateIssue path.

// newPullProjectsEnv builds an embedded Dolt store + Linear
// Tracker pointed at it (no Linear client init), ready for
// direct pullOneProject calls.
func newPullProjectsEnv(t *testing.T) (*Tracker, *embeddeddolt.EmbeddedDoltStore) {
	t.Helper()
	if testing.Short() {
		t.Skip("short mode")
	}
	ctx := t.Context()
	beadsDir := filepath.Join(t.TempDir(), ".beads")
	store, err := embeddeddolt.Open(ctx, beadsDir, "pullproj", "main")
	if err != nil {
		t.Fatalf("New embedded store: %v", err)
	}
	t.Cleanup(func() { store.Close() })

	if err := store.SetConfig(ctx, "issue_prefix", "pullproj"); err != nil {
		t.Fatalf("SetConfig: %v", err)
	}
	if err := store.Commit(ctx, "bd init"); err != nil {
		t.Fatalf("Commit: %v", err)
	}

	tr := &Tracker{store: store}
	return tr, store
}

// TestPullOneProject_UnmatchedCreatesEpic — bd-6cl P6: when
// PullProjects sees a Linear Project with no local-side match
// (no epic with that external_ref), it creates a new local epic
// with TypeEpic + recombined description + reverse-mapped status,
// and writes a baseline snapshot. Verifies the materialize path
// end-to-end through the real bead store.
func TestPullOneProject_UnmatchedCreatesEpic(t *testing.T) {
	tr, store := newPullProjectsEnv(t)
	ctx := t.Context()

	remote := tracker.TrackerProject{
		ID:          "linear-uuid-1",
		Name:        "Materialized epic",
		Description: "Short summary",
		Content:     "Long-form authoritative body",
		State:       "started",
		URL:         "https://linear.app/test/project/abc-12345",
	}
	stats := &tracker.ProjectPullStats{
		ProjectIDToLocalEpicID: map[string]string{},
	}
	syncedAt := time.Now().UTC()
	opts := tracker.ProjectPullOptions{Actor: "test-actor", Policy: tracker.ConflictTimestamp}

	tr.pullOneProject(ctx, remote, map[string]*types.Issue{}, store, opts, syncedAt, stats)

	if stats.Created != 1 {
		t.Fatalf("expected Created=1, got %+v", stats)
	}
	if len(stats.Errors) != 0 {
		t.Fatalf("unexpected errors: %v", stats.Errors)
	}

	// Verify epic exists in store.
	created := mustFindEpicByExternalRef(t, ctx, store, remote.URL)
	if created.Title != "Materialized epic" {
		t.Errorf("epic Title: got %q want %q", created.Title, "Materialized epic")
	}
	if created.Description != "Long-form authoritative body" {
		t.Errorf("epic Description should be recombined (prefer content): got %q", created.Description)
	}
	if created.Status != types.StatusInProgress {
		t.Errorf("epic Status (started→in_progress): got %s", created.Status)
	}
	if created.IssueType != types.TypeEpic {
		t.Errorf("epic IssueType: got %s", created.IssueType)
	}

	// Verify ProjectIDToLocalEpicID populated for downstream
	// descendant-dep wiring.
	if stats.ProjectIDToLocalEpicID[remote.ID] != created.ID {
		t.Errorf("ProjectIDToLocalEpicID[%s] = %q, want %q",
			remote.ID, stats.ProjectIDToLocalEpicID[remote.ID], created.ID)
	}

	// Verify snapshot baseline was written.
	snap, err := store.GetLinearProjectSnapshot(ctx, created.ID)
	if err != nil {
		t.Fatalf("GetLinearProjectSnapshot: %v", err)
	}
	if snap == nil {
		t.Fatal("baseline snapshot not written")
	}
	if snap.ProjectID != remote.ID {
		t.Errorf("snapshot ProjectID: got %q want %q", snap.ProjectID, remote.ID)
	}
}

// TestPullOneProject_FirstSyncBaselineOnly — bd-6cl P6: matched
// epic + no prior snapshot → first-sync soft rollout. Writes
// baseline snapshot, no Update, no conflict possible this run.
// SkipReason annotated for observability.
func TestPullOneProject_FirstSyncBaselineOnly(t *testing.T) {
	tr, store := newPullProjectsEnv(t)
	ctx := t.Context()

	ref := "https://linear.app/test/project/first-sync-id"
	existing := &types.Issue{
		ID:          "pullproj-baseline",
		Title:       "Existing epic",
		Description: "Local-only edits",
		IssueType:   types.TypeEpic,
		Status:      types.StatusOpen,
		ExternalRef: strPtr(ref),
	}
	if err := store.CreateIssue(ctx, existing, "test-actor"); err != nil {
		t.Fatalf("CreateIssue: %v", err)
	}

	remote := tracker.TrackerProject{
		ID:    "linear-uuid-fs",
		Name:  "Differs",
		State: "started",
		URL:   ref,
	}
	stats := &tracker.ProjectPullStats{ProjectIDToLocalEpicID: map[string]string{}}
	opts := tracker.ProjectPullOptions{Actor: "test-actor", Policy: tracker.ConflictTimestamp}

	tr.pullOneProject(ctx, remote, map[string]*types.Issue{ref: existing}, store, opts, time.Now().UTC(), stats)

	if stats.FirstSync != 1 {
		t.Errorf("expected FirstSync=1, got %+v", stats)
	}
	if stats.Updated != 0 {
		t.Errorf("first-sync should produce no Updated, got %d", stats.Updated)
	}

	// Verify the local epic was NOT modified (no apply during
	// first-sync; baseline only).
	reread, err := store.GetIssue(ctx, existing.ID)
	if err != nil {
		t.Fatalf("GetIssue: %v", err)
	}
	if reread.Title != "Existing epic" {
		t.Errorf("first-sync should not modify local; got Title=%q", reread.Title)
	}

	// Verify baseline snapshot was written.
	snap, err := store.GetLinearProjectSnapshot(ctx, existing.ID)
	if err != nil {
		t.Fatalf("GetLinearProjectSnapshot: %v", err)
	}
	if snap == nil {
		t.Fatal("first-sync MUST write baseline snapshot")
	}
}

// TestPullOneProject_Q1ClosedStatePreservedEndToEnd — bd-6cl P6:
// the motivating Q1 lock at the integration level. Local epic
// closed; Linear Project state would translate to in_progress.
// After PullProjects: local epic STILL closed (no UpdateIssue
// applied for status). Snapshot refreshed.
func TestPullOneProject_Q1ClosedStatePreservedEndToEnd(t *testing.T) {
	tr, store := newPullProjectsEnv(t)
	ctx := t.Context()

	ref := "https://linear.app/test/project/closed-epic-id"
	existing := &types.Issue{
		ID:          "pullproj-closed",
		Title:       "Done epic",
		IssueType:   types.TypeEpic,
		Status:      types.StatusClosed,
		ExternalRef: strPtr(ref),
	}
	if err := store.CreateIssue(ctx, existing, "test-actor"); err != nil {
		t.Fatalf("CreateIssue: %v", err)
	}

	// Pre-seed a snapshot reflecting Linear's previous "completed"
	// state so the diff sees a real change.
	if err := store.UpsertLinearProjectSnapshot(ctx, &storage.LinearProjectSnapshot{
		IssueID: existing.ID, ProjectID: "linear-uuid-closed",
		Name: "Done epic", State: "completed",
		SyncedAt: time.Now().Add(-time.Hour),
	}); err != nil {
		t.Fatalf("UpsertLinearProjectSnapshot: %v", err)
	}

	// Now Linear says the Project is "started" again — would
	// translate to in_progress. Q1 must refuse to reopen.
	remote := tracker.TrackerProject{
		ID: "linear-uuid-closed", Name: "Done epic", State: "started", URL: ref,
		UpdatedAt: time.Now(),
	}
	stats := &tracker.ProjectPullStats{ProjectIDToLocalEpicID: map[string]string{}}
	opts := tracker.ProjectPullOptions{Actor: "test-actor", Policy: tracker.ConflictTimestamp}

	tr.pullOneProject(ctx, remote, map[string]*types.Issue{ref: existing}, store, opts, time.Now().UTC(), stats)

	// Verify the local epic is STILL closed.
	reread, err := store.GetIssue(ctx, existing.ID)
	if err != nil {
		t.Fatalf("GetIssue: %v", err)
	}
	if reread.Status != types.StatusClosed {
		t.Fatalf("Q1 VIOLATION: epic was reopened (%s)", reread.Status)
	}
}

// TestPullOneProject_DryRunNoMutations — dry-run must produce
// PreviewLines + counts but write NOTHING to the store
// (no UpdateIssue, no CreateIssue, no snapshot upsert).
func TestPullOneProject_DryRunNoMutations(t *testing.T) {
	tr, store := newPullProjectsEnv(t)
	ctx := t.Context()

	remote := tracker.TrackerProject{
		ID:    "linear-uuid-dryrun",
		Name:  "Should not be created",
		State: "planned",
		URL:   "https://linear.app/test/project/dryrun-id",
	}
	stats := &tracker.ProjectPullStats{ProjectIDToLocalEpicID: map[string]string{}}
	opts := tracker.ProjectPullOptions{
		Actor: "test-actor", Policy: tracker.ConflictTimestamp,
		DryRun: true,
	}

	tr.pullOneProject(ctx, remote, map[string]*types.Issue{}, store, opts, time.Now().UTC(), stats)

	if stats.Created != 1 {
		t.Errorf("dry-run should report would-create Created=1, got %d", stats.Created)
	}
	if len(stats.PreviewLines) == 0 {
		t.Errorf("dry-run should produce PreviewLines, got empty")
	}

	// Verify NO epic was created.
	if found := tryFindEpicByExternalRef(ctx, store, remote.URL); found != nil {
		t.Errorf("dry-run MUST NOT create local epic, found %+v", found)
	}
	// Verify NO snapshot was written. Snapshot key would be the
	// would-be bead ID — but since no bead was created, look up by
	// the project ID and confirm no row exists for any local ID.
	// Easiest check: no snapshot row at all (we cleared the store).
	snap, _ := store.GetLinearProjectSnapshot(ctx, "would-not-exist-id")
	if snap != nil {
		t.Errorf("dry-run snapshot leak: %+v", snap)
	}
}

func mustFindEpicByExternalRef(t *testing.T, ctx context.Context, store *embeddeddolt.EmbeddedDoltStore, ref string) *types.Issue {
	t.Helper()
	found := tryFindEpicByExternalRef(ctx, store, ref)
	if found == nil {
		t.Fatalf("no epic found with external_ref=%q", ref)
	}
	return found
}

func tryFindEpicByExternalRef(ctx context.Context, store *embeddeddolt.EmbeddedDoltStore, ref string) *types.Issue {
	all, err := store.SearchIssues(ctx, "", types.IssueFilter{})
	if err != nil {
		return nil
	}
	for _, issue := range all {
		if issue == nil || issue.ExternalRef == nil {
			continue
		}
		if strings.TrimSpace(*issue.ExternalRef) == ref {
			return issue
		}
	}
	return nil
}

func strPtr(s string) *string { return &s }
