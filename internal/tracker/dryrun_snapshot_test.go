//go:build cgo

package tracker

import (
	"context"
	"testing"
	"time"

	"github.com/steveyegge/beads/internal/storage"
	"github.com/steveyegge/beads/internal/types"
)

// bd-p4m regression: dry-run must NOT write linear_issue_snapshots
// baseline rows during the first-sync soft-rollout path. Before the
// fix, every --dry-run that hit an unsnapshotted issue called
// snapshotter.RecordPullSnapshot, populating the table as a side
// effect. The next wet-run then skipped first-sync (the baselines
// were already written) and took a different code path than it
// would have without the dry-run — exactly the differential that
// the dry-run contract exists to prevent.
//
// These tests exercise detectFieldScopedConflict directly with a
// recording mock snapshotter so we don't need to stand up a full
// Dolt store + Linear HTTP server. The engine wiring (passing
// opts.DryRun through DetectConflicts) is locked at the engine
// test level.

// recordingSnapshotStore implements storage.LinearIssueSnapshotStore.
// Records every write so tests can assert "did the snapshot table
// get touched?" — the bd-p4m invariant.
type recordingSnapshotStore struct {
	storage.LinearIssueSnapshotStore // satisfies the interface without implementing every method by hand
	getCalls                         []string
	upsertCalls                      []string
	rows                             map[string]*storage.LinearIssueSnapshot
}

func newRecordingSnapshotStore() *recordingSnapshotStore {
	return &recordingSnapshotStore{rows: map[string]*storage.LinearIssueSnapshot{}}
}

func (r *recordingSnapshotStore) GetLinearIssueSnapshot(_ context.Context, issueID string) (*storage.LinearIssueSnapshot, error) {
	r.getCalls = append(r.getCalls, issueID)
	row, ok := r.rows[issueID]
	if !ok {
		return nil, nil
	}
	return row, nil
}

func (r *recordingSnapshotStore) UpsertLinearIssueSnapshot(_ context.Context, snap *storage.LinearIssueSnapshot) error {
	r.upsertCalls = append(r.upsertCalls, snap.IssueID)
	r.rows[snap.IssueID] = snap
	return nil
}

func (r *recordingSnapshotStore) DeleteLinearIssueSnapshot(_ context.Context, issueID string) error {
	delete(r.rows, issueID)
	return nil
}

// recordingSnapshotter implements PostPullSnapshotter. Records calls
// so the bd-p4m dry-run check can assert the snapshotter was NOT
// invoked even when the field-scoped detector hit a first-sync
// situation.
type recordingSnapshotter struct {
	*mockTracker
	store *recordingSnapshotStore
	calls []string
}

func (rs *recordingSnapshotter) RecordPullSnapshot(ctx context.Context, localBeadID string, fetched TrackerIssue) error {
	rs.calls = append(rs.calls, localBeadID)
	// Mirror real Linear behavior: the snapshotter routes to the
	// store. If RecordPullSnapshot is called when it shouldn't be,
	// the store sees an upsert.
	return rs.store.UpsertLinearIssueSnapshot(ctx, &storage.LinearIssueSnapshot{
		IssueID:  localBeadID,
		Title:    fetched.Title,
		SyncedAt: time.Now(),
	})
}

// TestDetectFieldScopedConflict_DryRunDoesNotWriteBaseline is the
// bd-p4m lock. Calls detectFieldScopedConflict directly with
// dryRun=true on an issue with no prior snapshot. Asserts:
//   - log line was emitted (preview happens)
//   - snapshotter.RecordPullSnapshot was NOT invoked
//   - snapshot store has no rows
func TestDetectFieldScopedConflict_DryRunDoesNotWriteBaseline(t *testing.T) {
	store := newRecordingSnapshotStore()
	snapshotter := &recordingSnapshotter{
		mockTracker: newMockTracker("dry-test"),
		store:       store,
	}
	e := &Engine{}

	issue := &types.Issue{ID: "bd-1", Title: "T"}
	extIssue := &TrackerIssue{Identifier: "TEAM-1", Title: "T"}

	got, err := e.detectFieldScopedConflict(
		context.Background(), issue, extIssue, "https://linear.app/x/issue/TEAM-1",
		time.Time{}, store, snapshotter /*dryRun=*/, true,
	)
	if err != nil {
		t.Fatalf("detectFieldScopedConflict err: %v", err)
	}
	if got != nil {
		t.Errorf("first-sync should emit no conflict, got %+v", got)
	}

	if len(snapshotter.calls) != 0 {
		t.Errorf("bd-p4m violation: snapshotter.RecordPullSnapshot called %d times in dry-run (want 0): %v",
			len(snapshotter.calls), snapshotter.calls)
	}
	if len(store.upsertCalls) != 0 {
		t.Errorf("bd-p4m violation: snapshot store upserts called %d times in dry-run (want 0): %v",
			len(store.upsertCalls), store.upsertCalls)
	}
	if len(store.rows) != 0 {
		t.Errorf("bd-p4m violation: dry-run populated snapshot rows: %v", store.rows)
	}
}

// TestDetectFieldScopedConflict_WetRunWritesBaseline — the
// positive case. Same scenario as above but dryRun=false. Asserts
// the baseline IS written, so the field-scoped path actually runs
// on the next sync.
func TestDetectFieldScopedConflict_WetRunWritesBaseline(t *testing.T) {
	store := newRecordingSnapshotStore()
	snapshotter := &recordingSnapshotter{
		mockTracker: newMockTracker("wet-test"),
		store:       store,
	}
	e := &Engine{}

	issue := &types.Issue{ID: "bd-1", Title: "T"}
	extIssue := &TrackerIssue{Identifier: "TEAM-1", Title: "T"}

	got, err := e.detectFieldScopedConflict(
		context.Background(), issue, extIssue, "https://linear.app/x/issue/TEAM-1",
		time.Time{}, store, snapshotter /*dryRun=*/, false,
	)
	if err != nil {
		t.Fatalf("detectFieldScopedConflict err: %v", err)
	}
	if got != nil {
		t.Errorf("first-sync should emit no conflict, got %+v", got)
	}

	if len(snapshotter.calls) != 1 || snapshotter.calls[0] != "bd-1" {
		t.Errorf("wet-run should write baseline; calls=%v", snapshotter.calls)
	}
	if _, ok := store.rows["bd-1"]; !ok {
		t.Errorf("wet-run baseline not persisted: %v", store.rows)
	}
}

// TestDetectFieldScopedConflict_DryRunIdempotency — two
// consecutive dry-runs produce identical store state (no rows
// either run). The original bug's symptom was the SECOND dry-run
// produced 0 first-sync lines because the first dry-run had
// populated the table.
func TestDetectFieldScopedConflict_DryRunIdempotency(t *testing.T) {
	store := newRecordingSnapshotStore()
	snapshotter := &recordingSnapshotter{
		mockTracker: newMockTracker("idempotent-test"),
		store:       store,
	}
	e := &Engine{}

	issue := &types.Issue{ID: "bd-1", Title: "T"}
	extIssue := &TrackerIssue{Identifier: "TEAM-1", Title: "T"}

	// First dry-run.
	_, err := e.detectFieldScopedConflict(
		context.Background(), issue, extIssue, "https://linear.app/x/issue/TEAM-1",
		time.Time{}, store, snapshotter, true)
	if err != nil {
		t.Fatalf("first dry-run err: %v", err)
	}
	if len(store.rows) != 0 {
		t.Fatalf("after first dry-run: rows should be empty, got %v", store.rows)
	}

	// Second dry-run. Must still see "first-sync" (nil snapshot)
	// because nothing was written between runs.
	_, err = e.detectFieldScopedConflict(
		context.Background(), issue, extIssue, "https://linear.app/x/issue/TEAM-1",
		time.Time{}, store, snapshotter, true)
	if err != nil {
		t.Fatalf("second dry-run err: %v", err)
	}
	if len(store.rows) != 0 {
		t.Errorf("after second dry-run: rows should still be empty, got %v", store.rows)
	}
	// GetLinearIssueSnapshot called twice (once per dry-run); both
	// returned nil. UpsertLinearIssueSnapshot never called.
	if len(store.getCalls) != 2 {
		t.Errorf("expected 2 GetLinearIssueSnapshot calls, got %d", len(store.getCalls))
	}
	if len(store.upsertCalls) != 0 {
		t.Errorf("expected 0 UpsertLinearIssueSnapshot calls, got %v", store.upsertCalls)
	}
}
