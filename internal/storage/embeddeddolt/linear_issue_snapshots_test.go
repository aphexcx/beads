//go:build cgo

package embeddeddolt_test

import (
	"testing"
	"time"

	"github.com/steveyegge/beads/internal/storage"
)

// TestLinearIssueSnapshotCRUD verifies the snapshot store satisfies the
// LinearIssueSnapshotStore interface and round-trips every column
// correctly. This is bd-ajn P2's regression test — without it, a column
// type or scan/bind mismatch in the store would silently corrupt the
// snapshots that drive field-scoped conflict detection.
func TestLinearIssueSnapshotCRUD(t *testing.T) {
	if testing.Short() {
		t.Skip("short mode")
	}
	env := newTestEnv(t, "snap")
	ctx := t.Context()

	// Snapshot lookup with no row → (nil, nil), the documented "first sync"
	// signal. Misreading this as an error would block the soft rollout.
	got, err := env.store.GetLinearIssueSnapshot(ctx, "snap-1")
	if err != nil {
		t.Fatalf("GetLinearIssueSnapshot empty: %v", err)
	}
	if got != nil {
		t.Fatalf("GetLinearIssueSnapshot empty: got %+v, want nil", got)
	}

	// Round-trip every column. Use values that exercise edges: long
	// description, distinct status vs state_id, all relation slots populated.
	now := time.Date(2026, 6, 8, 16, 30, 0, 0, time.UTC)
	want := &storage.LinearIssueSnapshot{
		IssueID:     "snap-1",
		Title:       "Test title",
		Description: "Long description body. " + repeat("x", 1024),
		Status:      "in_progress",
		StateID:     "state-uuid-abc",
		Priority:    1,
		AssigneeID:  "user-uuid-1",
		ProjectID:   "project-uuid-1",
		ParentID:    "parent-uuid-1",
		SyncedAt:    now,
	}
	if err := env.store.UpsertLinearIssueSnapshot(ctx, want); err != nil {
		t.Fatalf("UpsertLinearIssueSnapshot insert: %v", err)
	}

	got, err = env.store.GetLinearIssueSnapshot(ctx, "snap-1")
	if err != nil {
		t.Fatalf("GetLinearIssueSnapshot after insert: %v", err)
	}
	if got == nil {
		t.Fatal("GetLinearIssueSnapshot after insert: got nil")
	}
	assertSnapshotEqual(t, got, want)

	// Upsert with changed values → existing row replaced (not duplicated).
	want.Title = "Updated title"
	want.Status = "closed"
	want.StateID = "state-uuid-done"
	want.SyncedAt = now.Add(time.Hour)
	if err := env.store.UpsertLinearIssueSnapshot(ctx, want); err != nil {
		t.Fatalf("UpsertLinearIssueSnapshot replace: %v", err)
	}
	got, err = env.store.GetLinearIssueSnapshot(ctx, "snap-1")
	if err != nil {
		t.Fatalf("GetLinearIssueSnapshot after replace: %v", err)
	}
	assertSnapshotEqual(t, got, want)

	// Delete → subsequent lookup returns (nil, nil) again.
	if err := env.store.DeleteLinearIssueSnapshot(ctx, "snap-1"); err != nil {
		t.Fatalf("DeleteLinearIssueSnapshot: %v", err)
	}
	got, err = env.store.GetLinearIssueSnapshot(ctx, "snap-1")
	if err != nil {
		t.Fatalf("GetLinearIssueSnapshot after delete: %v", err)
	}
	if got != nil {
		t.Errorf("after delete: got %+v, want nil", got)
	}
}

// TestLinearIssueSnapshotEmptyRelations covers the common case where a
// just-created Linear issue has no assignee/project/parent yet — empty
// strings must round-trip cleanly (not return nil sentinels) so the diff
// logic in P4 can distinguish "field was empty at last sync" from "field
// was never recorded."
func TestLinearIssueSnapshotEmptyRelations(t *testing.T) {
	if testing.Short() {
		t.Skip("short mode")
	}
	env := newTestEnv(t, "snape")
	ctx := t.Context()

	want := &storage.LinearIssueSnapshot{
		IssueID:  "snape-1",
		Title:    "Minimal",
		Status:   "open",
		StateID:  "state-uuid-todo",
		Priority: 2,
		SyncedAt: time.Date(2026, 6, 8, 16, 30, 0, 0, time.UTC),
	}
	if err := env.store.UpsertLinearIssueSnapshot(ctx, want); err != nil {
		t.Fatalf("UpsertLinearIssueSnapshot: %v", err)
	}
	got, err := env.store.GetLinearIssueSnapshot(ctx, "snape-1")
	if err != nil {
		t.Fatalf("GetLinearIssueSnapshot: %v", err)
	}
	if got.AssigneeID != "" || got.ProjectID != "" || got.ParentID != "" {
		t.Errorf("empty relations not preserved: assignee=%q project=%q parent=%q",
			got.AssigneeID, got.ProjectID, got.ParentID)
	}
	assertSnapshotEqual(t, got, want)
}

func assertSnapshotEqual(t *testing.T, got, want *storage.LinearIssueSnapshot) {
	t.Helper()
	if got.IssueID != want.IssueID {
		t.Errorf("IssueID: got %q want %q", got.IssueID, want.IssueID)
	}
	if got.Title != want.Title {
		t.Errorf("Title: got %q want %q", got.Title, want.Title)
	}
	if got.Description != want.Description {
		t.Errorf("Description: got %q want %q (lengths %d/%d)",
			got.Description, want.Description, len(got.Description), len(want.Description))
	}
	if got.Status != want.Status {
		t.Errorf("Status: got %q want %q", got.Status, want.Status)
	}
	if got.StateID != want.StateID {
		t.Errorf("StateID: got %q want %q", got.StateID, want.StateID)
	}
	if got.Priority != want.Priority {
		t.Errorf("Priority: got %d want %d", got.Priority, want.Priority)
	}
	if got.AssigneeID != want.AssigneeID {
		t.Errorf("AssigneeID: got %q want %q", got.AssigneeID, want.AssigneeID)
	}
	if got.ProjectID != want.ProjectID {
		t.Errorf("ProjectID: got %q want %q", got.ProjectID, want.ProjectID)
	}
	if got.ParentID != want.ParentID {
		t.Errorf("ParentID: got %q want %q", got.ParentID, want.ParentID)
	}
	if !got.SyncedAt.Equal(want.SyncedAt) {
		t.Errorf("SyncedAt: got %v want %v", got.SyncedAt, want.SyncedAt)
	}
}

func repeat(s string, n int) string {
	b := make([]byte, 0, len(s)*n)
	for i := 0; i < n; i++ {
		b = append(b, s...)
	}
	return string(b)
}
