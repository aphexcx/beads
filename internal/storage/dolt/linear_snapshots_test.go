package dolt

import (
	"context"
	"testing"
	"time"

	"github.com/steveyegge/beads/internal/storage"
)

// bd-3p8: CRUD tests for the LinearIssueSnapshotStore +
// LinearProjectSnapshotStore impls on the Dolt-server backend.
// Mirrors the embeddeddolt CRUD tests so a future divergence
// between the two backends becomes a visible test failure.
//
// Without these tests (and the matching impls), bd-ajn and
// bd-6cl were silently falling through to capability-degrade on
// every production rig.

func TestDoltLinearIssueSnapshotCRUD(t *testing.T) {
	store, cleanup := setupTestStore(t)
	defer cleanup()
	ctx := context.Background()

	// First-sync signal: lookup with no row returns (nil, nil).
	got, err := store.GetLinearIssueSnapshot(ctx, "snap-1")
	if err != nil {
		t.Fatalf("GetLinearIssueSnapshot empty: %v", err)
	}
	if got != nil {
		t.Fatalf("GetLinearIssueSnapshot empty: got %+v, want nil", got)
	}

	// Round-trip every column.
	now := time.Date(2026, 6, 9, 5, 0, 0, 0, time.UTC)
	want := &storage.LinearIssueSnapshot{
		IssueID:     "snap-1",
		Title:       "Test title",
		Description: "Body " + repeatBytes("x", 1024),
		Status:      "in_progress",
		StateID:     "state-uuid-abc",
		Priority:    1,
		AssigneeID:  "user-uuid-1",
		ProjectID:   "project-uuid-1",
		ParentID:    "parent-uuid-1",
		SyncedAt:    now,
	}
	if err := store.UpsertLinearIssueSnapshot(ctx, want); err != nil {
		t.Fatalf("UpsertLinearIssueSnapshot insert: %v", err)
	}

	got, err = store.GetLinearIssueSnapshot(ctx, "snap-1")
	if err != nil {
		t.Fatalf("GetLinearIssueSnapshot after insert: %v", err)
	}
	if got == nil {
		t.Fatal("GetLinearIssueSnapshot after insert: got nil")
	}
	assertIssueSnapshotEqual(t, got, want)

	// Upsert with changed values → existing row replaced.
	want.Title = "Updated title"
	want.Status = "closed"
	want.StateID = "state-uuid-done"
	want.SyncedAt = now.Add(time.Hour)
	if err := store.UpsertLinearIssueSnapshot(ctx, want); err != nil {
		t.Fatalf("UpsertLinearIssueSnapshot replace: %v", err)
	}
	got, err = store.GetLinearIssueSnapshot(ctx, "snap-1")
	if err != nil {
		t.Fatalf("GetLinearIssueSnapshot after replace: %v", err)
	}
	assertIssueSnapshotEqual(t, got, want)

	// Delete → subsequent lookup returns (nil, nil) again.
	if err := store.DeleteLinearIssueSnapshot(ctx, "snap-1"); err != nil {
		t.Fatalf("DeleteLinearIssueSnapshot: %v", err)
	}
	got, err = store.GetLinearIssueSnapshot(ctx, "snap-1")
	if err != nil {
		t.Fatalf("GetLinearIssueSnapshot after delete: %v", err)
	}
	if got != nil {
		t.Errorf("after delete: got %+v, want nil", got)
	}
}

func TestDoltLinearProjectSnapshotCRUD(t *testing.T) {
	store, cleanup := setupTestStore(t)
	defer cleanup()
	ctx := context.Background()

	// First-sync signal.
	got, err := store.GetLinearProjectSnapshot(ctx, "psnap-1")
	if err != nil {
		t.Fatalf("GetLinearProjectSnapshot empty: %v", err)
	}
	if got != nil {
		t.Fatalf("GetLinearProjectSnapshot empty: got %+v, want nil", got)
	}

	now := time.Date(2026, 6, 9, 5, 0, 0, 0, time.UTC)
	want := &storage.LinearProjectSnapshot{
		IssueID:     "psnap-1",
		ProjectID:   "project-uuid-abc",
		Name:        "Test project",
		Description: "Short summary that fits in 255 chars",
		Content:     "Long content body. " + repeatBytes("y", 2048),
		State:       "started",
		SyncedAt:    now,
	}
	if err := store.UpsertLinearProjectSnapshot(ctx, want); err != nil {
		t.Fatalf("UpsertLinearProjectSnapshot insert: %v", err)
	}
	got, err = store.GetLinearProjectSnapshot(ctx, "psnap-1")
	if err != nil {
		t.Fatalf("GetLinearProjectSnapshot after insert: %v", err)
	}
	if got == nil {
		t.Fatal("GetLinearProjectSnapshot after insert: got nil")
	}
	assertProjectSnapshotEqual(t, got, want)

	// Upsert replacement.
	want.Name = "Renamed project"
	want.State = "completed"
	want.SyncedAt = now.Add(time.Hour)
	if err := store.UpsertLinearProjectSnapshot(ctx, want); err != nil {
		t.Fatalf("UpsertLinearProjectSnapshot replace: %v", err)
	}
	got, err = store.GetLinearProjectSnapshot(ctx, "psnap-1")
	if err != nil {
		t.Fatalf("GetLinearProjectSnapshot after replace: %v", err)
	}
	assertProjectSnapshotEqual(t, got, want)

	// Delete.
	if err := store.DeleteLinearProjectSnapshot(ctx, "psnap-1"); err != nil {
		t.Fatalf("DeleteLinearProjectSnapshot: %v", err)
	}
	got, err = store.GetLinearProjectSnapshot(ctx, "psnap-1")
	if err != nil {
		t.Fatalf("GetLinearProjectSnapshot after delete: %v", err)
	}
	if got != nil {
		t.Errorf("after delete: got %+v, want nil", got)
	}
}

// TestDoltStoreSatisfiesSnapshotInterfaces is a compile-time +
// runtime guard against regression of the bd-3p8 fix. If either
// snapshot impl regresses or gets removed, this test fails loudly
// instead of the engine silently degrading at runtime.
func TestDoltStoreSatisfiesSnapshotInterfaces(t *testing.T) {
	var s interface{} = &DoltStore{}
	if _, ok := s.(storage.LinearIssueSnapshotStore); !ok {
		t.Error("*DoltStore must implement storage.LinearIssueSnapshotStore (bd-ajn + bd-3p8)")
	}
	if _, ok := s.(storage.LinearProjectSnapshotStore); !ok {
		t.Error("*DoltStore must implement storage.LinearProjectSnapshotStore (bd-6cl + bd-3p8)")
	}
}

func assertIssueSnapshotEqual(t *testing.T, got, want *storage.LinearIssueSnapshot) {
	t.Helper()
	if got.IssueID != want.IssueID {
		t.Errorf("IssueID: got %q want %q", got.IssueID, want.IssueID)
	}
	if got.Title != want.Title {
		t.Errorf("Title: got %q want %q", got.Title, want.Title)
	}
	if got.Description != want.Description {
		t.Errorf("Description: lengths got=%d want=%d", len(got.Description), len(want.Description))
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

func assertProjectSnapshotEqual(t *testing.T, got, want *storage.LinearProjectSnapshot) {
	t.Helper()
	if got.IssueID != want.IssueID {
		t.Errorf("IssueID: got %q want %q", got.IssueID, want.IssueID)
	}
	if got.ProjectID != want.ProjectID {
		t.Errorf("ProjectID: got %q want %q", got.ProjectID, want.ProjectID)
	}
	if got.Name != want.Name {
		t.Errorf("Name: got %q want %q", got.Name, want.Name)
	}
	if got.Description != want.Description {
		t.Errorf("Description: got %q want %q", got.Description, want.Description)
	}
	if got.Content != want.Content {
		t.Errorf("Content: lengths got=%d want=%d (mismatch)", len(got.Content), len(want.Content))
	}
	if got.State != want.State {
		t.Errorf("State: got %q want %q", got.State, want.State)
	}
	if !got.SyncedAt.Equal(want.SyncedAt) {
		t.Errorf("SyncedAt: got %v want %v", got.SyncedAt, want.SyncedAt)
	}
}

func repeatBytes(s string, n int) string {
	b := make([]byte, 0, len(s)*n)
	for i := 0; i < n; i++ {
		b = append(b, s...)
	}
	return string(b)
}
