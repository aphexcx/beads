//go:build cgo

package embeddeddolt_test

import (
	"testing"
	"time"

	"github.com/steveyegge/beads/internal/storage"
)

// TestLinearProjectSnapshotCRUD is bd-6cl P1's regression test —
// without it, a column type or scan/bind mismatch in the Project
// snapshot store would silently corrupt the data that drives pull-
// side field-scoped conflict detection. Mirrors the linear_issue
// equivalent.
func TestLinearProjectSnapshotCRUD(t *testing.T) {
	if testing.Short() {
		t.Skip("short mode")
	}
	env := newTestEnv(t, "psnap")
	ctx := t.Context()

	// First-sync signal: lookup with no row returns (nil, nil).
	got, err := env.store.GetLinearProjectSnapshot(ctx, "psnap-1")
	if err != nil {
		t.Fatalf("GetLinearProjectSnapshot empty: %v", err)
	}
	if got != nil {
		t.Fatalf("GetLinearProjectSnapshot empty: got %+v, want nil", got)
	}

	// Round-trip every column. Use values that exercise edges: long
	// content body, distinct description vs content (post-bd-cs1
	// split shape).
	now := time.Date(2026, 6, 8, 18, 0, 0, 0, time.UTC)
	want := &storage.LinearProjectSnapshot{
		IssueID:     "psnap-1",
		ProjectID:   "project-uuid-abc",
		Name:        "Test project",
		Description: "Short summary that fits in 255 chars",
		Content:     "Long content body. " + repeat("y", 2048),
		State:       "started",
		SyncedAt:    now,
	}
	if err := env.store.UpsertLinearProjectSnapshot(ctx, want); err != nil {
		t.Fatalf("UpsertLinearProjectSnapshot insert: %v", err)
	}

	got, err = env.store.GetLinearProjectSnapshot(ctx, "psnap-1")
	if err != nil {
		t.Fatalf("GetLinearProjectSnapshot after insert: %v", err)
	}
	if got == nil {
		t.Fatal("GetLinearProjectSnapshot after insert: got nil")
	}
	assertProjectSnapshotEqual(t, got, want)

	// Upsert with changed values → existing row replaced.
	want.Name = "Renamed project"
	want.State = "completed"
	want.SyncedAt = now.Add(time.Hour)
	if err := env.store.UpsertLinearProjectSnapshot(ctx, want); err != nil {
		t.Fatalf("UpsertLinearProjectSnapshot replace: %v", err)
	}
	got, err = env.store.GetLinearProjectSnapshot(ctx, "psnap-1")
	if err != nil {
		t.Fatalf("GetLinearProjectSnapshot after replace: %v", err)
	}
	assertProjectSnapshotEqual(t, got, want)

	// Delete → subsequent lookup returns (nil, nil).
	if err := env.store.DeleteLinearProjectSnapshot(ctx, "psnap-1"); err != nil {
		t.Fatalf("DeleteLinearProjectSnapshot: %v", err)
	}
	got, err = env.store.GetLinearProjectSnapshot(ctx, "psnap-1")
	if err != nil {
		t.Fatalf("GetLinearProjectSnapshot after delete: %v", err)
	}
	if got != nil {
		t.Errorf("after delete: got %+v, want nil", got)
	}
}

// TestLinearProjectSnapshotEmptyContent covers the common case where
// a Project has only a short description (no content overflow) —
// the empty content column must round-trip cleanly so the diff
// logic can distinguish "Linear has no content" from "we never
// recorded content."
func TestLinearProjectSnapshotEmptyContent(t *testing.T) {
	if testing.Short() {
		t.Skip("short mode")
	}
	env := newTestEnv(t, "psnpe")
	ctx := t.Context()

	want := &storage.LinearProjectSnapshot{
		IssueID:     "psnpe-1",
		ProjectID:   "project-uuid-def",
		Name:        "Minimal project",
		Description: "Short body, no long-form needed",
		State:       "planned",
		SyncedAt:    time.Date(2026, 6, 8, 18, 0, 0, 0, time.UTC),
	}
	if err := env.store.UpsertLinearProjectSnapshot(ctx, want); err != nil {
		t.Fatalf("UpsertLinearProjectSnapshot: %v", err)
	}
	got, err := env.store.GetLinearProjectSnapshot(ctx, "psnpe-1")
	if err != nil {
		t.Fatalf("GetLinearProjectSnapshot: %v", err)
	}
	if got.Content != "" {
		t.Errorf("empty content not preserved: got %q", got.Content)
	}
	assertProjectSnapshotEqual(t, got, want)
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
		t.Errorf("Content: got len=%d want len=%d (mismatch)", len(got.Content), len(want.Content))
	}
	if got.State != want.State {
		t.Errorf("State: got %q want %q", got.State, want.State)
	}
	if !got.SyncedAt.Equal(want.SyncedAt) {
		t.Errorf("SyncedAt: got %v want %v", got.SyncedAt, want.SyncedAt)
	}
}
