package dolt

import (
	"testing"

	"github.com/steveyegge/beads/internal/storage"
	"github.com/steveyegge/beads/internal/types"
)

func TestLinearLabelSnapshotCRUD(t *testing.T) {
	store, cleanup := setupTestStore(t)
	defer cleanup()

	ctx, cancel := testContext(t)
	defer cancel()

	// Seed a bead so the FK is satisfied.
	issue := &types.Issue{
		ID:        "test-1",
		Title:     "test",
		Status:    types.StatusOpen,
		Priority:  2,
		IssueType: types.TypeTask,
	}
	if err := store.CreateIssue(ctx, issue, "tester"); err != nil {
		t.Fatalf("CreateIssue: %v", err)
	}

	want := []storage.LinearLabelSnapshotEntry{
		{LabelID: "lin-1", LabelName: "bug"},
		{LabelID: "lin-2", LabelName: "p1"},
	}

	if err := store.RunInTransaction(ctx, "test write", func(tx storage.Transaction) error {
		return tx.PutLinearLabelSnapshot(ctx, "test-1", want)
	}); err != nil {
		t.Fatalf("Put: %v", err)
	}

	var got []storage.LinearLabelSnapshotEntry
	if err := store.RunInTransaction(ctx, "test read", func(tx storage.Transaction) error {
		var err error
		got, err = tx.GetLinearLabelSnapshot(ctx, "test-1")
		return err
	}); err != nil {
		t.Fatalf("Get: %v", err)
	}

	if len(got) != len(want) {
		t.Fatalf("got %d entries, want %d", len(got), len(want))
	}
	gotByID := make(map[string]string, len(got))
	for _, e := range got {
		gotByID[e.LabelID] = e.LabelName
	}
	for _, w := range want {
		if gotByID[w.LabelID] != w.LabelName {
			t.Errorf("entry %s: got name %q, want %q", w.LabelID, gotByID[w.LabelID], w.LabelName)
		}
	}
}

func TestLinearLabelSnapshotReplace(t *testing.T) {
	store, cleanup := setupTestStore(t)
	defer cleanup()

	ctx, cancel := testContext(t)
	defer cancel()

	issue := &types.Issue{
		ID:        "test-1",
		Title:     "test",
		Status:    types.StatusOpen,
		Priority:  2,
		IssueType: types.TypeTask,
	}
	if err := store.CreateIssue(ctx, issue, "tester"); err != nil {
		t.Fatalf("CreateIssue: %v", err)
	}

	first := []storage.LinearLabelSnapshotEntry{{LabelID: "lin-1", LabelName: "old"}}
	second := []storage.LinearLabelSnapshotEntry{{LabelID: "lin-2", LabelName: "new"}}

	for _, batch := range [][]storage.LinearLabelSnapshotEntry{first, second} {
		if err := store.RunInTransaction(ctx, "test", func(tx storage.Transaction) error {
			return tx.PutLinearLabelSnapshot(ctx, "test-1", batch)
		}); err != nil {
			t.Fatalf("Put: %v", err)
		}
	}

	var got []storage.LinearLabelSnapshotEntry
	if err := store.RunInTransaction(ctx, "read", func(tx storage.Transaction) error {
		var err error
		got, err = tx.GetLinearLabelSnapshot(ctx, "test-1")
		return err
	}); err != nil {
		t.Fatalf("Get: %v", err)
	}

	if len(got) != 1 || got[0].LabelID != "lin-2" || got[0].LabelName != "new" {
		t.Fatalf("expected only [{lin-2, new}], got %+v", got)
	}
}
