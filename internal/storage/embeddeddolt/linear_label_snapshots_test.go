//go:build cgo

package embeddeddolt_test

import (
	"testing"

	"github.com/steveyegge/beads/internal/storage"
	"github.com/steveyegge/beads/internal/types"
)

func TestLinearLabelSnapshotCRUD(t *testing.T) {
	skipUnlessEmbeddedDolt(t)

	te := newTestEnv(t, "lls")
	ctx := t.Context()

	// Seed a bead so the FK is satisfied.
	issue := &types.Issue{
		ID:        "lls-1",
		Title:     "test",
		Status:    types.StatusOpen,
		Priority:  2,
		IssueType: types.TypeTask,
	}
	if err := te.store.CreateIssue(ctx, issue, "tester"); err != nil {
		t.Fatalf("CreateIssue: %v", err)
	}

	want := []storage.LinearLabelSnapshotEntry{
		{LabelID: "lin-1", LabelName: "bug"},
		{LabelID: "lin-2", LabelName: "p1"},
	}

	if err := te.store.RunInTransaction(ctx, "test write", func(tx storage.Transaction) error {
		return tx.PutLinearLabelSnapshot(ctx, "lls-1", want)
	}); err != nil {
		t.Fatalf("Put: %v", err)
	}

	var got []storage.LinearLabelSnapshotEntry
	if err := te.store.RunInTransaction(ctx, "test read", func(tx storage.Transaction) error {
		var err error
		got, err = tx.GetLinearLabelSnapshot(ctx, "lls-1")
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
	skipUnlessEmbeddedDolt(t)

	te := newTestEnv(t, "llr")
	ctx := t.Context()

	issue := &types.Issue{
		ID:        "llr-1",
		Title:     "test",
		Status:    types.StatusOpen,
		Priority:  2,
		IssueType: types.TypeTask,
	}
	if err := te.store.CreateIssue(ctx, issue, "tester"); err != nil {
		t.Fatalf("CreateIssue: %v", err)
	}

	first := []storage.LinearLabelSnapshotEntry{{LabelID: "lin-1", LabelName: "old"}}
	second := []storage.LinearLabelSnapshotEntry{{LabelID: "lin-2", LabelName: "new"}}

	for _, batch := range [][]storage.LinearLabelSnapshotEntry{first, second} {
		if err := te.store.RunInTransaction(ctx, "test", func(tx storage.Transaction) error {
			return tx.PutLinearLabelSnapshot(ctx, "llr-1", batch)
		}); err != nil {
			t.Fatalf("Put: %v", err)
		}
	}

	var got []storage.LinearLabelSnapshotEntry
	if err := te.store.RunInTransaction(ctx, "read", func(tx storage.Transaction) error {
		var err error
		got, err = tx.GetLinearLabelSnapshot(ctx, "llr-1")
		return err
	}); err != nil {
		t.Fatalf("Get: %v", err)
	}

	if len(got) != 1 || got[0].LabelID != "lin-2" || got[0].LabelName != "new" {
		t.Fatalf("expected only [{lin-2, new}], got %+v", got)
	}
}

func TestLinearLabelSnapshotClearOnEmpty(t *testing.T) {
	skipUnlessEmbeddedDolt(t)

	te := newTestEnv(t, "llc")
	ctx := t.Context()

	issue := &types.Issue{
		ID:        "llc-1",
		Title:     "test",
		Status:    types.StatusOpen,
		Priority:  2,
		IssueType: types.TypeTask,
	}
	if err := te.store.CreateIssue(ctx, issue, "tester"); err != nil {
		t.Fatalf("CreateIssue: %v", err)
	}

	// Seed with one entry, then clear with nil and assert empty.
	seeded := []storage.LinearLabelSnapshotEntry{{LabelID: "lin-1", LabelName: "bug"}}
	if err := te.store.RunInTransaction(ctx, "seed", func(tx storage.Transaction) error {
		return tx.PutLinearLabelSnapshot(ctx, "llc-1", seeded)
	}); err != nil {
		t.Fatalf("Put seed: %v", err)
	}
	if err := te.store.RunInTransaction(ctx, "clear", func(tx storage.Transaction) error {
		return tx.PutLinearLabelSnapshot(ctx, "llc-1", nil)
	}); err != nil {
		t.Fatalf("Put nil: %v", err)
	}

	var got []storage.LinearLabelSnapshotEntry
	if err := te.store.RunInTransaction(ctx, "read", func(tx storage.Transaction) error {
		var err error
		got, err = tx.GetLinearLabelSnapshot(ctx, "llc-1")
		return err
	}); err != nil {
		t.Fatalf("Get: %v", err)
	}
	if len(got) != 0 {
		t.Fatalf("expected empty after clear, got %+v", got)
	}
}
