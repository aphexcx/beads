package linear

import (
	"reflect"
	"sort"
	"testing"
)

func sortedNames(xs []string) []string {
	out := append([]string(nil), xs...)
	sort.Strings(out)
	return out
}

func TestApplyExclusionFilter(t *testing.T) {
	exclude := map[string]bool{"bug": true, "secret": true}

	in := LabelReconcileInput{
		Beads: []string{"bug", "p1", "secret", "Visible"},
		Linear: []LinearLabel{
			{Name: "BUG", ID: "L1"},
			{Name: "regression", ID: "L2"},
		},
		Snapshot: []SnapshotEntry{
			{Name: "secret", ID: "L0"},
			{Name: "p1", ID: "L9"},
		},
		Exclude: exclude,
	}

	beads, linear, snap := applyExclusionFilter(in)
	if got, want := sortedNames(beads), []string{"Visible", "p1"}; !reflect.DeepEqual(got, want) {
		t.Errorf("beads after filter: got %v, want %v", got, want)
	}
	if len(linear) != 1 || linear[0].Name != "regression" {
		t.Errorf("linear after filter: got %+v, want only [regression]", linear)
	}
	if len(snap) != 1 || snap[0].Name != "p1" {
		t.Errorf("snapshot after filter: got %+v, want only [p1]", snap)
	}
}

func TestClassifyRenames_AppliedRename(t *testing.T) {
	beads := []string{"old", "other"}
	linear := []LinearLabel{
		{Name: "new", ID: "X"},
		{Name: "other", ID: "Y"},
	}
	snap := []SnapshotEntry{
		{Name: "old", ID: "X"},
		{Name: "other", ID: "Y"},
	}
	r := classifyRenames(beads, linear, snap)
	if len(r.applied) != 1 {
		t.Fatalf("applied: got %d, want 1", len(r.applied))
	}
	if r.applied[0].OldName != "old" || r.applied[0].NewName != "new" || r.applied[0].ID != "X" {
		t.Errorf("applied[0]: got %+v", r.applied[0])
	}
	if !r.consumedSnapshotID["X"] || !r.consumedLinearID["X"] || !r.consumedBeadsName["old"] {
		t.Errorf("consumption flags wrong: %+v", r)
	}
}

func TestClassifyRenames_DroppedRename(t *testing.T) {
	beads := []string{"other"} // user deleted "old"
	linear := []LinearLabel{
		{Name: "new", ID: "X"},
		{Name: "other", ID: "Y"},
	}
	snap := []SnapshotEntry{
		{Name: "old", ID: "X"},
		{Name: "other", ID: "Y"},
	}
	r := classifyRenames(beads, linear, snap)
	if len(r.dropped) != 1 || r.dropped[0].ID != "X" {
		t.Fatalf("dropped: got %+v, want one entry for X", r.dropped)
	}
	if !r.consumedSnapshotID["X"] || !r.consumedLinearID["X"] {
		t.Errorf("consumption flags wrong: %+v", r)
	}
}

func TestClassifyRenames_DroppedRenameWithLocalReAdd(t *testing.T) {
	// User deleted "old" and independently added "new". Linear renamed old→new.
	// Pass-2 should consume the new-name beads row as well, so pass-3 doesn't
	// see "new" as a fresh add either way.
	beads := []string{"new"}
	linear := []LinearLabel{{Name: "new", ID: "X"}}
	snap := []SnapshotEntry{{Name: "old", ID: "X"}}
	r := classifyRenames(beads, linear, snap)
	if len(r.dropped) != 1 {
		t.Fatalf("dropped: got %d, want 1", len(r.dropped))
	}
	if !r.consumedBeadsName["new"] {
		t.Errorf("expected consumedBeadsName[new]=true to suppress add, got false")
	}
}

func TestClassifyRenames_CaseMismatchWithRenameDoesNotDelete(t *testing.T) {
	// Regression for the case-mismatch + rename data-loss bug:
	// snapshot "Bug" (Linear's case from prior sync), bead "bug" (mismatched case),
	// Linear renames the label to "flaky" (same ID L1). Without case-insensitive
	// beadsSet lookup, this would classify as DROPPED rename and emit
	// RemoveFromLinear[L1], destroying Linear's label. With the fix, it
	// correctly classifies as APPLIED rename and the bead's "bug" gets
	// renamed to "flaky".
	beads := []string{"bug"}
	linear := []LinearLabel{{Name: "flaky", ID: "L1"}}
	snap := []SnapshotEntry{{Name: "Bug", ID: "L1"}}
	r := classifyRenames(beads, linear, snap)
	if len(r.applied) != 1 {
		t.Fatalf("applied: got %d, want 1 (case-insensitive match should classify as applied, not dropped)", len(r.applied))
	}
	if r.applied[0].OldName != "bug" || r.applied[0].NewName != "flaky" {
		t.Errorf("applied[0]: got %+v, want OldName=bug NewName=flaky", r.applied[0])
	}
	if len(r.dropped) != 0 {
		t.Errorf("dropped: got %d, want 0 (must not destroy Linear label on casing mismatch)", len(r.dropped))
	}
	if !r.consumedBeadsName["bug"] {
		t.Errorf("expected consumedBeadsName[bug]=true (bead's actual spelling)")
	}
}

func TestApplyTruthTable_AllSevenRows(t *testing.T) {
	cases := []struct {
		name                string
		snap, beads, linear []string
		wantAddBeads        []string
		wantRemoveBeads     []string
		wantAddLinear       []string
		wantRemoveLinearIDs []string
	}{
		{
			name: "in_agreement_unchanged",
			snap: []string{"x"}, beads: []string{"x"}, linear: []string{"x"},
		},
		{
			name: "added_in_beads",
			snap: []string{}, beads: []string{"x"}, linear: []string{},
			wantAddLinear: []string{"x"},
		},
		{
			name: "added_in_linear",
			snap: []string{}, beads: []string{}, linear: []string{"x"},
			wantAddBeads: []string{"x"},
		},
		{
			name: "added_both_sides_in_agreement",
			snap: []string{}, beads: []string{"x"}, linear: []string{"x"},
		},
		{
			name: "removed_in_beads",
			snap: []string{"x"}, beads: []string{}, linear: []string{"x"},
			wantRemoveLinearIDs: []string{"id-x"},
		},
		{
			name: "removed_in_linear",
			snap: []string{"x"}, beads: []string{"x"}, linear: []string{},
			wantRemoveBeads: []string{"x"},
		},
		{
			name: "removed_both_sides_in_agreement",
			snap: []string{"x"}, beads: []string{}, linear: []string{},
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			beads := c.beads
			linear := make([]LinearLabel, len(c.linear))
			for i, n := range c.linear {
				linear[i] = LinearLabel{Name: n, ID: "id-" + n}
			}
			snap := make([]SnapshotEntry, len(c.snap))
			for i, n := range c.snap {
				snap[i] = SnapshotEntry{Name: n, ID: "id-" + n}
			}

			res := applyTruthTable(beads, linear, snap, renameClass{
				consumedSnapshotID: map[string]bool{},
				consumedLinearID:   map[string]bool{},
				consumedBeadsName:  map[string]bool{},
			})

			if !reflect.DeepEqual(sortedNames(res.AddToBeads), sortedNames(c.wantAddBeads)) {
				t.Errorf("AddToBeads: got %v, want %v", res.AddToBeads, c.wantAddBeads)
			}
			if !reflect.DeepEqual(sortedNames(res.RemoveFromBeads), sortedNames(c.wantRemoveBeads)) {
				t.Errorf("RemoveFromBeads: got %v, want %v", res.RemoveFromBeads, c.wantRemoveBeads)
			}
			if !reflect.DeepEqual(sortedNames(res.AddToLinear), sortedNames(c.wantAddLinear)) {
				t.Errorf("AddToLinear: got %v, want %v", res.AddToLinear, c.wantAddLinear)
			}
			if !reflect.DeepEqual(sortedNames(res.RemoveFromLinear), sortedNames(c.wantRemoveLinearIDs)) {
				t.Errorf("RemoveFromLinear: got %v, want %v", res.RemoveFromLinear, c.wantRemoveLinearIDs)
			}
		})
	}
}

func TestApplyTruthTable_RespectsConsumption(t *testing.T) {
	// Snapshot has X (consumed by pass-2), beads has X, linear has X — would
	// normally be "in agreement" but consumption removes it from consideration.
	res := applyTruthTable(
		[]string{"x"},
		[]LinearLabel{{Name: "x", ID: "id-x"}},
		[]SnapshotEntry{{Name: "x", ID: "id-x"}},
		renameClass{
			consumedSnapshotID: map[string]bool{"id-x": true},
			consumedLinearID:   map[string]bool{"id-x": true},
			consumedBeadsName:  map[string]bool{},
		},
	)
	// Both Linear and snapshot rows are consumed; beads row remains and looks
	// like "added in beads, not in snapshot, not in linear" — which would push.
	if len(res.AddToLinear) != 1 || res.AddToLinear[0] != "x" {
		t.Errorf("expected AddToLinear=[x] after Linear+snapshot consumed, got %+v", res.AddToLinear)
	}
}

func TestSynthesizeFirstSyncSnapshot(t *testing.T) {
	beads := []string{"a", "b"}
	linear := []LinearLabel{
		{Name: "a", ID: "ID-A"},
		{Name: "c", ID: "ID-C"},
	}
	got := synthesizeFirstSyncSnapshot(beads, linear)
	if len(got) != 1 || got[0].Name != "a" || got[0].ID != "ID-A" {
		t.Fatalf("expected intersection [{a, ID-A}], got %+v", got)
	}
}

func TestSynthesizeFirstSyncSnapshot_NoOverlap(t *testing.T) {
	got := synthesizeFirstSyncSnapshot([]string{"a"}, []LinearLabel{{Name: "b", ID: "ID-B"}})
	if len(got) != 0 {
		t.Fatalf("expected empty intersection, got %+v", got)
	}
}

func TestReconcileLabels_FirstSyncIntersectionPreservesBoth(t *testing.T) {
	// Empty snapshot, bead has [A, B], Linear has [A] → first-sync rule.
	// Expect: nothing removed; B pushed; new snapshot covers what's currently agreed.
	res := ReconcileLabels(LabelReconcileInput{
		Beads:    []string{"A", "B"},
		Linear:   []LinearLabel{{Name: "A", ID: "lin-A"}},
		Snapshot: nil,
	})
	if len(res.RemoveFromBeads) != 0 || len(res.RemoveFromLinear) != 0 {
		t.Errorf("first-sync should remove nothing, got removeBeads=%v removeLinear=%v",
			res.RemoveFromBeads, res.RemoveFromLinear)
	}
	if !reflect.DeepEqual(sortedNames(res.AddToLinear), []string{"B"}) {
		t.Errorf("AddToLinear: got %v, want [B]", res.AddToLinear)
	}
	if len(res.AddToBeads) != 0 {
		t.Errorf("AddToBeads should be empty (A in agreement), got %v", res.AddToBeads)
	}
	// New snapshot reflects what's CURRENTLY agreed; pushed B has no Linear
	// ID yet, so the orchestrator emits a snapshot containing only A. The
	// caller writes the post-push-resolved snapshot separately.
	if len(res.NewSnapshot) != 1 || res.NewSnapshot[0].Name != "A" {
		t.Errorf("NewSnapshot: got %+v, want one entry for A", res.NewSnapshot)
	}
}

func TestReconcileLabels_AppliedRename(t *testing.T) {
	res := ReconcileLabels(LabelReconcileInput{
		Beads:    []string{"old"},
		Linear:   []LinearLabel{{Name: "new", ID: "X"}},
		Snapshot: []SnapshotEntry{{Name: "old", ID: "X"}},
	})
	if !reflect.DeepEqual(sortedNames(res.RemoveFromBeads), []string{"old"}) {
		t.Errorf("RemoveFromBeads: got %v, want [old]", res.RemoveFromBeads)
	}
	if !reflect.DeepEqual(sortedNames(res.AddToBeads), []string{"new"}) {
		t.Errorf("AddToBeads: got %v, want [new]", res.AddToBeads)
	}
	if len(res.RenamesApplied) != 1 || res.RenamesApplied[0].ID != "X" {
		t.Errorf("RenamesApplied: got %+v", res.RenamesApplied)
	}
	if len(res.NewSnapshot) != 1 || res.NewSnapshot[0].Name != "new" || res.NewSnapshot[0].ID != "X" {
		t.Errorf("NewSnapshot: got %+v, want [{new, X}]", res.NewSnapshot)
	}
}

func TestReconcileLabels_DroppedRenameDeleteWins(t *testing.T) {
	// Decision #10 — user deleted "old" locally, Linear renamed to "new".
	// Delete wins: RemoveFromLinear, no AddToBeads.
	res := ReconcileLabels(LabelReconcileInput{
		Beads:    []string{},
		Linear:   []LinearLabel{{Name: "new", ID: "X"}},
		Snapshot: []SnapshotEntry{{Name: "old", ID: "X"}},
	})
	if !reflect.DeepEqual(sortedNames(res.RemoveFromLinear), []string{"X"}) {
		t.Errorf("RemoveFromLinear: got %v, want [X]", res.RemoveFromLinear)
	}
	if len(res.AddToBeads) != 0 {
		t.Errorf("AddToBeads should be empty (delete wins), got %v", res.AddToBeads)
	}
	if len(res.NewSnapshot) != 0 {
		t.Errorf("NewSnapshot: got %+v, want empty", res.NewSnapshot)
	}
}

func TestReconcileLabels_DroppedRenameWithLocalReAdd(t *testing.T) {
	res := ReconcileLabels(LabelReconcileInput{
		Beads:    []string{"new"},
		Linear:   []LinearLabel{{Name: "new", ID: "X"}},
		Snapshot: []SnapshotEntry{{Name: "old", ID: "X"}},
	})
	// Pass-2 consumed beads row "new" (suppressing AddToBeads), Linear row X,
	// snapshot row X. Pass-3 sees nothing. End-state in agreement.
	if len(res.AddToBeads) != 0 || len(res.AddToLinear) != 0 ||
		len(res.RemoveFromBeads) != 0 || len(res.RemoveFromLinear) != 0 {
		t.Errorf("expected no changes, got %+v", res)
	}
	if len(res.NewSnapshot) != 1 || res.NewSnapshot[0].Name != "new" || res.NewSnapshot[0].ID != "X" {
		t.Errorf("NewSnapshot: got %+v, want [{new, X}]", res.NewSnapshot)
	}
}

func TestReconcileLabels_OldDeleteNewAddIndependent(t *testing.T) {
	// No ID match — these are independent labels, not a rename.
	res := ReconcileLabels(LabelReconcileInput{
		Beads:    []string{"bar"},
		Linear:   []LinearLabel{{Name: "foo", ID: "F"}},
		Snapshot: []SnapshotEntry{{Name: "foo", ID: "F"}},
	})
	if !reflect.DeepEqual(sortedNames(res.RemoveFromLinear), []string{"F"}) {
		t.Errorf("RemoveFromLinear: got %v, want [F]", res.RemoveFromLinear)
	}
	if !reflect.DeepEqual(sortedNames(res.AddToLinear), []string{"bar"}) {
		t.Errorf("AddToLinear: got %v, want [bar]", res.AddToLinear)
	}
}

func TestReconcileLabels_StandardThreeWayMerge(t *testing.T) {
	// Snapshot has [A, B]. Linear added C, removed B. Beads added D, removed A.
	// Expect: A removed from Linear, B removed from beads, C added to beads,
	// D added to Linear.
	res := ReconcileLabels(LabelReconcileInput{
		Beads:  []string{"B", "D"},
		Linear: []LinearLabel{{Name: "A", ID: "ia"}, {Name: "C", ID: "ic"}},
		Snapshot: []SnapshotEntry{
			{Name: "A", ID: "ia"},
			{Name: "B", ID: "ib"},
		},
	})
	if !reflect.DeepEqual(sortedNames(res.RemoveFromLinear), []string{"ia"}) {
		t.Errorf("RemoveFromLinear: got %v, want [ia]", res.RemoveFromLinear)
	}
	if !reflect.DeepEqual(sortedNames(res.RemoveFromBeads), []string{"B"}) {
		t.Errorf("RemoveFromBeads: got %v, want [B]", res.RemoveFromBeads)
	}
	if !reflect.DeepEqual(sortedNames(res.AddToBeads), []string{"C"}) {
		t.Errorf("AddToBeads: got %v, want [C]", res.AddToBeads)
	}
	if !reflect.DeepEqual(sortedNames(res.AddToLinear), []string{"D"}) {
		t.Errorf("AddToLinear: got %v, want [D]", res.AddToLinear)
	}
}

func TestReconcileLabels_EmptyInputs(t *testing.T) {
	res := ReconcileLabels(LabelReconcileInput{})
	if len(res.AddToBeads)+len(res.RemoveFromBeads)+len(res.AddToLinear)+len(res.RemoveFromLinear) != 0 {
		t.Errorf("expected no changes, got %+v", res)
	}
	if len(res.NewSnapshot) != 0 {
		t.Errorf("NewSnapshot: got %+v, want empty", res.NewSnapshot)
	}
}
