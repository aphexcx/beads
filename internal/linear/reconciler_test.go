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
