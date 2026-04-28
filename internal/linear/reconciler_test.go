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
