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
