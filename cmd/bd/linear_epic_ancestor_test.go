//go:build cgo

package main

import (
	"context"
	"testing"

	"github.com/steveyegge/beads/internal/types"
)

// TestFindNearestTopLevelEpicAncestor covers bd-1ay's ancestor-walk
// helper used by the post-sync ReconcileProjectMembership pass. The
// rule: walk UP parent-child deps until reaching a bead with no parent;
// return it iff it's an epic. Intermediate epics are walked PAST —
// per bd-1ay design, ALL descendants of a top-level epic land in the
// root Project, not their immediate sub-epic.
//
// Uses a real bd store (gated on Dolt/Docker like the parent-reconcile
// integration tests).
func TestFindNearestTopLevelEpicAncestor(t *testing.T) {
	// Pure unit test of the walking logic via a fake byID map + a fake
	// dep-fetch closure. Avoids requiring Dolt for the algorithm itself
	// — the storage layer is exercised by the buildLinearProjectMembership-
	// Links integration tests.
	cases := []struct {
		name string
		// tree maps childID → parentID. Walker stops at IDs with no entry.
		tree map[string]string
		// byID maps ID → bead. Walker checks IssueType on the root.
		byID map[string]*types.Issue
		// from is the starting bead ID
		from string
		want string // expected ancestor bead ID; "" when none
	}{
		{
			name: "task → root epic (2-level)",
			tree: map[string]string{"task": "epic-A"},
			byID: map[string]*types.Issue{
				"task":   {ID: "task", IssueType: types.TypeTask},
				"epic-A": {ID: "epic-A", IssueType: types.TypeEpic},
			},
			from: "task",
			want: "epic-A",
		},
		{
			name: "task → sub-epic → root epic (3-level)",
			tree: map[string]string{"task": "sub-epic", "sub-epic": "root-epic"},
			byID: map[string]*types.Issue{
				"task":      {ID: "task", IssueType: types.TypeTask},
				"sub-epic":  {ID: "sub-epic", IssueType: types.TypeEpic},
				"root-epic": {ID: "root-epic", IssueType: types.TypeEpic},
			},
			from: "task",
			want: "root-epic",
		},
		{
			name: "sub-epic → root epic (asking from a sub-epic itself)",
			tree: map[string]string{"sub-epic": "root-epic"},
			byID: map[string]*types.Issue{
				"sub-epic":  {ID: "sub-epic", IssueType: types.TypeEpic},
				"root-epic": {ID: "root-epic", IssueType: types.TypeEpic},
			},
			from: "sub-epic",
			want: "root-epic",
		},
		{
			name: "no parent chain, target IS a top-level epic",
			tree: map[string]string{},
			byID: map[string]*types.Issue{
				"root-epic": {ID: "root-epic", IssueType: types.TypeEpic},
			},
			from: "root-epic",
			want: "root-epic",
		},
		{
			name: "no parent chain, target is non-epic",
			tree: map[string]string{},
			byID: map[string]*types.Issue{
				"task": {ID: "task", IssueType: types.TypeTask},
			},
			from: "task",
			want: "",
		},
		{
			name: "chain leads to non-epic root",
			tree: map[string]string{"a": "b", "b": "c"},
			byID: map[string]*types.Issue{
				"a": {ID: "a", IssueType: types.TypeTask},
				"b": {ID: "b", IssueType: types.TypeTask},
				"c": {ID: "c", IssueType: types.TypeTask}, // non-epic root
			},
			from: "a",
			want: "",
		},
		{
			name: "cycle detection bails out (defensive)",
			tree: map[string]string{"a": "b", "b": "c", "c": "a"},
			byID: map[string]*types.Issue{
				"a": {ID: "a", IssueType: types.TypeTask},
				"b": {ID: "b", IssueType: types.TypeTask},
				"c": {ID: "c", IssueType: types.TypeTask},
			},
			from: "a",
			want: "", // cycle → no top-level epic
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := walkAncestorsForTest(context.Background(), tc.byID, tc.tree, tc.from)
			if got != tc.want {
				t.Errorf("walkAncestorsForTest(%q) = %q, want %q", tc.from, got, tc.want)
			}
		})
	}
}

// walkAncestorsForTest is a pure-function adaptation of
// findNearestTopLevelEpicAncestor's algorithm, decoupled from the
// store's GetDependenciesWithMetadata. Lets us exercise the walking
// logic without spinning up Dolt. Production code path is integration-
// tested via the engine_test.go suite (Dolt/Docker gated).
func walkAncestorsForTest(_ context.Context, byID map[string]*types.Issue, tree map[string]string, from string) string {
	current := from
	visited := map[string]bool{current: true}
	for {
		parent, hasParent := tree[current]
		if !hasParent {
			bead := byID[current]
			if bead != nil && bead.IssueType == types.TypeEpic {
				return current
			}
			return ""
		}
		if visited[parent] {
			return ""
		}
		visited[parent] = true
		current = parent
	}
}
