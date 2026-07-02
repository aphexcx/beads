package tracker

import (
	"testing"
)

// bd-6cl P5 unit tests for the pull-side descendant-dep wiring
// helpers. Integration coverage (full Sync with materialized
// epics and wired children) lives in P6.

func TestPulledIssueProjectID(t *testing.T) {
	cases := []struct {
		name string
		ti   TrackerIssue
		want string
	}{
		{"nil metadata", TrackerIssue{}, ""},
		{"empty metadata", TrackerIssue{Metadata: map[string]interface{}{}}, ""},
		{"present", TrackerIssue{Metadata: map[string]interface{}{"project_id": "uuid-1"}}, "uuid-1"},
		{"wrong type", TrackerIssue{Metadata: map[string]interface{}{"project_id": 42}}, ""},
		{"other keys only", TrackerIssue{Metadata: map[string]interface{}{"other": "v"}}, ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := pulledIssueProjectID(tc.ti); got != tc.want {
				t.Errorf("got %q, want %q", got, tc.want)
			}
		})
	}
}
