package testutil

import "testing"

func TestRequireLinearFixture(t *testing.T) {
	for _, tc := range []struct {
		name     string
		skip     string
		wantSkip bool
	}{
		{name: "enabled by default"},
		{name: "explicit opt out", skip: "linear-fixture", wantSkip: true},
		{name: "comma separated with whitespace", skip: "dolt, linear-fixture ,slow", wantSkip: true},
		{name: "unrelated service", skip: "dolt"},
		{name: "exact name required", skip: "linear-fixture-extra"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv("BEADS_TEST_SKIP", tc.skip)
			ran := false
			t.Run("fixture", func(t *testing.T) {
				RequireLinearFixture(t)
				ran = true
			})
			if ran == tc.wantSkip {
				t.Errorf("fixture body ran = %v, want skip = %v", ran, tc.wantSkip)
			}
		})
	}
}
