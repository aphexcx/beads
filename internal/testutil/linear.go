package testutil

import (
	"os"
	"strings"
	"testing"
)

// RequireLinearFixture honors BEADS_TEST_SKIP=linear-fixture for tests using
// local Linear fixtures. These tests need no live Linear credentials; their
// existing build tags and store setup still enforce storage prerequisites.
func RequireLinearFixture(t *testing.T) {
	t.Helper()
	for _, service := range strings.Split(os.Getenv("BEADS_TEST_SKIP"), ",") {
		if strings.TrimSpace(service) == "linear-fixture" {
			t.Skip("skipping: local Linear fixture tests skipped (BEADS_TEST_SKIP=linear-fixture)")
		}
	}
}
