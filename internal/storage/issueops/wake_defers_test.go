package issueops

import (
	"strings"
	"testing"
)

func TestDeferWakeOwner(t *testing.T) {
	homes := map[string]string{"hw": "citadel", "beads-vscode": "laptop"}
	homeOf := func(prefix string) string { return homes[prefix] }
	cases := []struct{ name, id, label, want string }{
		{"owner label wins over the prefix home", "hw-abc", "owner:jadegate", "jadegate"},
		{"owner label is trimmed", "hw-abc", "owner: citadel ", "citadel"},
		{"unlabelled row belongs to the prefix home", "hw-abc", "", "citadel"},
		{"empty owner value is no label: the prefix home applies", "hw-abc", "owner:", "citadel"},
		{"empty owner value with no prefix home has no owner", "gp-abc", "owner: ", ""},
		{"wisp ids resolve their store prefix", "hw-wisp-abc", "", "citadel"},
		{"longest declared prefix wins", "beads-vscode-1", "", "laptop"},
		{"undeclared prefix has no owner", "gp-abc", "", ""},
		{"id without a hyphen has no owner", "abc", "", ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := deferWakeOwner(tc.id, tc.label, homeOf); got != tc.want {
				t.Fatalf("deferWakeOwner(%q, %q) = %q, want %q", tc.id, tc.label, got, tc.want)
			}
		})
	}
}

func TestFormatDeferWakeSkipSummary(t *testing.T) {
	line := formatDeferWakeSkipSummary([]DeferWakeSkip{
		{ID: "hw-1", Owner: "citadel"},
		{ID: "hw-2", Owner: ""},
		{ID: "hw-3", Owner: "citadel"},
	}, "jadegate")
	for _, want := range []string{
		`defer-wake: skipped 3 expired dated defers not owned by this node ("jadegate"): `,
		`"citadel" (2), no owner label and no federation.prefix_home.<prefix> (1).`,
	} {
		if !strings.Contains(line, want) {
			t.Errorf("summary missing %q:\n%s", want, line)
		}
	}
	if line := formatDeferWakeSkipSummary([]DeferWakeSkip{{ID: "hw-1", Owner: "citadel"}}, ""); !strings.Contains(line, "(node_id unset)") {
		t.Errorf("summary for a node without node_id should say so:\n%s", line)
	}
}
