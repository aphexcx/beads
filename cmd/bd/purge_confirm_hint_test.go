package main

import (
	"strings"
	"testing"

	"github.com/steveyegge/beads/issueops"
)

// TestSweepConfirmHintCarriesEverySelector pins the confirmation's suggested
// command (gf-c7n finding 2i, the gf-xid vs-integrated gate's Major at
// cmd/bd/purge.go:213, upstream's --limit feature): `bd purge --wisps-plane
// --older-than 168h --limit 1` previewed one deletion, but the command the
// confirmation suggested dropped `--limit 1`, and following it deleted every
// eligible row, no-history beads included. The hint is the caller's own
// command with --force added: every selector the caller gave is carried, the
// limit among them.
func TestSweepConfirmHintCarriesEverySelector(t *testing.T) {
	purge := purgeScope{cmdName: "purge", tier: issueops.SweepWispsPlane}
	prune := purgeScope{cmdName: "prune", tier: issueops.SweepDurable}
	tests := []struct {
		name      string
		scope     purgeScope
		olderThan string
		pattern   string
		limit     int
		want      string
	}{
		{"the gate's command", purge, "168h", "", 1, "bd purge --force --wisps-plane --older-than 168h --limit 1"},
		{"a larger limit", purge, "7d", "", 500, "bd purge --force --wisps-plane --older-than 7d --limit 500"},
		{"no limit asked", purge, "168h", "", 0, "bd purge --force --wisps-plane --older-than 168h"},
		{"pattern and limit", purge, "", "wi-*", 3, "bd purge --force --wisps-plane --pattern wi-* --limit 3"},
		{"prune defines no limit", prune, "30d", "", 0, "bd prune --force --older-than 30d"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := sweepConfirmHint(tt.scope, tt.olderThan, tt.pattern, tt.limit); got != tt.want {
				t.Errorf("sweepConfirmHint() = %q, want %q", got, tt.want)
			}
		})
	}
}

// TestEmitSweepConfirmSuggestsTheLimit drives the confirmation itself: the
// hint on stderr names the limit the caller gave.
func TestEmitSweepConfirmSuggestsTheLimit(t *testing.T) {
	scope := purgeScope{cmdName: "purge", subjectNoun: "closed wisps-plane bead", tier: issueops.SweepWispsPlane}
	result := issueops.SweepResult{Swept: 1, Remaining: 1}
	var err error
	stderr := captureStderr(t, func() {
		err = emitSweepConfirm(scope, "168h", "", 1, result)
	})
	if err == nil {
		t.Fatal("emitSweepConfirm returned nil, want the exit error that asks for --force")
	}
	if !strings.Contains(stderr, "bd purge --force --wisps-plane --older-than 168h --limit 1") {
		t.Errorf("the confirmation's suggested command dropped --limit 1:\n%s", stderr)
	}
}
