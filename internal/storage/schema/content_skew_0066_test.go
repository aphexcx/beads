package schema

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
)

// The two sanctioned contents of migration 0066 (gf-jr0, finding 2a): the
// bytes upstream v1.3.1 ships (every upstream-origin store, and every store
// that ran 0066 before the fork's comment edit of 2026-08-30, recorded this
// as row 66) and the bytes this lineage ships, which differ by one comment
// line naming the renumbered ignored-series twin.
const hash0066Upstream = "a524d42ba0eced9350edbf6402dc2952957ff8f75a896bc8339c4cbc8596c793"

// TestContentHashSkewAcceptsSanctionedPriorHashOf0066 pins the gf-4c9
// round-2 gate's Major (beads vs v1.3.1, 07e592d2b) at
// 0066_add_events_journal_actor.up.sql:19: the sanctioned-prior registry the
// round introduced covered 0072 only, so a store that recorded upstream's
// 0066 bytes read as a schema fork against one that recorded ours, although
// the two files differ by a comment. The pair is registered like 0072's: no
// skew either way, still skew against an unknown hash or on another version.
func TestContentHashSkewAcceptsSanctionedPriorHashOf0066(t *testing.T) {
	current := forkFileHash(t, mainSource, "0066_add_events_journal_actor.up.sql")
	if current == hash0066Upstream {
		t.Fatalf("0066 hashes %s, the same as upstream v1.3.1; the pair records nothing", current)
	}
	if got := ContentHashSkew(map[int]string{66: hash0066Upstream}, map[int]string{66: current}); len(got) != 0 {
		t.Fatalf("ContentHashSkew(upstream, current) = %v, want no skew for the sanctioned 0066 pair", got)
	}
	if got := ContentHashSkew(map[int]string{66: current}, map[int]string{66: hash0066Upstream}); len(got) != 0 {
		t.Fatalf("ContentHashSkew(current, upstream) = %v, want no skew for the sanctioned 0066 pair", got)
	}
	if !ContentHashesEquivalent(66, hash0066Upstream, current) {
		t.Fatal("ContentHashesEquivalent(66, upstream, current) = false, want the sanctioned pair")
	}
	if got := ContentHashSkew(map[int]string{66: hash0066Upstream}, map[int]string{66: strings.Repeat("ab", 32)}); !reflect.DeepEqual(got, []int{66}) {
		t.Fatalf("ContentHashSkew(upstream, unknown) = %v, want [66]", got)
	}
	if got := ContentHashSkew(map[int]string{65: hash0066Upstream}, map[int]string{65: current}); !reflect.DeepEqual(got, []int{65}) {
		t.Fatalf("ContentHashSkew on another version = %v, want [65]: the pair is sanctioned for 0066 only", got)
	}
}

// TestSmartGateAllowsTheSanctioned0066Pair is the same finding at the door a
// store opens through: the smart remote-migrate gate compares this store's
// recorded hashes with the remote's cached ref before it lets a migration
// run. A store whose row 66 is upstream's content and a remote whose row 66
// is ours must NOT be refused as a fork; one that differs in unknown content
// still is.
func TestSmartGateAllowsTheSanctioned0066Pair(t *testing.T) {
	current := forkFileHash(t, mainSource, "0066_add_events_journal_actor.up.sql")
	t.Run("upstream 0066 bytes here, ours on the remote: allowed", func(t *testing.T) {
		t.Setenv(SmartGateEnv, "1")
		t.Setenv(AllowRemoteMigrateEnv, "0")
		db, mock, _ := sqlmock.New()
		defer db.Close()
		expectSmartFiringGate(mock, 66)
		local := map[int]string{65: "h65", 66: hash0066Upstream}
		remote := map[int]string{65: "h65", 66: current}
		expectSmartRemoteRead(mock, local, remote)
		if err := CheckRemoteMigrateGate(context.Background(), db); err != nil {
			t.Fatalf("a store holding upstream's 0066 hash must open without a skew refusal, got %v", err)
		}
		if err := mock.ExpectationsWereMet(); err != nil {
			t.Fatalf("unmet expectations: %v", err)
		}
	})
	t.Run("unknown 0066 bytes on the remote: still a fork", func(t *testing.T) {
		t.Setenv(SmartGateEnv, "1")
		t.Setenv(AllowRemoteMigrateEnv, "0")
		db, mock, _ := sqlmock.New()
		defer db.Close()
		expectSmartFiringGate(mock, 66)
		local := map[int]string{65: "h65", 66: hash0066Upstream}
		remote := map[int]string{65: "h65", 66: strings.Repeat("cd", 32)}
		expectSmartRemoteRead(mock, local, remote)
		err := CheckRemoteMigrateGate(context.Background(), db)
		var gateErr *RemoteMigrateGateError
		if !errors.As(err, &gateErr) || gateErr.Decision != gateDecisionForkSkew {
			t.Fatalf("expected the fork-skew decision for unknown 0066 content, got %v", err)
		}
		if len(gateErr.SkewVersions) != 1 || gateErr.SkewVersions[0] != 66 {
			t.Fatalf("SkewVersions = %v, want [66]", gateErr.SkewVersions)
		}
	})
}
