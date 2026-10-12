package dolt

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"strings"
	"testing"

	"github.com/steveyegge/beads/internal/storage/issueops"
)

// TestBlockedRecheckDoltCommitRunsAfterTxCommit pins the gf-4c9 round-2
// gate's P1 (beads vs integrated-20260831, 07e592d2b) at
// internal/storage/dolt/store.go:1275, upstream commit e352b96d6 (#6719):
// the post-commit blocked-state recheck called doltAddAndCommitInTx inside
// its own still-open SQL transaction. DOLT_ADD stages the whole issues table
// from the session's BEGIN-time root, so a writer that committed during the
// recheck had its rows written back to their BEGIN-time values — the
// lost-update ordering TestIssueOperationDoltCommitRunsAfterTxCommit pins
// for the mutation path. The recheck now commits its SQL transaction first
// and stages and commits post-tx (doltAddAndCommitPostTx), the same
// contract: DOLT_ADD and DOLT_COMMIT strictly after the driver-level COMMIT,
// with no transaction open between the two.
func TestBlockedRecheckDoltCommitRunsAfterTxCommit(t *testing.T) {
	rec := &seqRecorder{}
	connector := &seqConnector{rec: rec}
	// The recompute pass loops until an UPDATE changes nothing: the first
	// issues UPDATE corrects one row, every later one none.
	updates := 0
	connector.execResult = func(query string) (driver.Result, bool) {
		if strings.Contains(query, "UPDATE issues") {
			updates++
			if updates == 1 {
				return driver.RowsAffected(1), true
			}
			return driver.RowsAffected(0), true
		}
		return nil, false
	}
	db := sql.OpenDB(connector)
	defer func() { _ = db.Close() }()

	s := &DoltStore{db: db}
	pending := issueops.BlockedRecheck{IssueIDs: []string{"test-1"}, Sources: []string{"close of test-0"}, SourceCount: 1}
	if err := s.recheckBlockedAfterCommit(context.Background(), pending); err != nil {
		t.Fatalf("recheckBlockedAfterCommit: %v", err)
	}

	events := rec.snapshot()
	idx := func(pred func(string) bool) int {
		for i, ev := range events {
			if pred(ev) {
				return i
			}
		}
		return -1
	}
	commitIdx := idx(func(ev string) bool { return ev == "COMMIT" })
	doltAddIdx := idx(func(ev string) bool { return strings.Contains(ev, "DOLT_ADD") })
	doltCommitIdx := idx(func(ev string) bool { return strings.Contains(ev, "DOLT_COMMIT") })
	if commitIdx == -1 {
		t.Fatalf("no driver-level tx COMMIT recorded; events: %v", events)
	}
	if doltAddIdx == -1 || doltCommitIdx == -1 {
		t.Fatalf("the recheck changed a row but minted no DOLT_ADD/DOLT_COMMIT; events: %v", events)
	}
	if doltAddIdx < commitIdx {
		t.Errorf("DOLT_ADD ran INSIDE the recheck's open transaction (index %d < COMMIT index %d) — lost-update ordering; events: %v",
			doltAddIdx, commitIdx, events)
	}
	if doltCommitIdx < commitIdx {
		t.Errorf("DOLT_COMMIT ran INSIDE the recheck's open transaction (index %d < COMMIT index %d) — lost-update ordering; events: %v",
			doltCommitIdx, commitIdx, events)
	}
	if doltCommitIdx < doltAddIdx {
		t.Errorf("DOLT_COMMIT before DOLT_ADD; events: %v", events)
	}
	for i := commitIdx + 1; i < doltCommitIdx; i++ {
		if events[i] == "BEGIN" {
			t.Errorf("unexpected BEGIN between the recheck's COMMIT and its DOLT_COMMIT at index %d; events: %v", i, events)
		}
	}
}

// TestBlockedRecheckWithoutChangesMintsNoDoltCommit pins the other half of
// the recheck's contract: a recompute that corrects no row leaves no Dolt
// commit behind, before and after the ordering change.
func TestBlockedRecheckWithoutChangesMintsNoDoltCommit(t *testing.T) {
	rec := &seqRecorder{}
	connector := &seqConnector{rec: rec}
	connector.execResult = func(query string) (driver.Result, bool) {
		if strings.Contains(query, "UPDATE issues") {
			return driver.RowsAffected(0), true
		}
		return nil, false
	}
	db := sql.OpenDB(connector)
	defer func() { _ = db.Close() }()

	s := &DoltStore{db: db}
	pending := issueops.BlockedRecheck{IssueIDs: []string{"test-1"}, Sources: []string{"close of test-0"}, SourceCount: 1}
	if err := s.recheckBlockedAfterCommit(context.Background(), pending); err != nil {
		t.Fatalf("recheckBlockedAfterCommit: %v", err)
	}
	for _, ev := range rec.snapshot() {
		if strings.Contains(ev, "DOLT_ADD") || strings.Contains(ev, "DOLT_COMMIT") {
			t.Fatalf("a recheck that changed nothing staged or committed; events: %v", rec.snapshot())
		}
	}
}
