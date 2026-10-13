package schema

import (
	"context"
	"errors"
	"strings"
	"testing"

	sqlmock "github.com/DATA-DOG/go-sqlmock"
)

// gf-c7n finding 2h (the gf-xid vs-v1.3.1 gate's Major at
// fork_reconcile.go:579): the planned cursor rewrites were applied as one
// autocommitted DELETE each, so a pass cut between the bd-dn6 main DELETE
// (rows 51-54) and the ignored DELETE (rows 10-11) left row 54 gone and row
// 11 standing; the retry, reading the ignored fingerprint only beside the
// main one, planned nothing for the ignored chain. The DELETEs of every
// planned chain now share one transaction on the pass's connection.

func forkLineageRewritesForTest() []cursorRewrite {
	return []cursorRewrite{
		{desc: "fork main cursor", table: "schema_migrations", lo: 51, hi: 54},
		{desc: "fork ignored cursor", table: "ignored_schema_migrations", lo: 10, hi: 11},
	}
}

func expectCursorDelete(mock sqlmock.Sqlmock, table string, lo, hi int) {
	mock.ExpectExec(`DELETE FROM `+table+` WHERE version BETWEEN \? AND \?`).
		WithArgs(lo, hi).
		WillReturnResult(sqlmock.NewResult(0, int64(hi-lo+1)))
}

// A cut between the two DELETEs rolls the first one back: no chain's
// fingerprint row outlives the other's.
func TestApplyForkLineageRewrites_CutBetweenDeletesRollsTheFirstBack(t *testing.T) {
	db, mock := newMockDB(t)
	cut := errors.New("connection cut")

	mock.ExpectExec(`^START TRANSACTION$`).WillReturnResult(sqlmock.NewResult(0, 0))
	expectCursorDelete(mock, "schema_migrations", 51, 54)
	mock.ExpectExec(`DELETE FROM ignored_schema_migrations WHERE version BETWEEN \? AND \?`).
		WithArgs(10, 11).
		WillReturnError(cut)
	mock.ExpectExec(`^ROLLBACK$`).WillReturnResult(sqlmock.NewResult(0, 0))

	changed, err := applyForkLineageRewrites(context.Background(), db, forkLineageRewritesForTest())
	if !errors.Is(err, cut) {
		t.Fatalf("applyForkLineageRewrites = %v, want the cut", err)
	}
	if !changed {
		t.Fatal("changed = false, want true: a write was attempted")
	}
	if !strings.Contains(err.Error(), "fork ignored cursor") {
		t.Fatalf("err = %q, want it to name the chain that was cut", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet expectations: %v", err)
	}
}

// Both DELETEs ride one transaction, committed once after the last.
func TestApplyForkLineageRewrites_EveryDeleteSharesOneTransaction(t *testing.T) {
	db, mock := newMockDB(t)

	mock.ExpectExec(`^START TRANSACTION$`).WillReturnResult(sqlmock.NewResult(0, 0))
	expectCursorDelete(mock, "schema_migrations", 51, 54)
	expectCursorDelete(mock, "ignored_schema_migrations", 10, 11)
	mock.ExpectExec(`^COMMIT$`).WillReturnResult(sqlmock.NewResult(0, 0))

	changed, err := applyForkLineageRewrites(context.Background(), db, forkLineageRewritesForTest())
	if err != nil {
		t.Fatalf("applyForkLineageRewrites: %v", err)
	}
	if !changed {
		t.Fatal("changed = false, want true")
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet expectations: %v", err)
	}
}

// A rollback that itself fails is reported beside the cause, never instead
// of it.
func TestApplyForkLineageRewrites_FailedRollbackIsReportedBesideTheCause(t *testing.T) {
	db, mock := newMockDB(t)
	cut := errors.New("connection cut")

	mock.ExpectExec(`^START TRANSACTION$`).WillReturnResult(sqlmock.NewResult(0, 0))
	expectCursorDelete(mock, "schema_migrations", 51, 54)
	mock.ExpectExec(`DELETE FROM ignored_schema_migrations WHERE version BETWEEN \? AND \?`).
		WithArgs(10, 11).
		WillReturnError(cut)
	mock.ExpectExec(`^ROLLBACK$`).WillReturnError(errors.New("rollback refused"))

	_, err := applyForkLineageRewrites(context.Background(), db, forkLineageRewritesForTest())
	if !errors.Is(err, cut) {
		t.Fatalf("applyForkLineageRewrites = %v, want the cut as the cause", err)
	}
	if !strings.Contains(err.Error(), "rollback refused") {
		t.Fatalf("err = %q, want the rollback failure named beside the cause", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet expectations: %v", err)
	}
}

// Nothing planned: no transaction is opened.
func TestApplyForkLineageRewrites_NothingPlannedOpensNoTransaction(t *testing.T) {
	db, mock := newMockDB(t)
	changed, err := applyForkLineageRewrites(context.Background(), db, nil)
	if err != nil || changed {
		t.Fatalf("applyForkLineageRewrites(nil) = %v, %v; want false, nil", changed, err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet expectations: %v", err)
	}
}
