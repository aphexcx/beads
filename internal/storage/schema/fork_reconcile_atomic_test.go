package schema

import (
	"context"
	"database/sql"
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

// cancelingConn is the pass's connection whose context is canceled by the
// second DELETE: the statement returns the cancellation, as a pass whose
// deadline expires between the DELETEs sees it. Everything else reaches db.
type cancelingConn struct {
	DBConn
	cancel context.CancelFunc
	table  string
}

func (c *cancelingConn) ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error) {
	if strings.HasPrefix(query, "DELETE FROM "+c.table+" ") {
		c.cancel()
		return nil, ctx.Err()
	}
	return c.DBConn.ExecContext(ctx, query, args...)
}

// A pass canceled between the DELETEs still rolls the first one back: the
// ROLLBACK runs under its own context, not the canceled one, so it reaches
// the database and the pinned session is not released with the transaction
// open (codex round 1 on gf-c7n 2h).
func TestApplyForkLineageRewrites_CanceledBetweenDeletesStillRollsBack(t *testing.T) {
	db, mock := newMockDB(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	conn := &cancelingConn{DBConn: db, cancel: cancel, table: "ignored_schema_migrations"}

	mock.ExpectExec(`^START TRANSACTION$`).WillReturnResult(sqlmock.NewResult(0, 0))
	expectCursorDelete(mock, "schema_migrations", 51, 54)
	mock.ExpectExec(`^ROLLBACK$`).WillReturnResult(sqlmock.NewResult(0, 0))

	_, err := applyForkLineageRewrites(ctx, conn, forkLineageRewritesForTest())
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("applyForkLineageRewrites = %v, want the cancellation as the cause", err)
	}
	if strings.Contains(err.Error(), "rolling the cursor rewrites back failed") {
		t.Fatalf("err = %q: the ROLLBACK did not reach the database", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet expectations (the ROLLBACK must run under its own context): %v", err)
	}
}

// When the rollback itself fails on a pinned connection, the session's
// transaction state is unknown and the connection is discarded: the next
// statement on it does not reach the database.
func TestApplyForkLineageRewrites_FailedRollbackDiscardsThePinnedSession(t *testing.T) {
	db, mock := newMockDB(t)
	cut := errors.New("connection cut")
	conn, err := db.Conn(context.Background())
	if err != nil {
		t.Fatalf("pin connection: %v", err)
	}
	defer conn.Close()

	mock.ExpectExec(`^START TRANSACTION$`).WillReturnResult(sqlmock.NewResult(0, 0))
	expectCursorDelete(mock, "schema_migrations", 51, 54)
	mock.ExpectExec(`DELETE FROM ignored_schema_migrations WHERE version BETWEEN \? AND \?`).
		WithArgs(10, 11).
		WillReturnError(cut)
	mock.ExpectExec(`^ROLLBACK$`).WillReturnError(errors.New("rollback refused"))
	mock.ExpectClose()

	_, err = applyForkLineageRewrites(context.Background(), conn, forkLineageRewritesForTest())
	if !errors.Is(err, cut) || !strings.Contains(err.Error(), "the pinned session was discarded") {
		t.Fatalf("applyForkLineageRewrites = %v, want the cut and the discarded session named", err)
	}
	if _, err := conn.ExecContext(context.Background(), "SELECT 1"); err == nil {
		t.Fatal("the discarded connection still serves statements")
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
