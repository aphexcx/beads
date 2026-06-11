package schema

import (
	"context"
	"database/sql"
	"regexp"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/steveyegge/beads/internal/storage/rowid"
)

var commentsTable = auxRekeyTables[1]

func nstr(s string) sql.NullString { return sql.NullString{String: s, Valid: true} }

func commentDigest(issueID, author, text, createdAt string) string {
	return rowid.Digest([]sql.NullString{nstr(issueID), nstr(author), nstr(text), nstr(createdAt)})
}

func expectCommentsSelect(mock sqlmock.Sqlmock) *sqlmock.ExpectedQuery {
	return mock.ExpectQuery(regexp.QuoteMeta(
		"SELECT id, issue_id, author, text, CAST(created_at AS CHAR) FROM comments"))
}

// TestRekeyAuxRowTableConvergesRandomIDs verifies the core convergence: rows
// carrying 0037's per-clone-random ids are rewritten to the deterministic
// content-derived value, so independently-migrated clones end up identical.
func TestRekeyAuxRowTableConvergesRandomIDs(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()

	expectColumnExists(mock, true)
	expectCommentsSelect(mock).
		WillReturnRows(sqlmock.NewRows([]string{"id", "issue_id", "author", "text", "created_at"}).
			AddRow("random-a", "bd-1", "steve", "first", "2026-06-09 12:00:00").
			AddRow("random-b", "bd-1", "steve", "second", "2026-06-09 12:00:01"))

	// Updates are issued in sorted-old-id order, each to ordinal 0 of its own
	// content group.
	mock.ExpectExec(regexp.QuoteMeta("UPDATE comments SET id = ? WHERE id = ?")).
		WithArgs(rowid.New("comments", 0, commentDigest("bd-1", "steve", "first", "2026-06-09 12:00:00")), "random-a").
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec(regexp.QuoteMeta("UPDATE comments SET id = ? WHERE id = ?")).
		WithArgs(rowid.New("comments", 0, commentDigest("bd-1", "steve", "second", "2026-06-09 12:00:01")), "random-b").
		WillReturnResult(sqlmock.NewResult(0, 1))

	wrote, err := rekeyAuxRowTable(context.Background(), db, commentsTable)
	if err != nil {
		t.Fatalf("rekeyAuxRowTable: %v", err)
	}
	if !wrote {
		t.Error("expected wrote=true when rows were re-keyed")
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet sql expectations: %v", err)
	}
}

// TestRekeyAuxRowTableDuplicateRowsGetDistinctOrdinals verifies exact-duplicate
// rows (no natural identity to tell them apart) take distinct ordinals of the
// same digest, so the re-key never collapses or collides them.
func TestRekeyAuxRowTableDuplicateRowsGetDistinctOrdinals(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()

	digest := commentDigest("bd-1", "steve", "same text", "2026-06-09 12:00:00")

	expectColumnExists(mock, true)
	expectCommentsSelect(mock).
		WillReturnRows(sqlmock.NewRows([]string{"id", "issue_id", "author", "text", "created_at"}).
			AddRow("zzz-random", "bd-1", "steve", "same text", "2026-06-09 12:00:00").
			AddRow("aaa-random", "bd-1", "steve", "same text", "2026-06-09 12:00:00"))

	// Free rows are assigned in sorted-current-id order: aaa-random takes
	// ordinal 0, zzz-random takes ordinal 1.
	mock.ExpectExec(regexp.QuoteMeta("UPDATE comments SET id = ? WHERE id = ?")).
		WithArgs(rowid.New("comments", 0, digest), "aaa-random").
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec(regexp.QuoteMeta("UPDATE comments SET id = ? WHERE id = ?")).
		WithArgs(rowid.New("comments", 1, digest), "zzz-random").
		WillReturnResult(sqlmock.NewResult(0, 1))

	wrote, err := rekeyAuxRowTable(context.Background(), db, commentsTable)
	if err != nil {
		t.Fatalf("rekeyAuxRowTable: %v", err)
	}
	if !wrote {
		t.Error("expected wrote=true when duplicate rows were re-keyed")
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet sql expectations: %v", err)
	}
}

// TestRekeyAuxRowTableKeepsHeldTargetsStable verifies a row already holding one
// of its group's deterministic ids keeps it: re-running after a partial pass
// (or after a new duplicate appears) must never swap ids within a group, which
// would collide mid-update.
func TestRekeyAuxRowTableKeepsHeldTargetsStable(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()

	digest := commentDigest("bd-1", "steve", "same text", "2026-06-09 12:00:00")
	held := rowid.New("comments", 1, digest)

	expectColumnExists(mock, true)
	expectCommentsSelect(mock).
		WillReturnRows(sqlmock.NewRows([]string{"id", "issue_id", "author", "text", "created_at"}).
			AddRow(held, "bd-1", "steve", "same text", "2026-06-09 12:00:00").
			AddRow("random-x", "bd-1", "steve", "same text", "2026-06-09 12:00:00"))

	// The held row is untouched; the random row takes the remaining ordinal 0.
	mock.ExpectExec(regexp.QuoteMeta("UPDATE comments SET id = ? WHERE id = ?")).
		WithArgs(rowid.New("comments", 0, digest), "random-x").
		WillReturnResult(sqlmock.NewResult(0, 1))

	wrote, err := rekeyAuxRowTable(context.Background(), db, commentsTable)
	if err != nil {
		t.Fatalf("rekeyAuxRowTable: %v", err)
	}
	if !wrote {
		t.Error("expected wrote=true when one row was re-keyed")
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet sql expectations: %v", err)
	}
}

// TestRekeyAuxRowTableIdempotent verifies that when every row already carries a
// deterministic id, no UPDATE is issued.
func TestRekeyAuxRowTableIdempotent(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()

	digest := commentDigest("bd-1", "steve", "hello", "2026-06-09 12:00:00")

	expectColumnExists(mock, true)
	expectCommentsSelect(mock).
		WillReturnRows(sqlmock.NewRows([]string{"id", "issue_id", "author", "text", "created_at"}).
			AddRow(rowid.New("comments", 0, digest), "bd-1", "steve", "hello", "2026-06-09 12:00:00"))
	// No ExpectExec: zero UPDATEs expected.

	wrote, err := rekeyAuxRowTable(context.Background(), db, commentsTable)
	if err != nil {
		t.Fatalf("rekeyAuxRowTable: %v", err)
	}
	if wrote {
		t.Error("expected wrote=false when all rows already deterministic")
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet sql expectations: %v", err)
	}
}

// TestRekeyAuxRowTableSkipsMissingTable verifies the backfill no-ops cleanly
// when the table/id column is absent (older or partial schema).
func TestRekeyAuxRowTableSkipsMissingTable(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()

	expectColumnExists(mock, false)

	wrote, err := rekeyAuxRowTable(context.Background(), db, commentsTable)
	if err != nil {
		t.Fatalf("rekeyAuxRowTable: %v", err)
	}
	if wrote {
		t.Error("expected wrote=false when the id column is absent")
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet sql expectations: %v", err)
	}
}

// TestRekeyAuxRowIDsSkipsWhenMarkerRecorded verifies the clone-local gate: once
// the ignored marker migration is recorded, the re-key never scans a table
// again, so steady-state opens do not churn synced rows.
func TestRekeyAuxRowIDsSkipsWhenMarkerRecorded(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()

	expectScalar(mock, "SELECT COALESCE(MAX(version), 0) FROM ignored_schema_migrations",
		"version", auxRowRekeyMarkerVersion)
	// No further expectations: no table may be probed or scanned.

	wrote, err := rekeyAuxRowIDs(context.Background(), db)
	if err != nil {
		t.Fatalf("rekeyAuxRowIDs: %v", err)
	}
	if wrote {
		t.Error("expected wrote=false when the marker is already recorded")
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet sql expectations: %v", err)
	}
}

// TestRekeyAuxRowIDsRunsAllTablesWhenMarkerPending verifies the one-time pass
// covers every synced aux table when the marker is still pending.
func TestRekeyAuxRowIDsRunsAllTablesWhenMarkerPending(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()

	expectScalar(mock, "SELECT COALESCE(MAX(version), 0) FROM ignored_schema_migrations",
		"version", auxRowRekeyMarkerVersion-1)
	// Each of the four tables is probed; this mocked world has none of them,
	// so each probe returns 0 and the loop completes without scanning.
	for range auxRekeyTables {
		expectColumnExists(mock, false)
	}

	wrote, err := rekeyAuxRowIDs(context.Background(), db)
	if err != nil {
		t.Fatalf("rekeyAuxRowIDs: %v", err)
	}
	if wrote {
		t.Error("expected wrote=false when no aux table exists")
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet sql expectations: %v", err)
	}
}
