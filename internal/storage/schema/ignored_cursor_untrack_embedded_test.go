//go:build cgo

package schema

import (
	"context"
	"database/sql"
	"errors"
	"strings"
	"testing"

	embedded "github.com/dolthub/driver/v2"
)

type cursorRecoveryDB struct {
	*sql.DB
	connector *embedded.Connector
}

func (db *cursorRecoveryDB) Close() error {
	return errors.Join(db.DB.Close(), db.connector.Close())
}

func openCursorRecoveryDB(t *testing.T, dir string, create bool) *cursorRecoveryDB {
	t.Helper()
	cfg := embedded.Config{
		Directory: dir, CommitName: "migration test", CommitEmail: "test@example.com",
	}
	if !create {
		cfg.Database = "cursor_recovery"
	}
	connector, err := embedded.NewConnector(cfg)
	if err != nil {
		t.Fatal(err)
	}
	db := &cursorRecoveryDB{sql.OpenDB(connector), connector}
	db.SetMaxOpenConns(1)
	t.Cleanup(func() { _ = db.Close() })
	if create {
		if _, err := db.Exec("CREATE DATABASE cursor_recovery"); err != nil {
			t.Fatal(err)
		}
		if err := db.Close(); err != nil {
			t.Fatal(err)
		}
		return openCursorRecoveryDB(t, dir, false)
	}
	return db
}

// interruptCursorRestore executes real SQL, then stops the restore immediately
// after the selected durable statement, before the next restore step.
type interruptCursorRestore struct {
	DBConn
	interrupted error
	after       string
}

func (db interruptCursorRestore) ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error) {
	result, err := db.DBConn.ExecContext(ctx, query, args...)
	if err == nil && strings.HasPrefix(query, db.after) {
		return result, db.interrupted
	}
	return result, err
}

func TestIgnoredCursorRestoreInterruptedAfterCreate(t *testing.T) {
	testInterruptedCursorRestore(t, "CREATE TABLE", false)
}

func TestIgnoredCursorRestoreInterruptedBeforeCleanup(t *testing.T) {
	for _, after := range []string{"INSERT IGNORE INTO", "RENAME TABLE"} {
		t.Run(after, func(t *testing.T) {
			for _, tracked := range []bool{false, true} {
				name := "untracked_scratch"
				if tracked {
					name = "tracked_scratch"
				}
				t.Run(name, func(t *testing.T) { testInterruptedCursorRestore(t, after, tracked) })
			}
		})
	}
}

func testInterruptedCursorRestore(t *testing.T, after string, tracked bool) {
	dir := t.TempDir()
	db := openCursorRecoveryDB(t, dir, true)
	ctx := context.Background()
	for _, query := range []string{
		ignoredCursorScratch.bootstrapSQL(),
		"INSERT INTO dolt_ignore VALUES ('ignored_schema_migrations', true), ('local_metadata', true), ('__temp__ignored_schema_migrations_restore', true)",
		"CREATE TABLE wisps (id INT PRIMARY KEY)",
		"CREATE TABLE wisp_dependencies (id INT PRIMARY KEY)",
		"CREATE TABLE leases (id INT PRIMARY KEY, granted_node TEXT)",
	} {
		if _, err := db.ExecContext(ctx, query); err != nil {
			t.Fatal(err)
		}
	}
	for _, version := range []int{1, LatestIgnoredVersion()} {
		if _, err := db.ExecContext(ctx, "INSERT INTO "+ignoredCursorUntrackTempTable+
			" (version, applied_at, content_hash) VALUES (?, '2026-09-01 12:00:00', ?)",
			version, strings.Repeat("a", 64)); err != nil {
			t.Fatal(err)
		}
	}
	interrupted := errors.New("interrupted after " + after)
	err := restoreIgnoredCursorRows(ctx, interruptCursorRestore{db, interrupted, after})
	if !errors.Is(err, interrupted) {
		t.Fatalf("restore error = %v, want injected interruption", err)
	}
	if tracked {
		if present, err := schemaTableExists(ctx, db, "__temp__ignored_schema_migrations_restore"); err != nil {
			t.Fatal(err)
		} else if present {
			// Model residue that predates the ignore entry (or was force-added).
			if err := DrainCall(ctx, db, "CALL DOLT_ADD('-f', '__temp__ignored_schema_migrations_restore')"); err != nil {
				t.Fatal(err)
			}
		}
		if err := DrainCall(ctx, db, "CALL DOLT_ADD('-A')"); err != nil {
			t.Fatal(err)
		}
		if err := DrainCall(ctx, db, "CALL DOLT_COMMIT('-m', 'fixture: incidental commit of recovery tables')"); err != nil {
			t.Fatal(err)
		}
	}
	// Close the embedded engine, then reopen the on-disk fixture. No synthetic SQL
	// results decide whether the incomplete cursor is mistaken for a live one.
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	db = openCursorRecoveryDB(t, dir, false)
	if healed, err := healTrackedIgnoredCursorTable(ctx, db); err != nil || !healed {
		t.Fatalf("reopened heal = %v, %v", healed, err)
	}
	var count int
	if err := db.QueryRowContext(ctx, "SELECT COUNT(*) FROM ignored_schema_migrations WHERE "+
		"applied_at = '2026-09-01 12:00:00' AND content_hash = ?", strings.Repeat("a", 64)).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 2 {
		t.Fatalf("restored cursor rows = %d, want 2 with original timestamps and hashes", count)
	}
	if pending, err := PendingIgnoredVersions(ctx, db); err != nil || len(pending) != 0 {
		t.Fatalf("migration replay after recovery: pending=%v, error=%v", pending, err)
	}
	if scratch, err := schemaTableExists(ctx, db, ignoredCursorUntrackTempTable); err != nil || scratch {
		t.Fatalf("scratch remains after recovery: %v, %v", scratch, err)
	}
	if tracked, err := tableTrackedAtHead(ctx, db, "", ignoredCursorUntrackTempTable); err != nil || tracked {
		t.Fatalf("scratch remains at HEAD: %v, %v", tracked, err)
	}
	if staging, err := schemaTableExists(ctx, db, "__temp__ignored_schema_migrations_restore"); err != nil || staging {
		t.Fatalf("staging table remains after recovery: %v, %v", staging, err)
	}
	if tracked, err := tableTrackedAtHead(ctx, db, "", "__temp__ignored_schema_migrations_restore"); err != nil || tracked {
		t.Fatalf("staging table remains at HEAD: %v, %v", tracked, err)
	}
}

type commitDuringCursorRestore struct{ DBConn }

func (db commitDuringCursorRestore) ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error) {
	result, err := db.DBConn.ExecContext(ctx, query, args...)
	if err == nil && strings.HasPrefix(query, "INSERT IGNORE INTO __temp__ignored_schema_migrations_restore") {
		if err := DrainCall(ctx, db.DBConn, "CALL DOLT_ADD('-A')"); err != nil {
			return result, err
		}
		err = DrainCall(ctx, db.DBConn, "CALL DOLT_COMMIT('-m', 'fixture: concurrent blanket commit')")
	}
	return result, err
}

// A blanket commit DURING the same restore must not turn its final rename into
// tracked dirt. Cleaning residue only at startup misses this window.
func TestIgnoredCursorRestoreConcurrentCommit(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	db := openCursorRecoveryDB(t, dir, true)
	if _, err := seedDoltIgnorePatterns(ctx, db); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, ignoredCursorScratch.bootstrapSQL()); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, "INSERT INTO "+ignoredCursorUntrackTempTable+" (version) VALUES (1), (2)"); err != nil {
		t.Fatal(err)
	}
	if err := restoreIgnoredCursorRows(ctx, commitDuringCursorRestore{db}); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	db = openCursorRecoveryDB(t, dir, false)
	var count int
	if err := db.QueryRowContext(ctx, "SELECT COUNT(*) FROM ignored_schema_migrations").Scan(&count); err != nil || count != 2 {
		t.Fatalf("cursor rows after reopen = %d, %v; want 2", count, err)
	}
	for _, table := range []string{ignoredSource.cursorTable, ignoredCursorUntrackTempTable, "__temp__ignored_schema_migrations_restore"} {
		if tracked, err := tableTrackedAtHead(ctx, db, "", table); err != nil || tracked {
			t.Fatalf("%s remains tracked after recovery: %v, %v", table, tracked, err)
		}
	}
	if dirty, err := committableDirtyTables(ctx, db); err != nil || len(dirty) != 0 {
		t.Fatalf("recovery left committable dirt: %v, %v", dirty, err)
	}
}
