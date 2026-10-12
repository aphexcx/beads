//go:build cgo

package embeddeddolt_test

import (
	"context"
	"database/sql"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/steveyegge/beads/internal/storage/embeddeddolt"
	"github.com/steveyegge/beads/internal/storage/schema"
)

// TestMain0072BackfillsUpdatedAtFromCreatedAtDeterministically is the
// behavioral form of gf-pu2 finding 2d (the gf-jr0 vs-v1.3.1 gate's Major at
// 0072_add_comment_external_ref.up.sql:43): adding comments.updated_at with
// DEFAULT CURRENT_TIMESTAMP in one step stamped every existing comment with
// the time the migration ran on that clone, so two replicas that upgraded the
// same pre-0072 data independently disagreed on identical rows and the
// comment conflict resolver refused the next pull. The column is now added
// nullable, backfilled from created_at and only then given its default.
//
// The simulation, twice over on two databases: migrate to latest, regress the
// comments tables to their pre-0072 shape with two existing comments whose
// creation times are fixed, delete the main rows from 72 on, and run
// MigrateUp — the clone's pass. Both rows come out with updated_at equal to
// their created_at, the two databases agree byte for byte, and a comment
// written after the migration still takes the default.
func TestMain0072BackfillsUpdatedAtFromCreatedAtDeterministically(t *testing.T) {
	if os.Getenv("BEADS_TEST_EMBEDDED_DOLT") != "1" {
		t.Skip("set BEADS_TEST_EMBEDDED_DOLT=1 to run embedded dolt tests")
	}
	ctx := context.Background()

	apply := func(t *testing.T, dbName string) map[string][2]string {
		t.Helper()
		dataDir := filepath.Join(t.TempDir(), ".beads", "embeddeddolt")
		if err := os.MkdirAll(dataDir, 0o755); err != nil {
			t.Fatalf("mkdir: %v", err)
		}
		adminDB, adminCleanup, err := embeddeddolt.OpenSQL(ctx, dataDir, "", "")
		if err != nil {
			t.Fatalf("OpenSQL (admin): %v", err)
		}
		if _, err := adminDB.ExecContext(ctx, "CREATE DATABASE IF NOT EXISTS `"+dbName+"`"); err != nil {
			t.Fatalf("create database: %v", err)
		}
		if err := adminCleanup(); err != nil {
			t.Fatalf("close admin connection: %v", err)
		}
		db, cleanup, err := embeddeddolt.OpenSQL(ctx, dataDir, dbName, "main")
		if err != nil {
			t.Fatalf("OpenSQL: %v", err)
		}
		t.Cleanup(func() { _ = cleanup() })
		conn, err := db.Conn(ctx)
		if err != nil {
			t.Fatalf("pin connection: %v", err)
		}
		t.Cleanup(func() { conn.Close() })
		exec := func(query string, args ...any) {
			t.Helper()
			if _, err := conn.ExecContext(ctx, query, args...); err != nil {
				t.Fatalf("exec %.100q: %v", query, err)
			}
		}

		// Origin state: both chains fully applied.
		if _, err := schema.MigrateUp(ctx, conn); err != nil {
			t.Fatalf("MigrateUp (origin): %v", err)
		}

		// The pre-0072 shape: no external_ref, no updated_at, no index, on
		// both comment tables; two comments with fixed creation times; the
		// main cursor back before 0072.
		for _, table := range []string{"comments", "wisp_comments"} {
			exec("DROP INDEX idx_" + table + "_external_ref ON " + table)
			exec("ALTER TABLE " + table + " DROP COLUMN updated_at")
			exec("ALTER TABLE " + table + " DROP COLUMN external_ref")
		}
		exec("INSERT INTO issues (id, title, description, design, acceptance_criteria, notes, status, priority, issue_type) VALUES ('bf-1', 'i', '', '', '', '', 'open', 2, 'task')")
		exec("INSERT INTO comments (id, issue_id, author, text, created_at) VALUES ('c-1', 'bf-1', 'a', 'first', '2025-01-01 10:00:00'), ('c-2', 'bf-1', 'b', 'second', '2025-06-01 12:30:00')")
		exec("DELETE FROM schema_migrations WHERE version >= 72")
		exec("CALL DOLT_ADD('-A')")
		exec("CALL DOLT_COMMIT('-m', 'test: regress the comments tables and the main cursor to the pre-0072 base')")
		assertScalar(t, ctx, conn, "SELECT COALESCE(MAX(version),0) FROM schema_migrations", 71)
		assertScalar(t, ctx, conn,
			"SELECT COUNT(*) FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'comments' AND COLUMN_NAME = 'updated_at'", 0)

		// The clone's pass: 0072 adds the columns and backfills updated_at.
		if _, err := schema.MigrateUp(ctx, conn); err != nil {
			t.Fatalf("MigrateUp (pre-0072 base): %v", err)
		}
		assertScalar(t, ctx, conn, "SELECT COALESCE(MAX(version),0) FROM schema_migrations", schema.LatestVersion())
		assertScalar(t, ctx, conn,
			"SELECT COUNT(*) FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'comments' AND COLUMN_NAME = 'updated_at' AND IS_NULLABLE = 'NO'", 1)

		got := readCommentTimes(t, ctx, conn)
		for id, times := range got {
			if times[0] != times[1] {
				t.Errorf("%s: comment %s updated_at = %s, want its created_at %s", dbName, id, times[1], times[0])
			}
		}
		if len(got) != 2 {
			t.Fatalf("%s: %d comments after the migration, want 2", dbName, len(got))
		}

		// A comment written after the migration takes the default.
		exec("INSERT INTO comments (id, issue_id, author, text) VALUES ('c-3', 'bf-1', 'c', 'third')")
		assertScalar(t, ctx, conn, "SELECT COUNT(*) FROM comments WHERE id = 'c-3' AND updated_at IS NOT NULL", 1)
		return got
	}

	first := apply(t, "backfilla")
	second := apply(t, "backfillb")
	if !reflect.DeepEqual(first, second) {
		t.Fatalf("two independent applies of 0072 disagree on identical comment rows:\n%v\n%v", first, second)
	}
}

// readCommentTimes returns each comment's (created_at, updated_at) as the
// server formats them.
func readCommentTimes(t *testing.T, ctx context.Context, conn *sql.Conn) map[string][2]string {
	t.Helper()
	rows, err := conn.QueryContext(ctx, "SELECT id, CAST(created_at AS CHAR), CAST(updated_at AS CHAR) FROM comments ORDER BY id")
	if err != nil {
		t.Fatalf("read comments: %v", err)
	}
	defer rows.Close()
	out := map[string][2]string{}
	for rows.Next() {
		var id, created, updated string
		if err := rows.Scan(&id, &created, &updated); err != nil {
			t.Fatalf("scan: %v", err)
		}
		out[id] = [2]string{created, updated}
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("rows: %v", err)
	}
	return out
}
