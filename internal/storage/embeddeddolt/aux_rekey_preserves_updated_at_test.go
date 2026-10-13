//go:build cgo

package embeddeddolt_test

import (
	"context"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/steveyegge/beads/internal/storage/embeddeddolt"
	"github.com/steveyegge/beads/internal/storage/schema"
)

// TestAuxRekeyPreservesTheBackfilledCommentUpdatedAt is the behavioral form
// of gf-xid finding 2f (the gf-pu2 vs-v1.3.1 gate's Major at
// 0072_add_comment_external_ref.up.sql:76): on a database whose migration
// pass still owes the aux row-id re-key — the gate's example is a main cursor
// at 60 with the ignored cursor at 17, so 0072 and the bd-ri8bd pass
// (shipped main 61, ignored marker 18) run in the same MigrateUp — 0072 adds
// comments.updated_at with ON UPDATE CURRENT_TIMESTAMP and backfills it from
// created_at, and then rekeyAuxRowIDsAllPasses rewrites every comment's id.
// Before the fix that UPDATE fired the ON UPDATE attribute and replaced the
// deterministic backfill with this clone's migration time, so two replicas
// that upgraded the same data independently disagreed on identical comment
// rows, which the merge resolver refuses. The re-key now assigns updated_at to
// itself in the same statement.
//
// The simulation, on two databases: migrate to latest, regress the comments
// tables to their pre-0072 shape with two comments at fixed creation times
// under ids the re-key will rewrite, set the main cursor to 60 and the ignored
// cursor to 17, commit, and run MigrateUp. Both rows must come out re-keyed
// (the pass ran), with updated_at equal to created_at, and the two databases
// must agree byte for byte.
func TestAuxRekeyPreservesTheBackfilledCommentUpdatedAt(t *testing.T) {
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

		// The pre-0072 comments shape with two comments whose ids are not
		// the content-derived ones, so the re-key has rows to rewrite; the
		// main cursor at 60 (0072 pending, below the bd-ri8bd pass's shipped
		// version 61) and the ignored cursor at 17 (marker 18 pending): the
		// gate's example of a pass that still owes the re-key.
		for _, table := range []string{"comments", "wisp_comments"} {
			exec("DROP INDEX idx_" + table + "_external_ref ON " + table)
			exec("ALTER TABLE " + table + " DROP COLUMN updated_at")
			exec("ALTER TABLE " + table + " DROP COLUMN external_ref")
		}
		exec("INSERT INTO issues (id, title, description, design, acceptance_criteria, notes, status, priority, issue_type) VALUES ('rk-1', 'i', '', '', '', '', 'open', 2, 'task')")
		exec("INSERT INTO comments (id, issue_id, author, text, created_at) VALUES ('c-1', 'rk-1', 'a', 'first', '2025-01-01 10:00:00'), ('c-2', 'rk-1', 'b', 'second', '2025-06-01 12:30:00')")
		exec("DELETE FROM schema_migrations WHERE version >= 61")
		exec("DELETE FROM ignored_schema_migrations WHERE version >= 18")
		exec("CALL DOLT_ADD('-A')")
		exec("CALL DOLT_COMMIT('-m', 'test: regress to main 60 / ignored 17 with two comments the re-key will rewrite')")
		assertScalar(t, ctx, conn, "SELECT COALESCE(MAX(version),0) FROM schema_migrations", 60)
		assertScalar(t, ctx, conn, "SELECT COALESCE(MAX(version),0) FROM ignored_schema_migrations", 17)

		// The clone's pass: 0072 adds and backfills updated_at (with its ON
		// UPDATE default), then the aux re-key rewrites both comment ids.
		if _, err := schema.MigrateUp(ctx, conn); err != nil {
			t.Fatalf("MigrateUp (main 60 / ignored 17): %v", err)
		}
		assertScalar(t, ctx, conn, "SELECT COALESCE(MAX(version),0) FROM schema_migrations", schema.LatestVersion())
		assertScalar(t, ctx, conn, "SELECT COALESCE(MAX(version),0) FROM ignored_schema_migrations", schema.LatestIgnoredVersion())
		// The re-key ran: neither seeded id survives.
		assertScalar(t, ctx, conn, "SELECT COUNT(*) FROM comments WHERE id IN ('c-1', 'c-2')", 0)
		assertScalar(t, ctx, conn, "SELECT COUNT(*) FROM comments", 2)

		got := readCommentTimes(t, ctx, conn)
		for id, times := range got {
			if times[0] != times[1] {
				t.Errorf("%s: re-keyed comment %s updated_at = %s, want its created_at %s (the re-key fired ON UPDATE CURRENT_TIMESTAMP)", dbName, id, times[1], times[0])
			}
		}
		return got
	}

	first := apply(t, "rekeya")
	second := apply(t, "rekeyb")
	if !reflect.DeepEqual(first, second) {
		t.Fatalf("two independent upgrades through the aux re-key disagree on identical comment rows:\n%v\n%v", first, second)
	}
}
