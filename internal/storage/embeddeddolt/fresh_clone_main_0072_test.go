//go:build cgo

package embeddeddolt_test

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/steveyegge/beads/internal/storage/embeddeddolt"
	"github.com/steveyegge/beads/internal/storage/schema"
)

// TestFreshCloneOfUpstreamBaseAppliesMain0072WithoutWispTables is the
// end-to-end form of the gf-4c9 row-1 fix (gf-9x2 codex gate Major at
// 0072_add_comment_external_ref.up.sql:49/:55): a clone of an upstream-shaped
// base (main cursor at 0066, the fork's 0070-0073 pending) has no clone-local
// wisp_comments yet, because the ignored chain that materializes the wisp
// tables runs AFTER the main chain in the same MigrateUp pass. 0072's wisp
// statements used to probe INFORMATION_SCHEMA.COLUMNS alone, so an absent
// table read as a missing column and the prepared `ALTER TABLE wisp_comments`
// failed the clone's first writable open ("table not found: wisp_comments").
//
// The simulation: migrate to latest, then drop every clone-local table the
// ignored chain owns (what a clone does not receive) and delete the fork
// main rows 70-73 so they are pending again, and run MigrateUp — the clone's
// first pass. It must finish: 0072 no-ops its wisp half, the ignored chain
// then rebuilds wisp_comments and ignored/0027 adds the columns.
func TestFreshCloneOfUpstreamBaseAppliesMain0072WithoutWispTables(t *testing.T) {
	if os.Getenv("BEADS_TEST_EMBEDDED_DOLT") != "1" {
		t.Skip("set BEADS_TEST_EMBEDDED_DOLT=1 to run embedded dolt tests")
	}

	ctx := context.Background()
	dataDir := filepath.Join(t.TempDir(), ".beads", "embeddeddolt")
	if err := os.MkdirAll(dataDir, 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}

	adminDB, adminCleanup, err := embeddeddolt.OpenSQL(ctx, dataDir, "", "")
	if err != nil {
		t.Fatalf("OpenSQL (admin): %v", err)
	}
	if _, err := adminDB.ExecContext(ctx, "CREATE DATABASE IF NOT EXISTS `clone0072`"); err != nil {
		t.Fatalf("create database: %v", err)
	}
	if err := adminCleanup(); err != nil {
		t.Fatalf("close admin connection: %v", err)
	}

	db, cleanup, err := embeddeddolt.OpenSQL(ctx, dataDir, "clone0072", "main")
	if err != nil {
		t.Fatalf("OpenSQL: %v", err)
	}
	t.Cleanup(func() { _ = cleanup() })

	conn, err := db.Conn(ctx)
	if err != nil {
		t.Fatalf("pin connection: %v", err)
	}
	t.Cleanup(func() { conn.Close() })

	exec := func(query string) {
		t.Helper()
		if _, err := conn.ExecContext(ctx, query); err != nil {
			t.Fatalf("exec %.80q: %v", query, err)
		}
	}

	// Origin state: both chains fully applied, committed per step.
	if _, err := schema.MigrateUp(ctx, conn); err != nil {
		t.Fatalf("MigrateUp (origin): %v", err)
	}

	// The clone: no clone-local tables (the ignored chain owns them; a clone
	// starts without any of them, cursor included) and the fork's main rows
	// 70-73 pending — the upstream-shaped v66 base the gate described.
	exec("SET FOREIGN_KEY_CHECKS = 0")
	for _, table := range []string{
		"wisp_labels", "wisp_dependencies", "wisp_events", "wisp_comments",
		"wisp_child_counters", "wisps", "repo_mtimes", "local_metadata",
		"linear_issue_snapshots", "linear_project_snapshots",
		"ignored_schema_migrations",
	} {
		exec("DROP TABLE IF EXISTS `" + table + "`")
	}
	exec("SET FOREIGN_KEY_CHECKS = 1")
	exec("DELETE FROM schema_migrations WHERE version >= 70")
	exec("CALL DOLT_ADD('schema_migrations')")
	exec("CALL DOLT_COMMIT('-m', 'test: regress the main cursor to the upstream v66 base')")
	assertScalar(t, ctx, conn, "SELECT COALESCE(MAX(version),0) FROM schema_migrations", 66)
	assertScalar(t, ctx, conn,
		"SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'wisp_comments'", 0)

	// The clone's first migration pass: main 0070-0073 run BEFORE the ignored
	// chain exists. 0072's wisp half must no-op on the absent table.
	if _, err := schema.MigrateUp(ctx, conn); err != nil {
		t.Fatalf("MigrateUp (clone of the upstream base): %v", err)
	}

	assertScalar(t, ctx, conn, "SELECT COALESCE(MAX(version),0) FROM schema_migrations", schema.LatestVersion())
	assertScalar(t, ctx, conn,
		"SELECT COALESCE(MAX(version),0) FROM ignored_schema_migrations", schema.LatestIgnoredVersion())
	// The wisp columns came back through the ignored chain (ignored/0027).
	for _, column := range []string{"external_ref", "updated_at"} {
		assertScalar(t, ctx, conn,
			"SELECT COUNT(*) FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'wisp_comments' AND COLUMN_NAME = '"+column+"'", 1)
	}
	assertScalar(t, ctx, conn,
		"SELECT COUNT(DISTINCT INDEX_NAME) FROM INFORMATION_SCHEMA.STATISTICS WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'wisp_comments' AND INDEX_NAME = 'idx_wisp_comments_external_ref'", 1)
	// The durable comments half is intact.
	for _, column := range []string{"external_ref", "updated_at"} {
		assertScalar(t, ctx, conn,
			"SELECT COUNT(*) FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'comments' AND COLUMN_NAME = '"+column+"'", 1)
	}

	// Replay stability: a second pass has nothing left to do.
	applied, err := schema.MigrateUp(ctx, conn)
	if err != nil {
		t.Fatalf("MigrateUp (second clone pass): %v", err)
	}
	if applied != 0 {
		t.Errorf("second MigrateUp applied %d migrations, want 0", applied)
	}
}
