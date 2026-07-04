//go:build cgo

package embeddeddolt_test

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"testing"

	"github.com/steveyegge/beads/internal/storage/embeddeddolt"
	"github.com/steveyegge/beads/internal/storage/schema"
)

// TestFreshCloneIgnoredChainRestoresWispCommentSyncColumns simulates the
// fresh-clone schema path (bd-5rs): wisp tables are clone-local
// (dolt-ignored), so a clone arrives with the replicated schema_migrations
// cursor already at latest — main 0072's guarded ALTERs never run — and
// rebuilds every ignored-chain table from scratch. ignored/0001 creates
// wisp_comments without external_ref/updated_at; only the ignored chain can
// add them (ignored/0022). Without that companion migration, wisp comment
// reads that select those columns (issueops.GetIssueCommentsInTx,
// doltTransaction.GetIssueComments) fail on every clone.
//
// The simulation: migrate a database to latest, drop everything the ignored
// chain owns (what a clone does not receive), and run MigrateUp again — the
// clone's first pass, which replays only the ignored chain.
func TestFreshCloneIgnoredChainRestoresWispCommentSyncColumns(t *testing.T) {
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
	if _, err := adminDB.ExecContext(ctx, "CREATE DATABASE IF NOT EXISTS `clonetest`"); err != nil {
		t.Fatalf("create database: %v", err)
	}
	if err := adminCleanup(); err != nil {
		t.Fatalf("close admin connection: %v", err)
	}

	db, cleanup, err := embeddeddolt.OpenSQL(ctx, dataDir, "clonetest", "main")
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

	// Origin state: both chains fully applied.
	if _, err := schema.MigrateUp(ctx, conn); err != nil {
		t.Fatalf("MigrateUp (origin): %v", err)
	}
	assertScalar(t, ctx, conn,
		"SELECT COUNT(*) FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'wisp_comments' AND COLUMN_NAME = 'external_ref'", 1)

	// Simulate the fresh clone: drop every clone-local table the ignored
	// chain owns (ignored/0001-0002 wisp tables and local state, 0020-0021
	// Linear snapshots) plus the clone-local cursor itself. These are all
	// dolt-ignored, so a clone starts without any of them.
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

	// The clone's first migration pass: main cursor is already at latest, so
	// only the ignored chain replays.
	if _, err := schema.MigrateUp(ctx, conn); err != nil {
		t.Fatalf("MigrateUp (clone): %v", err)
	}

	assertScalar(t, ctx, conn,
		"SELECT COALESCE(MAX(version),0) FROM ignored_schema_migrations", schema.LatestIgnoredVersion())

	// The columns main 0072 adds must come back via the ignored chain.
	for _, column := range []string{"external_ref", "updated_at"} {
		assertScalar(t, ctx, conn,
			"SELECT COUNT(*) FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'wisp_comments' AND COLUMN_NAME = '"+column+"'", 1)
	}
	assertScalar(t, ctx, conn,
		"SELECT COUNT(DISTINCT INDEX_NAME) FROM INFORMATION_SCHEMA.STATISTICS WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'wisp_comments' AND INDEX_NAME = 'idx_wisp_comments_external_ref'", 1)

	// The exact read shape the wisp comment sync paths use.
	if _, err := conn.ExecContext(ctx, `
		SELECT id, issue_id, author, text, created_at,
		       COALESCE(external_ref, '') AS external_ref,
		       COALESCE(updated_at, created_at) AS updated_at
		FROM wisp_comments LIMIT 0
	`); err != nil {
		t.Errorf("wisp comment sync read shape fails on fresh clone: %v", err)
	}

	// Replay stability: a second pass has nothing left to do.
	applied, err := schema.MigrateUp(ctx, conn)
	if err != nil {
		t.Fatalf("MigrateUp (second clone pass): %v", err)
	}
	if applied != 0 {
		t.Errorf("second MigrateUp applied %d migrations, want 0", applied)
	}

	// --- Pre-fix broken clone repair ---
	//
	// Clones provisioned before ignored/0022 already have the bare
	// wisp_comments (ignored cursor at 21) with local rows. Their wisp
	// tables sit in the working set as untracked dolt-ignored tables, and
	// MigrateUp guards pending ignored migrations against pre-existing
	// dirty tables — this leg proves that guard does not block 0022 from
	// repairing exactly the clones the fix targets, and that local rows
	// survive the repair.
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

	// Rebuild the pre-0022 clone state the way a pre-fix binary did: replay
	// ignored 0001-0021 from the source files and record their cursor rows.
	exec(`CREATE TABLE IF NOT EXISTS ignored_schema_migrations (
		version INT PRIMARY KEY,
		applied_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
		content_hash CHAR(64)
	)`)
	preFixFiles, err := filepath.Glob(filepath.Join(migrationsDir, "ignored", "*.up.sql"))
	if err != nil {
		t.Fatalf("glob ignored migrations: %v", err)
	}
	sort.Strings(preFixFiles) // NNNN_ prefixes: lexical order == version order
	replayed := 0
	for _, path := range preFixFiles {
		name := filepath.Base(path)
		version, err := strconv.Atoi(name[:4])
		if err != nil {
			t.Fatalf("parse version from %s: %v", name, err)
		}
		if version > 21 {
			continue
		}
		data, err := os.ReadFile(path)
		if err != nil {
			t.Fatalf("read %s: %v", name, err)
		}
		if _, err := conn.ExecContext(ctx, string(data)); err != nil {
			t.Fatalf("apply %s: %v", name, err)
		}
		sum := sha256.Sum256(data)
		if _, err := conn.ExecContext(ctx,
			"INSERT INTO ignored_schema_migrations (version, content_hash) VALUES (?, ?)",
			version, hex.EncodeToString(sum[:])); err != nil {
			t.Fatalf("record %s: %v", name, err)
		}
		replayed++
	}
	if replayed != 12 {
		t.Fatalf("replayed %d pre-fix ignored migrations, want 12 (0001-0010, 0020, 0021)", replayed)
	}
	assertScalar(t, ctx, conn,
		"SELECT COUNT(*) FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'wisp_comments' AND COLUMN_NAME = 'external_ref'", 0)
	exec("INSERT INTO wisps (id, title, description, design, acceptance_criteria, notes, status, priority, issue_type, ephemeral) VALUES ('bd-prefix', 'pre-fix wisp', '', '', '', '', 'open', 2, 'task', 1)")
	exec("INSERT INTO wisp_comments (id, issue_id, author, text) VALUES ('wc-prefix-1', 'bd-prefix', 'tester', 'local comment from before the fix')")

	// The broken clone's first pass under the fixed binary: only 0022 is
	// pending, and it must run despite wisp_comments pre-existing with rows.
	if _, err := schema.MigrateUp(ctx, conn); err != nil {
		t.Fatalf("MigrateUp (pre-fix clone repair): %v", err)
	}
	assertScalar(t, ctx, conn,
		"SELECT COALESCE(MAX(version),0) FROM ignored_schema_migrations", schema.LatestIgnoredVersion())
	for _, column := range []string{"external_ref", "updated_at"} {
		assertScalar(t, ctx, conn,
			"SELECT COUNT(*) FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'wisp_comments' AND COLUMN_NAME = '"+column+"'", 1)
	}
	// Local data survived the repair and satisfies the sync read shape.
	assertScalar(t, ctx, conn,
		"SELECT COUNT(*) FROM wisp_comments WHERE id = 'wc-prefix-1' AND COALESCE(external_ref, '') = '' AND COALESCE(updated_at, created_at) IS NOT NULL", 1)
}
