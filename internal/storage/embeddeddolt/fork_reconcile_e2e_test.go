//go:build cgo

package embeddeddolt_test

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	"github.com/steveyegge/beads/internal/storage/embeddeddolt"
	"github.com/steveyegge/beads/internal/storage/schema"
)

// migrationsDir is the on-disk source of the embedded migration files. The
// test reads the renumbered fork files from disk to reconstruct, byte-for-
// byte, the DDL and cursor rows a pre-merge fork binary left behind.
const migrationsDir = "../schema/migrations"

// TestForkLineageReconciliationEndToEnd rebuilds a database exactly as a
// pre-merge fork binary left it — main migrations 1-50 plus the fork's
// 0051-0054 (recorded under those numbers), ignored 1-9 plus the fork's
// 0010-0011 — then runs the merged binary's MigrateUp and verifies:
//
//   - upstream 0051-0053 and ignored 0010 actually applied (indexes exist,
//     aux id DEFAULTs dropped) instead of being skipped as "already run"
//   - the fork's DDL survived and its rows were re-recorded at 0070-0073 /
//     0020-0021 with correct content hashes
//   - the pre-merge rows (51-54 / 10-11) are gone
//   - a second MigrateUp is a clean no-op (reconciliation never re-fires)
func TestForkLineageReconciliationEndToEnd(t *testing.T) {
	if os.Getenv("BEADS_TEST_EMBEDDED_DOLT") != "1" {
		t.Skip("set BEADS_TEST_EMBEDDED_DOLT=1 to run embedded dolt tests")
	}

	ctx := context.Background()
	dataDir := filepath.Join(t.TempDir(), ".beads", "embeddeddolt")
	if err := os.MkdirAll(dataDir, 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}

	// Open without a database first to create it un-migrated; the simulation
	// below builds the schema the way the pre-merge fork binary did.
	adminDB, adminCleanup, err := embeddeddolt.OpenSQL(ctx, dataDir, "", "")
	if err != nil {
		t.Fatalf("OpenSQL (admin): %v", err)
	}
	if _, err := adminDB.ExecContext(ctx, "CREATE DATABASE IF NOT EXISTS `forktest`"); err != nil {
		t.Fatalf("create database: %v", err)
	}
	if err := adminCleanup(); err != nil {
		t.Fatalf("close admin connection: %v", err)
	}

	db, cleanup, err := embeddeddolt.OpenSQL(ctx, dataDir, "forktest", "main")
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
			t.Fatalf("exec %.80q: %v", query, err)
		}
	}
	execFile := func(rel string) []byte {
		t.Helper()
		data, err := os.ReadFile(filepath.Join(migrationsDir, rel))
		if err != nil {
			t.Fatalf("read %s: %v", rel, err)
		}
		if _, err := conn.ExecContext(ctx, string(data)); err != nil {
			t.Fatalf("apply %s: %v", rel, err)
		}
		return data
	}
	fileHash := func(rel string) string {
		t.Helper()
		data, err := os.ReadFile(filepath.Join(migrationsDir, rel))
		if err != nil {
			t.Fatalf("read %s: %v", rel, err)
		}
		sum := sha256.Sum256(data)
		return hex.EncodeToString(sum[:])
	}

	// --- Simulate the pre-merge fork database ---

	// Shared prefix: main migrations 1-50 are identical on both lineages.
	if _, err := schema.MigrateUpTo(ctx, conn, 50); err != nil {
		t.Fatalf("MigrateUpTo(50): %v", err)
	}

	// Fork main 0051-0054, recorded under the pre-merge numbers. The
	// renumbered files carry byte-identical content, so both the DDL and the
	// recorded hashes match what the fork binary produced.
	forkMain := map[int]string{
		51: "0070_create_linear_label_snapshots.up.sql",
		52: "0071_linear_snapshots_dolt_ignore.up.sql",
		53: "0072_add_comment_external_ref.up.sql",
		54: "0073_create_attachments.up.sql",
	}
	for _, v := range []int{51, 52, 53, 54} {
		data := execFile(forkMain[v])
		sum := sha256.Sum256(data)
		exec("INSERT INTO schema_migrations (version, content_hash) VALUES (?, ?)", v, hex.EncodeToString(sum[:]))
	}

	// Ignored chain: upstream 1-9 (shared) then the fork's snapshot tables
	// recorded as 10-11.
	exec(`CREATE TABLE IF NOT EXISTS ignored_schema_migrations (
		version INT PRIMARY KEY,
		applied_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
		content_hash CHAR(64)
	)`)
	exec("REPLACE INTO dolt_ignore VALUES ('ignored_schema_migrations', true)")

	sharedIgnored, err := filepath.Glob(filepath.Join(migrationsDir, "ignored", "000[1-9]_*.up.sql"))
	if err != nil {
		t.Fatalf("glob ignored: %v", err)
	}
	sort.Strings(sharedIgnored)
	if len(sharedIgnored) != 9 {
		t.Fatalf("expected 9 shared ignored migrations, found %d: %v", len(sharedIgnored), sharedIgnored)
	}
	for i, path := range sharedIgnored {
		data := execFile(filepath.Join("ignored", filepath.Base(path)))
		sum := sha256.Sum256(data)
		exec("INSERT INTO ignored_schema_migrations (version, content_hash) VALUES (?, ?)", i+1, hex.EncodeToString(sum[:]))
	}
	forkIgnored := map[int]string{
		10: "ignored/0020_create_linear_issue_snapshots.up.sql",
		11: "ignored/0021_create_linear_project_snapshots.up.sql",
	}
	for _, v := range []int{10, 11} {
		data := execFile(forkIgnored[v])
		sum := sha256.Sum256(data)
		exec("INSERT INTO ignored_schema_migrations (version, content_hash) VALUES (?, ?)", v, hex.EncodeToString(sum[:]))
	}

	// The old binary committed its migration passes; mirror that.
	if _, err := conn.ExecContext(ctx, "CALL DOLT_COMMIT('-Am', 'simulate pre-merge fork state')"); err != nil &&
		!strings.Contains(strings.ToLower(err.Error()), "nothing to commit") {
		t.Fatalf("commit simulated state: %v", err)
	}

	// Sanity: the fixture is what a fork prod DB reports.
	assertScalar(t, ctx, conn, "SELECT COALESCE(MAX(version),0) FROM schema_migrations", 54)
	assertScalar(t, ctx, conn, "SELECT COALESCE(MAX(version),0) FROM ignored_schema_migrations", 11)
	report, err := schema.VerifyForkLineageState(ctx, conn)
	if err != nil {
		t.Fatalf("VerifyForkLineageState (pre): %v", err)
	}
	if report.Status != schema.ForkLineagePreMerge {
		t.Fatalf("pre-migration lineage = %q, want %q", report.Status, schema.ForkLineagePreMerge)
	}

	// --- Run the merged binary's migration pass ---
	if _, err := schema.MigrateUp(ctx, conn); err != nil {
		t.Fatalf("MigrateUp: %v", err)
	}

	// Cursor state: pre-merge rows gone, renumbered rows present, upstream
	// rows recorded with upstream content.
	assertScalar(t, ctx, conn, "SELECT COALESCE(MAX(version),0) FROM schema_migrations", schema.LatestVersion())
	assertScalar(t, ctx, conn, "SELECT COUNT(*) FROM schema_migrations WHERE version = 54", 0)
	assertScalar(t, ctx, conn, "SELECT COUNT(*) FROM ignored_schema_migrations WHERE version = 11", 0)
	assertScalar(t, ctx, conn, "SELECT COALESCE(MAX(version),0) FROM ignored_schema_migrations", schema.LatestIgnoredVersion())

	cursorHashes := map[string]struct {
		table   string
		version int
		file    string
	}{
		"upstream 0051":         {"schema_migrations", 51, "0051_drop_aux_id_defaults.up.sql"},
		"upstream 0052":         {"schema_migrations", 52, "0052_add_date_indexes.up.sql"},
		"upstream 0053":         {"schema_migrations", 53, "0053_repair_rig_wisps.up.sql"},
		"fork 0070":             {"schema_migrations", 70, "0070_create_linear_label_snapshots.up.sql"},
		"fork 0071":             {"schema_migrations", 71, "0071_linear_snapshots_dolt_ignore.up.sql"},
		"fork 0072":             {"schema_migrations", 72, "0072_add_comment_external_ref.up.sql"},
		"fork 0073":             {"schema_migrations", 73, "0073_create_attachments.up.sql"},
		"upstream ignored 0010": {"ignored_schema_migrations", 10, "ignored/0010_drop_wisp_id_defaults.up.sql"},
		"fork ignored 0020":     {"ignored_schema_migrations", 20, "ignored/0020_create_linear_issue_snapshots.up.sql"},
		"fork ignored 0021":     {"ignored_schema_migrations", 21, "ignored/0021_create_linear_project_snapshots.up.sql"},
	}
	for desc, tc := range cursorHashes {
		var recorded sql.NullString
		//nolint:gosec // test-only, table names are constants above
		err := conn.QueryRowContext(ctx,
			fmt.Sprintf("SELECT content_hash FROM %s WHERE version = %d", tc.table, tc.version)).Scan(&recorded)
		if err != nil {
			t.Errorf("%s: reading recorded hash: %v", desc, err)
			continue
		}
		if !recorded.Valid || recorded.String != fileHash(tc.file) {
			t.Errorf("%s: recorded hash %v, want hash of %s", desc, recorded, tc.file)
		}
	}

	// Upstream 0051 effect: aux id DEFAULTs dropped (would have been silently
	// skipped without reconciliation).
	for _, table := range []string{"events", "comments", "issue_snapshots", "compaction_snapshots", "wisp_events", "wisp_comments"} {
		var columnDefault sql.NullString
		if err := conn.QueryRowContext(ctx, `
			SELECT COLUMN_DEFAULT FROM INFORMATION_SCHEMA.COLUMNS
			WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = ? AND COLUMN_NAME = 'id'
		`, table).Scan(&columnDefault); err != nil {
			t.Fatalf("reading %s.id default: %v", table, err)
		}
		if columnDefault.Valid {
			t.Errorf("%s.id still has DEFAULT %q; upstream 0051/ignored 0010 was skipped", table, columnDefault.String)
		}
	}

	// Upstream 0052 effect: date indexes replaced the single-column status
	// index.
	// STATISTICS has one row per column of an index, so count index names.
	assertScalar(t, ctx, conn, "SELECT COUNT(DISTINCT INDEX_NAME) FROM INFORMATION_SCHEMA.STATISTICS WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'issues' AND INDEX_NAME = 'idx_issues_status_updated_at'", 1)
	assertScalar(t, ctx, conn, "SELECT COUNT(DISTINCT INDEX_NAME) FROM INFORMATION_SCHEMA.STATISTICS WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'issues' AND INDEX_NAME = 'idx_issues_defer_until'", 1)
	assertScalar(t, ctx, conn, "SELECT COUNT(DISTINCT INDEX_NAME) FROM INFORMATION_SCHEMA.STATISTICS WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'issues' AND INDEX_NAME = 'idx_issues_status'", 0)

	// Fork DDL survived the re-run untouched.
	for _, table := range []string{"linear_label_snapshots", "attachments", "linear_issue_snapshots", "linear_project_snapshots"} {
		if _, err := conn.ExecContext(ctx, "SELECT 1 FROM `"+table+"` LIMIT 0"); err != nil {
			t.Errorf("fork table %s not queryable after reconciliation: %v", table, err)
		}
	}
	assertScalar(t, ctx, conn, "SELECT COUNT(*) FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'comments' AND COLUMN_NAME = 'external_ref'", 1)

	// Doctor-facing verification agrees.
	report, err = schema.VerifyForkLineageState(ctx, conn)
	if err != nil {
		t.Fatalf("VerifyForkLineageState (post): %v", err)
	}
	if report.Status != schema.ForkLineageReconciled {
		t.Fatalf("post-migration lineage = %q (problems: %v), want %q", report.Status, report.Problems, schema.ForkLineageReconciled)
	}

	// Second pass: nothing to do, nothing re-fires.
	applied, err := schema.MigrateUp(ctx, conn)
	if err != nil {
		t.Fatalf("MigrateUp (second pass): %v", err)
	}
	if applied != 0 {
		t.Errorf("second MigrateUp applied %d migrations, want 0", applied)
	}
}

func assertScalar(t *testing.T, ctx context.Context, conn *sql.Conn, query string, want int) {
	t.Helper()
	var got int
	if err := conn.QueryRowContext(ctx, query).Scan(&got); err != nil {
		t.Fatalf("%s: %v", query, err)
	}
	if got != want {
		t.Errorf("%s = %d, want %d", query, got, want)
	}
}
