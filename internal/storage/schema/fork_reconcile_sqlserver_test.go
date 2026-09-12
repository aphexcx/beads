package schema

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/steveyegge/beads/internal/testutil"
)

// Real-dolt regression tests for the fork-lineage cursor reconciliation
// (gp-w0nu, boomtown's 2026-09-11 bd 1.2.2 roll). Everything here runs
// against a throwaway `dolt sql-server` on a free port under an empty HOME,
// over ONE pinned connection — the same shape as the production server-mode
// open (initSchemaOnDBWithBootstrapHeal pins a *sql.Conn and hands it to
// MigrateUpWithLock). The stores are built from this binary's own migration
// files, so the fixtures are the shapes the reconciler documents, not copies
// of anything live.

// startScratchDoltServer starts a dolt sql-server rooted in an empty temp dir
// and returns its port. The server runs with an empty HOME so the test does
// not depend on the developer's ~/.dolt (DOLT_COMMIT falls back to
// root@localhost). It is killed when the test ends.
func startScratchDoltServer(t *testing.T) int {
	t.Helper()
	testutil.RequireDoltBinary(t)

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("find free port: %v", err)
	}
	port := ln.Addr().(*net.TCPAddr).Port
	_ = ln.Close()

	dataDir := filepath.Join(t.TempDir(), "data")
	home := filepath.Join(t.TempDir(), "home")
	for _, dir := range []string{dataDir, home} {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			t.Fatalf("mkdir %s: %v", dir, err)
		}
	}
	cmd := exec.Command("dolt", "sql-server",
		"--host", "127.0.0.1",
		"--port", strconv.Itoa(port),
		"--loglevel", "error",
	)
	cmd.Dir = dataDir
	cmd.Env = []string{"HOME=" + home, "PATH=" + os.Getenv("PATH")}
	logFile, err := os.Create(filepath.Join(t.TempDir(), "dolt-sql-server.log"))
	if err != nil {
		t.Fatalf("create server log: %v", err)
	}
	cmd.Stdout = logFile
	cmd.Stderr = logFile
	if err := cmd.Start(); err != nil {
		t.Fatalf("start dolt sql-server: %v", err)
	}
	t.Cleanup(func() {
		_ = cmd.Process.Kill()
		_ = cmd.Wait()
		_ = logFile.Close()
	})

	addr := fmt.Sprintf("127.0.0.1:%d", port)
	deadline := time.Now().Add(30 * time.Second)
	for {
		conn, err := net.DialTimeout("tcp", addr, time.Second)
		if err == nil {
			_ = conn.Close()
			return port
		}
		if time.Now().After(deadline) {
			t.Fatalf("dolt sql-server did not accept connections on %s within 30s", addr)
		}
		time.Sleep(100 * time.Millisecond)
	}
}

// openScratchDatabase creates database name on the scratch server and returns
// a pool selecting it. Callers pin a *sql.Conn from it for the migration
// session and may open a second Conn for a fresh-session read.
func openScratchDatabase(t *testing.T, ctx context.Context, port int, name string) *sql.DB {
	t.Helper()
	admin, err := sql.Open("mysql", fmt.Sprintf("root@tcp(127.0.0.1:%d)/", port))
	if err != nil {
		t.Fatalf("open admin pool: %v", err)
	}
	defer admin.Close()
	if _, err := admin.ExecContext(ctx, "CREATE DATABASE `"+name+"`"); err != nil {
		t.Fatalf("create database %s: %v", name, err)
	}
	db, err := sql.Open("mysql", fmt.Sprintf("root@tcp(127.0.0.1:%d)/%s?parseTime=true&multiStatements=true", port, name))
	if err != nil {
		t.Fatalf("open database pool: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return db
}

func pinConn(t *testing.T, ctx context.Context, db *sql.DB) *sql.Conn {
	t.Helper()
	conn, err := db.Conn(ctx)
	if err != nil {
		t.Fatalf("pin connection: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return conn
}

// applyRecordedAs applies one embedded migration file from src and records
// it in src's cursor table under recordAs — how a renumbered fork migration
// was recorded by the binary that shipped it under its old number.
func applyRecordedAs(t *testing.T, ctx context.Context, db DBConn, src migrationSource, file string, recordAs int) {
	t.Helper()
	data, err := src.files.ReadFile(src.dir + "/" + file)
	if err != nil {
		t.Fatalf("reading embedded %s: %v", file, err)
	}
	if err := execMigrationBody(ctx, db, string(data)); err != nil {
		t.Fatalf("applying %s: %v", file, err)
	}
	sum := sha256.Sum256(data)
	if _, err := db.ExecContext(ctx,
		"INSERT IGNORE INTO "+src.cursorTable+" (version, content_hash) VALUES (?, ?)",
		recordAs, hex.EncodeToString(sum[:])); err != nil {
		t.Fatalf("recording %s as %d: %v", file, recordAs, err)
	}
}

func runSourceTo(t *testing.T, ctx context.Context, db DBConn, src migrationSource, upTo int) {
	t.Helper()
	if _, err := db.ExecContext(ctx, src.bootstrapSQL()); err != nil {
		t.Fatalf("bootstrap %s: %v", src.cursorTable, err)
	}
	if _, err := src.ensureContentHashColumn(ctx, db); err != nil {
		t.Fatalf("content_hash column on %s: %v", src.cursorTable, err)
	}
	if _, err := runMigrations(ctx, db, src, 0, upTo, false); err != nil {
		t.Fatalf("running %s migrations to %d: %v", src.dir, upTo, err)
	}
}

func commitAll(t *testing.T, ctx context.Context, db DBConn, message string) {
	t.Helper()
	if err := DrainCall(ctx, db, "CALL DOLT_ADD('-A')"); err != nil {
		t.Fatalf("dolt add -A: %v", err)
	}
	if err := DrainCall(ctx, db, "CALL DOLT_COMMIT('-m', ?)", message); err != nil &&
		!strings.Contains(strings.ToLower(err.Error()), "nothing to commit") {
		t.Fatalf("dolt commit: %v", err)
	}
}

func cursorVersions(t *testing.T, ctx context.Context, db DBConn, table string) []int {
	t.Helper()
	rows, err := db.QueryContext(ctx, "SELECT version FROM "+table+" ORDER BY version")
	if err != nil {
		t.Fatalf("reading %s: %v", table, err)
	}
	defer rows.Close()
	var versions []int
	for rows.Next() {
		var v int
		if err := rows.Scan(&v); err != nil {
			t.Fatalf("scanning %s: %v", table, err)
		}
		versions = append(versions, v)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterating %s: %v", table, err)
	}
	return versions
}

func versionRange(lo, hi int) []int {
	out := make([]int, 0, hi-lo+1)
	for v := lo; v <= hi; v++ {
		out = append(out, v)
	}
	return out
}

// embeddedVersions lists every migration version this binary embeds for src.
func embeddedVersions(src migrationSource) []int {
	files := src.list()
	out := make([]int, 0, len(files))
	for _, f := range files {
		out = append(out, f.version)
	}
	return out
}

func requireVersions(t *testing.T, what string, got, want []int) {
	t.Helper()
	if fmt.Sprint(got) != fmt.Sprint(want) {
		t.Fatalf("%s versions = %v, want %v", what, got, want)
	}
}

// doltStatusRows returns every dolt_status row (ignored tables included) as
// "table:staged:status" strings, sorted — the operator's view of the working
// set.
func doltStatusRows(t *testing.T, ctx context.Context, db DBConn) []string {
	t.Helper()
	rows, err := db.QueryContext(ctx, "SELECT table_name, staged, status FROM dolt_status")
	if err != nil {
		t.Fatalf("reading dolt_status: %v", err)
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var table, status string
		var staged bool
		if err := rows.Scan(&table, &staged, &status); err != nil {
			t.Fatalf("scanning dolt_status: %v", err)
		}
		out = append(out, fmt.Sprintf("%s:%t:%s", table, staged, status))
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterating dolt_status: %v", err)
	}
	sort.Strings(out)
	return out
}

func requireColumn(t *testing.T, ctx context.Context, db DBConn, table, column string, want bool) {
	t.Helper()
	got, err := columnExists(ctx, db, table, column)
	if err != nil {
		t.Fatalf("probing %s.%s: %v", table, column, err)
	}
	if got != want {
		t.Fatalf("%s.%s present = %t, want %t", table, column, got, want)
	}
}

// buildPreUpmergeStore materializes the exact shape boomtown's stores (and
// citadel's gp before 2026-08-31) were in after the Jul-27 fork binary
// (5f149e74b, "bd 1.1.2") last migrated them, measured on a store that binary
// built under a dolt 2.1.10 sql-server (gp-w0nu evidence 01):
//
//	schema_migrations          1-55, 70-73   (MAX 73; row 54 = upstream lease
//	                                          columns, row 55 dropped them again)
//	ignored_schema_migrations  1-13, 20-22   (MAX 22; 20-22 = the fork files now
//	                                          embedded as 0025-0027, byte-identical)
//	issues.lease_expires_at    absent        (0055)
//	leases.granted_node        absent        (upstream ignored 0016 never ran)
//	wisp_comments.external_ref present       (fork ignored 0022 → 0027)
//
// Main-chain rows are committed; the ignored chain and every dolt-ignored
// table live only in the working set, as on a real clone.
func buildPreUpmergeStore(t *testing.T, ctx context.Context, db DBConn) {
	t.Helper()
	if _, err := seedDoltIgnorePatterns(ctx, db); err != nil {
		t.Fatalf("seed dolt_ignore: %v", err)
	}
	runSourceTo(t, ctx, db, mainSource, 55)
	for _, v := range []int{70, 71, 72, 73} {
		applyRecordedAs(t, ctx, db, mainSource, upmergeMainFiles[v], v)
	}
	commitAll(t, ctx, db, "fixture: pre-upmerge main chain (1-55, 70-73)")

	runSourceTo(t, ctx, db, ignoredSource, 13)
	for _, v := range []int{20, 21, 22} {
		applyRecordedAs(t, ctx, db, ignoredSource, upmergeRenumberedIgnoredFiles[v], v)
	}
	// The ignored series touches one committed table on the way (ignored
	// 0011 cleans child_counters); the production pass commits that with
	// its final DOLT_COMMIT, so a real store arrives here clean. dolt_ignore
	// keeps the clone-local tables themselves out of this commit.
	commitAll(t, ctx, db, "fixture: ignored chain side effects on committed tables")

	requireVersions(t, "fixture schema_migrations",
		cursorVersions(t, ctx, db, mainSource.cursorTable),
		append(versionRange(1, 55), 70, 71, 72, 73))
	requireVersions(t, "fixture ignored_schema_migrations",
		cursorVersions(t, ctx, db, ignoredSource.cursorTable),
		append(versionRange(1, 13), 20, 21, 22))
	requireColumn(t, ctx, db, "issues", "lease_expires_at", false)
	requireColumn(t, ctx, db, "leases", "granted_node", false)
	requireColumn(t, ctx, db, "wisp_comments", "external_ref", true)
	if dirty, err := dirtyTables(ctx, db, true); err != nil {
		t.Fatalf("dirtyTables: %v", err)
	} else if len(dirty) != 0 {
		t.Fatalf("fixture left committable tables dirty: %v", sortedDirtyTableNames(dirty))
	}
}

// buildMergedStore is the shape every citadel store has had since 2026-08-17
// and the one the 2026-08-31 roll moved: main through 0073 via the merged
// lineage (0056-0065 present), ignored 1-27.
func buildMergedStore(t *testing.T, ctx context.Context, db DBConn) {
	t.Helper()
	if _, err := seedDoltIgnorePatterns(ctx, db); err != nil {
		t.Fatalf("seed dolt_ignore: %v", err)
	}
	runSourceTo(t, ctx, db, mainSource, 0)
	commitAll(t, ctx, db, "fixture: merged main chain")
	runSourceTo(t, ctx, db, ignoredSource, 27)
	commitAll(t, ctx, db, "fixture: ignored chain side effects on committed tables")
	requireVersions(t, "fixture ignored_schema_migrations",
		cursorVersions(t, ctx, db, ignoredSource.cursorTable), versionRange(1, 27))
	requireColumn(t, ctx, db, "leases", "granted_node", true)
}

// TestForkReconcile_PreUpmergeStore_MigratesOnSQLServer is boomtown's 9/11
// refusal, reproduced and fixed. On the base the pass refused with
// "ignored_schema_migrations has pre-upmerge row 22 without row 14 but
// MAX(version)=0": the ignored MAX read 22 on the wire inside the very same
// session (pinned below), but currentVersion's cursor-reality check saw
// leases.granted_node absent and healed the reading to 0, and the reconciler
// compared its precondition against that. The fixed pass reads the recorded
// MAX, rewrites both chains, and the same pass carries the store to the
// merged lineage.
func TestForkReconcile_PreUpmergeStore_MigratesOnSQLServer(t *testing.T) {
	port := startScratchDoltServer(t)
	ctx := context.Background()
	db := openScratchDatabase(t, ctx, port, "preupmerge")
	conn := pinConn(t, ctx, db)
	buildPreUpmergeStore(t, ctx, conn)
	if report, err := VerifyForkLineageState(ctx, conn); err != nil || report.Status != ForkLineagePreMerge {
		t.Fatalf("lineage before MigrateUp = %+v, %v; want pre-merge", report, err)
	}

	// The diagnosis, pinned in the migration session itself: the table's
	// recorded MAX is 22 and the fork column is present — the in-session
	// reads see the working set — while the reality-checked reading is 0
	// because the pre-upmerge chain never ran upstream ignored 0016.
	if got, err := cursorMaxVersion(ctx, conn, ignoredSource.cursorTable); err != nil || got != 22 {
		t.Fatalf("cursorMaxVersion(ignored) = %d, %v; want 22 (the recorded cursor)", got, err)
	}
	if got, err := ignoredSource.currentVersion(ctx, conn); err != nil || got != 0 {
		t.Fatalf("ignoredSource.currentVersion = %d, %v; want 0 (sentinel leases.granted_node absent on a pre-upmerge chain)", got, err)
	}

	applied, err := MigrateUp(ctx, conn)
	if err != nil {
		t.Fatalf("MigrateUp on the pre-upmerge shape: %v", err)
	}
	if applied == 0 {
		t.Fatal("MigrateUp applied 0 main migrations, want upstream 0056-0065 and the re-recorded 0070-0073")
	}

	// Every embedded main migration is now recorded: upstream 0056-0066
	// applied for real once the cursor dropped to 55, and the fork's 0070-0073
	// re-ran as guarded no-ops and were re-recorded.
	requireVersions(t, "post-pass schema_migrations",
		cursorVersions(t, ctx, conn, mainSource.cursorTable), embeddedVersions(mainSource))
	requireVersions(t, "post-pass ignored_schema_migrations",
		cursorVersions(t, ctx, conn, ignoredSource.cursorTable), embeddedVersions(ignoredSource))
	requireColumn(t, ctx, conn, "leases", "granted_node", true)
	requireColumn(t, ctx, conn, "wisp_comments", "external_ref", true)
	if report, err := VerifyForkLineageState(ctx, conn); err != nil || report.Status != ForkLineageReconciled {
		t.Fatalf("lineage after MigrateUp = %+v, %v; want reconciled", report, err)
	}
	if dirty, err := dirtyTables(ctx, conn, true); err != nil {
		t.Fatalf("dirtyTables: %v", err)
	} else if len(dirty) != 0 {
		t.Fatalf("pass left committable tables dirty: %v", sortedDirtyTableNames(dirty))
	}

	// A fresh session agrees: the rewrite and the pass are committed, not a
	// working-set illusion of the pinned session.
	fresh := pinConn(t, ctx, db)
	requireVersions(t, "fresh-session schema_migrations AS OF HEAD",
		cursorVersions(t, ctx, fresh, mainSource.cursorTable+" AS OF 'HEAD'"), embeddedVersions(mainSource))
}

// A recorded fork migration cannot promise reconciliation when its DDL is
// missing. The healthy fixture is built independently by the migration test.
func TestForkReconcile_MissingMainEffectOnSQLServer(t *testing.T) {
	port := startScratchDoltServer(t)
	ctx := context.Background()
	db := openScratchDatabase(t, ctx, port, "missing_main_effect")
	conn := pinConn(t, ctx, db)
	buildPreUpmergeStore(t, ctx, conn)
	if _, err := conn.ExecContext(ctx, "ALTER TABLE comments DROP COLUMN external_ref"); err != nil {
		t.Fatal(err)
	}

	want := "schema_migrations records fork migrations 70-73 but column comments.external_ref (fork 0072) is missing; schema does not match the recorded cursor"
	report, err := VerifyForkLineageState(ctx, conn)
	if err != nil {
		t.Fatal(err)
	}
	// Dropping the column also removes its index; doctor now reports both.
	if report.Status != ForkLineageInconsistent || len(report.Problems) != 2 || report.Problems[0] != want || report.Problems[1] != "index comments.idx_comments_external_ref (fork 0072)" {
		t.Errorf("lineage with missing main effect = %+v; want inconsistent with %q", report, want)
	}
	if _, err := MigrateUp(ctx, conn); !errors.Is(err, errRefusedRewrite) || !strings.Contains(err.Error(), want) {
		t.Fatalf("MigrateUp = %v; want the same missing-effect refusal wrapping errRefusedRewrite", err)
	}
}

// Upstream 0052 already ran on this shape, so reconciliation starting at
// main 0056 cannot restore its missing index.
func TestForkReconcile_PreUpmergeMissingUpstreamIndexOnSQLServer(t *testing.T) {
	port := startScratchDoltServer(t)
	ctx := context.Background()
	db := openScratchDatabase(t, ctx, port, "missing_upstream_index")
	conn := pinConn(t, ctx, db)
	buildPreUpmergeStore(t, ctx, conn)
	if report, err := VerifyForkLineageState(ctx, conn); err != nil || report.Status != ForkLineagePreMerge {
		t.Fatalf("healthy pre-upmerge lineage = %+v, %v; want pre-merge", report, err)
	}
	if _, err := conn.ExecContext(ctx, "DROP INDEX idx_issues_defer_until ON issues"); err != nil {
		t.Fatal(err)
	}
	report, err := VerifyForkLineageState(ctx, conn)
	if err != nil {
		t.Fatal(err)
	}
	want := "index issues.idx_issues_defer_until (upstream 0052)"
	if report.Status != ForkLineageInconsistent || len(report.Problems) != 1 || report.Problems[0] != want {
		t.Fatalf("lineage with missing upstream index = %+v; want inconsistent with %q", report, want)
	}
}

// TestForkReconcile_RefusalLeavesWorkingSetAsFoundOnSQLServer pins scope
// item 3 of gp-w0nu. The fixture is the pre-upmerge shape with the fork
// column wisp_comments.external_ref removed — boomtown's `cp -a` copy really
// lacked it, and the reconciler must still refuse that store. Before the fix
// the upmerge main DELETE (rows 70-73) had already landed in the working set
// when the ignored chain refused, and the dolt_ignore seed rows sat beside it
// uncommitted: dolt_status read "schema_migrations: modified, dolt_ignore:
// modified", HEAD was intact, and bd 1.1.2 then misread the store as "row 54
// but MAX=55" until DOLT_CHECKOUT('schema_migrations'). Now the refusal
// leaves dolt_status exactly as it was found: the plan runs before the pass
// writes anything, so a refused pass neither seeds dolt_ignore, nor resets a
// table the operator had staged (round 2: codex gate r1 found the previous
// order ran unstagePreExistingTables before the plan), nor deletes a cursor
// row.
func TestForkReconcile_RefusalLeavesWorkingSetAsFoundOnSQLServer(t *testing.T) {
	port := startScratchDoltServer(t)
	ctx := context.Background()
	db := openScratchDatabase(t, ctx, port, "refusal")
	conn := pinConn(t, ctx, db)
	buildPreUpmergeStore(t, ctx, conn)

	// The copy artifact: the fork column the cursor says 0022 added is gone.
	if _, err := conn.ExecContext(ctx, "ALTER TABLE wisp_comments DROP COLUMN external_ref"); err != nil {
		t.Fatalf("drop wisp_comments.external_ref: %v", err)
	}
	// Give the dolt_ignore seed real work, so the test proves the seed is
	// committed before the reconciler can refuse (it was the second table
	// boomtown found dirty).
	if _, err := conn.ExecContext(ctx, "DELETE FROM dolt_ignore WHERE pattern = 'bd_events_journal'"); err != nil {
		t.Fatalf("unseed dolt_ignore: %v", err)
	}
	commitAll(t, ctx, conn, "fixture: under-seeded dolt_ignore")
	// An operator's own staged table (a DOLT_ADD before the roll). MigrateUp
	// unstages pre-existing staged tables before its seed commit; the plan
	// now runs before that DOLT_RESET, so a refusal must leave the table
	// staged exactly as found.
	if _, err := conn.ExecContext(ctx, "CREATE TABLE operator_scratch (id INT PRIMARY KEY)"); err != nil {
		t.Fatalf("create operator_scratch: %v", err)
	}
	if err := DrainCall(ctx, conn, "CALL DOLT_ADD(?)", "operator_scratch"); err != nil {
		t.Fatalf("stage operator_scratch: %v", err)
	}

	mainBefore := cursorVersions(t, ctx, conn, mainSource.cursorTable)
	ignoredBefore := cursorVersions(t, ctx, conn, ignoredSource.cursorTable)
	statusBefore := doltStatusRows(t, ctx, conn)
	const stagedRow = "operator_scratch:true:new table"
	if len(statusBefore) != 1 || statusBefore[0] != stagedRow {
		t.Fatalf("fixture dolt_status = %v, want exactly [%s] (the operator's staged table and nothing else)", statusBefore, stagedRow)
	}

	_, err := MigrateUp(ctx, conn)
	if err == nil {
		t.Fatal("MigrateUp succeeded on a store whose recorded fork column is missing; want the refuse-to-rewrite error")
	}
	for _, want := range []string{"wisp_comments.external_ref", "refusing to rewrite", "working set was left as found"} {
		if !strings.Contains(err.Error(), want) {
			t.Fatalf("refusal = %q, want it to contain %q", err, want)
		}
	}

	requireVersions(t, "post-refusal schema_migrations", cursorVersions(t, ctx, conn, mainSource.cursorTable), mainBefore)
	requireVersions(t, "post-refusal ignored_schema_migrations", cursorVersions(t, ctx, conn, ignoredSource.cursorTable), ignoredBefore)
	if got := doltStatusRows(t, ctx, conn); fmt.Sprint(got) != fmt.Sprint(statusBefore) {
		t.Fatalf("dolt_status after the refusal = %v, want exactly as found %v", got, statusBefore)
	}
	// Spelled out: the operator's table is still staged (no DOLT_RESET ran)
	// and dolt_ignore is not dirty (the seed never ran).
	if got := doltStatusRows(t, ctx, conn); len(got) != 1 || got[0] != stagedRow {
		t.Fatalf("dolt_status after the refusal = %v, want the operator's table still staged [%s]", got, stagedRow)
	}
	// A refused pass seeds nothing and commits nothing: the under-seeded
	// pattern is neither in the working set nor at HEAD.
	for _, q := range []struct{ where, sql string }{
		{"working set", "SELECT COUNT(*) FROM dolt_ignore WHERE pattern = 'bd_events_journal'"},
		{"HEAD", "SELECT COUNT(*) FROM dolt_ignore AS OF 'HEAD' WHERE pattern = 'bd_events_journal'"},
	} {
		var seeded int
		if err := conn.QueryRowContext(ctx, q.sql).Scan(&seeded); err != nil {
			t.Fatalf("reading dolt_ignore (%s): %v", q.where, err)
		}
		if seeded != 0 {
			t.Fatalf("dolt_ignore (%s) has %d bd_events_journal rows after the refusal, want 0 (a refused pass seeds nothing)", q.where, seeded)
		}
	}

	// The previous binary's view: a fresh session still sees MAX(version)=73
	// with rows 70-73, so it never enters its "row 54 but MAX=55" refusal.
	fresh := pinConn(t, ctx, db)
	if got, err := cursorMaxVersion(ctx, fresh, mainSource.cursorTable); err != nil || got != 73 {
		t.Fatalf("fresh-session MAX(schema_migrations.version) = %d, %v; want 73", got, err)
	}
}

// TestForkReconcile_MergedStore_MovesIgnoredCursorOnSQLServer is citadel's
// 2026-08-31 result, kept: a store already on the merged lineage (main 73
// with 0056-0065, ignored 1-27) must have nothing to reconcile and simply
// apply ignored 0028, main untouched.
func TestForkReconcile_MergedStore_MovesIgnoredCursorOnSQLServer(t *testing.T) {
	port := startScratchDoltServer(t)
	ctx := context.Background()
	db := openScratchDatabase(t, ctx, port, "merged")
	conn := pinConn(t, ctx, db)
	buildMergedStore(t, ctx, conn)

	mainBefore := cursorVersions(t, ctx, conn, mainSource.cursorTable)
	rewrites, err := planForkLineageRewrites(ctx, conn)
	if err != nil {
		t.Fatalf("planForkLineageRewrites on the merged shape: %v", err)
	}
	if len(rewrites) != 0 {
		t.Fatalf("planned %d cursor rewrites on the merged shape, want 0", len(rewrites))
	}

	if _, err := MigrateUp(ctx, conn); err != nil {
		t.Fatalf("MigrateUp on the merged shape: %v", err)
	}
	requireVersions(t, "post-pass schema_migrations", cursorVersions(t, ctx, conn, mainSource.cursorTable), mainBefore)
	requireVersions(t, "post-pass ignored_schema_migrations",
		cursorVersions(t, ctx, conn, ignoredSource.cursorTable), embeddedVersions(ignoredSource))
	if dirty, err := dirtyTables(ctx, conn, true); err != nil {
		t.Fatalf("dirtyTables: %v", err)
	} else if len(dirty) != 0 {
		t.Fatalf("pass left committable tables dirty: %v", sortedDirtyTableNames(dirty))
	}
}

// A partial pass can reconcile main while leaving the clone-local ignored
// cursor pre-upmerge. Missing main effects must still be diagnosed.
func TestForkReconcile_MixedChainMissingMainEffectOnSQLServer(t *testing.T) {
	port := startScratchDoltServer(t)
	ctx := context.Background()
	db := openScratchDatabase(t, ctx, port, "mixed_chain_missing_main_effect")
	conn := pinConn(t, ctx, db)
	buildPreUpmergeStore(t, ctx, conn)
	if changed, err := applyPlanned(ctx, conn, planUpmergeMainCursor); err != nil || !changed {
		t.Fatalf("reconcile fixture main cursor = %t, %v; want rewrite", changed, err)
	}
	if _, err := runMigrations(ctx, conn, mainSource, 55, 73, false); err != nil {
		t.Fatalf("complete fixture main migrations: %v", err)
	}
	commitAll(t, ctx, conn, "fixture: main reconciled with ignored still pre-upmerge")
	requireVersions(t, "mixed fixture main", cursorVersions(t, ctx, conn, mainSource.cursorTable), embeddedVersions(mainSource))
	if pre, err := hasPreUpmergeIgnoredCursor(ctx, conn); err != nil || !pre {
		t.Fatalf("fixture ignored pre-upmerge = %t, %v; want true", pre, err)
	}
	if report, err := VerifyForkLineageState(ctx, conn); err != nil || report.Status != ForkLineagePreMerge {
		t.Fatalf("healthy mixed lineage = %+v, %v; want pre-merge", report, err)
	}
	if _, err := conn.ExecContext(ctx, "ALTER TABLE comments DROP COLUMN external_ref"); err != nil {
		t.Fatal(err)
	}
	report, err := VerifyForkLineageState(ctx, conn)
	if err != nil {
		t.Fatal(err)
	}
	if report.Status != ForkLineageInconsistent || !strings.Contains(strings.Join(report.Problems, "; "), "column comments.external_ref (fork 0072)") {
		t.Fatalf("mixed lineage with missing main effect = %+v; want inconsistent naming comments.external_ref", report)
	}
	if len(report.PreMergeSchemes) != 1 || report.PreMergeSchemes[0] != "2026-08 upmerge" {
		t.Fatalf("PreMergeSchemes = %v; want only 2026-08 upmerge", report.PreMergeSchemes)
	}
}
