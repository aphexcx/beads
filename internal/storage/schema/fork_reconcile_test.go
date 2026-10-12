package schema

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"

	sqlmock "github.com/DATA-DOG/go-sqlmock"
)

// forkFileHash returns the SHA-256 hex of a renumbered fork migration file as
// embedded in this binary — the hash a pre-merge fork binary recorded, since
// the renumbering kept the content byte-identical.
func forkFileHash(t *testing.T, src migrationSource, name string) string {
	t.Helper()
	data, err := src.files.ReadFile(src.dir + "/" + name)
	if err != nil {
		t.Fatalf("reading embedded %s: %v", name, err)
	}
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}

func expectCursorRowProbe(mock sqlmock.Sqlmock, table string, version, count int) {
	mock.ExpectQuery(`SELECT COUNT\(\*\) FROM ` + table + ` WHERE version = \?`).
		WithArgs(version).
		WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(count))
}

func expectMaxVersion(mock sqlmock.Sqlmock, table string, version int) {
	mock.ExpectQuery(`SELECT COALESCE\(MAX\(version\), 0\) FROM ` + table).
		WillReturnRows(sqlmock.NewRows([]string{"version"}).AddRow(version))
}

func expectTableProbe(mock sqlmock.Sqlmock, table string, exists bool) {
	count := 0
	if exists {
		count = 1
	}
	mock.ExpectQuery(`SELECT COUNT\(\*\) FROM INFORMATION_SCHEMA\.TABLES`).
		WithArgs(table).
		WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(count))
}

func expectColumnProbe(mock sqlmock.Sqlmock, table, column string, exists bool) {
	count := 0
	if exists {
		count = 1
	}
	mock.ExpectQuery(`SELECT COUNT\(\*\) FROM INFORMATION_SCHEMA\.COLUMNS`).
		WithArgs(table, column).
		WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(count))
}

func expectHasContentHashColumn(mock sqlmock.Sqlmock, table string, has bool) {
	// hasContentHashColumn probes with SHOW COLUMNS ... LIKE (upstream perf
	// change: Dolt does not push the INFORMATION_SCHEMA.COLUMNS predicate
	// down) and reads the Field column of the result.
	rows := sqlmock.NewRows([]string{"Field", "Type", "Null", "Key", "Default", "Extra"})
	if has {
		rows.AddRow("content_hash", "char(64)", "YES", "", nil, "")
	}
	mock.ExpectQuery(`SHOW COLUMNS FROM ` + table + ` LIKE 'content_hash'`).
		WillReturnRows(rows)
}

func expectRecordedHash(mock sqlmock.Sqlmock, table string, version int, hash any) {
	mock.ExpectQuery(`SELECT content_hash FROM ` + table + ` WHERE version = \?`).
		WithArgs(version).
		WillReturnRows(sqlmock.NewRows([]string{"content_hash"}).AddRow(hash))
}

// applyPlanned composes one chain's plan and apply halves, so the chain-level
// tests below keep exercising verify+DELETE as one unit. Production never
// applies a single chain: MigrateUp runs planForkLineageRewrites over every
// chain first and applies only when all of them verified.
func applyPlanned(ctx context.Context, db DBConn, plan func(context.Context, DBConn) (*cursorRewrite, error)) (bool, error) {
	rewrite, err := plan(ctx, db)
	if err != nil || rewrite == nil {
		return false, err
	}
	if err := rewrite.apply(ctx, db); err != nil {
		return true, err
	}
	return true, nil
}

func reconcileForkMainCursor(ctx context.Context, db DBConn) (bool, error) {
	return applyPlanned(ctx, db, planForkMainCursor)
}

func reconcileForkIgnoredCursor(ctx context.Context, db DBConn) (bool, error) {
	return applyPlanned(ctx, db, planForkIgnoredCursor)
}

func expectCursorRewrite(mock sqlmock.Sqlmock, table string, lo, hi int) {
	mock.ExpectExec(`DELETE FROM `+table+` WHERE version BETWEEN \? AND \?`).
		WithArgs(lo, hi).
		WillReturnResult(sqlmock.NewResult(0, int64(hi-lo+1)))
}

// expectForkMainVerification queues the happy-path probe sequence for
// reconcileForkMainCursor up to (not including) the DELETE.
func expectForkMainVerification(t *testing.T, mock sqlmock.Sqlmock) {
	t.Helper()
	expectCursorRowProbe(mock, "schema_migrations", 54, 1)
	expectColumnProbe(mock, "issues", "lease_expires_at", false)
	expectCursorRowProbe(mock, "schema_migrations", 55, 0)
	expectMaxVersion(mock, "schema_migrations", 54)
	expectTableProbe(mock, "linear_label_snapshots", true)
	expectColumnProbe(mock, "comments", "external_ref", true)
	expectColumnProbe(mock, "comments", "updated_at", true)
	expectTableProbe(mock, "attachments", true)
	expectHasContentHashColumn(mock, "schema_migrations", true)
	for _, v := range []int{51, 52, 53, 54} {
		expectRecordedHash(mock, "schema_migrations", v, forkFileHash(t, mainSource, forkRenumberedMainFiles[v]))
	}
	// Pre-squash drift repair probes: all six gt-role columns present, so no
	// ALTER and no repair commit.
	for _, col := range forkIssueDriftColumns {
		expectColumnProbe(mock, "issues", col.name, true)
	}
}

func TestReconcileForkMainCursor_HappyPath_DeletesForkRows(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()

	expectForkMainVerification(t, mock)
	expectCursorRewrite(mock, "schema_migrations", 51, 54)

	changed, err := reconcileForkMainCursor(context.Background(), db)
	if err != nil {
		t.Fatalf("reconcileForkMainCursor: %v", err)
	}
	if !changed {
		t.Fatal("changed = false, want true for a pre-merge fork cursor")
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet expectations: %v", err)
	}
}

func TestReconcileForkMainCursor_NoRow54_NoOp(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()

	expectCursorRowProbe(mock, "schema_migrations", 54, 0)
	// No further queries: absence of the fingerprint ends the check.

	changed, err := reconcileForkMainCursor(context.Background(), db)
	if err != nil {
		t.Fatalf("reconcileForkMainCursor: %v", err)
	}
	if changed {
		t.Fatal("changed = true, want false without the row-54 fingerprint")
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet expectations: %v", err)
	}
}

func TestReconcileForkMainCursor_MissingCursorTable_NoOp(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()

	mock.ExpectQuery(`SELECT COUNT\(\*\) FROM schema_migrations WHERE version = \?`).
		WithArgs(54).
		WillReturnError(&mockMySQLTableNotExistErr{})

	changed, err := reconcileForkMainCursor(context.Background(), db)
	if err != nil {
		t.Fatalf("reconcileForkMainCursor: %v", err)
	}
	if changed {
		t.Fatal("changed = true, want false on a fresh database without cursor tables")
	}
}

func TestReconcileForkMainCursor_MaxBeyond54_Errors(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()

	expectCursorRowProbe(mock, "schema_migrations", 54, 1)
	expectColumnProbe(mock, "issues", "lease_expires_at", false)
	expectCursorRowProbe(mock, "schema_migrations", 55, 0)
	expectMaxVersion(mock, "schema_migrations", 73)

	_, err = reconcileForkMainCursor(context.Background(), db)
	if err == nil || !strings.Contains(err.Error(), "refusing to rewrite") {
		t.Fatalf("err = %v, want refusal when row 54 coexists with MAX(version)=73", err)
	}
}

// TestReconcileForkMainCursor_UpstreamLeaseRow54_Skips: since the
// upstream-20260710 merge, upstream also owns migration 0054
// (add_lease_columns). A database whose row 54 came from upstream's chain
// (issues.lease_expires_at present) is NOT pre-merge fork lineage and the
// reconciler must no-op instead of tripping its refuse-to-rewrite guard.
func TestReconcileForkMainCursor_UpstreamLeaseRow54_Skips(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()

	expectCursorRowProbe(mock, "schema_migrations", 54, 1)
	expectColumnProbe(mock, "issues", "lease_expires_at", true)

	changed, err := reconcileForkMainCursor(context.Background(), db)
	if err != nil {
		t.Fatalf("reconcileForkMainCursor: %v", err)
	}
	if changed {
		t.Fatal("changed = true, want false (upstream lease lineage, nothing to rewrite)")
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet expectations: %v", err)
	}
}

func TestReconcileForkMainCursor_MissingForkArtifact_Errors(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()

	expectCursorRowProbe(mock, "schema_migrations", 54, 1)
	expectColumnProbe(mock, "issues", "lease_expires_at", false)
	expectCursorRowProbe(mock, "schema_migrations", 55, 0)
	expectMaxVersion(mock, "schema_migrations", 54)
	expectTableProbe(mock, "linear_label_snapshots", false) // fork DDL missing

	_, err = reconcileForkMainCursor(context.Background(), db)
	if err == nil || !strings.Contains(err.Error(), "linear_label_snapshots") {
		t.Fatalf("err = %v, want refusal naming the missing fork artifact", err)
	}
}

func TestReconcileForkMainCursor_HashMismatch_Errors(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()

	expectCursorRowProbe(mock, "schema_migrations", 54, 1)
	expectColumnProbe(mock, "issues", "lease_expires_at", false)
	expectCursorRowProbe(mock, "schema_migrations", 55, 0)
	expectMaxVersion(mock, "schema_migrations", 54)
	expectTableProbe(mock, "linear_label_snapshots", true)
	expectColumnProbe(mock, "comments", "external_ref", true)
	expectColumnProbe(mock, "comments", "updated_at", true)
	expectTableProbe(mock, "attachments", true)
	expectHasContentHashColumn(mock, "schema_migrations", true)
	expectRecordedHash(mock, "schema_migrations", 51, strings.Repeat("ab", 32))

	_, err = reconcileForkMainCursor(context.Background(), db)
	if err == nil || !strings.Contains(err.Error(), "does not match fork migration") {
		t.Fatalf("err = %v, want refusal on a content hash this binary has never seen", err)
	}
}

func TestReconcileForkMainCursor_NullHashes_FallBackToProbes(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()

	expectCursorRowProbe(mock, "schema_migrations", 54, 1)
	expectColumnProbe(mock, "issues", "lease_expires_at", false)
	expectCursorRowProbe(mock, "schema_migrations", 55, 0)
	expectMaxVersion(mock, "schema_migrations", 54)
	expectTableProbe(mock, "linear_label_snapshots", true)
	expectColumnProbe(mock, "comments", "external_ref", true)
	expectColumnProbe(mock, "comments", "updated_at", true)
	expectTableProbe(mock, "attachments", true)
	expectHasContentHashColumn(mock, "schema_migrations", true)
	for _, v := range []int{51, 52, 53, 54} {
		expectRecordedHash(mock, "schema_migrations", v, nil) // pre-hash-column rows
	}
	for _, col := range forkIssueDriftColumns {
		expectColumnProbe(mock, "issues", col.name, true)
	}
	expectCursorRewrite(mock, "schema_migrations", 51, 54)

	changed, err := reconcileForkMainCursor(context.Background(), db)
	if err != nil {
		t.Fatalf("reconcileForkMainCursor: %v", err)
	}
	if !changed {
		t.Fatal("changed = false, want true when NULL hashes fall back to schema probes")
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet expectations: %v", err)
	}
}

// TestReconcileForkMainCursor_PreSquashDrift_RepairsAndCommits covers the
// ace/beads_witness shape: issues lacks the six gt-role columns (pre-squash
// lineage), so the reconciliation adds them and commits the repair before
// rewriting the cursor.
func TestReconcileForkMainCursor_PreSquashDrift_RepairsAndCommits(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()

	expectCursorRowProbe(mock, "schema_migrations", 54, 1)
	expectColumnProbe(mock, "issues", "lease_expires_at", false)
	expectCursorRowProbe(mock, "schema_migrations", 55, 0)
	expectMaxVersion(mock, "schema_migrations", 54)
	expectTableProbe(mock, "linear_label_snapshots", true)
	expectColumnProbe(mock, "comments", "external_ref", true)
	expectColumnProbe(mock, "comments", "updated_at", true)
	expectTableProbe(mock, "attachments", true)
	expectHasContentHashColumn(mock, "schema_migrations", true)
	for _, v := range []int{51, 52, 53, 54} {
		expectRecordedHash(mock, "schema_migrations", v, forkFileHash(t, mainSource, forkRenumberedMainFiles[v]))
	}
	for i, col := range forkIssueDriftColumns {
		expectColumnProbe(mock, "issues", col.name, false)
		if i == 0 {
			// Clean working set (no dirty issues, nothing staged), so the
			// repair may proceed.
			mock.ExpectQuery(`SELECT COUNT\(\*\) FROM dolt_status WHERE table_name = 'issues' OR staged = true`).
				WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(0))
		}
		mock.ExpectExec(`ALTER TABLE issues ADD COLUMN ` + col.name).
			WillReturnResult(sqlmock.NewResult(0, 0))
	}
	mock.ExpectExec(`CALL DOLT_ADD\('issues'\)`).
		WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectExec(`CALL DOLT_COMMIT\('-m', 'schema: repair pre-squash issues column drift \(bd-dn6\)'\)`).
		WillReturnResult(sqlmock.NewResult(0, 0))
	expectCursorRewrite(mock, "schema_migrations", 51, 54)

	changed, err := reconcileForkMainCursor(context.Background(), db)
	if err != nil {
		t.Fatalf("reconcileForkMainCursor: %v", err)
	}
	if !changed {
		t.Fatal("changed = false, want true")
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet expectations: %v", err)
	}
}

// TestReconcileForkMainCursor_DriftWithDirtyIssues_Errors: a drifted issues
// table with uncommitted changes must refuse the repair rather than sweep
// user writes into the repair commit.
func TestReconcileForkMainCursor_DriftWithDirtyIssues_Errors(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()

	expectCursorRowProbe(mock, "schema_migrations", 54, 1)
	expectColumnProbe(mock, "issues", "lease_expires_at", false)
	expectCursorRowProbe(mock, "schema_migrations", 55, 0)
	expectMaxVersion(mock, "schema_migrations", 54)
	expectTableProbe(mock, "linear_label_snapshots", true)
	expectColumnProbe(mock, "comments", "external_ref", true)
	expectColumnProbe(mock, "comments", "updated_at", true)
	expectTableProbe(mock, "attachments", true)
	expectHasContentHashColumn(mock, "schema_migrations", true)
	for _, v := range []int{51, 52, 53, 54} {
		expectRecordedHash(mock, "schema_migrations", v, forkFileHash(t, mainSource, forkRenumberedMainFiles[v]))
	}
	expectColumnProbe(mock, "issues", forkIssueDriftColumns[0].name, false)
	mock.ExpectQuery(`SELECT COUNT\(\*\) FROM dolt_status WHERE table_name = 'issues' OR staged = true`).
		WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(1))

	_, err = reconcileForkMainCursor(context.Background(), db)
	if err == nil || !strings.Contains(err.Error(), "uncommitted or staged changes") {
		t.Fatalf("err = %v, want refusal on dirty/staged working set", err)
	}
}

// TestVerifyForkLineageState_MixedCursor_Inconsistent: a fingerprint row
// coexisting with an unexpected MAX is a state the reconciler refuses, so the
// doctor must report it as inconsistent, not "will reconcile on next write".
func TestVerifyForkLineageState_MixedCursor_Inconsistent(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()

	// currentVersion for both chains (be-bv7x existence probe, then MAX; gh
	// 5033 corroborates a non-zero ignored cursor against its sentinels), then
	// the recorded ignored MAX the reconciler itself compares against.
	expectCursorProbe(mock, "schema_migrations", true)
	expectMaxVersion(mock, "schema_migrations", 73)
	expectCursorProbe(mock, "ignored_schema_migrations", true)
	expectMaxVersion(mock, "ignored_schema_migrations", 11)
	expectIgnoredSentinelProbes(mock, true)
	expectMaxVersion(mock, "ignored_schema_migrations", 11)
	expectCursorRowProbe(mock, "schema_migrations", 54, 1) // row 54 despite MAX=73
	expectCursorRowProbe(mock, "ignored_schema_migrations", 11, 1)
	expectColumnProbe(mock, "issues", "lease_expires_at", false)
	expectCursorRowProbe(mock, "schema_migrations", 55, 0)

	expectCursorRowProbe(mock, "schema_migrations", 73, 0)
	expectCursorRowProbe(mock, "ignored_schema_migrations", 22, 0)

	report, err := VerifyForkLineageState(context.Background(), db)
	if err != nil {
		t.Fatalf("VerifyForkLineageState: %v", err)
	}
	if report.Status != ForkLineageInconsistent {
		t.Fatalf("Status = %q, want %q (row 54 with MAX=73 is unreconcilable)", report.Status, ForkLineageInconsistent)
	}
	if len(report.Problems) == 0 {
		t.Fatal("Problems is empty, want the mixed-cursor description")
	}
}

func TestReconcileForkIgnoredCursor_HappyPath_DeletesForkRows(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()

	expectCursorRowProbe(mock, "ignored_schema_migrations", 11, 1)
	// The recorded MAX, read raw (cursorMaxVersion): a pre-merge ignored
	// chain predates every gh 5033 sentinel column, so the reality check's
	// healed reading would be 0 here and must not be what the fingerprint
	// precondition compares against (gp-w0nu).
	expectMaxVersion(mock, "ignored_schema_migrations", 11)
	expectTableProbe(mock, "linear_issue_snapshots", true)
	expectTableProbe(mock, "linear_project_snapshots", true)
	expectHasContentHashColumn(mock, "ignored_schema_migrations", true)
	for _, v := range []int{10, 11} {
		expectRecordedHash(mock, "ignored_schema_migrations", v, forkFileHash(t, ignoredSource, forkRenumberedIgnoredFiles[v]))
	}
	expectCursorRewrite(mock, "ignored_schema_migrations", 10, 11)

	changed, err := reconcileForkIgnoredCursor(context.Background(), db)
	if err != nil {
		t.Fatalf("reconcileForkIgnoredCursor: %v", err)
	}
	if !changed {
		t.Fatal("changed = false, want true for a pre-merge fork ignored cursor")
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet expectations: %v", err)
	}
}

func TestReconcileForkIgnoredCursor_NoRow11_NoOp(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()

	expectCursorRowProbe(mock, "ignored_schema_migrations", 11, 0)

	changed, err := reconcileForkIgnoredCursor(context.Background(), db)
	if err != nil {
		t.Fatalf("reconcileForkIgnoredCursor: %v", err)
	}
	if changed {
		t.Fatal("changed = true, want false without the row-11 fingerprint")
	}
}

// TestForkRenumberedFilesExist pins the renumbered filenames against the
// embedded FS: if someone renumbers again without updating the reconciliation
// maps, this fails at test time instead of at reconcile time on a prod DB.
func TestForkRenumberedFilesExist(t *testing.T) {
	for _, tc := range []struct {
		src   migrationSource
		files map[int]string
	}{
		{mainSource, forkRenumberedMainFiles},
		{ignoredSource, forkRenumberedIgnoredFiles},
	} {
		for v, name := range tc.files {
			if _, err := tc.src.files.ReadFile(tc.src.dir + "/" + name); err != nil {
				t.Errorf("fork cursor row %d maps to %s, which is not embedded: %v", v, name, err)
			}
		}
	}
}

// TestNoDuplicateMigrationVersions asserts the merged migration set has no
// version collisions in either chain — the panic this merge's renumbering
// exists to avoid. list() panics on duplicates, so surviving these calls IS
// the assertion.
func TestNoDuplicateMigrationVersions(t *testing.T) {
	if got := len(mainSource.list()); got == 0 {
		t.Fatal("main migration list is empty")
	}
	if got := len(ignoredSource.list()); got == 0 {
		t.Fatal("ignored migration list is empty")
	}
	if want, got := 73, LatestVersion(); got != want {
		t.Errorf("LatestVersion() = %d, want %d (upstream 0053 tail + fork 0070-0073)", got, want)
	}
	if want, got := 29, LatestIgnoredVersion(); got != want {
		t.Errorf("LatestIgnoredVersion() = %d, want %d (upstream ignored tail 0024 + fork 0025-0027 + upstream's 0025 twin renumbered to 0028 + upstream 0026 marker renumbered to 0029)", got, want)
	}
}

// mockMySQLTableNotExistErr mimics the driver error dberrors.IsTableNotExist
// recognizes for a missing table.
type mockMySQLTableNotExistErr struct{}

func (e *mockMySQLTableNotExistErr) Error() string {
	return "Error 1146 (42S02): table not found: schema_migrations"
}

// TestReconcileForkMainCursor_PostUpmergeLeaselessShape_NoOp pins the hw-augjs
// regression: after the 2026-08 upmerge applies upstream 0055, every fork
// store shows row 54 with NO issues.lease_expires_at (0055 dropped it) and
// MAX(version)=73. The lease-column disambiguation alone misread that as a
// pre-merge fork fingerprint and refused — permanently, on every open with any
// pending work (hw's ignored chain was wedged at 11 by exactly this). Row 55
// is the surviving upstream-lineage evidence and must end the check quietly.
func TestReconcileForkMainCursor_PostUpmergeLeaselessShape_NoOp(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()

	expectCursorRowProbe(mock, "schema_migrations", 54, 1)
	expectColumnProbe(mock, "issues", "lease_expires_at", false)
	expectCursorRowProbe(mock, "schema_migrations", 55, 1)
	// No further expectations: row 55 proves upstream lineage; nothing may be
	// verified, rewritten, or refused.

	changed, err := reconcileForkMainCursor(context.Background(), db)
	if err != nil {
		t.Fatalf("reconcileForkMainCursor: %v (must not refuse the post-upmerge shape)", err)
	}
	if changed {
		t.Fatal("changed = true, want false for a post-upmerge upstream-lineage cursor")
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet expectations: %v", err)
	}
}

// expectUpmergeMainVerification queues the pre-upmerge main chain's read-only
// verification (row 73 without 56, recorded MAX 73, the fork DDL, the hashes).
func expectUpmergeMainVerification(t *testing.T, mock sqlmock.Sqlmock) {
	t.Helper()
	expectCursorRowProbe(mock, "schema_migrations", 73, 1)
	expectCursorRowProbe(mock, "schema_migrations", 56, 0)
	expectMaxVersion(mock, "schema_migrations", 73)
	expectTableProbe(mock, "linear_label_snapshots", true)
	expectColumnProbe(mock, "comments", "external_ref", true)
	expectColumnProbe(mock, "comments", "updated_at", true)
	expectTableProbe(mock, "attachments", true)
	expectHasContentHashColumn(mock, "schema_migrations", true)
	for _, v := range []int{70, 71, 72, 73} {
		expectRecordedHash(mock, "schema_migrations", v, forkFileHash(t, mainSource, upmergeMainFiles[v]))
	}
}

// TestPlanUpmergeIgnoredCursor_PreUpmergeShape_ComparesRecordedMax is
// boomtown's 2026-09-11 refusal at the unit level. The pre-upmerge ignored
// chain (1-13, 20-22) has row 22 without row 14 and a recorded MAX of 22, but
// never ran upstream ignored 0016, so currentVersion's reality check would
// read it as 0 (sentinel leases.granted_node absent) and the precondition
// refused with "MAX(version)=0". The plan must read the recorded MAX — one
// statement, no INFORMATION_SCHEMA existence probe and no sentinel probes —
// and verify the fork DDL.
func TestPlanUpmergeIgnoredCursor_PreUpmergeShape_ComparesRecordedMax(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()

	expectCursorRowProbe(mock, "ignored_schema_migrations", 22, 1)
	expectCursorRowProbe(mock, "ignored_schema_migrations", 14, 0)
	expectMaxVersion(mock, "ignored_schema_migrations", 22)
	expectTableProbe(mock, "linear_issue_snapshots", true)
	expectTableProbe(mock, "linear_project_snapshots", true)
	expectColumnProbe(mock, "wisp_comments", "external_ref", true)
	expectHasContentHashColumn(mock, "ignored_schema_migrations", true)
	for _, v := range []int{20, 21, 22} {
		expectRecordedHash(mock, "ignored_schema_migrations", v, forkFileHash(t, ignoredSource, upmergeRenumberedIgnoredFiles[v]))
	}

	rewrite, err := planUpmergeIgnoredCursor(context.Background(), db)
	if err != nil {
		t.Fatalf("planUpmergeIgnoredCursor: %v", err)
	}
	if rewrite == nil || rewrite.table != "ignored_schema_migrations" || rewrite.lo != 20 || rewrite.hi != 22 {
		t.Fatalf("rewrite = %+v, want ignored_schema_migrations rows 20-22", rewrite)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet expectations: %v", err)
	}
}

// TestPlanForkLineageRewrites_RefusalIssuesNoWrite pins the refusal-hygiene
// rule: the upmerge main chain verifies clean, the upmerge ignored chain
// refuses (a recorded fork table is missing), and NOTHING is deleted —
// sqlmock fails any Exec that was not expected, and no DELETE is expected.
// On the base the main DELETE (70-73) had already run by the time the
// ignored chain was probed.
func TestPlanForkLineageRewrites_RefusalIssuesNoWrite(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()

	expectCursorRowProbe(mock, "schema_migrations", 54, 0) // no bd-dn6 fingerprint
	expectSchemaTableExists(mock, ignoredSource.cursorTable, true)
	expectUpmergeMainVerification(t, mock)
	expectCursorRowProbe(mock, "ignored_schema_migrations", 22, 1)
	expectCursorRowProbe(mock, "ignored_schema_migrations", 14, 0)
	expectMaxVersion(mock, "ignored_schema_migrations", 22)
	expectTableProbe(mock, "linear_issue_snapshots", false) // recorded but absent

	_, err = planForkLineageCursors(context.Background(), db)
	if err == nil {
		t.Fatal("planForkLineageCursors succeeded, want the ignored-chain refusal")
	}
	for _, want := range []string{"linear_issue_snapshots", "refusing to rewrite", "working set was left as found"} {
		if !strings.Contains(err.Error(), want) {
			t.Fatalf("err = %q, want it to contain %q", err, want)
		}
	}
	if !errors.Is(err, errRefusedRewrite) {
		t.Fatalf("err = %q does not wrap errRefusedRewrite", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet expectations: %v", err)
	}
}

// A pre-upmerge store can need only ignored migrations (main is already 73).
// Refusal must precede the ignore seed and upstream tracked-cursor repair,
// including DDL, commits, and unstaging any operator changes.
func TestMigrateUpForkLineageRefusalPrecedesOpenTimeRepairs(t *testing.T) {
	db, mock := newMockDB(t)
	expectCursorProbe(mock, "schema_migrations", true)
	expectMaxVersion(mock, "schema_migrations", LatestVersion())
	expectCursorProbe(mock, "ignored_schema_migrations", true)
	expectMaxVersion(mock, "ignored_schema_migrations", 22)
	expectTableProbe(mock, "wisps", true)
	expectTableProbe(mock, "wisp_dependencies", true)
	expectColumnProbe(mock, "leases", "granted_node", false)

	expectCursorRowProbe(mock, "schema_migrations", 54, 0)
	expectSchemaTableExists(mock, ignoredSource.cursorTable, true)
	expectUpmergeMainVerification(t, mock)
	expectCursorRowProbe(mock, "ignored_schema_migrations", 22, 1)
	expectCursorRowProbe(mock, "ignored_schema_migrations", 14, 0)
	expectMaxVersion(mock, "ignored_schema_migrations", 22)
	expectTableProbe(mock, "linear_issue_snapshots", false)

	applied, err := MigrateUp(context.Background(), db)
	if applied != 0 || !errors.Is(err, errRefusedRewrite) {
		t.Fatalf("MigrateUp = %d, %v; want no work and the lineage refusal", applied, err)
	}
	if !strings.Contains(err.Error(), "working set was left as found") {
		t.Fatalf("refusal lost working-set guarantee: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

// TestPlanForkLineageCursors_TransientErrorIsNotWordedAsRefusal: a SQL error
// on a plan probe is not a refusal. The plan issues no write, so the working
// set IS as found, but the caller must not dress a transient failure in the
// refusal's wording (an operator reads "refused ... repair manually" as a
// verdict on the store); it says only that no cursor row was rewritten
// (Fable read r1 on gp-w0nu).
func TestPlanForkLineageCursors_TransientErrorIsNotWordedAsRefusal(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()

	mock.ExpectQuery(`SELECT COUNT\(\*\) FROM schema_migrations WHERE version = \?`).
		WithArgs(54).
		WillReturnError(errors.New("connection reset"))

	_, err = planForkLineageCursors(context.Background(), db)
	if err == nil {
		t.Fatal("planForkLineageCursors succeeded, want the injected probe failure")
	}
	for _, want := range []string{"connection reset", "no cursor row was rewritten"} {
		if !strings.Contains(err.Error(), want) {
			t.Fatalf("err = %q, want it to contain %q", err, want)
		}
	}
	for _, forbidden := range []string{"refusing to rewrite", "working set was left as found"} {
		if strings.Contains(err.Error(), forbidden) {
			t.Fatalf("err = %q wears the refusal's wording %q on a transient error", err, forbidden)
		}
	}
	if errors.Is(err, errRefusedRewrite) {
		t.Fatalf("err = %q wraps errRefusedRewrite, want a plain probe error", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet expectations: %v", err)
	}
}

// TestPlanForkLineageRewrites_AppliesAfterEveryChainVerified: both upmerge
// chains verify, and only then are both DELETEs issued, main first.
func TestPlanForkLineageRewrites_AppliesAfterEveryChainVerified(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()

	expectCursorRowProbe(mock, "schema_migrations", 54, 0)
	expectSchemaTableExists(mock, ignoredSource.cursorTable, true)
	expectUpmergeMainVerification(t, mock)
	expectCursorRowProbe(mock, "ignored_schema_migrations", 22, 1)
	expectCursorRowProbe(mock, "ignored_schema_migrations", 14, 0)
	expectMaxVersion(mock, "ignored_schema_migrations", 22)
	expectTableProbe(mock, "linear_issue_snapshots", true)
	expectTableProbe(mock, "linear_project_snapshots", true)
	expectColumnProbe(mock, "wisp_comments", "external_ref", true)
	expectHasContentHashColumn(mock, "ignored_schema_migrations", true)
	for _, v := range []int{20, 21, 22} {
		expectRecordedHash(mock, "ignored_schema_migrations", v, forkFileHash(t, ignoredSource, upmergeRenumberedIgnoredFiles[v]))
	}
	expectCursorRewrite(mock, "schema_migrations", 70, 73)
	expectCursorRewrite(mock, "ignored_schema_migrations", 20, 22)

	rewrites, err := planForkLineageCursors(context.Background(), db)
	if err != nil {
		t.Fatalf("planForkLineageCursors: %v", err)
	}
	changed, err := applyForkLineageRewrites(context.Background(), db, rewrites)
	if err != nil {
		t.Fatalf("applyForkLineageRewrites: %v", err)
	}
	if !changed {
		t.Fatal("changed = false, want true after both upmerge rewrites")
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet expectations: %v", err)
	}
}

// These cases pin the classification consumed by doctor, including a healed
// ignored reading of 11 that must never replace the recorded MAX in the check.
func TestVerifyForkLineageState_Upmerge(t *testing.T) {
	cases := []struct {
		name                                               string
		mainMax, ignoredMax                                int
		mainFingerprint, ignoredFingerprint, healedIgnored bool
		oldMain                                            bool
		want                                               ForkLineageStatus
		problems                                           []string
	}{
		{name: "both pre-upmerge", mainMax: 73, ignoredMax: 22, mainFingerprint: true, ignoredFingerprint: true, healedIgnored: true, want: ForkLineagePreMerge},
		{name: "main only", mainMax: 73, ignoredMax: 28, mainFingerprint: true, want: ForkLineagePreMerge},
		{name: "ignored only", mainMax: 73, ignoredMax: 22, ignoredFingerprint: true, healedIgnored: true, want: ForkLineagePreMerge},
		{name: "main unexpected MAX", mainMax: 74, ignoredMax: 22, mainFingerprint: true, ignoredFingerprint: true, want: ForkLineageInconsistent, problems: []string{"schema_migrations row 73 coexists with MAX(version)=74; reconciliation will refuse this cursor"}},
		{name: "ignored unexpected recorded MAX despite replay floor", mainMax: 73, ignoredMax: 28, mainFingerprint: true, ignoredFingerprint: true, healedIgnored: true, want: ForkLineageInconsistent, problems: []string{"ignored_schema_migrations row 22 coexists with MAX(version)=28; reconciliation will refuse this cursor"}},
		{name: "both unexpected MAX", mainMax: 74, ignoredMax: 23, mainFingerprint: true, ignoredFingerprint: true, want: ForkLineageInconsistent, problems: []string{"schema_migrations row 73 coexists with MAX(version)=74; reconciliation will refuse this cursor", "ignored_schema_migrations row 22 coexists with MAX(version)=23; reconciliation will refuse this cursor"}},
		{name: "bd-dn6 main with inconsistent upmerge ignored", mainMax: 54, ignoredMax: 28, oldMain: true, ignoredFingerprint: true, healedIgnored: true, want: ForkLineageInconsistent, problems: []string{"ignored_schema_migrations row 22 coexists with MAX(version)=28; reconciliation will refuse this cursor"}},
		{name: "merged main 73 ignored 1-28", mainMax: 73, ignoredMax: 28, want: ForkLineageReconciled},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			db, mock, err := sqlmock.New()
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			expectCursorProbe(mock, "schema_migrations", true)
			expectMaxVersion(mock, "schema_migrations", tc.mainMax)
			expectCursorProbe(mock, "ignored_schema_migrations", true)
			expectMaxVersion(mock, "ignored_schema_migrations", tc.ignoredMax)
			expectTableProbe(mock, "wisps", true)
			expectTableProbe(mock, "wisp_dependencies", true)
			expectColumnProbe(mock, "leases", "granted_node", !tc.healedIgnored)
			expectMaxVersion(mock, "ignored_schema_migrations", tc.ignoredMax)
			expectCursorRowProbe(mock, "schema_migrations", 54, 1)
			if tc.oldMain {
				expectCursorRowProbe(mock, "ignored_schema_migrations", 11, 0)
			} else {
				expectCursorRowProbe(mock, "ignored_schema_migrations", 11, 1)
			}
			expectColumnProbe(mock, "issues", "lease_expires_at", false)
			if tc.oldMain {
				expectCursorRowProbe(mock, "schema_migrations", 55, 0)
				expectCursorRowProbe(mock, "schema_migrations", 73, 0)
			} else {
				expectCursorRowProbe(mock, "schema_migrations", 55, 1)
				expectCursorRowProbe(mock, "schema_migrations", 73, 1)
			}
			main56, ignored14 := 1, 1
			if tc.mainFingerprint {
				main56 = 0
			}
			if tc.ignoredFingerprint {
				ignored14 = 0
			}
			if !tc.oldMain {
				expectCursorRowProbe(mock, "schema_migrations", 56, main56)
			}
			expectCursorRowProbe(mock, "ignored_schema_migrations", 22, 1)
			expectCursorRowProbe(mock, "ignored_schema_migrations", 14, ignored14)
			if tc.want == ForkLineagePreMerge {
				if tc.mainFingerprint {
					expectTableProbe(mock, "linear_label_snapshots", true)
					expectColumnProbe(mock, "comments", "external_ref", true)
					expectColumnProbe(mock, "comments", "updated_at", true)
					expectTableProbe(mock, "attachments", true)
					expectPreUpmergeMainTailProbes(mock, "")
				}
				if tc.ignoredFingerprint {
					expectTableProbe(mock, "linear_issue_snapshots", true)
					expectTableProbe(mock, "linear_project_snapshots", true)
					expectColumnProbe(mock, "wisp_comments", "external_ref", true)
					expectPreUpmergeIgnoredTailProbes(mock, "")
				}
			}
			if tc.want == ForkLineagePreMerge {
				if !tc.mainFingerprint && !tc.oldMain && tc.mainMax >= 70 {
					expectMainReconciledLineageProbes(mock, true)
				}
				if !tc.ignoredFingerprint && !tc.healedIgnored && tc.ignoredMax >= 20 {
					expectIgnoredReconciledLineageProbes(mock, tc.ignoredMax, true)
				}
			}
			if tc.want == ForkLineageReconciled {
				expectReconciledLineageProbes(mock)
			}

			report, err := VerifyForkLineageState(context.Background(), db)
			if err != nil {
				t.Fatal(err)
			}
			if report.Status != tc.want {
				t.Fatalf("Status = %q, want %q; report: %+v", report.Status, tc.want, report)
			}
			var wantSchemes []string
			if tc.oldMain {
				wantSchemes = []string{"bd-dn6", "2026-08 upmerge"}
			} else if tc.want != ForkLineageReconciled {
				wantSchemes = []string{"2026-08 upmerge"}
			}
			if !reflect.DeepEqual(report.PreMergeSchemes, wantSchemes) {
				t.Fatalf("PreMergeSchemes = %v, want %v", report.PreMergeSchemes, wantSchemes)
			}
			if !reflect.DeepEqual(report.Problems, tc.problems) {
				t.Fatalf("Problems = %v, want %v", report.Problems, tc.problems)
			}
			if tc.healedIgnored && (report.IgnoredVersion != 11 || report.IgnoredCursorMax != tc.ignoredMax || report.IgnoredCursorNote == "") {
				t.Fatalf("lost recorded/healed ignored distinction: %+v", report)
			}
			if err := mock.ExpectationsWereMet(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func expectReconciledLineageProbes(mock sqlmock.Sqlmock) {
	expectMainReconciledLineageProbes(mock, true)
	expectIgnoredReconciledLineageProbes(mock, 27, true)
}

func expectMainReconciledLineageProbes(mock sqlmock.Sqlmock, externalRef bool) {
	for _, v := range []int{70, 71, 72, 73} {
		expectCursorRowProbe(mock, "schema_migrations", v, 1)
	}
	expectTableProbe(mock, "linear_label_snapshots", true)
	expectColumnProbe(mock, "comments", "external_ref", externalRef)
	expectColumnProbe(mock, "comments", "updated_at", true)
	expectLineageIndexProbe(mock, "comments", "idx_comments_external_ref")
	expectTableProbe(mock, "attachments", true)
	expectLineageIndexProbe(mock, "issues", "idx_issues_status_updated_at")
	expectLineageIndexProbe(mock, "issues", "idx_issues_defer_until")
	expectLineageDefaultProbe(mock, "events")
	expectLineageDefaultProbe(mock, "comments")
}

func expectIgnoredReconciledLineageProbes(mock sqlmock.Sqlmock, version int, issueSnapshots bool) {
	for _, v := range []int{20, 21} {
		expectCursorRowProbe(mock, "ignored_schema_migrations", v, 1)
	}
	expectTableProbe(mock, "linear_issue_snapshots", issueSnapshots)
	expectTableProbe(mock, "linear_project_snapshots", true)
	expectLineageDefaultProbe(mock, "wisp_events")
	if version >= 25 {
		for _, v := range []int{25, 26, 27} {
			expectCursorRowProbe(mock, "ignored_schema_migrations", v, 1)
		}
	}
}

func expectLineageIndexProbe(mock sqlmock.Sqlmock, table, index string) {
	mock.ExpectQuery(`SELECT COUNT\(\*\) FROM INFORMATION_SCHEMA\.STATISTICS`).WithArgs(table, index).WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(1))
}

func expectLineageDefaultProbe(mock sqlmock.Sqlmock, table string) {
	mock.ExpectQuery(`SELECT COLUMN_DEFAULT FROM INFORMATION_SCHEMA\.COLUMNS`).WithArgs(table, "id").WillReturnRows(sqlmock.NewRows([]string{"COLUMN_DEFAULT"}).AddRow(nil))
}

// Each missing planner-probed effect must prevent the next-write promise,
// including on the older bd-dn6 scheme. Expectations use the recorded DDL
// independently of the production probe lists.
func TestVerifyForkLineageState_PreMergeEffects(t *testing.T) {
	type effect struct {
		table, column, desc string
	}
	for _, tc := range []struct {
		name, cursor, migrations               string
		mainMax, ignoredMax                    int
		oldMain, oldIgnored, upMain, upIgnored bool
		effects                                []effect
	}{
		{"upmerge main after row 55 clears bd-dn6", "schema_migrations", "70-73", 73, 0, false, false, true, false, []effect{
			{"linear_label_snapshots", "", "table linear_label_snapshots (fork 0070)"},
			{"comments", "external_ref", "column comments.external_ref (fork 0072)"},
			{"comments", "updated_at", "column comments.updated_at (fork 0072)"},
			{"attachments", "", "table attachments (fork 0073)"},
		}},
		{"upmerge ignored", "ignored_schema_migrations", "20-22", 50, 22, false, false, false, true, []effect{
			{"linear_issue_snapshots", "", "table linear_issue_snapshots (fork ignored 0020)"},
			{"linear_project_snapshots", "", "table linear_project_snapshots (fork ignored 0021)"},
			{"wisp_comments", "external_ref", "column wisp_comments.external_ref (fork ignored 0022)"},
		}},
		{"bd-dn6 main", "schema_migrations", "51-54", 54, 0, true, false, false, false, []effect{
			{"linear_label_snapshots", "", "table linear_label_snapshots (fork 0051)"},
			{"comments", "external_ref", "column comments.external_ref (fork 0053)"},
			{"comments", "updated_at", "column comments.updated_at (fork 0053)"},
			{"attachments", "", "table attachments (fork 0054)"},
		}},
		{"bd-dn6 ignored", "ignored_schema_migrations", "10-11", 50, 11, false, true, false, false, []effect{
			{"linear_issue_snapshots", "", "table linear_issue_snapshots (fork ignored 0010)"},
			{"linear_project_snapshots", "", "table linear_project_snapshots (fork ignored 0011)"},
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			for missing := -1; missing < len(tc.effects); missing++ {
				name := "all effects present"
				if missing >= 0 {
					name = "missing " + tc.effects[missing].desc
				}
				t.Run(name, func(t *testing.T) {
					db, mock, err := sqlmock.New()
					if err != nil {
						t.Fatal(err)
					}
					defer db.Close()
					expectCursorProbe(mock, "schema_migrations", true)
					expectMaxVersion(mock, "schema_migrations", tc.mainMax)
					expectCursorProbe(mock, "ignored_schema_migrations", true)
					expectMaxVersion(mock, "ignored_schema_migrations", tc.ignoredMax)
					if tc.ignoredMax > 0 {
						expectTableProbe(mock, "wisps", false)
					}
					expectMaxVersion(mock, "ignored_schema_migrations", tc.ignoredMax)
					count := func(present bool) int {
						if present {
							return 1
						}
						return 0
					}
					expectCursorRowProbe(mock, "schema_migrations", 54, count(tc.oldMain || tc.upMain))
					expectCursorRowProbe(mock, "ignored_schema_migrations", 11, count(tc.oldIgnored))
					if tc.oldMain || tc.oldIgnored || tc.upMain {
						expectColumnProbe(mock, "issues", "lease_expires_at", false)
						// Upmerge stores retain row 54 after upstream row 55 drops the lease columns.
						expectCursorRowProbe(mock, "schema_migrations", 55, count(tc.upMain))
					}
					expectCursorRowProbe(mock, "schema_migrations", 73, count(tc.upMain))
					if tc.upMain {
						expectCursorRowProbe(mock, "schema_migrations", 56, 0)
					}
					expectCursorRowProbe(mock, "ignored_schema_migrations", 22, count(tc.upIgnored))
					if tc.upIgnored {
						expectCursorRowProbe(mock, "ignored_schema_migrations", 14, 0)
					}
					for i, effect := range tc.effects {
						if effect.column == "" {
							expectTableProbe(mock, effect.table, i != missing)
						} else {
							expectColumnProbe(mock, effect.table, effect.column, i != missing)
						}
					}
					if tc.upMain {
						expectPreUpmergeMainTailProbes(mock, "")
					}
					if tc.upIgnored {
						missingTable := ""
						if missing >= 0 && tc.effects[missing].column == "" {
							missingTable = tc.effects[missing].table
						}
						expectPreUpmergeIgnoredTailProbes(mock, missingTable)
					}
					report, err := VerifyForkLineageState(context.Background(), db)
					if err != nil {
						t.Fatal(err)
					}
					want := ForkLineagePreMerge
					var problems []string
					if missing >= 0 {
						want = ForkLineageInconsistent
						problems = []string{fmt.Sprintf("%s records fork migrations %s but %s is missing; schema does not match the recorded cursor", tc.cursor, tc.migrations, tc.effects[missing].desc)}
					}
					if tc.upIgnored && missing >= 0 && tc.effects[missing].column == "" {
						version := "0020/0025"
						if missing == 1 {
							version = "0021/0026"
						}
						problems = append(problems, fmt.Sprintf("table %s (fork ignored %s)", tc.effects[missing].table, version))
					}
					if report.Status != want || !reflect.DeepEqual(report.Problems, problems) {
						t.Fatalf("report = %+v; want %s with Problems %v", report, want, problems)
					}
					if err := mock.ExpectationsWereMet(); err != nil {
						t.Fatal(err)
					}
				})
			}
		})
	}
}

func TestVerifyForkLineageState_NotApplicable(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	expectCursorProbe(mock, "schema_migrations", true)
	expectMaxVersion(mock, "schema_migrations", 50)
	expectCursorProbe(mock, "ignored_schema_migrations", true)
	expectMaxVersion(mock, "ignored_schema_migrations", 0)
	expectMaxVersion(mock, "ignored_schema_migrations", 0)
	expectCursorRowProbe(mock, "schema_migrations", 54, 0)
	expectCursorRowProbe(mock, "ignored_schema_migrations", 11, 0)
	// Absent fingerprint rows short-circuit their complementary row probes.
	expectCursorRowProbe(mock, "schema_migrations", 73, 0)
	expectCursorRowProbe(mock, "ignored_schema_migrations", 22, 0)
	report, err := VerifyForkLineageState(context.Background(), db)
	if err != nil {
		t.Fatal(err)
	}
	if report.Status != ForkLineageNotApplicable || len(report.PreMergeSchemes) != 0 || len(report.Problems) != 0 {
		t.Fatalf("report = %+v; want not applicable without schemes or problems", report)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

// A pre-merge chain cannot hide drift in the other, already-reconciled chain.
func TestVerifyForkLineageState_MixedChains(t *testing.T) {
	for _, tc := range []struct {
		name           string
		preMain        bool
		ignoredVersion int
		missing        string
	}{
		{"merged main missing column beside pre-upmerge ignored", false, 22, "column comments.external_ref (fork 0072)"},
		{"pre-upmerge main beside reconciled ignored missing table", true, 27, "table linear_issue_snapshots (fork ignored 0020/0025)"},
		{"healthy merged main beside pre-upmerge ignored", false, 22, ""},
		{"healthy pre-upmerge main beside reconciled ignored", true, 27, ""},
		{"pre-upmerge main beside empty ignored cursor", true, 0, ""},
		{"pre-upmerge main beside ignored at first reconciled threshold", true, 20, ""},
		{"pre-upmerge main beside ignored at upmerge threshold", true, 25, ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			db, mock, err := sqlmock.New()
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			expectCursorProbe(mock, "schema_migrations", true)
			expectMaxVersion(mock, "schema_migrations", 73)
			expectCursorProbe(mock, "ignored_schema_migrations", true)
			expectMaxVersion(mock, "ignored_schema_migrations", tc.ignoredVersion)
			if tc.ignoredVersion > 0 {
				expectTableProbe(mock, "wisps", true)
				expectTableProbe(mock, "wisp_dependencies", true)
				expectColumnProbe(mock, "leases", "granted_node", true)
			}
			expectMaxVersion(mock, "ignored_schema_migrations", tc.ignoredVersion)
			expectCursorRowProbe(mock, "schema_migrations", 54, 1)
			expectCursorRowProbe(mock, "ignored_schema_migrations", 11, 0)
			expectColumnProbe(mock, "issues", "lease_expires_at", false)
			expectCursorRowProbe(mock, "schema_migrations", 55, 1)
			expectCursorRowProbe(mock, "schema_migrations", 73, 1)
			main56 := 1
			if tc.preMain {
				main56 = 0
			}
			expectCursorRowProbe(mock, "schema_migrations", 56, main56)
			if tc.ignoredVersion >= 22 {
				expectCursorRowProbe(mock, "ignored_schema_migrations", 22, 1)
				ignored14 := 0
				if tc.preMain {
					ignored14 = 1
				}
				expectCursorRowProbe(mock, "ignored_schema_migrations", 14, ignored14)
			} else {
				expectCursorRowProbe(mock, "ignored_schema_migrations", 22, 0)
			}
			if tc.preMain {
				expectTableProbe(mock, "linear_label_snapshots", true)
				expectColumnProbe(mock, "comments", "external_ref", true)
				expectColumnProbe(mock, "comments", "updated_at", true)
				expectTableProbe(mock, "attachments", true)
				expectPreUpmergeMainTailProbes(mock, "")
				if tc.ignoredVersion >= 20 {
					expectIgnoredReconciledLineageProbes(mock, tc.ignoredVersion, tc.missing == "")
				}
			} else {
				expectTableProbe(mock, "linear_issue_snapshots", true)
				expectTableProbe(mock, "linear_project_snapshots", true)
				expectColumnProbe(mock, "wisp_comments", "external_ref", true)
				expectPreUpmergeIgnoredTailProbes(mock, "")
				expectMainReconciledLineageProbes(mock, tc.missing == "")
			}
			report, err := VerifyForkLineageState(context.Background(), db)
			if err != nil {
				t.Fatal(err)
			}
			want := ForkLineagePreMerge
			var problems []string
			if tc.missing != "" {
				want = ForkLineageInconsistent
				problems = []string{tc.missing}
			}
			if report.Status != want || !reflect.DeepEqual(report.Problems, problems) {
				t.Fatalf("report = %+v; want %s with Problems %v", report, want, problems)
			}
			if !reflect.DeepEqual(report.PreMergeSchemes, []string{"2026-08 upmerge"}) {
				t.Fatalf("PreMergeSchemes = %v; want only 2026-08 upmerge", report.PreMergeSchemes)
			}
			if err := mock.ExpectationsWereMet(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

// These expectations describe the tail additions independently of the
// production lists. Planner probes with identical descriptions run only once.
func expectPreUpmergeMainTailProbes(mock sqlmock.Sqlmock, missing string) {
	for _, v := range []int{70, 71, 72, 73} {
		expectCursorRowProbe(mock, "schema_migrations", v, 1)
	}
	for _, index := range []struct{ table, name string }{
		{"comments", "idx_comments_external_ref"},
		{"issues", "idx_issues_status_updated_at"},
		{"issues", "idx_issues_defer_until"},
	} {
		count := 1
		if index.name == missing {
			count = 0
		}
		mock.ExpectQuery(`SELECT COUNT\(\*\) FROM INFORMATION_SCHEMA\.STATISTICS`).WithArgs(index.table, index.name).
			WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(count))
	}
	expectLineageDefaultProbe(mock, "events")
	expectLineageDefaultProbe(mock, "comments")
}

func expectPreUpmergeIgnoredTailProbes(mock sqlmock.Sqlmock, missing string) {
	for _, v := range []int{20, 21} {
		expectCursorRowProbe(mock, "ignored_schema_migrations", v, 1)
	}
	// Tail descriptions include both old and new numbers, so these are
	// distinct probes from the planner's single-number descriptions.
	expectTableProbe(mock, "linear_issue_snapshots", missing != "linear_issue_snapshots")
	expectTableProbe(mock, "linear_project_snapshots", missing != "linear_project_snapshots")
	var value any
	if missing == "wisp_events" {
		value = "uuid()"
	}
	mock.ExpectQuery(`SELECT COLUMN_DEFAULT FROM INFORMATION_SCHEMA\.COLUMNS`).WithArgs("wisp_events", "id").
		WillReturnRows(sqlmock.NewRows([]string{"COLUMN_DEFAULT"}).AddRow(value))
}

func TestVerifyForkLineageState_PreUpmergeTailEffects(t *testing.T) {
	for _, tc := range []struct {
		name, missing, problem string
		main                   bool
	}{
		{"missing upstream defer index", "idx_issues_defer_until", "index issues.idx_issues_defer_until (upstream 0052)", true},
		{"missing fork comment index", "idx_comments_external_ref", "index comments.idx_comments_external_ref (fork 0072)", true},
		{"healed ignored missing upstream default drop", "wisp_events", "wisp_events.id DEFAULT dropped (upstream ignored 0010)", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			db, mock, err := sqlmock.New()
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			mainMax, ignoredMax := 50, 22
			if tc.main {
				mainMax, ignoredMax = 73, 0
			}
			expectCursorProbe(mock, "schema_migrations", true)
			expectMaxVersion(mock, "schema_migrations", mainMax)
			expectCursorProbe(mock, "ignored_schema_migrations", true)
			expectMaxVersion(mock, "ignored_schema_migrations", ignoredMax)
			if !tc.main {
				expectTableProbe(mock, "wisps", true)
				expectTableProbe(mock, "wisp_dependencies", true)
				expectColumnProbe(mock, "leases", "granted_node", false)
			}
			expectMaxVersion(mock, "ignored_schema_migrations", ignoredMax)
			if tc.main {
				expectCursorRowProbe(mock, "schema_migrations", 54, 1)
				expectCursorRowProbe(mock, "ignored_schema_migrations", 11, 0)
				expectColumnProbe(mock, "issues", "lease_expires_at", false)
				expectCursorRowProbe(mock, "schema_migrations", 55, 1)
				expectCursorRowProbe(mock, "schema_migrations", 73, 1)
				expectCursorRowProbe(mock, "schema_migrations", 56, 0)
				expectCursorRowProbe(mock, "ignored_schema_migrations", 22, 0)
				expectTableProbe(mock, "linear_label_snapshots", true)
				expectColumnProbe(mock, "comments", "external_ref", true)
				expectColumnProbe(mock, "comments", "updated_at", true)
				expectTableProbe(mock, "attachments", true)
				expectPreUpmergeMainTailProbes(mock, tc.missing)
			} else {
				expectCursorRowProbe(mock, "schema_migrations", 54, 0)
				expectCursorRowProbe(mock, "ignored_schema_migrations", 11, 0)
				expectCursorRowProbe(mock, "schema_migrations", 73, 0)
				expectCursorRowProbe(mock, "ignored_schema_migrations", 22, 1)
				expectCursorRowProbe(mock, "ignored_schema_migrations", 14, 0)
				expectTableProbe(mock, "linear_issue_snapshots", true)
				expectTableProbe(mock, "linear_project_snapshots", true)
				expectColumnProbe(mock, "wisp_comments", "external_ref", true)
				expectPreUpmergeIgnoredTailProbes(mock, tc.missing)
			}
			report, err := VerifyForkLineageState(context.Background(), db)
			if err != nil {
				t.Fatal(err)
			}
			if report.Status != ForkLineageInconsistent || !reflect.DeepEqual(report.Problems, []string{tc.problem}) {
				t.Fatalf("report = %+v; want inconsistent with %q", report, tc.problem)
			}
			if !tc.main && (report.IgnoredCursorMax != 22 || report.IgnoredVersion != 11) {
				t.Fatalf("lost recorded/healed cursor distinction: %+v", report)
			}
			if !reflect.DeepEqual(report.PreMergeSchemes, []string{"2026-08 upmerge"}) {
				t.Fatalf("lost upmerge scheme: %+v", report)
			}
			if err := mock.ExpectationsWereMet(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

// Every reconciled probe must be checked before PreMerge or explicitly
// classified here. Keep descriptions literal so new tail probes cannot hide
// behind migration-number ranges or a list generated from production code.
func TestPreMergeExpectedProbes_EnumerateReconciledTail(t *testing.T) {
	ctx := context.Background()
	// Main's fingerprint requires MAX 73; it has no version-gated tail.
	mainUpmerge := preMergeMainExpectedProbes(ctx, nil, true)
	ignoredUpmerge := preMergeIgnoredExpectedProbes(ctx, nil, true, 22)
	mainLegacy := preMergeMainExpectedProbes(ctx, nil, false)
	ignoredLegacy := preMergeIgnoredExpectedProbes(ctx, nil, false, 11)

	lateIgnored := map[string]string{
		"cursor row 0025 (fork ignored create_linear_issue_snapshots)":   "The pre-upmerge fork records this at 20; the next pass records 25.",
		"cursor row 0026 (fork ignored create_linear_project_snapshots)": "The pre-upmerge fork records this at 21; the next pass records 26.",
		"cursor row 0027 (fork ignored add_wisp_comment_external_ref)":   "The pre-upmerge fork records this at 22; the next pass records 27.",
	}
	legacyMainAbsent := map[string]string{
		"cursor row 0070 (fork create_linear_label_snapshots)":      "bd-dn6 records this at 51; the next pass records 70.",
		"cursor row 0071 (fork linear_snapshots_dolt_ignore)":       "bd-dn6 records this at 52; the next pass records 71.",
		"cursor row 0072 (fork add_comment_external_ref)":           "bd-dn6 records this at 53; the next pass records 72.",
		"cursor row 0073 (fork create_attachments)":                 "bd-dn6 records this at 54; the next pass records 73.",
		"index comments.idx_comments_external_ref (fork 0072)":      "The legacy planner does not require it; guarded 0072 creates it on the next pass.",
		"index issues.idx_issues_status_updated_at (upstream 0052)": "Upstream 0052 first runs after the fork's 51-54 rows are removed.",
		"index issues.idx_issues_defer_until (upstream 0052)":       "Upstream 0052 first runs after the fork's 51-54 rows are removed.",
		"events.id DEFAULT dropped (upstream 0051)":                 "Upstream 0051 first runs after the fork's 51-54 rows are removed.",
		"comments.id DEFAULT dropped (upstream 0051)":               "Upstream 0051 first runs after the fork's 51-54 rows are removed.",
	}
	legacyIgnoredAbsent := map[string]string{
		"cursor row 0020 (ignored chain)":                                "bd-dn6 has only reached ignored 11; upstream 20 is applied after the rewrite.",
		"cursor row 0021 (ignored chain)":                                "bd-dn6 has only reached ignored 11; upstream 21 is applied after the rewrite.",
		"wisp_events.id DEFAULT dropped (upstream ignored 0010)":         "Upstream ignored 0010 first runs after the fork's 10-11 rows are removed.",
		"cursor row 0025 (fork ignored create_linear_issue_snapshots)":   "bd-dn6 records this at 10; the next pass records 25.",
		"cursor row 0026 (fork ignored create_linear_project_snapshots)": "bd-dn6 records this at 11; the next pass records 26.",
		"cursor row 0027 (fork ignored add_wisp_comment_external_ref)":   "The legacy fork has no ignored comment-ref migration; 27 runs on the next pass.",
	}
	for _, tc := range []struct {
		name            string
		expected        []preMergeExpectedProbe
		tail, planner   []lineageEffectProbe
		absent, renamed map[string]string
	}{
		{"upmerge main 73", mainUpmerge, mainReconciledProbes(ctx, nil), nil, nil, nil},
		{"upmerge ignored recorded 22", ignoredUpmerge, ignoredReconciledProbes(ctx, nil, 27), nil, lateIgnored, nil},
		{"bd-dn6 main 54", mainLegacy, mainReconciledProbes(ctx, nil), forkMainEffectProbes(ctx, nil), legacyMainAbsent, map[string]string{
			"table linear_label_snapshots (fork 0070)": "table linear_label_snapshots (fork 0051)", // Same table, original fork number.
			"column comments.external_ref (fork 0072)": "column comments.external_ref (fork 0053)", // Same column, original fork number.
			"column comments.updated_at (fork 0072)":   "column comments.updated_at (fork 0053)",   // Same column, original fork number.
			"table attachments (fork 0073)":            "table attachments (fork 0054)",            // Same table, original fork number.
		}},
		{"bd-dn6 ignored 11", ignoredLegacy, ignoredReconciledProbes(ctx, nil, 27), forkIgnoredEffectProbes(ctx, nil), legacyIgnoredAbsent, map[string]string{
			"table linear_issue_snapshots (fork ignored 0020/0025)":   "table linear_issue_snapshots (fork ignored 0010)",   // Same table, original fork number.
			"table linear_project_snapshots (fork ignored 0021/0026)": "table linear_project_snapshots (fork ignored 0011)", // Same table, original fork number.
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			expected := make(map[string]bool)
			var gotDescs, plannerDescs []string
			for _, probe := range tc.expected {
				if expected[probe.desc] {
					t.Errorf("duplicate expected probe %q", probe.desc)
				}
				expected[probe.desc] = true
				gotDescs = append(gotDescs, probe.desc)
			}
			if tc.planner != nil {
				for _, probe := range tc.planner {
					plannerDescs = append(plannerDescs, probe.desc)
				}
				if !reflect.DeepEqual(gotDescs, plannerDescs) {
					t.Fatalf("bd-dn6 expected set = %v; want exactly planner order %v", gotDescs, plannerDescs)
				}
			}
			tail := make(map[string]bool)
			for _, probe := range tc.tail {
				tail[probe.desc] = true
				classes := 0
				if expected[probe.desc] {
					classes++
				}
				if reason, ok := tc.absent[probe.desc]; ok {
					classes++
					if reason == "" {
						t.Errorf("missing reason for %q", probe.desc)
					}
				}
				if old, ok := tc.renamed[probe.desc]; ok {
					classes++
					if !expected[old] {
						t.Errorf("renamed probe %q has no legacy probe %q", probe.desc, old)
					}
				}
				if classes != 1 {
					t.Errorf("tail probe %q has %d classifications; want exactly one", probe.desc, classes)
				}
			}
			for desc := range tc.absent {
				if !tail[desc] {
					t.Errorf("stale absent-probe allowlist entry %q", desc)
				}
			}
			for desc := range tc.renamed {
				if !tail[desc] {
					t.Errorf("stale renamed-probe entry %q", desc)
				}
			}
		})
	}
}
