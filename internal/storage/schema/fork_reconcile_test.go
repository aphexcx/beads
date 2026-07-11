package schema

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
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

// expectForkMainVerification queues the happy-path probe sequence for
// reconcileForkMainCursor up to (not including) the DELETE.
func expectForkMainVerification(t *testing.T, mock sqlmock.Sqlmock) {
	t.Helper()
	expectCursorRowProbe(mock, "schema_migrations", 54, 1)
	expectColumnProbe(mock, "issues", "lease_expires_at", false)
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
	mock.ExpectExec(`DELETE FROM schema_migrations WHERE version BETWEEN 51 AND \?`).
		WithArgs(54).
		WillReturnResult(sqlmock.NewResult(0, 4))

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
	mock.ExpectExec(`DELETE FROM schema_migrations WHERE version BETWEEN 51 AND \?`).
		WithArgs(54).
		WillReturnResult(sqlmock.NewResult(0, 4))

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
	mock.ExpectExec(`DELETE FROM schema_migrations WHERE version BETWEEN 51 AND \?`).
		WithArgs(54).
		WillReturnResult(sqlmock.NewResult(0, 4))

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

	expectMaxVersion(mock, "schema_migrations", 73)
	expectMaxVersion(mock, "ignored_schema_migrations", 11)
	expectCursorRowProbe(mock, "schema_migrations", 54, 1) // row 54 despite MAX=73
	expectCursorRowProbe(mock, "ignored_schema_migrations", 11, 1)
	expectColumnProbe(mock, "issues", "lease_expires_at", false)

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
	expectMaxVersion(mock, "ignored_schema_migrations", 11)
	expectTableProbe(mock, "linear_issue_snapshots", true)
	expectTableProbe(mock, "linear_project_snapshots", true)
	expectHasContentHashColumn(mock, "ignored_schema_migrations", true)
	for _, v := range []int{10, 11} {
		expectRecordedHash(mock, "ignored_schema_migrations", v, forkFileHash(t, ignoredSource, forkRenumberedIgnoredFiles[v]))
	}
	mock.ExpectExec(`DELETE FROM ignored_schema_migrations WHERE version BETWEEN 10 AND \?`).
		WithArgs(11).
		WillReturnResult(sqlmock.NewResult(0, 2))

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
	if want, got := 22, LatestIgnoredVersion(); got != want {
		t.Errorf("LatestIgnoredVersion() = %d, want %d (upstream ignored 0010 + fork 0020-0022)", got, want)
	}
}

// mockMySQLTableNotExistErr mimics the driver error dberrors.IsTableNotExist
// recognizes for a missing table.
type mockMySQLTableNotExistErr struct{}

func (e *mockMySQLTableNotExistErr) Error() string {
	return "Error 1146 (42S02): table not found: schema_migrations"
}
