package schema

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"fmt"
	"sort"

	"github.com/steveyegge/beads/internal/storage/dberrors"
)

// Fork-lineage cursor reconciliation (bd-dn6).
//
// Before the v1.1.0-rc.1 upstream merge, the fork shipped four main-chain
// migrations as 0051-0054 (create_linear_label_snapshots,
// linear_snapshots_dolt_ignore, add_comment_external_ref, create_attachments)
// and two ignored-chain migrations as 0010-0011 (create_linear_issue_snapshots,
// create_linear_project_snapshots). Upstream v1.1.0-rc.1 ships different
// migrations under the same numbers: main 0051-0053 (drop_aux_id_defaults,
// add_date_indexes, repair_rig_wisps) and ignored 0010
// (drop_wisp_id_defaults). The fork files were renumbered byte-identically to
// main 0070-0073 and ignored 0020-0021.
//
// Databases migrated by a pre-merge fork binary record MAX(version)=54 (main)
// and MAX(version)=11 (ignored) with fork semantics. Because the cursors are
// MAX-based, this binary would otherwise treat upstream's 0051-0053 and
// ignored 0010 as already applied and silently skip their DDL — leaving the
// database without the date indexes, aux-id default drops, and rig-wisp
// repair while claiming the same version as databases that have them.
//
// The reconciliation verifies the fork DDL is actually present (column-by-
// column INFORMATION_SCHEMA probes, plus content-hash cross-checks where the
// cursor recorded hashes) and then deletes the fork's cursor rows (main
// 51-54, ignored 10-11). That demotes the cursors to 50/9, so the same
// migration pass applies upstream 0051-0053 / ignored 0010 for real and
// re-runs the renumbered fork migrations as guarded no-ops (CREATE TABLE IF
// NOT EXISTS / INFORMATION_SCHEMA-guarded ALTERs / REPLACE INTO), re-recording
// them at 0070-0073 / 0020-0021 with correct content hashes. After one pass
// the cursor state is indistinguishable from a database that migrated through
// the merged lineage from scratch, and this reconciliation never fires again
// (the trigger rows 54/11 no longer exist).
const (
	forkPreMergeMainMax    = 54
	forkPreMergeIgnoredMax = 11
)

// forkRenumberedMainFiles maps each pre-merge fork main-chain cursor row to
// the renumbered migration file carrying the byte-identical content that row
// was recorded against.
var forkRenumberedMainFiles = map[int]string{
	51: "0070_create_linear_label_snapshots.up.sql",
	52: "0071_linear_snapshots_dolt_ignore.up.sql",
	53: "0072_add_comment_external_ref.up.sql",
	54: "0073_create_attachments.up.sql",
}

// forkRenumberedIgnoredFiles is the ignored-chain equivalent of
// forkRenumberedMainFiles.
var forkRenumberedIgnoredFiles = map[int]string{
	10: "0020_create_linear_issue_snapshots.up.sql",
	11: "0021_create_linear_project_snapshots.up.sql",
}

// reconcileForkLineageCursors detects a database whose migration cursors were
// written by a pre-merge fork binary and rewrites them to the renumbered
// scheme. Returns whether either cursor was changed. Called from MigrateUp
// before pending versions are computed; a no-op on fresh databases, upstream-
// lineage databases, and databases already reconciled.
func reconcileForkLineageCursors(ctx context.Context, db DBConn) (bool, error) {
	mainChanged, err := reconcileForkMainCursor(ctx, db)
	if err != nil {
		return false, err
	}
	if !mainChanged {
		// No pre-merge fork main cursor (or row 54 was upstream's own lease
		// migration — see the disambiguation in reconcileForkMainCursor).
		// Since the upstream-20260710 merge, upstream also owns an ignored
		// migration 0011 (cleanup_orphaned_child_counters), so the ignored
		// fingerprint row 11 is only meaningful together with the main one:
		// running the ignored pass on a genuine upstream database would trip
		// its refuse-to-rewrite guard.
		return false, nil
	}
	ignoredChanged, err := reconcileForkIgnoredCursor(ctx, db)
	if err != nil {
		return mainChanged, err
	}
	return mainChanged || ignoredChanged, nil
}

func reconcileForkMainCursor(ctx context.Context, db DBConn) (bool, error) {
	// Row 54 is the fork-lineage fingerprint: upstream's chain has never had a
	// migration 0054 (it jumps 0053 → this merge's 0070), so only a pre-merge
	// fork binary can have recorded it.
	has54, err := cursorRowExists(ctx, db, mainSource.cursorTable, forkPreMergeMainMax)
	if err != nil || !has54 {
		return false, err
	}

	// Post-merge disambiguation: since the upstream-20260710 merge, upstream
	// ALSO owns a migration 0054 (add_lease_columns, schema v54), so row 54
	// alone no longer proves pre-merge fork lineage. Fork 0054 was
	// create_attachments and never touched issues; upstream's 0054 adds
	// issues.lease_expires_at. If the lease column is present, row 54 came
	// from upstream's chain (a genuine upstream database, or a fork database
	// already reconciled that then applied upstream 0054) — nothing to
	// rewrite, and the strict MAX(version) check below must not fire.
	hasLease, err := columnExists(ctx, db, "issues", "lease_expires_at")
	if err != nil {
		return false, fmt.Errorf("disambiguating fork lineage (issues.lease_expires_at): %w", err)
	}
	if hasLease {
		return false, nil
	}

	current, err := mainSource.currentVersion(ctx, db)
	if err != nil {
		return false, err
	}
	if current != forkPreMergeMainMax {
		return false, fmt.Errorf(
			"schema_migrations has fork-lineage row %d but MAX(version)=%d; cursor state is neither pre-merge fork (MAX=%d) nor reconciled (no row %d) — refusing to rewrite, repair manually",
			forkPreMergeMainMax, current, forkPreMergeMainMax, forkPreMergeMainMax)
	}

	// Column-by-column verification that the fork DDL recorded under 51-54
	// actually ran here. 0052 (dolt_ignore registration) has no
	// INFORMATION_SCHEMA footprint; its renumbered re-run (0071) is a pair of
	// idempotent REPLACE INTOs, so it needs no verification.
	probes := []struct {
		desc string
		ok   func() (bool, error)
	}{
		{"table linear_label_snapshots (fork 0051)", func() (bool, error) { return tableExists(ctx, db, "linear_label_snapshots") }},
		{"column comments.external_ref (fork 0053)", func() (bool, error) { return columnExists(ctx, db, "comments", "external_ref") }},
		{"column comments.updated_at (fork 0053)", func() (bool, error) { return columnExists(ctx, db, "comments", "updated_at") }},
		{"table attachments (fork 0054)", func() (bool, error) { return tableExists(ctx, db, "attachments") }},
	}
	for _, p := range probes {
		ok, err := p.ok()
		if err != nil {
			return false, fmt.Errorf("verifying fork lineage (%s): %w", p.desc, err)
		}
		if !ok {
			return false, fmt.Errorf(
				"schema_migrations records fork migrations 51-%d but %s is missing; schema does not match the recorded cursor — refusing to rewrite, repair manually",
				forkPreMergeMainMax, p.desc)
		}
	}

	if err := verifyForkCursorHashes(ctx, db, mainSource, forkRenumberedMainFiles); err != nil {
		return false, err
	}

	if err := repairForkIssueColumnDrift(ctx, db); err != nil {
		return false, err
	}

	if _, err := db.ExecContext(ctx,
		"DELETE FROM "+mainSource.cursorTable+" WHERE version BETWEEN 51 AND ?", forkPreMergeMainMax); err != nil {
		return false, fmt.Errorf("rewriting fork main cursor: %w", err)
	}
	return true, nil
}

// forkIssueDriftColumns are the gt-role columns that exist only in the
// squashed 0001_create_issues: no ALTER migration ever added them, so
// fork-lineage databases created before the squash carry an issues table
// without them (observed on 2 of the 15 prod databases). Upstream migration
// 0053_repair_rig_wisps lists these columns in its promote-INSERT and fails
// where they are missing. Definitions mirror 0001 (and the wisps twin in
// 0020) verbatim.
var forkIssueDriftColumns = []struct {
	name string
	ddl  string
}{
	{"hook_bead", "VARCHAR(255) DEFAULT ''"},
	{"role_bead", "VARCHAR(255) DEFAULT ''"},
	{"agent_state", "VARCHAR(32) DEFAULT ''"},
	{"last_activity", "DATETIME"},
	{"role_type", "VARCHAR(32) DEFAULT ''"},
	{"rig", "VARCHAR(255) DEFAULT ''"},
}

// repairForkIssueColumnDrift converges a pre-squash fork issues table to the
// canonical 0001 shape by adding whichever gt-role columns are missing. Runs
// only on the verified fork-lineage path, is guarded per column, and heals
// exactly the drift class the content-hash/doctor machinery exists to catch:
// same recorded version, different actual schema.
//
// The ALTERs are committed here, before MigrateUp snapshots dirty tables —
// otherwise the repair itself would make issues "pre-existing dirty" and the
// pass's own guard would refuse to run 0052 (which indexes issues). For the
// same reason the repair refuses to touch an issues table that already has
// uncommitted changes: committing would sweep user writes into the repair
// commit.
func repairForkIssueColumnDrift(ctx context.Context, db DBConn) error {
	altered := false
	for _, col := range forkIssueDriftColumns {
		present, err := columnExists(ctx, db, "issues", col.name)
		if err != nil {
			return fmt.Errorf("probing issues.%s: %w", col.name, err)
		}
		if present {
			continue
		}
		if !altered {
			// Refuse when issues itself is dirty (the repair would commit
			// user data changes alongside the ALTERs) or when ANY table is
			// already staged (DOLT_COMMIT commits the whole staged set, so
			// pre-staged user changes would be swept into the repair commit;
			// this runs before MigrateUp's own unstagePreExistingTables).
			var blocked int
			if err := db.QueryRowContext(ctx,
				"SELECT COUNT(*) FROM dolt_status WHERE table_name = 'issues' OR staged = true").Scan(&blocked); err != nil {
				return fmt.Errorf("reading dolt_status before drift repair: %w", err)
			}
			if blocked > 0 {
				return fmt.Errorf(
					"issues is missing column %s (pre-squash drift) but the working set has uncommitted or staged changes; commit the working set, then rerun",
					col.name)
			}
		}
		if _, err := db.ExecContext(ctx,
			"ALTER TABLE issues ADD COLUMN "+col.name+" "+col.ddl); err != nil {
			return fmt.Errorf("repairing issues.%s drift: %w", col.name, err)
		}
		altered = true
	}
	if !altered {
		return nil
	}
	if _, err := db.ExecContext(ctx, "CALL DOLT_ADD('issues')"); err != nil {
		return fmt.Errorf("staging issues drift repair: %w", err)
	}
	if _, err := db.ExecContext(ctx,
		"CALL DOLT_COMMIT('-m', 'schema: repair pre-squash issues column drift (bd-dn6)')"); err != nil {
		return fmt.Errorf("committing issues drift repair: %w", err)
	}
	return nil
}

func reconcileForkIgnoredCursor(ctx context.Context, db DBConn) (bool, error) {
	// Row 11 is the ignored-chain fork fingerprint: upstream's ignored chain
	// tops out at 0010 and this merge renumbers the fork's 0011 to 0021, so
	// only a pre-merge fork binary can have recorded row 11.
	has11, err := cursorRowExists(ctx, db, ignoredSource.cursorTable, forkPreMergeIgnoredMax)
	if err != nil || !has11 {
		return false, err
	}

	current, err := ignoredSource.currentVersion(ctx, db)
	if err != nil {
		return false, err
	}
	if current != forkPreMergeIgnoredMax {
		return false, fmt.Errorf(
			"ignored_schema_migrations has fork-lineage row %d but MAX(version)=%d; cursor state is neither pre-merge fork (MAX=%d) nor reconciled (no row %d) — refusing to rewrite, repair manually",
			forkPreMergeIgnoredMax, current, forkPreMergeIgnoredMax, forkPreMergeIgnoredMax)
	}

	for _, probe := range []struct {
		table string
		desc  string
	}{
		{"linear_issue_snapshots", "table linear_issue_snapshots (fork ignored 0010)"},
		{"linear_project_snapshots", "table linear_project_snapshots (fork ignored 0011)"},
	} {
		ok, err := tableExists(ctx, db, probe.table)
		if err != nil {
			return false, fmt.Errorf("verifying fork lineage (%s): %w", probe.desc, err)
		}
		if !ok {
			return false, fmt.Errorf(
				"ignored_schema_migrations records fork migrations 10-%d but %s is missing; schema does not match the recorded cursor — refusing to rewrite, repair manually",
				forkPreMergeIgnoredMax, probe.desc)
		}
	}

	if err := verifyForkCursorHashes(ctx, db, ignoredSource, forkRenumberedIgnoredFiles); err != nil {
		return false, err
	}

	if _, err := db.ExecContext(ctx,
		"DELETE FROM "+ignoredSource.cursorTable+" WHERE version BETWEEN 10 AND ?", forkPreMergeIgnoredMax); err != nil {
		return false, fmt.Errorf("rewriting fork ignored cursor: %w", err)
	}
	return true, nil
}

// verifyForkCursorHashes cross-checks each recorded content hash in the
// fork's cursor rows against the renumbered (byte-identical) migration file.
// Rows recorded before the content_hash column shipped carry NULL and are
// skipped — the INFORMATION_SCHEMA probes above carry the evidence for those.
// A non-NULL hash that does not match means the row was recorded against
// content this binary has never seen; refuse to rewrite in that case.
func verifyForkCursorHashes(ctx context.Context, db DBConn, src migrationSource, files map[int]string) error {
	hasHash, err := src.hasContentHashColumn(ctx, db)
	if err != nil {
		return err
	}
	if !hasHash {
		return nil
	}
	versions := make([]int, 0, len(files))
	for v := range files {
		versions = append(versions, v)
	}
	sort.Ints(versions)
	for _, version := range versions {
		file := files[version]
		var recorded sql.NullString
		err := db.QueryRowContext(ctx,
			"SELECT content_hash FROM "+src.cursorTable+" WHERE version = ?", version).Scan(&recorded)
		if err == sql.ErrNoRows {
			continue
		}
		if err != nil {
			return fmt.Errorf("reading %s content hash for version %d: %w", src.cursorTable, version, err)
		}
		if !recorded.Valid || recorded.String == "" {
			continue
		}
		data, err := src.files.ReadFile(src.dir + "/" + file)
		if err != nil {
			return fmt.Errorf("reading embedded %s: %w", file, err)
		}
		sum := sha256.Sum256(data)
		if want := hex.EncodeToString(sum[:]); recorded.String != want {
			return fmt.Errorf(
				"%s version %d content hash %s does not match fork migration %s (%s); the recorded migration content is unknown to this binary — refusing to rewrite, repair manually",
				src.cursorTable, version, recorded.String, file, want)
		}
	}
	return nil
}

// ForkLineageStatus classifies a database's position relative to the bd-dn6
// fork migration renumbering. Used by `bd doctor` to verify prod databases
// before and after the binary swap.
type ForkLineageStatus string

const (
	// ForkLineagePreMerge: pre-merge fork cursor rows present (main 54 and/or
	// ignored 11); reconciliation will run on the next migration pass.
	ForkLineagePreMerge ForkLineageStatus = "pre-merge"
	// ForkLineageReconciled: renumbered rows recorded and every verified
	// effect from both lineages is present.
	ForkLineageReconciled ForkLineageStatus = "reconciled"
	// ForkLineageNotApplicable: no fork fingerprint and the cursor is below
	// the renumbered range — a fresh, behind, or upstream-lineage database
	// with nothing to reconcile or verify yet.
	ForkLineageNotApplicable ForkLineageStatus = "not-applicable"
	// ForkLineageInconsistent: the cursor claims a state the actual schema
	// contradicts — the silent-skew condition this machinery exists to catch.
	ForkLineageInconsistent ForkLineageStatus = "inconsistent"
)

// ForkLineageReport is the result of VerifyForkLineageState.
type ForkLineageReport struct {
	Status         ForkLineageStatus
	MainVersion    int
	IgnoredVersion int
	Problems       []string // populated when Status == ForkLineageInconsistent
}

// VerifyForkLineageState probes the cursors and the actual schema
// (column-by-column) and classifies the database. Read-only.
func VerifyForkLineageState(ctx context.Context, db DBConn) (ForkLineageReport, error) {
	report := ForkLineageReport{}
	var err error
	if report.MainVersion, err = mainSource.currentVersion(ctx, db); err != nil {
		return report, err
	}
	if report.IgnoredVersion, err = ignoredSource.currentVersion(ctx, db); err != nil {
		return report, err
	}
	has54, err := cursorRowExists(ctx, db, mainSource.cursorTable, forkPreMergeMainMax)
	if err != nil {
		return report, err
	}
	has11, err := cursorRowExists(ctx, db, ignoredSource.cursorTable, forkPreMergeIgnoredMax)
	if err != nil {
		return report, err
	}

	// Post-merge disambiguation (see reconcileForkMainCursor): since the
	// upstream-20260710 merge, upstream owns a main 0054 (add_lease_columns)
	// and an ignored 0011 (cleanup_orphaned_child_counters), so those cursor
	// rows are only fork fingerprints on a database that has NOT applied
	// upstream's 0054 (issues.lease_expires_at absent).
	if has54 || has11 {
		hasLease, leaseErr := columnExists(ctx, db, "issues", "lease_expires_at")
		if leaseErr != nil {
			return report, leaseErr
		}
		if hasLease {
			has54, has11 = false, false
		}
	}

	if has54 || has11 {
		// A fingerprint row that coexists with an unexpected MAX is a state
		// the reconciler refuses to rewrite — report it as inconsistent
		// rather than "will reconcile on the next write" (which would be a
		// false promise).
		if has54 && report.MainVersion != forkPreMergeMainMax {
			report.Problems = append(report.Problems, fmt.Sprintf(
				"schema_migrations row %d coexists with MAX(version)=%d; reconciliation will refuse this cursor",
				forkPreMergeMainMax, report.MainVersion))
		}
		if has11 && report.IgnoredVersion != forkPreMergeIgnoredMax {
			report.Problems = append(report.Problems, fmt.Sprintf(
				"ignored_schema_migrations row %d coexists with MAX(version)=%d; reconciliation will refuse this cursor",
				forkPreMergeIgnoredMax, report.IgnoredVersion))
		}
		if len(report.Problems) > 0 {
			report.Status = ForkLineageInconsistent
			return report, nil
		}
		report.Status = ForkLineagePreMerge
		return report, nil
	}

	if report.MainVersion < 70 {
		report.Status = ForkLineageNotApplicable
		return report, nil
	}

	// Main cursor is in the renumbered range: verify both lineages' effects.
	type probe struct {
		desc string
		ok   func() (bool, error)
	}
	probes := []probe{
		{"cursor row 0070 (fork create_linear_label_snapshots)", func() (bool, error) { return cursorRowExists(ctx, db, mainSource.cursorTable, 70) }},
		{"cursor row 0071 (fork linear_snapshots_dolt_ignore)", func() (bool, error) { return cursorRowExists(ctx, db, mainSource.cursorTable, 71) }},
		{"cursor row 0072 (fork add_comment_external_ref)", func() (bool, error) { return cursorRowExists(ctx, db, mainSource.cursorTable, 72) }},
		{"cursor row 0073 (fork create_attachments)", func() (bool, error) { return cursorRowExists(ctx, db, mainSource.cursorTable, 73) }},
		{"table linear_label_snapshots (fork 0070)", func() (bool, error) { return tableExists(ctx, db, "linear_label_snapshots") }},
		{"column comments.external_ref (fork 0072)", func() (bool, error) { return columnExists(ctx, db, "comments", "external_ref") }},
		{"column comments.updated_at (fork 0072)", func() (bool, error) { return columnExists(ctx, db, "comments", "updated_at") }},
		{"index comments.idx_comments_external_ref (fork 0072)", func() (bool, error) { return indexExists(ctx, db, "comments", "idx_comments_external_ref") }},
		{"table attachments (fork 0073)", func() (bool, error) { return tableExists(ctx, db, "attachments") }},
		{"index issues.idx_issues_status_updated_at (upstream 0052)", func() (bool, error) { return indexExists(ctx, db, "issues", "idx_issues_status_updated_at") }},
		{"index issues.idx_issues_defer_until (upstream 0052)", func() (bool, error) { return indexExists(ctx, db, "issues", "idx_issues_defer_until") }},
		{"events.id DEFAULT dropped (upstream 0051)", func() (bool, error) { return columnDefaultAbsent(ctx, db, "events", "id") }},
		{"comments.id DEFAULT dropped (upstream 0051)", func() (bool, error) { return columnDefaultAbsent(ctx, db, "comments", "id") }},
	}
	// The ignored chain is clone-local; a fresh clone of a reconciled
	// database legitimately has an empty ignored cursor until its first
	// migration pass materializes the local tables. Only verify the ignored
	// effects once the cursor reached the renumbered range.
	if report.IgnoredVersion >= 20 {
		probes = append(probes,
			probe{"cursor row 0020 (fork ignored create_linear_issue_snapshots)", func() (bool, error) { return cursorRowExists(ctx, db, ignoredSource.cursorTable, 20) }},
			probe{"cursor row 0021 (fork ignored create_linear_project_snapshots)", func() (bool, error) { return cursorRowExists(ctx, db, ignoredSource.cursorTable, 21) }},
			probe{"table linear_issue_snapshots (fork ignored 0020)", func() (bool, error) { return tableExists(ctx, db, "linear_issue_snapshots") }},
			probe{"table linear_project_snapshots (fork ignored 0021)", func() (bool, error) { return tableExists(ctx, db, "linear_project_snapshots") }},
			probe{"wisp_events.id DEFAULT dropped (upstream ignored 0010)", func() (bool, error) { return columnDefaultAbsent(ctx, db, "wisp_events", "id") }},
		)
	}
	for _, p := range probes {
		ok, err := p.ok()
		if err != nil {
			return report, fmt.Errorf("verifying %s: %w", p.desc, err)
		}
		if !ok {
			report.Problems = append(report.Problems, p.desc)
		}
	}
	if len(report.Problems) > 0 {
		report.Status = ForkLineageInconsistent
		return report, nil
	}
	report.Status = ForkLineageReconciled
	return report, nil
}

// indexExists reports whether the named index is present on table.
func indexExists(ctx context.Context, db DBConn, table, index string) (bool, error) {
	var count int
	if err := db.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM INFORMATION_SCHEMA.STATISTICS
		 WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = ? AND INDEX_NAME = ?`,
		table, index).Scan(&count); err != nil {
		return false, err
	}
	return count > 0, nil
}

// columnDefaultAbsent reports whether table.column exists and carries no
// DEFAULT. A missing table or column reads as false (the probe's caller
// treats that as a problem, which it is).
func columnDefaultAbsent(ctx context.Context, db DBConn, table, column string) (bool, error) {
	var columnDefault sql.NullString
	err := db.QueryRowContext(ctx,
		`SELECT COLUMN_DEFAULT FROM INFORMATION_SCHEMA.COLUMNS
		 WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = ? AND COLUMN_NAME = ?`,
		table, column).Scan(&columnDefault)
	if err == sql.ErrNoRows {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return !columnDefault.Valid, nil
}

// cursorRowExists reports whether the cursor table has a row for version.
// A missing cursor table reads as "no row" (fresh database).
func cursorRowExists(ctx context.Context, db DBConn, table string, version int) (bool, error) {
	var count int
	err := db.QueryRowContext(ctx,
		"SELECT COUNT(*) FROM "+table+" WHERE version = ?", version).Scan(&count)
	if err != nil {
		if dberrors.IsTableNotExist(err) {
			return false, nil
		}
		return false, fmt.Errorf("probing %s for version %d: %w", table, version, err)
	}
	return count > 0, nil
}

// tableExists reports whether a table is present in the current schema.
func tableExists(ctx context.Context, db DBConn, table string) (bool, error) {
	var count int
	if err := db.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES
		 WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = ?`,
		table).Scan(&count); err != nil {
		return false, err
	}
	return count > 0, nil
}
