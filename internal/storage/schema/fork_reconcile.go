package schema

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"errors"
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
// main 0070-0073 and ignored 0020-0021 (renumbered again to 0025-0026 by the
// 2026-08 upmerge — see below).
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
// forkRenumberedMainFiles. The 2026-08 upmerge renumbered the fork ignored
// files a second time (0020-0021 → 0025-0026, see the upmerge reconciliation
// below); the content stays byte-identical, so the pre-merge rows 10-11 are
// still verified against the same bytes under the new names.
var forkRenumberedIgnoredFiles = map[int]string{
	10: "0025_create_linear_issue_snapshots.up.sql",
	11: "0026_create_linear_project_snapshots.up.sql",
}

// ---- 2026-08 upstream-main upmerge reconciliation ----
//
// The 2026-08 upmerge brings upstream main's migrations 0056-0065 and ignored
// 0014-0024 into a lineage whose databases already recorded the fork's
// migrations at main 0070-0073 and ignored 0020-0022 (the pre-upmerge fork
// numbers for create_linear_issue_snapshots, create_linear_project_snapshots
// and add_wisp_comment_external_ref — renumbered by this upmerge to ignored
// 0025-0027 because upstream now owns 0020-0022). Because the cursors are
// MAX-based, such a database (main MAX=73, ignored MAX=22) would silently
// skip every upstream migration numbered below its cursor — leaving it
// without storage_class, provenance_events, the events journal, the
// lease granted_node column and the rest, while claiming a version at or
// above databases that have them.
//
// Same cure as bd-dn6 above: verify that the DDL the fork rows recorded is
// actually present (content-hash cross-checks where recorded), then delete
// the fork cursor rows. Main drops to 55, so the same pass applies upstream
// 0056-0065 for real and re-runs the fork's guarded 0070-0073, re-recording
// them; ignored drops to 13, so the pass applies upstream 0014-0024 and the
// renumbered fork 0025-0027. After one pass the cursor state is
// indistinguishable from a database that migrated through the merged lineage
// from scratch, and the triggers (a fork row coexisting with the absence of
// upstream's 0056 / 0014 row) never fire again.

// upmergeRenumberedIgnoredFiles maps each pre-upmerge fork ignored cursor row
// to the renumbered migration file carrying the byte-identical content that
// row was recorded against.
var upmergeRenumberedIgnoredFiles = map[int]string{
	20: "0025_create_linear_issue_snapshots.up.sql",
	21: "0026_create_linear_project_snapshots.up.sql",
	22: "0027_add_wisp_comment_external_ref.up.sql",
}

// upmergeMainFiles maps the fork main-chain rows 70-73 to their (unchanged)
// files, for hash verification before the rows are deleted and re-recorded.
var upmergeMainFiles = map[int]string{
	70: "0070_create_linear_label_snapshots.up.sql",
	71: "0071_linear_snapshots_dolt_ignore.up.sql",
	72: "0072_add_comment_external_ref.up.sql",
	73: "0073_create_attachments.up.sql",
}

// cursorRewrite is one verified fork-cursor rewrite. The reconcile pass is
// split into a planning half that only READS (every fingerprint probe, every
// schema probe and every content-hash cross-check, for every chain) and an
// apply half that only WRITES, so a refusal on any chain happens before the
// first DELETE and leaves the working set exactly as it was found. Before the
// split the upmerge main DELETE (rows 70-73) ran before the ignored chain was
// even probed; when the ignored chain then refused, that DELETE stayed in the
// working set uncommitted (HEAD intact, dolt_status dirty) and the previous
// binary read the store as "row 54 but MAX=55" until an operator ran
// DOLT_CHECKOUT('schema_migrations') — boomtown's 2026-09-11 write outage
// (gp-w0nu).
type cursorRewrite struct {
	// desc names the chain in errors.
	desc string
	// prepare is a write that must precede the DELETE and is only safe once
	// every chain has verified: the bd-dn6 pre-squash issues column-drift
	// repair. nil for the other chains.
	prepare func(ctx context.Context, db DBConn) error
	table   string
	lo, hi  int
}

func (r cursorRewrite) apply(ctx context.Context, db DBConn) error {
	if r.prepare != nil {
		if err := r.prepare(ctx, db); err != nil {
			return err
		}
	}
	//nolint:gosec // G202: r.table is one of the two hardcoded cursor table names.
	if _, err := db.ExecContext(ctx,
		"DELETE FROM "+r.table+" WHERE version BETWEEN ? AND ?", r.lo, r.hi); err != nil {
		return fmt.Errorf("rewriting %s: %w", r.desc, err)
	}
	return nil
}

// cursorMaxVersion reads the cursor table's recorded MAX(version): the number
// every refusal text in this file compares against.
//
// It deliberately bypasses migrationSource.currentVersion. currentVersion
// heals a cursor whose clone-local sentinel effects are absent by reading it
// as 0 (gh 5033), and on exactly the shapes reconciled here a sentinel is
// legitimately absent: a pre-upmerge ignored chain (1-13, 20-22) never ran
// upstream ignored 0016, so leases.granted_node is missing and the cursor
// reads as 0 while MAX(version) on the wire is 22. Comparing the fingerprint
// precondition against the healed reading refused every pre-upmerge store
// with "row 22 without row 14 but MAX(version)=0" (boomtown, 2026-09-11,
// gp-w0nu). The heal is right for the migration pass, which re-runs the
// re-runnable series from that reading; the fingerprint check needs the
// number the cursor actually records. A missing table reads as 0 — callers
// probe a fingerprint row first, so this never issues the first (poisoning,
// be-bv7x) statement against an absent table.
func cursorMaxVersion(ctx context.Context, db DBConn, table string) (int, error) {
	var current int
	//nolint:gosec // G202: table is one of the two hardcoded cursor table names.
	err := db.QueryRowContext(ctx, "SELECT COALESCE(MAX(version), 0) FROM "+table).Scan(&current)
	if err != nil {
		if dberrors.IsTableNotExist(err) {
			return 0, nil
		}
		return 0, fmt.Errorf("reading %s MAX(version): %w", table, err)
	}
	return current, nil
}

func planUpmergeMainCursor(ctx context.Context, db DBConn) (*cursorRewrite, error) {
	// Row 73 (fork create_attachments) without row 56 (upstream
	// add_comments_keyset_index) is the pre-upmerge fingerprint: any database
	// that migrated through the merged lineage records 0056 before it can
	// record 0073, and a crash mid-pass records prefixes only — it can leave
	// 56 without 73, never 73 without 56.
	has73, err := cursorRowExists(ctx, db, mainSource.cursorTable, 73)
	if err != nil || !has73 {
		return nil, err
	}
	has56, err := cursorRowExists(ctx, db, mainSource.cursorTable, 56)
	if err != nil || has56 {
		return nil, err
	}

	current, err := cursorMaxVersion(ctx, db, mainSource.cursorTable)
	if err != nil {
		return nil, err
	}
	if current != 73 {
		return nil, fmt.Errorf(
			"schema_migrations has pre-upmerge row 73 without row 56 but MAX(version)=%d; cursor state is neither pre-upmerge (MAX=73) nor merged-lineage (row 56 present) — %w",
			current, errRefusedRewrite)
	}

	// Verify the fork DDL recorded under 70-73 actually ran here before
	// deleting the rows that say it did. 0071 (dolt_ignore registration) has
	// no INFORMATION_SCHEMA footprint; its re-run is a pair of idempotent
	// REPLACE INTOs, so it needs no verification.
	probes := []struct {
		desc string
		ok   func() (bool, error)
	}{
		{"table linear_label_snapshots (fork 0070)", func() (bool, error) { return tableExists(ctx, db, "linear_label_snapshots") }},
		{"column comments.external_ref (fork 0072)", func() (bool, error) { return columnExists(ctx, db, "comments", "external_ref") }},
		{"column comments.updated_at (fork 0072)", func() (bool, error) { return columnExists(ctx, db, "comments", "updated_at") }},
		{"table attachments (fork 0073)", func() (bool, error) { return tableExists(ctx, db, "attachments") }},
	}
	for _, p := range probes {
		ok, err := p.ok()
		if err != nil {
			return nil, fmt.Errorf("verifying pre-upmerge lineage (%s): %w", p.desc, err)
		}
		if !ok {
			return nil, fmt.Errorf(
				"schema_migrations records fork migrations 70-73 but %s is missing; schema does not match the recorded cursor — %w",
				p.desc, errRefusedRewrite)
		}
	}

	if err := verifyForkCursorHashes(ctx, db, mainSource, upmergeMainFiles); err != nil {
		return nil, err
	}
	return &cursorRewrite{desc: "pre-upmerge main cursor", table: mainSource.cursorTable, lo: 70, hi: 73}, nil
}

func planUpmergeIgnoredCursor(ctx context.Context, db DBConn) (*cursorRewrite, error) {
	// Row 22 (fork add_wisp_comment_external_ref) without row 14 (upstream
	// add_wisp_comments_keyset_index) is the pre-upmerge ignored fingerprint,
	// by the same prefix argument as the main chain: the merged series
	// records 14 before anything can record 22.
	has22, err := cursorRowExists(ctx, db, ignoredSource.cursorTable, 22)
	if err != nil || !has22 {
		return nil, err
	}
	has14, err := cursorRowExists(ctx, db, ignoredSource.cursorTable, 14)
	if err != nil || has14 {
		return nil, err
	}

	// The recorded MAX, not currentVersion's healed reading: on this shape
	// leases.granted_node (upstream ignored 0016) is legitimately absent and
	// the reality check reads the cursor as 0. See cursorMaxVersion.
	current, err := cursorMaxVersion(ctx, db, ignoredSource.cursorTable)
	if err != nil {
		return nil, err
	}
	if current != 22 {
		return nil, fmt.Errorf(
			"ignored_schema_migrations has pre-upmerge row 22 without row 14 but MAX(version)=%d; cursor state is neither pre-upmerge (MAX=22) nor merged-lineage (row 14 present) — %w",
			current, errRefusedRewrite)
	}

	for _, probe := range []struct {
		desc string
		ok   func() (bool, error)
	}{
		{"table linear_issue_snapshots (fork ignored 0020)", func() (bool, error) { return tableExists(ctx, db, "linear_issue_snapshots") }},
		{"table linear_project_snapshots (fork ignored 0021)", func() (bool, error) { return tableExists(ctx, db, "linear_project_snapshots") }},
		{"column wisp_comments.external_ref (fork ignored 0022)", func() (bool, error) { return columnExists(ctx, db, "wisp_comments", "external_ref") }},
	} {
		ok, err := probe.ok()
		if err != nil {
			return nil, fmt.Errorf("verifying pre-upmerge lineage (%s): %w", probe.desc, err)
		}
		if !ok {
			return nil, fmt.Errorf(
				"ignored_schema_migrations records fork migrations 20-22 but %s is missing; schema does not match the recorded cursor — %w",
				probe.desc, errRefusedRewrite)
		}
	}

	if err := verifyForkCursorHashes(ctx, db, ignoredSource, upmergeRenumberedIgnoredFiles); err != nil {
		return nil, err
	}
	return &cursorRewrite{desc: "pre-upmerge ignored cursor", table: ignoredSource.cursorTable, lo: 20, hi: 22}, nil
}

// errRefusedRewrite is the sentinel every refuse-to-rewrite guard in this file
// wraps, so the caller can tell a refusal (the cursor state is one this binary
// declines to rewrite) from a transient SQL error on the same probes.
var errRefusedRewrite = errors.New("refusing to rewrite, repair manually")

// planForkLineageCursors is the read-only half of the fork-lineage cursor
// reconciliation: it detects a database whose migration cursors were written
// by a pre-merge or pre-upmerge fork binary and returns the rewrites that
// carry them to the renumbered scheme, in apply order. MigrateUp runs it
// BEFORE the pass writes anything (the dolt_ignore seed, the unstaging of
// pre-existing staged tables, the seed commit), so an error here leaves the
// working set exactly as it was found: a refusal says so, and a transient SQL
// error on the same probes only says that no cursor row was rewritten (the
// two must not share the refusal's wording). A no-op on fresh databases,
// upstream-lineage databases, and databases already reconciled.
func planForkLineageCursors(ctx context.Context, db DBConn) ([]cursorRewrite, error) {
	rewrites, err := planForkLineageRewrites(ctx, db)
	if err != nil {
		if errors.Is(err, errRefusedRewrite) {
			return nil, fmt.Errorf("%w (refused before any cursor rewrite; the working set was left as found)", err)
		}
		return nil, fmt.Errorf("%w (no cursor row was rewritten)", err)
	}
	return rewrites, nil
}

// applyForkLineageRewrites is the write half: it applies the rewrites
// planForkLineageCursors verified, after MigrateUp has unstaged pre-existing
// staged tables and committed the dolt_ignore seed, and before pending
// versions are computed. Returns whether any cursor was changed. The DELETEs
// only touch the cursor tables and are committed with the rest of the pass.
func applyForkLineageRewrites(ctx context.Context, db DBConn, rewrites []cursorRewrite) (bool, error) {
	for _, r := range rewrites {
		if err := r.apply(ctx, db); err != nil {
			return true, err
		}
	}
	return len(rewrites) > 0, nil
}

// planForkLineageRewrites runs every chain's verification and returns the
// cursor rewrites to apply, in apply order. It issues no writes.
func planForkLineageRewrites(ctx context.Context, db DBConn) ([]cursorRewrite, error) {
	var rewrites []cursorRewrite

	preMain, err := planForkMainCursor(ctx, db)
	if err != nil {
		return nil, err
	}
	if preMain != nil {
		rewrites = append(rewrites, *preMain)
		// Since the upstream-20260710 merge, upstream also owns an ignored
		// migration 0011 (cleanup_orphaned_child_counters), so the ignored
		// fingerprint row 11 is only meaningful together with the main one:
		// running the ignored pass on a genuine upstream database would trip
		// its refuse-to-rewrite guard.
		preIgnored, err := planForkIgnoredCursor(ctx, db)
		if err != nil {
			return nil, err
		}
		if preIgnored != nil {
			rewrites = append(rewrites, *preIgnored)
		}
	}

	// 2026-08 upmerge (see the block comment above the upmerge maps). The two
	// upmerge chains are probed independently: unlike bd-dn6's ignored row 11,
	// the fingerprints (row 73 without 56, row 22 without 14) are unambiguous
	// on their own, and a crash between the two DELETEs must leave each chain
	// individually recoverable on the next pass.
	upMain, err := planUpmergeMainCursor(ctx, db)
	if err != nil {
		return nil, err
	}
	if upMain != nil {
		rewrites = append(rewrites, *upMain)
	}
	upIgnored, err := planUpmergeIgnoredCursor(ctx, db)
	if err != nil {
		return nil, err
	}
	if upIgnored != nil {
		rewrites = append(rewrites, *upIgnored)
	}
	return rewrites, nil
}

func planForkMainCursor(ctx context.Context, db DBConn) (*cursorRewrite, error) {
	// Row 54 is the fork-lineage fingerprint: upstream's chain has never had a
	// migration 0054 (it jumps 0053 → this merge's 0070), so only a pre-merge
	// fork binary can have recorded it.
	has54, err := cursorRowExists(ctx, db, mainSource.cursorTable, forkPreMergeMainMax)
	if err != nil || !has54 {
		return nil, err
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
		return nil, fmt.Errorf("disambiguating fork lineage (issues.lease_expires_at): %w", err)
	}
	if hasLease {
		return nil, nil
	}

	// Upstream 0055 (move_leases_to_table) DROPS the lease columns 0054
	// added, destroying the disambiguating evidence above on exactly the
	// stores that have migrated furthest. Row 55 is equivalent evidence: the
	// pre-merge fork chain ended at 54, so only upstream's chain can have
	// recorded a migration 0055 — and a store that ran upstream 0055
	// necessarily ran upstream 0054 first, making row 54 upstream's lease
	// migration rather than the fork fingerprint. Observed on the hw clone
	// after the 2026-08 upmerge applied 0055 (hw-augjs): row 54 + no lease
	// columns + MAX=73 tripped the refusal below on every subsequent open,
	// wedging that clone's still-pending ignored chain; row 54's recorded
	// content hash there is upstream 0054's, proving the misread.
	has55, err := cursorRowExists(ctx, db, mainSource.cursorTable, 55)
	if err != nil {
		return nil, fmt.Errorf("disambiguating fork lineage (row 55): %w", err)
	}
	if has55 {
		return nil, nil
	}

	current, err := cursorMaxVersion(ctx, db, mainSource.cursorTable)
	if err != nil {
		return nil, err
	}
	if current != forkPreMergeMainMax {
		return nil, fmt.Errorf(
			"schema_migrations has fork-lineage row %d but MAX(version)=%d; cursor state is neither pre-merge fork (MAX=%d) nor reconciled (no row %d) — %w",
			forkPreMergeMainMax, current, forkPreMergeMainMax, forkPreMergeMainMax, errRefusedRewrite)
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
			return nil, fmt.Errorf("verifying fork lineage (%s): %w", p.desc, err)
		}
		if !ok {
			return nil, fmt.Errorf(
				"schema_migrations records fork migrations 51-%d but %s is missing; schema does not match the recorded cursor — %w",
				forkPreMergeMainMax, p.desc, errRefusedRewrite)
		}
	}

	if err := verifyForkCursorHashes(ctx, db, mainSource, forkRenumberedMainFiles); err != nil {
		return nil, err
	}

	// The pre-squash drift repair is a write (ALTERs plus its own commit), so
	// it runs in the apply half, after every chain has verified.
	return &cursorRewrite{
		desc:    "fork main cursor",
		prepare: repairForkIssueColumnDrift,
		table:   mainSource.cursorTable,
		lo:      51,
		hi:      forkPreMergeMainMax,
	}, nil
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
			// still staged (DOLT_COMMIT commits the whole staged set, so a
			// staged user change would be swept into the repair commit).
			// This is the apply half: it runs after MigrateUp's
			// unstagePreExistingTables and its dolt_ignore seed commit, so
			// a staged table here is unexpected and the guard is the last
			// line of defense, not the first.
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

func planForkIgnoredCursor(ctx context.Context, db DBConn) (*cursorRewrite, error) {
	// Row 11 is the ignored-chain fork fingerprint: upstream's ignored chain
	// tops out at 0010 and this merge renumbers the fork's 0011 to 0021, so
	// only a pre-merge fork binary can have recorded row 11.
	has11, err := cursorRowExists(ctx, db, ignoredSource.cursorTable, forkPreMergeIgnoredMax)
	if err != nil || !has11 {
		return nil, err
	}

	// The recorded MAX, not currentVersion's healed reading (see
	// cursorMaxVersion): a pre-merge ignored chain predates every sentinel
	// column the reality check looks for.
	current, err := cursorMaxVersion(ctx, db, ignoredSource.cursorTable)
	if err != nil {
		return nil, err
	}
	if current != forkPreMergeIgnoredMax {
		return nil, fmt.Errorf(
			"ignored_schema_migrations has fork-lineage row %d but MAX(version)=%d; cursor state is neither pre-merge fork (MAX=%d) nor reconciled (no row %d) — %w",
			forkPreMergeIgnoredMax, current, forkPreMergeIgnoredMax, forkPreMergeIgnoredMax, errRefusedRewrite)
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
			return nil, fmt.Errorf("verifying fork lineage (%s): %w", probe.desc, err)
		}
		if !ok {
			return nil, fmt.Errorf(
				"ignored_schema_migrations records fork migrations 10-%d but %s is missing; schema does not match the recorded cursor — %w",
				forkPreMergeIgnoredMax, probe.desc, errRefusedRewrite)
		}
	}

	if err := verifyForkCursorHashes(ctx, db, ignoredSource, forkRenumberedIgnoredFiles); err != nil {
		return nil, err
	}
	return &cursorRewrite{desc: "fork ignored cursor", table: ignoredSource.cursorTable, lo: 10, hi: forkPreMergeIgnoredMax}, nil
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
				"%s version %d content hash %s does not match fork migration %s (%s); the recorded migration content is unknown to this binary — %w",
				src.cursorTable, version, recorded.String, file, want, errRefusedRewrite)
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
	// IgnoredCursorMax is the recorded MAX(version) of ignored_schema_migrations.
	// It differs from IgnoredVersion when the cursor-reality check (gh 5033)
	// has healed the reading to 0 because a clone-local sentinel is absent —
	// the pre-upmerge shape whose refusal read "MAX(version)=0" (gp-w0nu).
	// IgnoredCursorNote then says so in one line; the reconciler compares the
	// recorded MAX (cursorMaxVersion), so the doctor's fingerprint arithmetic
	// below uses it too.
	IgnoredCursorMax  int
	IgnoredCursorNote string
	Problems          []string // populated when Status == ForkLineageInconsistent
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
	if report.IgnoredCursorMax, err = cursorMaxVersion(ctx, db, ignoredSource.cursorTable); err != nil {
		return report, err
	}
	if report.IgnoredCursorMax != report.IgnoredVersion {
		report.IgnoredCursorNote = fmt.Sprintf(
			"ignored_schema_migrations records MAX(version)=%d but the cursor-reality check reads it as %d because a clone-local sentinel (wisps, wisp_dependencies or leases.granted_node) is absent; the ignored series is clone-local and the next migration pass re-runs it from %d — the fork reconciler compares the recorded MAX, not this reading",
			report.IgnoredCursorMax, report.IgnoredVersion, report.IgnoredVersion)
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
		// Same 0055 complement as reconcileForkMainCursor: once upstream
		// 0055 has dropped the lease columns, row 55 carries the
		// upstream-lineage evidence.
		has55, err := cursorRowExists(ctx, db, mainSource.cursorTable, 55)
		if err != nil {
			return report, err
		}
		if has55 {
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
		if has11 && report.IgnoredCursorMax != forkPreMergeIgnoredMax {
			report.Problems = append(report.Problems, fmt.Sprintf(
				"ignored_schema_migrations row %d coexists with MAX(version)=%d; reconciliation will refuse this cursor",
				forkPreMergeIgnoredMax, report.IgnoredCursorMax))
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
			// Row 20 carried the fork's create_linear_issue_snapshots before
			// the 2026-08 upmerge and carries upstream's add_wisp_storage_class
			// after it; either way a cursor at or past 20 must have the row.
			probe{"cursor row 0020 (ignored chain)", func() (bool, error) { return cursorRowExists(ctx, db, ignoredSource.cursorTable, 20) }},
			probe{"cursor row 0021 (ignored chain)", func() (bool, error) { return cursorRowExists(ctx, db, ignoredSource.cursorTable, 21) }},
			probe{"table linear_issue_snapshots (fork ignored 0020/0025)", func() (bool, error) { return tableExists(ctx, db, "linear_issue_snapshots") }},
			probe{"table linear_project_snapshots (fork ignored 0021/0026)", func() (bool, error) { return tableExists(ctx, db, "linear_project_snapshots") }},
			probe{"wisp_events.id DEFAULT dropped (upstream ignored 0010)", func() (bool, error) { return columnDefaultAbsent(ctx, db, "wisp_events", "id") }},
		)
	}
	// Past the 2026-08 upmerge renumbering, the fork's ignored migrations are
	// recorded at 25-27.
	if report.IgnoredVersion >= 25 {
		probes = append(probes,
			probe{"cursor row 0025 (fork ignored create_linear_issue_snapshots)", func() (bool, error) { return cursorRowExists(ctx, db, ignoredSource.cursorTable, 25) }},
			probe{"cursor row 0026 (fork ignored create_linear_project_snapshots)", func() (bool, error) { return cursorRowExists(ctx, db, ignoredSource.cursorTable, 26) }},
			probe{"cursor row 0027 (fork ignored add_wisp_comment_external_ref)", func() (bool, error) { return cursorRowExists(ctx, db, ignoredSource.cursorTable, 27) }},
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
