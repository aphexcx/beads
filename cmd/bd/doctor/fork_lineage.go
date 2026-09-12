package doctor

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/steveyegge/beads/internal/configfile"
	"github.com/steveyegge/beads/internal/doltserver"
	"github.com/steveyegge/beads/internal/storage/embeddeddolt"
	"github.com/steveyegge/beads/internal/storage/schema"
)

const forkLineageCheckName = "Fork Migration Lineage"

// CheckForkMigrationLineage verifies both the bd-dn6 fork renumbering
// (main 0051-0054, ignored 0010-0011) and the 2026-08 upmerge renumbering
// (main 0070-0073, ignored 0020-0022). The migration runner reconciles these
// cursors automatically; this check verifies the required schema effects
// before reporting a pre-merge cursor as reconcilable, and verifies both
// fork and upstream effects on a reconciled cursor. It catches silent skew
// where a recorded version claims DDL that never ran.
//
// Read-only diagnostic; it never gates anything.
func CheckForkMigrationLineage(ss *SharedStore) DoctorCheck {
	if store := ss.Store(); store != nil {
		return checkForkMigrationLineage(context.Background(), store.DB())
	}
	beadsDir := sharedStoreBeadsDir(ss)
	if check, ok := checkForkMigrationLineageEmbedded(context.Background(), beadsDir); ok {
		return check
	}
	// A server-mode store whose write-mode open the migration pass REFUSED
	// (the fork reconciler's refuse-to-rewrite guard is the case this check
	// exists to explain) has no SharedStore and no embeddeddolt directory, so
	// the check reported "N/A (no database)" on exactly the store the
	// operator was diagnosing (boomtown, 2026-09-11, gp-w0nu). Read it over
	// doctor's own read-only server connection instead.
	if check, ok := checkForkMigrationLineageServer(context.Background(), beadsDir); ok {
		return check
	}
	return DoctorCheck{
		Name:     forkLineageCheckName,
		Status:   StatusOK,
		Message:  "N/A (no database)",
		Category: CategoryData,
	}
}

func checkForkMigrationLineageEmbedded(ctx context.Context, beadsDir string) (DoctorCheck, bool) {
	if beadsDir == "" {
		return DoctorCheck{}, false
	}
	dataDir := filepath.Join(beadsDir, "embeddeddolt")
	if _, err := os.Stat(dataDir); err != nil {
		return DoctorCheck{}, false
	}
	database := configfile.DefaultDoltDatabase
	if cfg, err := configfile.Load(beadsDir); err == nil && cfg != nil {
		database = cfg.GetDoltDatabase()
	}
	db, cleanup, err := embeddeddolt.OpenSQL(ctx, dataDir, database, "")
	if err != nil {
		return DoctorCheck{
			Name:     forkLineageCheckName,
			Status:   StatusWarning,
			Message:  fmt.Sprintf("Could not check fork migration lineage (open embedded database): %v", err),
			Detail:   "The lineage check failed to run; this does not mean the database is skewed. Re-run `bd doctor` and report the error if it persists.",
			Category: CategoryData,
		}, true
	}
	defer func() { _ = cleanup() }()
	return checkForkMigrationLineage(ctx, db), true
}

// forkLineageStoreHasServer reports whether the store at beadsDir has a
// server for checkForkMigrationLineageServer to ask. The first leg is the
// storage-mode predicate SharedStore itself used to decide whether this
// store had a server to open (configfile.IsDoltServerMode: an empty
// dolt_mode on a localhost host reads embedded), not the lifecycle resolver
// (doltserver.ResolveServerMode reads that same empty mode as Owned), so
// this fallback and the open it stands in for cannot disagree (Fable read
// r1, gp-w0nu). The second leg is shared-server intent, which the storage
// predicate reads only from BEADS_DOLT_SHARED_SERVER while bd's own open
// path (doltserver.IsSharedServerMode) also honors config.yaml
// dolt.shared-server; without it a YAML shared-server store with no local
// database directory lost the diagnostic (codex round 2, gp-0i4o).
func forkLineageStoreHasServer(beadsDir string) bool {
	return !sharedStoreNeedsLocalDoltDir(beadsDir) || doltserver.IsSharedServerMode()
}

func checkForkMigrationLineageServer(ctx context.Context, beadsDir string) (DoctorCheck, bool) {
	if beadsDir == "" || !IsDoltBackend(beadsDir) {
		return DoctorCheck{}, false
	}
	// Only a store that is in server mode has a server to ask. IsDoltBackend
	// is also true for an embedded configuration; an embedded checkout with
	// no embeddeddolt directory yet (the Embedded probe above declined) must
	// not have some other database's lineage reported as its own over a
	// retained server endpoint (codex gate r1, gp-w0nu).
	if !forkLineageStoreHasServer(beadsDir) {
		return DoctorCheck{}, false
	}
	conn, err := openDoltConn(beadsDir)
	if err != nil {
		// No server to ask (embedded mode, or a server-mode store whose
		// server is down): the caller's "N/A (no database)" stands.
		return DoctorCheck{}, false
	}
	defer conn.Close()
	return checkForkMigrationLineage(ctx, conn.db), true
}

func checkForkMigrationLineage(ctx context.Context, db schema.DBConn) DoctorCheck {
	report, check := classifyForkMigrationLineage(ctx, db)
	if report.IgnoredCursorNote == "" {
		return check
	}
	// One line naming the clone-local domain: the recorded ignored cursor and
	// the reality-checked reading disagree, which is the shape whose refusal
	// read "MAX(version)=0" over a table whose MAX was 22 on the wire.
	if check.Status == StatusOK {
		check.Status = StatusWarning
	}
	if check.Detail == "" {
		check.Detail = report.IgnoredCursorNote
	} else {
		check.Detail = strings.TrimSuffix(check.Detail, ".") + ". " + report.IgnoredCursorNote
	}
	return check
}

func classifyForkMigrationLineage(ctx context.Context, db schema.DBConn) (schema.ForkLineageReport, DoctorCheck) {
	report, err := schema.VerifyForkLineageState(ctx, db)
	if err != nil {
		return report, DoctorCheck{
			Name:     forkLineageCheckName,
			Status:   StatusWarning,
			Message:  fmt.Sprintf("Could not check fork migration lineage: %v", err),
			Detail:   "The lineage check failed to run; this does not mean the database is skewed. Re-run `bd doctor` and report the error if it persists.",
			Category: CategoryData,
		}
	}

	switch report.Status {
	case schema.ForkLineagePreMerge:
		schemeWord := "scheme"
		if len(report.PreMergeSchemes) > 1 {
			schemeWord = "schemes"
		}
		return report, DoctorCheck{
			Name:   forkLineageCheckName,
			Status: StatusWarning,
			Message: fmt.Sprintf(
				"Pre-merge fork migration cursor detected (main=v%d, ignored=v%d)",
				report.MainVersion, report.IgnoredVersion),
			Detail: fmt.Sprintf(
				"This database shows the %s %s. The next bd write command will reconcile the cursor and apply pending upstream migrations. Supported renumberings: bd-dn6 (fork 0051-0054 → 0070-0073, ignored 0010-0011 → 0020-0021 before the upmerge, now 0025-0026); 2026-08 upmerge (main 0070-0073 re-recorded after upstream 0056-0066, ignored 0020-0022 → 0025-0027).",
				strings.Join(report.PreMergeSchemes, " and "), schemeWord),
			Fix:      "Run any bd write command (e.g. `bd migrate`) with the new binary to reconcile.",
			Category: CategoryData,
		}
	case schema.ForkLineageReconciled:
		return report, DoctorCheck{
			Name:     forkLineageCheckName,
			Status:   StatusOK,
			Message:  fmt.Sprintf("Reconciled (main=v%d, ignored=v%d); fork and upstream schema effects verified", report.MainVersion, report.IgnoredVersion),
			Category: CategoryData,
		}
	case schema.ForkLineageInconsistent:
		return report, DoctorCheck{
			Name:   forkLineageCheckName,
			Status: StatusError,
			Message: fmt.Sprintf(
				"Migration cursor claims schema state the database does not have (main=v%d, ignored=v%d)",
				report.MainVersion, report.IgnoredVersion),
			Detail:   "Missing: " + strings.Join(report.Problems, "; ") + ". This is the silent-skew condition the fork renumbering guards against — the recorded version says these migrations ran, but their effects are absent.",
			Fix:      "Escalate before writing to this database; compare schema_migrations rows against INFORMATION_SCHEMA on a copy.",
			Category: CategoryData,
		}
	default: // ForkLineageNotApplicable
		return report, DoctorCheck{
			Name:     forkLineageCheckName,
			Status:   StatusOK,
			Message:  fmt.Sprintf("N/A (main=v%d predates the renumbered range; nothing to verify)", report.MainVersion),
			Category: CategoryData,
		}
	}
}
