package doctor

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/steveyegge/beads/internal/configfile"
	"github.com/steveyegge/beads/internal/storage/embeddeddolt"
	"github.com/steveyegge/beads/internal/storage/schema"
)

const forkLineageCheckName = "Fork Migration Lineage"

// CheckForkMigrationLineage verifies the bd-dn6 fork migration renumbering
// state: databases migrated by a pre-merge fork binary recorded the fork's
// migrations under numbers upstream v1.1.0-rc.1 now owns (main 0051-0054,
// ignored 0010-0011). The migration runner reconciles those cursors
// automatically; this check reports where a database stands and — critically —
// verifies column-by-column that a reconciled cursor matches the actual
// schema, catching the silent skew where a version number claims DDL that
// never ran.
//
// Read-only diagnostic; it never gates anything.
func CheckForkMigrationLineage(ss *SharedStore) DoctorCheck {
	if store := ss.Store(); store != nil {
		return checkForkMigrationLineage(context.Background(), store.DB())
	}
	if check, ok := checkForkMigrationLineageEmbedded(context.Background(), sharedStoreBeadsDir(ss)); ok {
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

func checkForkMigrationLineage(ctx context.Context, db schema.DBConn) DoctorCheck {
	report, err := schema.VerifyForkLineageState(ctx, db)
	if err != nil {
		return DoctorCheck{
			Name:     forkLineageCheckName,
			Status:   StatusWarning,
			Message:  fmt.Sprintf("Could not check fork migration lineage: %v", err),
			Detail:   "The lineage check failed to run; this does not mean the database is skewed. Re-run `bd doctor` and report the error if it persists.",
			Category: CategoryData,
		}
	}

	switch report.Status {
	case schema.ForkLineagePreMerge:
		return DoctorCheck{
			Name:   forkLineageCheckName,
			Status: StatusWarning,
			Message: fmt.Sprintf(
				"Pre-merge fork migration cursor detected (main=v%d, ignored=v%d)",
				report.MainVersion, report.IgnoredVersion),
			Detail:   "This database was last migrated by a pre-merge fork binary. The next bd write command will reconcile the cursor to the renumbered scheme (fork 0051-0054 → 0070-0073, ignored 0010-0011 → 0020-0021) and apply upstream's 0051-0053.",
			Fix:      "Run any bd write command (e.g. `bd migrate`) with the new binary to reconcile.",
			Category: CategoryData,
		}
	case schema.ForkLineageReconciled:
		return DoctorCheck{
			Name:     forkLineageCheckName,
			Status:   StatusOK,
			Message:  fmt.Sprintf("Reconciled (main=v%d, ignored=v%d); fork and upstream schema effects verified", report.MainVersion, report.IgnoredVersion),
			Category: CategoryData,
		}
	case schema.ForkLineageInconsistent:
		return DoctorCheck{
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
		return DoctorCheck{
			Name:     forkLineageCheckName,
			Status:   StatusOK,
			Message:  fmt.Sprintf("N/A (main=v%d predates the renumbered range; nothing to verify)", report.MainVersion),
			Category: CategoryData,
		}
	}
}
