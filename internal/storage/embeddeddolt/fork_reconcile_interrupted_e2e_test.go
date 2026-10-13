//go:build cgo

package embeddeddolt_test

import (
	"context"
	"database/sql"
	"errors"
	"os"
	"strings"
	"testing"

	"github.com/steveyegge/beads/internal/storage/schema"
)

// cutConn is the pass's connection with one statement cut: the first DELETE
// against the table named in cutTable fails with errCut, as a pass that
// loses its connection between the fork-lineage cursor DELETEs would. Every
// other statement reaches the pinned connection unchanged.
type cutConn struct {
	*sql.Conn
	cutTable string
	cuts     int
}

var errCut = errors.New("connection cut between the cursor DELETEs")

func (c *cutConn) ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error) {
	if c.cuts == 0 && strings.HasPrefix(query, "DELETE FROM "+c.cutTable+" ") {
		c.cuts++
		return nil, errCut
	}
	return c.Conn.ExecContext(ctx, query, args...)
}

// TestForkLineageReconciliationSurvivesACutBetweenTheCursorDeletes is gf-c7n
// finding 2h (the gf-xid vs-v1.3.1 gate's Major at fork_reconcile.go:579): the
// bd-dn6 reconciliation deleted the main cursor rows 51-54 and the ignored
// rows 10-11 as two autocommitted statements. A pass cut between them left
// row 54 gone and row 11 standing, and because the ignored fingerprint is
// only read beside the main one (upstream also owns an ignored 0011), the
// retry planned nothing for the ignored chain and resumed above 11:
// upstream's ignored 0010 (drop_wisp_id_defaults) and 0011
// (cleanup_orphaned_child_counters) were never applied while the cursor
// eventually reported latest. The two DELETEs now share one transaction, so
// the cut leaves both fingerprints in place and the retry reconciles both
// chains: the ignored rows are repaired, the migrations applied, the final
// cursor honest.
func TestForkLineageReconciliationSurvivesACutBetweenTheCursorDeletes(t *testing.T) {
	if os.Getenv("BEADS_TEST_EMBEDDED_DOLT") != "1" {
		t.Skip("set BEADS_TEST_EMBEDDED_DOLT=1 to run embedded dolt tests")
	}

	ctx := context.Background()
	conn, fileHash := openPreMergeForkFixture(t, ctx)

	// --- The pass cut between the main DELETE and the ignored DELETE ---
	cut := &cutConn{Conn: conn, cutTable: "ignored_schema_migrations"}
	if _, err := schema.MigrateUp(ctx, cut); !errors.Is(err, errCut) {
		t.Fatalf("MigrateUp over the cut connection = %v, want the cut", err)
	}
	if cut.cuts != 1 {
		t.Fatalf("the ignored DELETE was cut %d times, want once", cut.cuts)
	}

	// Both fingerprints must still stand: the main DELETE did not outlive
	// the ignored one.
	assertScalar(t, ctx, conn, "SELECT COALESCE(MAX(version),0) FROM schema_migrations", 54)
	assertScalar(t, ctx, conn, "SELECT COUNT(*) FROM schema_migrations WHERE version BETWEEN 51 AND 54", 4)
	assertScalar(t, ctx, conn, "SELECT COALESCE(MAX(version),0) FROM ignored_schema_migrations", 11)
	report, err := schema.VerifyForkLineageState(ctx, conn)
	if err != nil {
		t.Fatalf("VerifyForkLineageState (after the cut): %v", err)
	}
	if report.Status != schema.ForkLineagePreMerge {
		t.Fatalf("lineage after the cut = %q (problems: %v), want %q: the cut pass left a half-rewritten cursor", report.Status, report.Problems, schema.ForkLineagePreMerge)
	}

	// --- The retry, on the intact connection ---
	if _, err := schema.MigrateUp(ctx, conn); err != nil {
		t.Fatalf("MigrateUp (retry): %v", err)
	}
	assertScalar(t, ctx, conn, "SELECT COALESCE(MAX(version),0) FROM schema_migrations", schema.LatestVersion())
	assertScalar(t, ctx, conn, "SELECT COALESCE(MAX(version),0) FROM ignored_schema_migrations", schema.LatestIgnoredVersion())

	// The ignored chain was repaired: rows 10-11 now carry upstream's
	// content, and the fork's tables are re-recorded at 25-26.
	for desc, tc := range map[string]struct {
		table   string
		version int
		file    string
	}{
		"upstream ignored 0010": {"ignored_schema_migrations", 10, "ignored/0010_drop_wisp_id_defaults.up.sql"},
		"upstream ignored 0011": {"ignored_schema_migrations", 11, "ignored/0011_cleanup_orphaned_child_counters.up.sql"},
		"fork ignored 0025":     {"ignored_schema_migrations", 25, "ignored/0025_create_linear_issue_snapshots.up.sql"},
		"fork ignored 0026":     {"ignored_schema_migrations", 26, "ignored/0026_create_linear_project_snapshots.up.sql"},
		"upstream 0051":         {"schema_migrations", 51, "0051_drop_aux_id_defaults.up.sql"},
		"fork 0073":             {"schema_migrations", 73, "0073_create_attachments.up.sql"},
	} {
		var recorded sql.NullString
		//nolint:gosec // test-only, table names are constants above
		if err := conn.QueryRowContext(ctx,
			"SELECT content_hash FROM "+tc.table+" WHERE version = ?", tc.version).Scan(&recorded); err != nil {
			t.Errorf("%s: reading recorded hash: %v", desc, err)
			continue
		}
		if !recorded.Valid || recorded.String != fileHash(tc.file) {
			t.Errorf("%s: recorded hash %v, want hash of %s (the chain was skipped after the cut)", desc, recorded, tc.file)
		}
	}

	// Upstream ignored 0010's effect: the wisp aux id DEFAULTs are dropped.
	for _, table := range []string{"wisp_events", "wisp_comments"} {
		var columnDefault sql.NullString
		if err := conn.QueryRowContext(ctx, `
			SELECT COLUMN_DEFAULT FROM INFORMATION_SCHEMA.COLUMNS
			WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = ? AND COLUMN_NAME = 'id'
		`, table).Scan(&columnDefault); err != nil {
			t.Fatalf("reading %s.id default: %v", table, err)
		}
		if columnDefault.Valid {
			t.Errorf("%s.id still has DEFAULT %q; ignored 0010 was skipped after the cut", table, columnDefault.String)
		}
	}

	report, err = schema.VerifyForkLineageState(ctx, conn)
	if err != nil {
		t.Fatalf("VerifyForkLineageState (after the retry): %v", err)
	}
	if report.Status != schema.ForkLineageReconciled {
		t.Fatalf("lineage after the retry = %q (problems: %v), want %q", report.Status, report.Problems, schema.ForkLineageReconciled)
	}
	if applied, err := schema.MigrateUp(ctx, conn); err != nil || applied != 0 {
		t.Fatalf("third MigrateUp = %d, %v; want a clean no-op", applied, err)
	}
}
