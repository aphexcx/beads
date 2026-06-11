package dolt

import (
	"context"
	"database/sql"
	"sort"
	"testing"

	"github.com/steveyegge/beads/internal/storage/rowid"
	"github.com/steveyegge/beads/internal/storage/schema"
)

// auxRekeyMarkerVersion mirrors the unexported clone-local ignored-migration
// marker in internal/storage/schema. Deleting that row from the (dolt-ignored)
// cursor simulates a clone that has not yet run the one-time re-key — the
// state every pre-fix database is in when it upgrades.
const auxRekeyMarkerVersion = 9

// seedAuxRekeyFixture writes the same logical history rows under
// clone-specific random primary keys, simulating what migration 0037's
// UUID() backfill left behind on one clone (bd-6dnrw.2), and commits them so
// MigrateUp sees a clean working set, exactly like a real upgrade pass.
func seedAuxRekeyFixture(ctx context.Context, t *testing.T, db *sql.DB, clonePrefix string) {
	t.Helper()

	if _, err := db.ExecContext(ctx,
		"INSERT INTO issues (id, title, description, design, acceptance_criteria, notes, status, priority, issue_type) VALUES ('bd-rek-1', 'rekey fixture', '', '', '', '', 'open', 2, 'task')"); err != nil {
		t.Fatalf("seed issue: %v", err)
	}

	randomID := func(n byte) string {
		return clonePrefix + "-0000-0000-0000-00000000000" + string('0'+n)
	}
	// One distinct comment plus an exact-duplicate pair (no natural identity).
	for i, c := range []struct{ author, text, created string }{
		{"alice", "hello", "2026-06-01 10:00:00"},
		{"bob", "same", "2026-06-01 11:00:00"},
		{"bob", "same", "2026-06-01 11:00:00"},
	} {
		if _, err := db.ExecContext(ctx,
			"INSERT INTO comments (id, issue_id, author, text, created_at) VALUES (?, 'bd-rek-1', ?, ?, ?)",
			randomID(byte(i)), c.author, c.text, c.created); err != nil {
			t.Fatalf("seed comment %d: %v", i, err)
		}
	}
	// One event with NULLs, to cover the NULL-vs-empty encoding end to end.
	if _, err := db.ExecContext(ctx,
		"INSERT INTO events (id, issue_id, event_type, actor, old_value, new_value, comment, created_at) VALUES (?, 'bd-rek-1', 'created', 'alice', NULL, 'open', NULL, '2026-06-01 10:00:00')",
		randomID(9)); err != nil {
		t.Fatalf("seed event: %v", err)
	}

	if _, err := db.ExecContext(ctx, "CALL DOLT_COMMIT('-Am', 'seed pre-rekey rows')"); err != nil {
		t.Fatalf("commit seed: %v", err)
	}

	// Un-record the clone-local marker so MigrateUp treats this clone as not
	// yet converged. The cursor is dolt-ignored, so no commit is involved.
	if _, err := db.ExecContext(ctx,
		"DELETE FROM ignored_schema_migrations WHERE version = ?", auxRekeyMarkerVersion); err != nil {
		t.Fatalf("unrecord marker: %v", err)
	}
}

func readIDs(ctx context.Context, t *testing.T, db *sql.DB, table string) []string {
	t.Helper()
	rows, err := db.QueryContext(ctx, "SELECT id FROM "+table+" WHERE issue_id = 'bd-rek-1'")
	if err != nil {
		t.Fatalf("read %s ids: %v", table, err)
	}
	defer rows.Close()
	var ids []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			t.Fatalf("scan %s id: %v", table, err)
		}
		ids = append(ids, id)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate %s ids: %v", table, err)
	}
	sort.Strings(ids)
	return ids
}

func nv(s string) sql.NullString { return sql.NullString{String: s, Valid: true} }

// expectedAuxRekeyIDs returns the deterministic ids the fixture rows must
// converge to — identical for every clone by construction.
func expectedAuxRekeyIDs() (comments, events []string) {
	helloDigest := rowid.Digest([]sql.NullString{nv("bd-rek-1"), nv("alice"), nv("hello"), nv("2026-06-01 10:00:00")})
	dupDigest := rowid.Digest([]sql.NullString{nv("bd-rek-1"), nv("bob"), nv("same"), nv("2026-06-01 11:00:00")})
	comments = []string{
		rowid.New("comments", 0, helloDigest),
		rowid.New("comments", 0, dupDigest),
		rowid.New("comments", 1, dupDigest),
	}
	sort.Strings(comments)
	eventDigest := rowid.Digest([]sql.NullString{nv("bd-rek-1"), nv("created"), nv("alice"), {}, nv("open"), {}, nv("2026-06-01 10:00:00")})
	events = []string{rowid.New("events", 0, eventDigest)}
	return comments, events
}

func equalStrings(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// TestAuxRowIDRekeyConvergesIndependentClones is the end-to-end regression for
// bd-6dnrw.2: two clones holding the same logical events/comments under
// different random primary keys (migration 0037's per-clone UUID() backfill)
// must converge to byte-identical ids after each runs its own one-time re-key
// pass — including an exact-duplicate comment pair, which must keep two
// distinct rows. A second MigrateUp must then leave the ids untouched: the
// clone-local marker makes the pass one-time, so steady-state opens do not
// churn synced tables.
func TestAuxRowIDRekeyConvergesIndependentClones(t *testing.T) {
	// Each clone is a subtest with its own store (setupTestStore calls
	// t.Parallel, so one call per test). Both assert against the same
	// precomputed deterministic id sets — converging to them from different
	// random pre-states is exactly the cross-clone guarantee under test.
	for _, tc := range []struct{ name, prefix string }{
		// Distinct prefixes give each clone different pre-rekey random ids —
		// the divergence the backfill must heal.
		{"clone-a", "aaaaaaaa"},
		{"clone-b", "bbbbbbbb"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			store, cleanup := setupTestStore(t)
			defer cleanup()

			ctx, cancel := testContext(t)
			defer cancel()

			wantComments, wantEvents := expectedAuxRekeyIDs()
			seedAuxRekeyFixture(ctx, t, store.db, tc.prefix)

			if _, err := schema.MigrateUp(ctx, store.db); err != nil {
				t.Fatalf("MigrateUp: %v", err)
			}

			if got := readIDs(ctx, t, store.db, "comments"); !equalStrings(got, wantComments) {
				t.Errorf("comments ids = %v, want %v", got, wantComments)
			}
			if got := readIDs(ctx, t, store.db, "events"); !equalStrings(got, wantEvents) {
				t.Errorf("events ids = %v, want %v", got, wantEvents)
			}

			// Marker recorded again: the pass is one-time-per-clone.
			var markerCount int
			if err := store.db.QueryRowContext(ctx,
				"SELECT COUNT(*) FROM ignored_schema_migrations WHERE version = ?", auxRekeyMarkerVersion).Scan(&markerCount); err != nil {
				t.Fatalf("read marker: %v", err)
			}
			if markerCount != 1 {
				t.Errorf("marker version %d recorded %d times, want 1", auxRekeyMarkerVersion, markerCount)
			}

			// Steady state: another MigrateUp must not touch the ids.
			if _, err := schema.MigrateUp(ctx, store.db); err != nil {
				t.Fatalf("second MigrateUp: %v", err)
			}
			if got := readIDs(ctx, t, store.db, "comments"); !equalStrings(got, wantComments) {
				t.Errorf("comments ids churned on second pass: %v", got)
			}

			// The re-key's writes were staged and committed by MigrateUp: the
			// synced tables must not be left dirty.
			var dirty int
			if err := store.db.QueryRowContext(ctx,
				"SELECT COUNT(*) FROM dolt_status WHERE table_name IN ('comments', 'events')").Scan(&dirty); err != nil {
				t.Fatalf("read dolt_status: %v", err)
			}
			if dirty != 0 {
				t.Errorf("comments/events left dirty after MigrateUp")
			}
		})
	}
}
