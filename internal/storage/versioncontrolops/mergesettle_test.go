package versioncontrolops

import (
	"context"
	"database/sql/driver"
	"regexp"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"

	"github.com/steveyegge/beads/internal/storage/schema"
)

// The two sanctioned contents of migration 0072 (gf-4c9 row 1): the bytes
// every store that migrated through the fork lineage before 2026-10-12
// recorded as row 72, and the bytes of the guarded file a fresh clone applies
// today. schema.ContentHashesEquivalent is the source of truth for the pair;
// the precondition below fails loudly if either value drifts from it.
const (
	hash0072Prior   = "ef072196333301e0fa50a0d6eac9f23f529da8e04b5586d96c753a37da0d78c2"
	hash0072Current = "0425363f7ff7adbae17329fa32d9ac533eeecb986a3aa1284d26dcf021161066"
	hash0072Foreign = "1111111111111111111111111111111111111111111111111111111111111111"
)

const schemaMigrationsConflictsSQL = `
		SELECT our_version, their_version, our_content_hash, their_content_hash
		FROM dolt_conflicts_schema_migrations`

func schemaMigrationsConflictRows(rows ...[]driver.Value) *sqlmock.Rows {
	out := sqlmock.NewRows([]string{"our_version", "their_version", "our_content_hash", "their_content_hash"})
	for _, r := range rows {
		out.AddRow(r...)
	}
	return out
}

// TestSchemaMigrationsConflictsAreVintageOnly_SanctionedHashPairMerges pins
// the gf-4c9 codex round 2 finding: two clones that applied 0072 from a
// common pre-0072 ancestor, one with the old binary and one with the new,
// carry different row-72 hashes. ContentHashSkew already reads that pair as
// the same lineage; the merge resolver must too, or the pull fails with an
// unresolved schema_migrations conflict between compatible clones.
func TestSchemaMigrationsConflictsAreVintageOnly_SanctionedHashPairMerges(t *testing.T) {
	if !schema.ContentHashesEquivalent(72, hash0072Prior, hash0072Current) {
		t.Fatalf("precondition: schema no longer sanctions the 0072 hash pair (%s, %s); update the constants", hash0072Prior, hash0072Current)
	}
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	mock.ExpectQuery(regexp.QuoteMeta(schemaMigrationsConflictsSQL)).
		WillReturnRows(schemaMigrationsConflictRows([]driver.Value{72, 72, hash0072Prior, hash0072Current}))

	ok, err := schemaMigrationsConflictsAreVintageOnly(context.Background(), db)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !ok {
		t.Fatalf("a conflict between the two sanctioned 0072 contents must be vintage-only (auto-resolvable), got false")
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestSchemaMigrationsConflictsAreVintageOnly_UnknownHashStaysOperatorDecision(t *testing.T) {
	cases := []struct {
		name string
		row  []driver.Value
	}{
		{"foreign vs current", []driver.Value{72, 72, hash0072Foreign, hash0072Current}},
		{"foreign vs prior", []driver.Value{72, 72, hash0072Prior, hash0072Foreign}},
		{"sanctioned pair on the wrong version", []driver.Value{71, 71, hash0072Prior, hash0072Current}},
		{"version mismatch", []driver.Value{72, 73, hash0072Current, hash0072Current}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			db, mock, err := sqlmock.New()
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			mock.ExpectQuery(regexp.QuoteMeta(schemaMigrationsConflictsSQL)).
				WillReturnRows(schemaMigrationsConflictRows(tc.row))
			ok, err := schemaMigrationsConflictsAreVintageOnly(context.Background(), db)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if ok {
				t.Fatalf("%s must NOT be vintage-only", tc.name)
			}
		})
	}
}

func TestSchemaMigrationsConflictsAreVintageOnly_EqualOrEmptyHashes(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	mock.ExpectQuery(regexp.QuoteMeta(schemaMigrationsConflictsSQL)).
		WillReturnRows(schemaMigrationsConflictRows(
			[]driver.Value{70, 70, hash0072Current, hash0072Current},
			[]driver.Value{71, 71, nil, hash0072Current},
			[]driver.Value{72, 72, hash0072Prior, nil},
		))
	ok, err := schemaMigrationsConflictsAreVintageOnly(context.Background(), db)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !ok {
		t.Fatalf("equal hashes and NULL-on-one-side rows are vintage-only, got false")
	}
}
