package versioncontrolops

import (
	"context"
	"database/sql/driver"
	"regexp"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"

	"github.com/steveyegge/beads/internal/storage/schema"
)

// Three sanctioned contents of migration 0072: the bytes every store that
// migrated through the fork lineage before 2026-10-12 recorded as row 72
// (gf-4c9 row 1's prior), the gf-4c9 row-1 bytes, and the bytes a fresh clone
// applies today after the updated_at backfill (gf-pu2 finding 2d).
// schema.ContentHashesEquivalent is the source of truth for the lineage; the
// preconditions below fail loudly if a value drifts from it.
const (
	hash0072Prior    = "ef072196333301e0fa50a0d6eac9f23f529da8e04b5586d96c753a37da0d78c2"
	hash0072Current  = "0425363f7ff7adbae17329fa32d9ac533eeecb986a3aa1284d26dcf021161066"
	hash0072Backfill = "4798ce8520665a4e44c998f07679d84bce45c7870c2dfd5152f9f9dae6b74959"
	hash0072Foreign  = "1111111111111111111111111111111111111111111111111111111111111111"
)

// TestSchemaMigrationsConflictsAreVintageOnly_SanctionedLineageMerges0072Backfill
// pins gf-pu2 finding 2d at the merge resolver: a clone that applied the
// gf-4c9 text (or the older one) and a clone that applied the backfill text
// carry different row-72 hashes and must still merge as vintage-only.
func TestSchemaMigrationsConflictsAreVintageOnly_SanctionedLineageMerges0072Backfill(t *testing.T) {
	for _, older := range []string{hash0072Prior, hash0072Current} {
		if !schema.ContentHashesEquivalent(72, older, hash0072Backfill) {
			t.Fatalf("precondition: schema does not sanction the 0072 pair (%s, %s); update the constants", older, hash0072Backfill)
		}
		db, mock, err := sqlmock.New()
		if err != nil {
			t.Fatal(err)
		}
		mock.ExpectQuery(regexp.QuoteMeta(schemaMigrationsConflictsSQL)).
			WillReturnRows(schemaMigrationsConflictRows([]driver.Value{72, 72, older, hash0072Backfill}))
		ok, err := schemaMigrationsConflictsAreVintageOnly(context.Background(), db)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if !ok {
			t.Fatalf("a conflict between %s and the backfill text must be vintage-only (auto-resolvable), got false", older)
		}
		if err := mock.ExpectationsWereMet(); err != nil {
			t.Fatal(err)
		}
		db.Close()
	}
}

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

// The two sanctioned contents of migration 0066 (gf-jr0, finding 2a): the
// bytes upstream v1.3.1 ships and the bytes this lineage ships, which differ
// by one comment line. An upstream-origin store meeting one of ours carries
// different row-66 hashes and must still merge.
const (
	hash0066Upstream = "a524d42ba0eced9350edbf6402dc2952957ff8f75a896bc8339c4cbc8596c793"
	hash0066Current  = "4fcb31077205ecb9c6e6fa2890aa2b4cf9dabf96cd0498ecac1c7a6d97a651c7"
)

// TestSchemaMigrationsConflictsAreVintageOnly_SanctionedHashPairMerges0066
// pins the gf-4c9 round-2 gate's Major on 0066: the sanctioned-pair
// equivalence covered 0072 only, so a schema_migrations conflict between
// upstream's row 66 and ours stayed an operator decision.
func TestSchemaMigrationsConflictsAreVintageOnly_SanctionedHashPairMerges0066(t *testing.T) {
	if !schema.ContentHashesEquivalent(66, hash0066Upstream, hash0066Current) {
		t.Fatalf("schema does not sanction the 0066 hash pair (%s, %s)", hash0066Upstream, hash0066Current)
	}
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	mock.ExpectQuery(regexp.QuoteMeta(schemaMigrationsConflictsSQL)).
		WillReturnRows(schemaMigrationsConflictRows([]driver.Value{66, 66, hash0066Upstream, hash0066Current}))

	ok, err := schemaMigrationsConflictsAreVintageOnly(context.Background(), db)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !ok {
		t.Fatalf("a conflict between the two sanctioned 0066 contents must be vintage-only (auto-resolvable), got false")
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}
