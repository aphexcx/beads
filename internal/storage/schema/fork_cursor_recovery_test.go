package schema

import (
	"context"
	"errors"
	"slices"
	"strings"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
)

func expectRecoveryMainPlan(t *testing.T, mock sqlmock.Sqlmock, preMerge bool) {
	t.Helper()
	if !preMerge {
		expectCursorRowProbe(mock, "schema_migrations", 54, 0)
		return
	}
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
}

func expectRecoveryScratchSelection(mock sqlmock.Sqlmock, patterns []doltIgnoreRow) {
	expectSchemaTableExists(mock, ignoredSource.cursorTable, false)
	expectSchemaTableExists(mock, ignoredCursorUntrackTempTable, true)
	expectIgnoreResolution(mock, "", ignoredSource.cursorTable, patterns)
}

func expectRecoveryIgnoredPlan(t *testing.T, mock sqlmock.Sqlmock, preMerge, invalidHash bool) {
	t.Helper()
	table := ignoredCursorUntrackTempTable
	versions := []int{20, 21, 22}
	files := upmergeRenumberedIgnoredFiles
	if preMerge {
		versions = []int{10, 11}
		files = forkRenumberedIgnoredFiles
		expectCursorRowProbe(mock, table, 11, 1)
		expectMaxVersion(mock, table, 11)
	} else {
		expectCursorRowProbe(mock, "schema_migrations", 73, 0)
		expectCursorRowProbe(mock, table, 22, 1)
		expectCursorRowProbe(mock, table, 14, 0)
		expectMaxVersion(mock, table, 22)
	}
	expectTableProbe(mock, "linear_issue_snapshots", true)
	expectTableProbe(mock, "linear_project_snapshots", true)
	if !preMerge {
		expectColumnProbe(mock, "wisp_comments", "external_ref", true)
	}
	expectHasContentHashColumn(mock, table, true)
	for _, v := range versions {
		hash := forkFileHash(t, ignoredSource, files[v])
		if invalidHash {
			hash = "unknown-migration-content"
		}
		expectRecordedHash(mock, table, v, hash)
		if invalidHash {
			return
		}
	}
	if preMerge {
		expectCursorRowProbe(mock, "schema_migrations", 73, 0)
		expectCursorRowProbe(mock, table, 22, 0)
	}
}

// Both sides of the DROP commit must plan against the surviving cursor copy,
// then rewrite the restored live cursor so upstream ordinals remain pending.
func TestForkCursorRecoveryPlansRestoredRows(t *testing.T) {
	for _, preMerge := range []bool{true, false} {
		for _, tracked := range []bool{false, true} {
			name := "pre_upmerge/after_drop_commit"
			if preMerge {
				name = "pre_merge/after_drop_commit"
			}
			if tracked {
				name = strings.ReplaceAll(name, "after_drop_commit", "before_drop_commit")
			}
			t.Run(name, func(t *testing.T) {
				db, mock := newMockDB(t)
				ctx := context.Background()
				expectRecoveryMainPlan(t, mock, preMerge)
				expectRecoveryScratchSelection(mock, exactlyIgnored(true))
				expectRecoveryIgnoredPlan(t, mock, preMerge, false)
				rewrites, err := planForkLineageCursors(ctx, db)
				if err != nil {
					t.Fatal(err)
				}
				matches := []doltIgnoreRow(nil)
				if tracked {
					matches = exactlyIgnored(true)
				}
				expectIgnoredCursorGate(mock, "", tracked, matches, true)
				if tracked {
					mock.ExpectExec("(?s)^CREATE TABLE IF NOT EXISTS " + ignoredCursorUntrackTempTable).WillReturnResult(sqlmock.NewResult(0, 0))
					expectSchemaTableExists(mock, ignoredSource.cursorTable, false)
					expectIgnoredCursorUnstage(mock)
					expectIgnoredCursorUntrackCommit(mock)
				} else {
					expectIgnoreResolution(mock, "", ignoredSource.cursorTable, exactlyIgnored(true))
					expectSchemaTableExists(mock, ignoredSource.cursorTable, false)
				}
				expectIgnoredCursorRestore(mock, false)
				healed, err := healTrackedIgnoredCursorTable(ctx, db)
				if err != nil || !healed {
					t.Fatalf("heal=%v,%v", healed, err)
				}
				lo, hi, remaining := 20, 22, 13
				if preMerge {
					lo, hi, remaining = 10, 11, 9
				}
				var ignoredRewrite *cursorRewrite
				for i := range rewrites {
					if rewrites[i].table == ignoredSource.cursorTable {
						ignoredRewrite = &rewrites[i]
					}
				}
				if ignoredRewrite == nil || ignoredRewrite.lo != lo || ignoredRewrite.hi != hi {
					t.Fatalf("missing live cursor rewrite %d-%d: %+v", lo, hi, rewrites)
				}
				expectCursorRewrite(mock, ignoredSource.cursorTable, lo, hi)
				if err := ignoredRewrite.apply(ctx, db); err != nil {
					t.Fatal(err)
				}
				expectCursorProbe(mock, ignoredSource.cursorTable, true)
				expectMaxVersion(mock, ignoredSource.cursorTable, remaining)
				expectTableProbe(mock, "wisps", true)
				expectTableProbe(mock, "wisp_dependencies", true)
				expectColumnProbe(mock, "leases", "granted_node", false)
				pending, err := PendingIgnoredVersions(ctx, db)
				if err != nil {
					t.Fatal(err)
				}
				if !slices.Contains(pending, lo) || !slices.Contains(pending, hi) {
					t.Fatalf("upstream versions %d-%d skipped after restoring and rewriting: %v", lo, hi, pending)
				}
				if err := mock.ExpectationsWereMet(); err != nil {
					t.Fatal(err)
				}
			})
		}
	}
}

func TestMigrateUpRefusesUnknownScratchHashesBeforeAnyWrite(t *testing.T) {
	for _, preMerge := range []bool{true, false} {
		name := "pre_upmerge"
		if preMerge {
			name = "pre_merge"
		}
		t.Run(name, func(t *testing.T) {
			db, mock := newMockDB(t)
			expectCursorProbe(mock, "schema_migrations", true)
			expectMaxVersion(mock, "schema_migrations", 54)
			expectRecoveryMainPlan(t, mock, preMerge)
			// The exact pattern is absent; the seed would add true, so this scratch
			// must already be verified even though a broad operator rule is false.
			expectRecoveryScratchSelection(mock, []doltIgnoreRow{{pattern: "%", ignored: false}})
			expectRecoveryIgnoredPlan(t, mock, preMerge, true)
			applied, err := MigrateUp(context.Background(), db)
			if applied != 0 || !errors.Is(err, errRefusedRewrite) || !strings.Contains(err.Error(), "working set was left as found") {
				t.Fatalf("MigrateUp=%d,%v; want refusal before seed/heal", applied, err)
			}
			if err := mock.ExpectationsWereMet(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestForkIgnoredCursorRecoverySource(t *testing.T) {
	cases := []struct {
		name          string
		live, scratch bool
		patterns      []doltIgnoreRow
		wantScratch   bool
	}{
		{name: "live cursor takes precedence", live: true, scratch: true},
		{name: "missing cursor and scratch"},
		{name: "explicit exact veto", scratch: true, patterns: exactlyIgnored(false)},
		{name: "explicit exact allow", scratch: true, patterns: exactlyIgnored(true), wantScratch: true},
		{name: "seed adds missing exact pattern", scratch: true, wantScratch: true},
		{name: "seed exact pattern overrides broad veto", scratch: true, patterns: []doltIgnoreRow{{pattern: "%", ignored: false}}, wantScratch: true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			db, mock := newMockDB(t)
			expectSchemaTableExists(mock, ignoredSource.cursorTable, tc.live)
			if !tc.live {
				expectSchemaTableExists(mock, ignoredCursorUntrackTempTable, tc.scratch)
				if tc.scratch {
					expectIgnoreResolution(mock, "", ignoredSource.cursorTable, tc.patterns)
				}
			}
			src, err := forkIgnoredCursorSource(context.Background(), db)
			if err != nil {
				t.Fatal(err)
			}
			want := ignoredSource.cursorTable
			if tc.wantScratch {
				want = ignoredCursorUntrackTempTable
			}
			if src.cursorTable != want {
				t.Fatalf("source=%s, want %s", src.cursorTable, want)
			}
			if err := mock.ExpectationsWereMet(); err != nil {
				t.Fatal(err)
			}
		})
	}
}
