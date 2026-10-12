package schema

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
)

// Every text migration 0072 has had before this lineage's current bytes
// (gf-pu2 finding 2d): the fork's duplicate-numbered 0024, the fork's 0053 /
// the pre-gf-4c9 0072 (what all four citadel stores recorded as row 72), and
// the gf-4c9 row-1 bytes that this round's updated_at backfill replaced.
var hash0072Priors = []string{
	"cc12cfba4fcd5e51d6059dafe58056ecc155f188f576258189e95515645e8c8b",
	"ef072196333301e0fa50a0d6eac9f23f529da8e04b5586d96c753a37da0d78c2",
	"0425363f7ff7adbae17329fa32d9ac533eeecb986a3aa1284d26dcf021161066",
}

// TestContentHashSkewAcceptsEveryPriorContentOf0072 pins the registration
// the 0072 backfill edit needs: the current bytes and every prior text are
// one lineage — no skew in any pairing, equivalent to every reader — while
// an unknown hash is still a fork and the sanction holds for version 72 only.
// Without the registration a store that applied the old text (every live
// store) would read as a schema fork against a fresh init of the new one.
func TestContentHashSkewAcceptsEveryPriorContentOf0072(t *testing.T) {
	current := forkFileHash(t, mainSource, "0072_add_comment_external_ref.up.sql")
	for _, prior := range hash0072Priors {
		if current == prior {
			t.Fatalf("0072 hashes %s, one of its registered priors; the registration records nothing", current)
		}
	}
	lineage := append([]string{current}, hash0072Priors...)
	for _, a := range lineage {
		for _, b := range lineage {
			if got := ContentHashSkew(map[int]string{72: a}, map[int]string{72: b}); len(got) != 0 {
				t.Errorf("ContentHashSkew(%.8s, %.8s) = %v, want no skew inside the 0072 lineage", a, b, got)
			}
			if !ContentHashesEquivalent(72, a, b) {
				t.Errorf("ContentHashesEquivalent(72, %.8s, %.8s) = false, want the sanctioned lineage", a, b)
			}
		}
	}
	if got := ContentHashSkew(map[int]string{72: current}, map[int]string{72: strings.Repeat("ab", 32)}); !reflect.DeepEqual(got, []int{72}) {
		t.Fatalf("ContentHashSkew(current, unknown) = %v, want [72]", got)
	}
	if got := ContentHashSkew(map[int]string{71: hash0072Priors[2]}, map[int]string{71: current}); !reflect.DeepEqual(got, []int{71}) {
		t.Fatalf("ContentHashSkew on another version = %v, want [71]: the lineage is sanctioned for 0072 only", got)
	}
}

// TestSmartGateAllowsThe0072Lineage is the same registration at the door a
// store opens through: a store whose row 72 is any prior text of 0072 against
// a remote at the current one opens without a skew refusal; unknown content
// is still a fork.
func TestSmartGateAllowsThe0072Lineage(t *testing.T) {
	current := forkFileHash(t, mainSource, "0072_add_comment_external_ref.up.sql")
	for _, prior := range hash0072Priors {
		t.Run("prior "+prior[:8]+" here, current on the remote: allowed", func(t *testing.T) {
			t.Setenv(SmartGateEnv, "1")
			t.Setenv(AllowRemoteMigrateEnv, "0")
			db, mock, _ := sqlmock.New()
			defer db.Close()
			expectSmartFiringGate(mock, 72)
			local := map[int]string{71: "h71", 72: prior}
			remote := map[int]string{71: "h71", 72: current}
			expectSmartRemoteRead(mock, local, remote)
			if err := CheckRemoteMigrateGate(context.Background(), db); err != nil {
				t.Fatalf("a store holding a prior 0072 hash must open without a skew refusal, got %v", err)
			}
			if err := mock.ExpectationsWereMet(); err != nil {
				t.Fatalf("unmet expectations: %v", err)
			}
		})
	}
	t.Run("unknown 0072 bytes on the remote: still a fork", func(t *testing.T) {
		t.Setenv(SmartGateEnv, "1")
		t.Setenv(AllowRemoteMigrateEnv, "0")
		db, mock, _ := sqlmock.New()
		defer db.Close()
		expectSmartFiringGate(mock, 72)
		local := map[int]string{71: "h71", 72: hash0072Priors[1]}
		remote := map[int]string{71: "h71", 72: strings.Repeat("cd", 32)}
		expectSmartRemoteRead(mock, local, remote)
		err := CheckRemoteMigrateGate(context.Background(), db)
		var gateErr *RemoteMigrateGateError
		if !errors.As(err, &gateErr) || gateErr.Decision != gateDecisionForkSkew {
			t.Fatalf("expected the fork-skew decision for unknown 0072 content, got %v", err)
		}
		if len(gateErr.SkewVersions) != 1 || gateErr.SkewVersions[0] != 72 {
			t.Fatalf("SkewVersions = %v, want [72]", gateErr.SkewVersions)
		}
	})
}

// TestMigration0072BackfillsUpdatedAtDeterministically pins the shape of the
// edit itself, as text: updated_at is added nullable, backfilled from
// created_at, and only then given its CURRENT_TIMESTAMP default, on both
// tables, and the file carries no one-step ADD COLUMN with the default — the
// statement that stamped existing rows with each clone's migration time. The
// behavioral proof (two independent applies, identical bytes) is the embedded
// dolt test TestMain0072BackfillsUpdatedAtFromCreatedAtDeterministically.
func TestMigration0072BackfillsUpdatedAtDeterministically(t *testing.T) {
	data, err := mainSource.files.ReadFile(mainSource.dir + "/0072_add_comment_external_ref.up.sql")
	if err != nil {
		t.Fatal(err)
	}
	text := string(data)
	if strings.Contains(text, "ADD COLUMN updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP") {
		t.Fatal("0072 still adds updated_at with its default in one step, which stamps existing rows with the migration time")
	}
	for _, table := range []string{"comments", "wisp_comments"} {
		add := strings.Index(text, "ALTER TABLE "+table+" ADD COLUMN updated_at DATETIME NULL")
		fill := strings.Index(text, "UPDATE "+table+" SET updated_at = ")
		def := strings.Index(text, "ALTER TABLE "+table+" MODIFY COLUMN updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP")
		if add < 0 || fill < 0 || def < 0 || !(add < fill && fill < def) {
			t.Fatalf("%s.updated_at must be added nullable (%d), backfilled from created_at (%d) and then given its default (%d), in that order", table, add, fill, def)
		}
		if !strings.Contains(text[fill:def], "created_at") {
			t.Fatalf("%s.updated_at backfill does not read created_at", table)
		}
		// The backfill and the default are guarded on the column being
		// nullable, re-read after the ADD, so a migration interrupted between
		// the ADD and the MODIFY resumes (Dolt DDL commits per statement).
		guard := strings.LastIndex(text[:fill], "IS_NULLABLE = 'YES'")
		if guard < 0 || guard < add {
			t.Fatalf("%s.updated_at backfill and default are not guarded on the column being nullable after the ADD (guard %d, add %d, fill %d)", table, guard, add, fill)
		}
	}
}
