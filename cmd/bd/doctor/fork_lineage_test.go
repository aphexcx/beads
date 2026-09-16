package doctor

import (
	"context"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"

	sqlmock "github.com/DATA-DOG/go-sqlmock"

	"github.com/steveyegge/beads/internal/config"
	"github.com/steveyegge/beads/internal/configfile"
)

// TestForkLineageStoreHasServer pins the predicate behind the fork-lineage
// server fallback: it agrees with the storage-mode predicate SharedStore used
// (an empty dolt_mode on a localhost host is embedded and has no server to
// ask), and it also honors shared-server intent from config.yaml, which the
// storage-mode predicate reads only from the environment (codex round 2,
// gp-0i4o: a YAML shared-server store with no local database directory
// otherwise lost the diagnostic).
func TestForkLineageStoreHasServer(t *testing.T) {
	// No inherited server-mode signal: the cases below set their own.
	t.Setenv("BEADS_DOLT_SERVER_MODE", "")
	t.Setenv("BEADS_DOLT_SHARED_SERVER", "")
	t.Setenv("BEADS_DOLT_SERVER_HOST", "")
	t.Setenv("BEADS_DOLT_HOST", "")
	// No user-global config.yaml (dolt.port, dolt.shared-server) may leak in.
	home := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv("XDG_CONFIG_HOME", filepath.Join(home, "xdg"))
	t.Setenv("BEADS_TEST_IGNORE_REPO_CONFIG", "1")

	cases := []struct {
		name string
		cfg  configfile.Config
		yaml string
		env  map[string]string
		want bool
	}{
		{
			name: "embedded default: empty dolt_mode, localhost, no shared-server intent",
			cfg:  configfile.Config{Backend: configfile.BackendDolt, DoltDatabase: "beads_test"},
			want: false,
		},
		{
			name: "metadata.json dolt_mode=server",
			cfg:  configfile.Config{Backend: configfile.BackendDolt, DoltDatabase: "beads_test", DoltMode: configfile.DoltModeServer},
			want: true,
		},
		{
			name: "BEADS_DOLT_SHARED_SERVER=1 (the storage predicate's own shared-server leg)",
			cfg:  configfile.Config{Backend: configfile.BackendDolt, DoltDatabase: "beads_test"},
			env:  map[string]string{"BEADS_DOLT_SHARED_SERVER": "1"},
			want: true,
		},
		{
			name: "config.yaml dolt.shared-server: true (read by bd's open path, not by the storage predicate)",
			cfg:  configfile.Config{Backend: configfile.BackendDolt, DoltDatabase: "beads_test"},
			yaml: "dolt:\n  shared-server: true\n",
			want: true,
		},
		{
			name: "config.yaml dolt.mode: server",
			cfg:  configfile.Config{Backend: configfile.BackendDolt, DoltDatabase: "beads_test"},
			yaml: "dolt:\n  mode: server\n",
			want: true,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			beadsDir := filepath.Join(t.TempDir(), ".beads")
			if err := os.MkdirAll(beadsDir, 0o755); err != nil {
				t.Fatal(err)
			}
			cfg := tc.cfg
			if err := cfg.Save(beadsDir); err != nil {
				t.Fatalf("save metadata.json: %v", err)
			}
			if tc.yaml != "" {
				if err := os.WriteFile(filepath.Join(beadsDir, "config.yaml"), []byte(tc.yaml), 0o644); err != nil {
					t.Fatalf("write config.yaml: %v", err)
				}
			}
			for k, v := range tc.env {
				t.Setenv(k, v)
			}
			// config.yaml is read through the global viper state, the way
			// bd's own open path reads dolt.shared-server.
			t.Setenv("BEADS_DIR", beadsDir)
			config.ResetForTesting()
			if err := config.Initialize(); err != nil {
				t.Fatalf("config.Initialize: %v", err)
			}
			t.Cleanup(config.ResetForTesting)

			if got := forkLineageStoreHasServer(beadsDir); got != tc.want {
				t.Fatalf("forkLineageStoreHasServer = %v, want %v", got, tc.want)
			}
		})
	}
}

// A pre-upmerge ignored cursor is clamped to 11 because leases.granted_node
// does not exist yet. Doctor must still name the August scheme, or report
// the recorded MAX refusal, rather than claim the schema was verified.
func TestCheckForkMigrationLineage_CursorSchemes(t *testing.T) {
	cases := []struct {
		name                                  string
		mainMax, ignoredMax                   int
		oldScheme, mixedScheme, missingEffect bool
		status                                string
		detail                                string
	}{
		{"bd-dn6", 54, 11, true, false, false, StatusWarning, "This database shows the bd-dn6 scheme."},
		{"2026-08 upmerge", 73, 22, false, false, false, StatusWarning, "This database shows the 2026-08 upmerge scheme."},
		{"both schemes", 54, 22, true, true, false, StatusWarning, "This database shows the bd-dn6 and 2026-08 upmerge schemes."},
		{"missing upstream tail effect", 73, 22, false, false, false, StatusError, "Missing: index issues.idx_issues_defer_until (upstream 0052)."},
		{"missing main effect", 73, 22, false, false, true, StatusError, "schema_migrations records fork migrations 70-73 but column comments.external_ref (fork 0072) is missing; schema does not match the recorded cursor"},
		{"unexpected main MAX", 74, 22, false, false, false, StatusError, "schema_migrations row 73 coexists with MAX(version)=74; reconciliation will refuse this cursor"},
		{"unexpected ignored MAX", 73, 28, false, false, false, StatusError, "ignored_schema_migrations row 22 coexists with MAX(version)=28; reconciliation will refuse this cursor"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			db, mock, err := sqlmock.New()
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			query := func(sql string, value any, args ...any) {
				expectation := mock.ExpectQuery(regexp.QuoteMeta(sql))
				if len(args) == 1 {
					expectation.WithArgs(args[0])
				}
				if len(args) == 2 {
					expectation.WithArgs(args[0], args[1])
				}
				expectation.WillReturnRows(sqlmock.NewRows([]string{"value"}).AddRow(value))
			}
			row := func(table string, version, count int) {
				query("SELECT COUNT(*) FROM "+table+" WHERE version = ?", count, version)
			}
			query("SELECT COUNT(*) FROM information_schema.tables", 1, "schema_migrations")
			query("SELECT COALESCE(MAX(version), 0) FROM schema_migrations", tc.mainMax)
			query("SELECT COUNT(*) FROM information_schema.tables", 1, "ignored_schema_migrations")
			query("SELECT COALESCE(MAX(version), 0) FROM ignored_schema_migrations", tc.ignoredMax)
			query("SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES", 1, "wisps")
			query("SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES", 1, "wisp_dependencies")
			query("SELECT COUNT(*) FROM INFORMATION_SCHEMA.COLUMNS", 0, "leases", "granted_node")
			query("SELECT COALESCE(MAX(version), 0) FROM ignored_schema_migrations", tc.ignoredMax)
			row("schema_migrations", 54, 1)
			if tc.mixedScheme {
				row("ignored_schema_migrations", 11, 0)
			} else {
				row("ignored_schema_migrations", 11, 1)
			}
			query("SELECT COUNT(*) FROM INFORMATION_SCHEMA.COLUMNS", 0, "issues", "lease_expires_at")
			if tc.oldScheme {
				row("schema_migrations", 55, 0)
				row("schema_migrations", 73, 0)
				if tc.mixedScheme {
					row("ignored_schema_migrations", 22, 1)
					row("ignored_schema_migrations", 14, 0)
				} else {
					row("ignored_schema_migrations", 22, 0)
				}
			} else {
				row("schema_migrations", 55, 1)
				row("schema_migrations", 73, 1)
				row("schema_migrations", 56, 0)
				row("ignored_schema_migrations", 22, 1)
				row("ignored_schema_migrations", 14, 0)
			}

			if tc.status == StatusWarning || tc.missingEffect || tc.name == "missing upstream tail effect" {
				query("SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES", 1, "linear_label_snapshots")
				externalRef := 1
				if tc.missingEffect {
					externalRef = 0
				}
				query("SELECT COUNT(*) FROM INFORMATION_SCHEMA.COLUMNS", externalRef, "comments", "external_ref")
				query("SELECT COUNT(*) FROM INFORMATION_SCHEMA.COLUMNS", 1, "comments", "updated_at")
				query("SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES", 1, "attachments")
				if !tc.oldScheme {
					for _, v := range []int{70, 71, 72, 73} {
						row("schema_migrations", v, 1)
					}
					query("SELECT COUNT(*) FROM INFORMATION_SCHEMA.STATISTICS", 1, "comments", "idx_comments_external_ref")
					query("SELECT COUNT(*) FROM INFORMATION_SCHEMA.STATISTICS", 1, "issues", "idx_issues_status_updated_at")
					deferIndex := 1
					if tc.name == "missing upstream tail effect" {
						deferIndex = 0
					}
					query("SELECT COUNT(*) FROM INFORMATION_SCHEMA.STATISTICS", deferIndex, "issues", "idx_issues_defer_until")
					query("SELECT COLUMN_DEFAULT FROM INFORMATION_SCHEMA.COLUMNS", nil, "events", "id")
					query("SELECT COLUMN_DEFAULT FROM INFORMATION_SCHEMA.COLUMNS", nil, "comments", "id")
				}
				query("SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES", 1, "linear_issue_snapshots")
				query("SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES", 1, "linear_project_snapshots")
				if !tc.oldScheme || tc.mixedScheme {
					query("SELECT COUNT(*) FROM INFORMATION_SCHEMA.COLUMNS", 1, "wisp_comments", "external_ref")
					row("ignored_schema_migrations", 20, 1)
					row("ignored_schema_migrations", 21, 1)
					query("SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES", 1, "linear_issue_snapshots")
					query("SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES", 1, "linear_project_snapshots")
					query("SELECT COLUMN_DEFAULT FROM INFORMATION_SCHEMA.COLUMNS", nil, "wisp_events", "id")
				}
			}

			check := checkForkMigrationLineage(context.Background(), db)
			if check.Status != tc.status {
				t.Fatalf("Status = %q, want %q; check: %+v", check.Status, tc.status, check)
			}
			if !strings.Contains(check.Detail, tc.detail) {
				t.Fatalf("Detail = %q, want %q", check.Detail, tc.detail)
			}
			if strings.Contains(check.Message, "verified") {
				t.Fatalf("false verification: %s", check.Message)
			}
			if tc.status == StatusWarning {
				for _, text := range []string{"bd-dn6 (fork 0051-0054", "ignored 0010-0011 → 0020-0021 before the upmerge, now 0025-0026", "2026-08 upmerge (main 0070-0073", "ignored 0020-0022"} {
					if !strings.Contains(check.Detail, text) {
						t.Errorf("Detail does not contain %q: %s", text, check.Detail)
					}
				}
				// At recorded 11 the replay floor leaves the reading unchanged;
				// only a higher recorded cursor needs the clone-local mismatch note.
				if got := strings.Contains(check.Detail, "clone-local"); got != (tc.ignoredMax > 11) {
					t.Errorf("clone-local cursor note present = %t for recorded ignored cursor %d", got, tc.ignoredMax)
				}
				if check.Fix != "Run any bd write command (e.g. `bd migrate`) with the new binary to reconcile." {
					t.Fatalf("unexpected Fix: %s", check.Fix)
				}
			}
			if err := mock.ExpectationsWereMet(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

// Reconciliation of the ignored chain cannot repair drift in the merged main.
func TestCheckForkMigrationLineage_MixedChainMissingMainEffect(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	query := func(sql string, value any, args ...any) {
		expectation := mock.ExpectQuery(regexp.QuoteMeta(sql))
		if len(args) == 1 {
			expectation.WithArgs(args[0])
		}
		if len(args) == 2 {
			expectation.WithArgs(args[0], args[1])
		}
		expectation.WillReturnRows(sqlmock.NewRows([]string{"value"}).AddRow(value))
	}
	row := func(table string, version, count int) {
		query("SELECT COUNT(*) FROM "+table+" WHERE version = ?", count, version)
	}
	query("SELECT COUNT(*) FROM information_schema.tables", 1, "schema_migrations")
	query("SELECT COALESCE(MAX(version), 0) FROM schema_migrations", 73)
	query("SELECT COUNT(*) FROM information_schema.tables", 1, "ignored_schema_migrations")
	query("SELECT COALESCE(MAX(version), 0) FROM ignored_schema_migrations", 22)
	query("SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES", 1, "wisps")
	query("SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES", 1, "wisp_dependencies")
	query("SELECT COUNT(*) FROM INFORMATION_SCHEMA.COLUMNS", 0, "leases", "granted_node")
	query("SELECT COALESCE(MAX(version), 0) FROM ignored_schema_migrations", 22)
	row("schema_migrations", 54, 1)
	row("ignored_schema_migrations", 11, 1)
	query("SELECT COUNT(*) FROM INFORMATION_SCHEMA.COLUMNS", 0, "issues", "lease_expires_at")
	row("schema_migrations", 55, 1)
	row("schema_migrations", 73, 1)
	row("schema_migrations", 56, 1)
	row("ignored_schema_migrations", 22, 1)
	row("ignored_schema_migrations", 14, 0)
	query("SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES", 1, "linear_issue_snapshots")
	query("SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES", 1, "linear_project_snapshots")
	query("SELECT COUNT(*) FROM INFORMATION_SCHEMA.COLUMNS", 1, "wisp_comments", "external_ref")
	row("ignored_schema_migrations", 20, 1)
	row("ignored_schema_migrations", 21, 1)
	query("SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES", 1, "linear_issue_snapshots")
	query("SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES", 1, "linear_project_snapshots")
	query("SELECT COLUMN_DEFAULT FROM INFORMATION_SCHEMA.COLUMNS", nil, "wisp_events", "id")
	for _, version := range []int{70, 71, 72, 73} {
		row("schema_migrations", version, 1)
	}
	query("SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES", 1, "linear_label_snapshots")
	query("SELECT COUNT(*) FROM INFORMATION_SCHEMA.COLUMNS", 0, "comments", "external_ref")
	query("SELECT COUNT(*) FROM INFORMATION_SCHEMA.COLUMNS", 1, "comments", "updated_at")
	query("SELECT COUNT(*) FROM INFORMATION_SCHEMA.STATISTICS", 1, "comments", "idx_comments_external_ref")
	query("SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES", 1, "attachments")
	query("SELECT COUNT(*) FROM INFORMATION_SCHEMA.STATISTICS", 1, "issues", "idx_issues_status_updated_at")
	query("SELECT COUNT(*) FROM INFORMATION_SCHEMA.STATISTICS", 1, "issues", "idx_issues_defer_until")
	query("SELECT COLUMN_DEFAULT FROM INFORMATION_SCHEMA.COLUMNS", nil, "events", "id")
	query("SELECT COLUMN_DEFAULT FROM INFORMATION_SCHEMA.COLUMNS", nil, "comments", "id")

	report, check := classifyForkMigrationLineage(context.Background(), db)
	if check.Status != StatusError || !strings.Contains(check.Detail, "Missing: column comments.external_ref (fork 0072).") {
		t.Fatalf("check = %+v; want error naming the missing merged-main column", check)
	}
	if len(report.PreMergeSchemes) != 1 || report.PreMergeSchemes[0] != "2026-08 upmerge" {
		t.Fatalf("report = %+v; want the pre-upmerge ignored scheme retained", report)
	}
	if strings.Contains(check.Detail, "next bd write command") || strings.Contains(check.Message, "verified") {
		t.Fatalf("check promises reconciliation of a missing merged-main effect: %+v", check)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}
