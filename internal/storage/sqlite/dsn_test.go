package sqlite

import (
	"database/sql"
	"path/filepath"
	"testing"
)

// TestDSNCaseSensitiveLike pins the mechanism behind bd-oyvc2.10: dsn() must carry
// _pragma=case_sensitive_like(1), and modernc.org/sqlite must apply it on EVERY new
// connection (DSN _pragma params are per-connection), so raw-cased LIKE matches the
// other backends' case-sensitive collations. The behavioral contract is covered by
// the conformance corpus (Audit/*CaseSensitive); this guards the DSN wiring itself.
func TestDSNCaseSensitiveLike(t *testing.T) {
	db, err := sql.Open("sqlite", dsn(filepath.Join(t.TempDir(), "like.db")))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	// Multiple pooled connections, each of which must have the pragma applied.
	db.SetMaxOpenConns(4)
	for i := 0; i < 4; i++ {
		var v int
		if err := db.QueryRow("SELECT 'a' LIKE 'A'").Scan(&v); err != nil {
			t.Fatal(err)
		}
		if v != 0 {
			t.Fatalf("'a' LIKE 'A' = %d, want 0 (case-sensitive; SQLite default is ASCII-case-insensitive)", v)
		}
	}
}
