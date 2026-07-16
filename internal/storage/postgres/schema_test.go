package postgres

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/steveyegge/beads/internal/storage/pgdialect"
)

// TestInitSchema applies the embedded DDL into a throwaway schema on a live
// Postgres and asserts the core tables materialize. It is gated on
// BEADS_PG_TEST_URL (a pgx-parseable DSN) and skips otherwise.
func TestInitSchema(t *testing.T) {
	url := os.Getenv("BEADS_PG_TEST_URL")
	if url == "" {
		t.Skip("BEADS_PG_TEST_URL not set; skipping Postgres schema test")
	}

	schema := fmt.Sprintf("bd_schema_test_%d", time.Now().UnixNano())

	// DDL runs over a RAW (non-translating) DB; assertions below use ? bindings
	// and so need the translating DB.
	raw, err := pgdialect.OpenRaw(url, schema)
	if err != nil {
		t.Fatalf("openraw: %v", err)
	}
	db, err := pgdialect.Open(url, schema)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	// Registered first so they run last (cleanups are LIFO): the schema drop
	// below still has open connections when it runs.
	t.Cleanup(func() { _ = raw.Close() })
	t.Cleanup(func() { _ = db.Close() })

	ctx := context.Background()

	if err := InitSchema(ctx, raw, schema); err != nil {
		t.Fatalf("InitSchema: %v", err)
	}
	t.Cleanup(func() {
		if _, err := db.ExecContext(ctx, `DROP SCHEMA IF EXISTS "`+schema+`" CASCADE`); err != nil {
			t.Errorf("drop schema %q: %v", schema, err)
		}
	})

	for _, table := range []string{"issues", "dependencies"} {
		if !tableExists(ctx, t, db, schema, table) {
			t.Errorf("expected table %q to exist in schema %q", table, schema)
		}
	}
}

func tableExists(ctx context.Context, t *testing.T, db *sql.DB, schema, table string) bool {
	t.Helper()
	var count int
	err := db.QueryRowContext(ctx,
		`SELECT count(*) FROM information_schema.tables WHERE table_schema = ? AND table_name = ?`,
		schema, table).Scan(&count)
	if err != nil {
		t.Fatalf("query information_schema.tables for %q: %v", table, err)
	}
	return count > 0
}

// TestSchemaAdvisoryLockKey pins the advisory-lock key derivation. The key is a
// cross-process contract: every bd binary that opens the same workspace schema
// must derive the same key, or their InitSchema calls stop serializing against
// each other. The golden values fail loudly if the hash, prefix, or truncation
// ever changes — such a change would silently break lock interop with
// already-deployed binaries during a rolling upgrade.
func TestSchemaAdvisoryLockKey(t *testing.T) {
	golden := map[string]int64{
		"beads":  -7207352757236036547,
		"public": -3420031902527778135,
	}
	for schema, want := range golden {
		if got := schemaAdvisoryLockKey(schema); got != want {
			t.Errorf("schemaAdvisoryLockKey(%q) = %d, want %d — changing the derivation breaks locking against deployed binaries", schema, got, want)
		}
	}
	if a, b := schemaAdvisoryLockKey("workspace_a"), schemaAdvisoryLockKey("workspace_b"); a == b {
		t.Errorf("distinct schemas derived the same advisory-lock key %d; per-workspace inits must not serialize against each other", a)
	}
}

// TestInitSchemaConcurrentOpens is the regression test for the catalog race the
// advisory lock fixes: InitSchema runs on EVERY open and re-applies idempotent
// DDL, and bd is a short-lived CLI, so a busy deployment runs many concurrent
// opens. Without per-schema serialization those opens rewrite the same pg
// catalog tuples and fail with "tuple concurrently updated" (SQLSTATE XX000).
// Each worker gets its own *sql.DB pool — its own backend session — to mimic
// separate bd processes, and all hammer InitSchema on the same fresh schema.
// Gated on BEADS_PG_TEST_URL like TestInitSchema.
func TestInitSchemaConcurrentOpens(t *testing.T) {
	url := os.Getenv("BEADS_PG_TEST_URL")
	if url == "" {
		t.Skip("BEADS_PG_TEST_URL not set; skipping Postgres schema test")
	}

	schema := fmt.Sprintf("bd_schema_race_%d", time.Now().UnixNano())
	ctx := context.Background()

	const workers = 8
	const opensPerWorker = 3

	dbs := make([]*sql.DB, workers)
	for i := range dbs {
		raw, err := pgdialect.OpenRaw(url, schema)
		if err != nil {
			t.Fatalf("openraw %d: %v", i, err)
		}
		dbs[i] = raw
	}
	// Registered first so it runs last (cleanups are LIFO): the schema drop
	// below still has open pools when it runs.
	t.Cleanup(func() {
		for _, db := range dbs {
			_ = db.Close()
		}
	})
	t.Cleanup(func() {
		if _, err := dbs[0].ExecContext(ctx, `DROP SCHEMA IF EXISTS "`+schema+`" CASCADE`); err != nil {
			t.Errorf("drop schema %q: %v", schema, err)
		}
	})

	start := make(chan struct{})
	errs := make(chan error, workers)
	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func(db *sql.DB) {
			defer wg.Done()
			<-start
			for j := 0; j < opensPerWorker; j++ {
				if err := InitSchema(ctx, db, schema); err != nil {
					errs <- err
					return
				}
			}
		}(dbs[i])
	}
	close(start)
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Errorf("concurrent InitSchema: %v", err)
	}
}
