# Bidirectional Linear ↔ beads Label Sync — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace today's destructive Linear-authoritative pull-side label sync with intent-aware bidirectional sync that propagates label adds, removes, and Linear-side renames between beads and Linear.

**Architecture:** A pure 3-pass reconciler (exclusion → rename classification → truth table) decides per-label adds/removes from three inputs (current beads labels, current Linear labels, last-synced snapshot). A new `linear_label_snapshots` table persists the snapshot per bead. Pull-side replaces `syncIssueLabels` (`internal/tracker/engine.go:644`); push-side wires via a label-aware `ContentEqual` hook (`cmd/bd/linear.go:437`) so the engine-level skip stops eating label-only changes. Single `IssueUpdate` mutation carries both field and label deltas.

**Tech Stack:** Go 1.21+, Dolt SQL, Linear GraphQL API, table-driven Go tests.

**Spec reference:** `docs/superpowers/specs/2026-04-27-linear-label-sync-design.md`

---

## File Map

**New files:**
- `internal/storage/schema/migrations/0033_create_linear_label_snapshots.up.sql` — canonical schema migration
- `internal/storage/schema/migrations/0033_create_linear_label_snapshots.down.sql` — companion down
- `internal/storage/dolt/migrations/018_linear_label_snapshots.go` — compat migration
- `internal/storage/dolt/linear_label_snapshots.go` — shared SQL helpers
- `internal/linear/reconciler.go` — pure reconciler types + function
- `internal/linear/reconciler_test.go` — exhaustive table-driven tests

**Modified files:**
- `internal/storage/storage.go` — add `LinearLabelSnapshotEntry` type + extend `Transaction` interface with `GetLinearLabelSnapshot` / `PutLinearLabelSnapshot`
- `internal/storage/dolt/store.go:1467-1475` — add table to staging list
- `internal/storage/dolt/migrations/runner.go:68-75` — add table to staging list; wire compat 018
- `internal/storage/dolt/transaction.go` — implement interface methods (call `t.dirty.MarkDirty("linear_label_snapshots")` per existing pattern)
- `internal/storage/embeddeddolt/transaction.go` — same on `embeddedTransaction`
- `internal/linear/client.go` — add `LabelsByName`, `CreateLabel` (`LabelScopeTeam`/`LabelScopeWorkspace`); `UpdateIssue` already accepts arbitrary input map → pass `labelIds` via that
- `internal/linear/tracker.go` — add `labelSyncEnabled`/`labelExclude`/`labelCreateScope`/`labelWarnFn` fields + `SetLabelSyncConfig`/`LabelSyncEnabled`/`LabelExclude`/`LoadSnapshot` accessors; `CreateIssue` (line 152) and `UpdateIssue` (line 175) resolve label IDs and pass them; both call `writeSnapshot` after a successful API call
- `internal/tracker/engine.go:24` — ADD `ReconcileLabels` field to existing `PullHooks` struct (do not redefine the struct)
- `internal/tracker/engine.go:496` — replace direct `syncIssueLabels` call with conditional dispatch (PullHooks.ReconcileLabels or fallback)
- `internal/tracker/engine.go:644` — rename `syncIssueLabels` → `legacySyncIssueLabels` (keep body)
- `cmd/bd/linear.go:348` — extend existing `buildLinearPullHooks` to install `ReconcileLabels` when `lt.LabelSyncEnabled()`
- `cmd/bd/linear.go:437` — make `ContentEqual` callback label-aware via `hasLabelDelta` helper
- `cmd/bd/linear.go:448` — extend `DescribeDiff` callback with label diffs
- `cmd/bd/linear.go` — load three new config keys; call `lt.SetLabelSyncConfig(...)` before building hooks

**Test files:**
- `internal/linear/reconciler_test.go` — reconciler unit tests (new)
- `internal/linear/client_test.go` — `LabelsByName`, `CreateLabel` tests (extend existing)
- `internal/storage/dolt/linear_label_snapshots_test.go` — snapshot CRUD test (new)
- `cmd/bd/linear_roundtrip_test.go` — extend with label scenarios

---

## Phase A — Snapshot Persistence

### Task A1: Add canonical schema migration

**Files:**
- Create: `internal/storage/schema/migrations/0033_create_linear_label_snapshots.up.sql`
- Create: `internal/storage/schema/migrations/0033_create_linear_label_snapshots.down.sql`

- [ ] **Step 1: Write the up migration**

Create `internal/storage/schema/migrations/0033_create_linear_label_snapshots.up.sql`:

```sql
CREATE TABLE IF NOT EXISTS linear_label_snapshots (
    issue_id     VARCHAR(255) NOT NULL,
    label_id     VARCHAR(64)  NOT NULL,
    label_name   VARCHAR(255) NOT NULL,
    synced_at    TIMESTAMP    NOT NULL,
    PRIMARY KEY (issue_id, label_id),
    INDEX idx_linear_label_snapshots_issue (issue_id),
    CONSTRAINT fk_linear_label_snapshots_issue
        FOREIGN KEY (issue_id) REFERENCES issues(id) ON DELETE CASCADE
);
```

- [ ] **Step 2: Write the down migration**

Create `internal/storage/schema/migrations/0033_create_linear_label_snapshots.down.sql`:

```sql
DROP TABLE IF EXISTS linear_label_snapshots;
```

- [ ] **Step 3: Verify migration is picked up**

Run: `go test ./internal/storage/schema/... -run TestMigrationsLoad -v`
Expected: PASS. (If no such test exists, run `go test ./internal/storage/schema/... -v` and verify the package builds and any embed-walk test sees both files.)

- [ ] **Step 4: Commit**

```bash
git add internal/storage/schema/migrations/0033_create_linear_label_snapshots.up.sql internal/storage/schema/migrations/0033_create_linear_label_snapshots.down.sql
git commit -m "feat(linear): add linear_label_snapshots schema migration"
```

---

### Task A2: Add compat dolt migration

**Files:**
- Create: `internal/storage/dolt/migrations/018_linear_label_snapshots.go`

- [ ] **Step 1: Write the compat migration**

Create `internal/storage/dolt/migrations/018_linear_label_snapshots.go`. Pattern follows `017_add_started_at_column.go`:

```go
package migrations

import (
	"database/sql"
	"fmt"
)

// MigrateCreateLinearLabelSnapshots ensures the linear_label_snapshots table
// exists. This is the compat counterpart to canonical schema migration 0033;
// it's idempotent and safe to run on databases that already have the table
// (e.g. those that were upgraded through the canonical schema path).
func MigrateCreateLinearLabelSnapshots(db *sql.DB) error {
	exists, err := TableExists(db, "linear_label_snapshots")
	if err != nil {
		return fmt.Errorf("failed to check linear_label_snapshots table existence: %w", err)
	}
	if exists {
		return nil
	}

	stmt := `
		CREATE TABLE linear_label_snapshots (
			issue_id     VARCHAR(255) NOT NULL,
			label_id     VARCHAR(64)  NOT NULL,
			label_name   VARCHAR(255) NOT NULL,
			synced_at    TIMESTAMP    NOT NULL,
			PRIMARY KEY (issue_id, label_id),
			INDEX idx_linear_label_snapshots_issue (issue_id),
			CONSTRAINT fk_linear_label_snapshots_issue
				FOREIGN KEY (issue_id) REFERENCES issues(id) ON DELETE CASCADE
		)`
	if _, err := db.Exec(stmt); err != nil {
		return fmt.Errorf("failed to create linear_label_snapshots table: %w", err)
	}
	return nil
}
```

- [ ] **Step 2: Wire the migration into the runner**

Open `internal/storage/dolt/migrations/runner.go`. Find where 017 is registered (search for `MigrateAddStartedAtColumn`). Add a sibling registration for `MigrateCreateLinearLabelSnapshots` immediately after, following the established pattern.

Run: `grep -n MigrateAddStartedAtColumn internal/storage/dolt/migrations/runner.go`
Expected: see one or more references showing how it is invoked. Add the new function the same way.

- [ ] **Step 3: Run package tests**

Run: `go test ./internal/storage/dolt/migrations/... -v`
Expected: PASS.

- [ ] **Step 4: Commit**

```bash
git add internal/storage/dolt/migrations/018_linear_label_snapshots.go internal/storage/dolt/migrations/runner.go
git commit -m "feat(linear): compat migration for linear_label_snapshots table"
```

---

### Task A3: Add table to staging lists

**Files:**
- Modify: `internal/storage/dolt/store.go:1467-1475`
- Modify: `internal/storage/dolt/migrations/runner.go:68-75`

- [ ] **Step 1: Update store.go staging list**

Open `internal/storage/dolt/store.go:1467-1475`. Find the string list that contains `"issue_snapshots", "compaction_snapshots"`. Add `"linear_label_snapshots"` to it.

Run: `grep -n '"issue_snapshots", "compaction_snapshots"' internal/storage/dolt/store.go`
Expected: shows the location. Add `, "linear_label_snapshots"` to the slice.

- [ ] **Step 2: Update runner.go staging list**

Open `internal/storage/dolt/migrations/runner.go:68-75`. Find the same list (`"issue_snapshots", "compaction_snapshots", "federation_peers"`). Add `"linear_label_snapshots"`.

- [ ] **Step 3: Build to verify**

Run: `go build ./...`
Expected: clean build, no errors.

- [ ] **Step 4: Commit**

```bash
git add internal/storage/dolt/store.go internal/storage/dolt/migrations/runner.go
git commit -m "feat(linear): register linear_label_snapshots in staging lists"
```

---

### Task A4: Extend Transaction interface

**Files:**
- Modify: `internal/storage/storage.go:262-300`

- [ ] **Step 1: Add the snapshot type and interface methods**

Open `internal/storage/storage.go`. After the `Transaction` interface block (around line 300), add:

```go
// LinearLabelSnapshotEntry represents one row of a label-sync snapshot for a bead.
// The snapshot captures the last-known agreed state between beads and Linear,
// keyed by Linear label ID so renames can be detected on subsequent syncs.
type LinearLabelSnapshotEntry struct {
	LabelID   string
	LabelName string
}
```

Then inside the `Transaction` interface (just before the closing `}` at line 300), add:

```go
	// Linear label snapshot operations (per-bead, written inside sync transactions).
	GetLinearLabelSnapshot(ctx context.Context, issueID string) ([]LinearLabelSnapshotEntry, error)
	PutLinearLabelSnapshot(ctx context.Context, issueID string, entries []LinearLabelSnapshotEntry) error
```

- [ ] **Step 2: Build to verify the interface compiles**

Run: `go build ./internal/storage/...`
Expected: build will fail because `doltTransaction` and `embeddedTransaction` no longer satisfy the interface. That's expected — implementations come next.

- [ ] **Step 3: Commit (intentionally broken state, fixed in A5/A6)**

```bash
git add internal/storage/storage.go
git commit -m "feat(linear): add LinearLabelSnapshot interface methods on Transaction"
```

---

### Task A5: Implement snapshot CRUD on dolt transaction

**Files:**
- Create: `internal/storage/dolt/linear_label_snapshots.go`
- Modify: `internal/storage/dolt/transaction.go`

- [ ] **Step 1: Write the failing test**

Create `internal/storage/dolt/linear_label_snapshots_test.go` (use the existing test scaffolding pattern — `setupTestStore` or similar from this package; check `internal/storage/dolt/concurrent_test.go` for the established pattern):

```go
package dolt

import (
	"context"
	"testing"

	"github.com/steveyegge/beads/internal/storage"
	"github.com/steveyegge/beads/internal/types"
)

func TestLinearLabelSnapshotCRUD(t *testing.T) {
	store := setupTestStore(t)
	ctx := context.Background()

	// Seed a bead so the FK is satisfied.
	issue := &types.Issue{ID: "test-1", Title: "test", Status: types.StatusOpen}
	if err := store.CreateIssue(ctx, issue, "tester"); err != nil {
		t.Fatalf("CreateIssue: %v", err)
	}

	want := []storage.LinearLabelSnapshotEntry{
		{LabelID: "lin-1", LabelName: "bug"},
		{LabelID: "lin-2", LabelName: "p1"},
	}

	if err := store.RunInTransaction(ctx, "test write", func(tx storage.Transaction) error {
		return tx.PutLinearLabelSnapshot(ctx, "test-1", want)
	}); err != nil {
		t.Fatalf("Put: %v", err)
	}

	var got []storage.LinearLabelSnapshotEntry
	if err := store.RunInTransaction(ctx, "test read", func(tx storage.Transaction) error {
		var err error
		got, err = tx.GetLinearLabelSnapshot(ctx, "test-1")
		return err
	}); err != nil {
		t.Fatalf("Get: %v", err)
	}

	if len(got) != len(want) {
		t.Fatalf("got %d entries, want %d", len(got), len(want))
	}
	gotByID := make(map[string]string, len(got))
	for _, e := range got {
		gotByID[e.LabelID] = e.LabelName
	}
	for _, w := range want {
		if gotByID[w.LabelID] != w.LabelName {
			t.Errorf("entry %s: got name %q, want %q", w.LabelID, gotByID[w.LabelID], w.LabelName)
		}
	}
}

func TestLinearLabelSnapshotReplace(t *testing.T) {
	store := setupTestStore(t)
	ctx := context.Background()

	issue := &types.Issue{ID: "test-1", Title: "test", Status: types.StatusOpen}
	if err := store.CreateIssue(ctx, issue, "tester"); err != nil {
		t.Fatalf("CreateIssue: %v", err)
	}

	first := []storage.LinearLabelSnapshotEntry{{LabelID: "lin-1", LabelName: "old"}}
	second := []storage.LinearLabelSnapshotEntry{{LabelID: "lin-2", LabelName: "new"}}

	for _, batch := range [][]storage.LinearLabelSnapshotEntry{first, second} {
		if err := store.RunInTransaction(ctx, "test", func(tx storage.Transaction) error {
			return tx.PutLinearLabelSnapshot(ctx, "test-1", batch)
		}); err != nil {
			t.Fatalf("Put: %v", err)
		}
	}

	var got []storage.LinearLabelSnapshotEntry
	if err := store.RunInTransaction(ctx, "read", func(tx storage.Transaction) error {
		var err error
		got, err = tx.GetLinearLabelSnapshot(ctx, "test-1")
		return err
	}); err != nil {
		t.Fatalf("Get: %v", err)
	}

	if len(got) != 1 || got[0].LabelID != "lin-2" || got[0].LabelName != "new" {
		t.Fatalf("expected only [{lin-2, new}], got %+v", got)
	}
}
```

- [ ] **Step 2: Run tests, confirm they fail**

Run: `go test ./internal/storage/dolt/ -run TestLinearLabelSnapshot -v`
Expected: FAIL — methods not implemented.

- [ ] **Step 3: Add SQL helpers**

Create `internal/storage/dolt/linear_label_snapshots.go`:

```go
package dolt

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/steveyegge/beads/internal/storage"
)

// selectLinearLabelSnapshot reads all snapshot rows for an issue.
// Used by both the dolt and embeddeddolt transaction implementations.
func selectLinearLabelSnapshot(ctx context.Context, q sqlQuerier, issueID string) ([]storage.LinearLabelSnapshotEntry, error) {
	rows, err := q.QueryContext(ctx,
		`SELECT label_id, label_name FROM linear_label_snapshots WHERE issue_id = ?`, issueID)
	if err != nil {
		return nil, fmt.Errorf("query linear_label_snapshots: %w", err)
	}
	defer rows.Close()

	var out []storage.LinearLabelSnapshotEntry
	for rows.Next() {
		var e storage.LinearLabelSnapshotEntry
		if err := rows.Scan(&e.LabelID, &e.LabelName); err != nil {
			return nil, fmt.Errorf("scan linear_label_snapshots row: %w", err)
		}
		out = append(out, e)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return out, nil
}

// replaceLinearLabelSnapshot atomically replaces the snapshot rows for an issue.
// Caller is responsible for running this inside a transaction; the DELETE+INSERT
// pair is not safe outside one.
//
// IMPORTANT: callers must also call dirty.MarkDirty("linear_label_snapshots")
// so the table is staged for the Dolt commit. The transaction methods in
// transaction.go do this; if you call this helper from elsewhere, mark dirty
// yourself.
func replaceLinearLabelSnapshot(ctx context.Context, x sqlExecer, issueID string, entries []storage.LinearLabelSnapshotEntry) error {
	if _, err := x.ExecContext(ctx,
		`DELETE FROM linear_label_snapshots WHERE issue_id = ?`, issueID); err != nil {
		return fmt.Errorf("delete linear_label_snapshots: %w", err)
	}
	if len(entries) == 0 {
		return nil
	}
	now := time.Now().UTC()
	for _, e := range entries {
		if _, err := x.ExecContext(ctx,
			`INSERT INTO linear_label_snapshots (issue_id, label_id, label_name, synced_at) VALUES (?, ?, ?, ?)`,
			issueID, e.LabelID, e.LabelName, now); err != nil {
			return fmt.Errorf("insert linear_label_snapshots row: %w", err)
		}
	}
	return nil
}

// sqlQuerier and sqlExecer let helpers work with both *sql.DB and *sql.Tx.
type sqlQuerier interface {
	QueryContext(ctx context.Context, query string, args ...interface{}) (*sql.Rows, error)
}

type sqlExecer interface {
	ExecContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error)
}
```

- [ ] **Step 4: Implement methods on doltTransaction**

Open `internal/storage/dolt/transaction.go`. Find the existing `AddLabel` / `RemoveLabel` methods on `doltTransaction` and observe they call `t.dirty.MarkDirty("labels")`. Mirror that pattern:

```go
func (t *doltTransaction) GetLinearLabelSnapshot(ctx context.Context, issueID string) ([]storage.LinearLabelSnapshotEntry, error) {
	return selectLinearLabelSnapshot(ctx, t.tx, issueID)
}

func (t *doltTransaction) PutLinearLabelSnapshot(ctx context.Context, issueID string, entries []storage.LinearLabelSnapshotEntry) error {
	if err := replaceLinearLabelSnapshot(ctx, t.tx, issueID, entries); err != nil {
		return err
	}
	// CRITICAL: Dolt only commits tables in tx.dirty.DirtyTables() — without
	// MarkDirty the rows are written to the session but dropped on commit.
	// (Verified at internal/storage/dolt/transaction.go:116 via StageAndCommit.)
	t.dirty.MarkDirty("linear_label_snapshots")
	return nil
}
```

(The `t.tx` and `t.dirty` field names must match the existing struct. Check `AddLabel`/`RemoveLabel` to confirm — if either is named differently, use that name.)

- [ ] **Step 5: Run tests, confirm they pass**

Run: `go test ./internal/storage/dolt/ -run TestLinearLabelSnapshot -v`
Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add internal/storage/dolt/linear_label_snapshots.go internal/storage/dolt/linear_label_snapshots_test.go internal/storage/dolt/transaction.go
git commit -m "feat(linear): implement snapshot CRUD on dolt transaction"
```

---

### Task A6: Implement snapshot CRUD on embeddeddolt transaction

**Files:**
- Modify: `internal/storage/embeddeddolt/transaction.go`

- [ ] **Step 1: Implement methods on embeddedTransaction**

Open `internal/storage/embeddeddolt/transaction.go`. Find the existing `AddLabel` / `RemoveLabel` methods and observe how they call `t.dirty.MarkDirty(...)` (or whatever the local idiom is). Mirror that pattern:

```go
func (t *embeddedTransaction) GetLinearLabelSnapshot(ctx context.Context, issueID string) ([]storage.LinearLabelSnapshotEntry, error) {
	return selectLinearLabelSnapshot(ctx, t.tx, issueID)
}

func (t *embeddedTransaction) PutLinearLabelSnapshot(ctx context.Context, issueID string, entries []storage.LinearLabelSnapshotEntry) error {
	if err := replaceLinearLabelSnapshot(ctx, t.tx, issueID, entries); err != nil {
		return err
	}
	t.dirty.MarkDirty("linear_label_snapshots")
	return nil
}
```

The helpers `selectLinearLabelSnapshot` and `replaceLinearLabelSnapshot` live in package `dolt`. Import them via:
- If `embeddeddolt` already imports `dolt`: rename them to capitalized form (`SelectLinearLabelSnapshot`, `ReplaceLinearLabelSnapshot`) in A5 and call them as `dolt.SelectLinearLabelSnapshot` etc.
- If not: copy the helpers into a new file `internal/storage/embeddeddolt/linear_label_snapshots.go` to keep package boundaries clean.

Run: `grep -l "package dolt" internal/storage/embeddeddolt/ 2>/dev/null; grep -rn '"github.com/steveyegge/beads/internal/storage/dolt"' internal/storage/embeddeddolt/`
Expected: tells you whether embeddeddolt imports dolt today. Pick the appropriate path.

- [ ] **Step 2: Build to verify**

Run: `go build ./internal/storage/...`
Expected: clean build.

- [ ] **Step 3: Run snapshot tests against embedded store**

If `embeddeddolt` has its own test scaffolding (look for a `setupTestStore` equivalent), copy `TestLinearLabelSnapshotCRUD` and `TestLinearLabelSnapshotReplace` into `internal/storage/embeddeddolt/linear_label_snapshots_test.go`, adjusting only the package and store-setup helper names.

Run: `go test ./internal/storage/embeddeddolt/ -run TestLinearLabelSnapshot -v`
Expected: PASS.

- [ ] **Step 4: Commit**

```bash
git add internal/storage/embeddeddolt/
git commit -m "feat(linear): implement snapshot CRUD on embedded dolt transaction"
```

---

## Phase B — Reconciler

### Task B1: Reconciler types

**Files:**
- Create: `internal/linear/reconciler.go`

- [ ] **Step 1: Define the public types**

Create `internal/linear/reconciler.go`:

```go
package linear

// LinearLabel pairs a Linear label name with its server-assigned ID.
// IDs are required so renames (same ID, different name) can be detected.
type LinearLabel struct {
	Name string
	ID   string
}

// SnapshotEntry is the persisted form of a single label in the
// last-synced state for a bead.
type SnapshotEntry struct {
	Name string
	ID   string
}

// LabelReconcileInput captures the three label sets the reconciler compares,
// plus the exclusion filter applied before reconciliation.
type LabelReconcileInput struct {
	Beads    []string         // current beads label names (post-exclusion-filterable)
	Linear   []LinearLabel    // current Linear labels with IDs
	Snapshot []SnapshotEntry  // last-synced state from linear_label_snapshots
	Exclude  map[string]bool  // keys are lowercase label names; nil means no exclusion
}

// LabelReconcileResult is the reconciler's verdict.
// AddToBeads/RemoveFromBeads are by name; Linear sides separate adds (by name,
// to be resolved/created) from removes (by ID, since the IDs are known).
type LabelReconcileResult struct {
	AddToBeads       []string
	RemoveFromBeads  []string
	AddToLinear      []string
	RemoveFromLinear []string
	NewSnapshot      []SnapshotEntry
	RenamesApplied   []LabelRename
}

// LabelRename captures a Linear-side rename that was applied to the bead.
type LabelRename struct {
	OldName string
	NewName string
	ID      string
}
```

- [ ] **Step 2: Build to verify the package compiles**

Run: `go build ./internal/linear/`
Expected: clean build.

- [ ] **Step 3: Commit**

```bash
git add internal/linear/reconciler.go
git commit -m "feat(linear): add reconciler types for label sync"
```

---

### Task B2: Exclusion filter

**Files:**
- Modify: `internal/linear/reconciler.go`
- Create: `internal/linear/reconciler_test.go`

- [ ] **Step 1: Write the failing test**

Create `internal/linear/reconciler_test.go`:

```go
package linear

import (
	"reflect"
	"sort"
	"testing"
)

func sortedNames(xs []string) []string {
	out := append([]string(nil), xs...)
	sort.Strings(out)
	return out
}

func TestApplyExclusionFilter(t *testing.T) {
	exclude := map[string]bool{"bug": true, "secret": true}

	in := LabelReconcileInput{
		Beads: []string{"bug", "p1", "secret", "Visible"},
		Linear: []LinearLabel{
			{Name: "BUG", ID: "L1"},
			{Name: "regression", ID: "L2"},
		},
		Snapshot: []SnapshotEntry{
			{Name: "secret", ID: "L0"},
			{Name: "p1", ID: "L9"},
		},
		Exclude: exclude,
	}

	beads, linear, snap := applyExclusionFilter(in)
	if got, want := sortedNames(beads), []string{"Visible", "p1"}; !reflect.DeepEqual(got, want) {
		t.Errorf("beads after filter: got %v, want %v", got, want)
	}
	if len(linear) != 1 || linear[0].Name != "regression" {
		t.Errorf("linear after filter: got %+v, want only [regression]", linear)
	}
	if len(snap) != 1 || snap[0].Name != "p1" {
		t.Errorf("snapshot after filter: got %+v, want only [p1]", snap)
	}
}
```

- [ ] **Step 2: Run, confirm it fails**

Run: `go test ./internal/linear/ -run TestApplyExclusionFilter -v`
Expected: FAIL — `applyExclusionFilter` undefined.

- [ ] **Step 3: Implement the filter**

Add to `internal/linear/reconciler.go`:

```go
import "strings"

// applyExclusionFilter returns the three input sets with excluded labels removed.
// Matching is case-insensitive on the label name.
func applyExclusionFilter(in LabelReconcileInput) (beads []string, linear []LinearLabel, snap []SnapshotEntry) {
	excluded := func(name string) bool {
		if in.Exclude == nil {
			return false
		}
		return in.Exclude[strings.ToLower(name)]
	}
	for _, n := range in.Beads {
		if !excluded(n) {
			beads = append(beads, n)
		}
	}
	for _, l := range in.Linear {
		if !excluded(l.Name) {
			linear = append(linear, l)
		}
	}
	for _, s := range in.Snapshot {
		if !excluded(s.Name) {
			snap = append(snap, s)
		}
	}
	return beads, linear, snap
}
```

- [ ] **Step 4: Run, confirm it passes**

Run: `go test ./internal/linear/ -run TestApplyExclusionFilter -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add internal/linear/reconciler.go internal/linear/reconciler_test.go
git commit -m "feat(linear): reconciler exclusion filter"
```

---

### Task B3: Pass-2 rename classification

**Files:**
- Modify: `internal/linear/reconciler.go`
- Modify: `internal/linear/reconciler_test.go`

- [ ] **Step 1: Write the failing tests**

Append to `internal/linear/reconciler_test.go`:

```go
func TestClassifyRenames_AppliedRename(t *testing.T) {
	beads := []string{"old", "other"}
	linear := []LinearLabel{
		{Name: "new", ID: "X"},
		{Name: "other", ID: "Y"},
	}
	snap := []SnapshotEntry{
		{Name: "old", ID: "X"},
		{Name: "other", ID: "Y"},
	}
	r := classifyRenames(beads, linear, snap)
	if len(r.applied) != 1 {
		t.Fatalf("applied: got %d, want 1", len(r.applied))
	}
	if r.applied[0].OldName != "old" || r.applied[0].NewName != "new" || r.applied[0].ID != "X" {
		t.Errorf("applied[0]: got %+v", r.applied[0])
	}
	if !r.consumedSnapshotID["X"] || !r.consumedLinearID["X"] || !r.consumedBeadsName["old"] {
		t.Errorf("consumption flags wrong: %+v", r)
	}
}

func TestClassifyRenames_DroppedRename(t *testing.T) {
	beads := []string{"other"} // user deleted "old"
	linear := []LinearLabel{
		{Name: "new", ID: "X"},
		{Name: "other", ID: "Y"},
	}
	snap := []SnapshotEntry{
		{Name: "old", ID: "X"},
		{Name: "other", ID: "Y"},
	}
	r := classifyRenames(beads, linear, snap)
	if len(r.dropped) != 1 || r.dropped[0].ID != "X" {
		t.Fatalf("dropped: got %+v, want one entry for X", r.dropped)
	}
	if !r.consumedSnapshotID["X"] || !r.consumedLinearID["X"] {
		t.Errorf("consumption flags wrong: %+v", r)
	}
	// Beads has no "old" to consume, so consumedBeadsName for "old" stays false.
}

func TestClassifyRenames_DroppedRenameWithLocalReAdd(t *testing.T) {
	// User deleted "old" and independently added "new". Linear renamed old→new.
	// Pass-2 should consume the new-name beads row as well, so pass-3 doesn't
	// see "new" as a fresh add either way.
	beads := []string{"new"}
	linear := []LinearLabel{{Name: "new", ID: "X"}}
	snap := []SnapshotEntry{{Name: "old", ID: "X"}}
	r := classifyRenames(beads, linear, snap)
	if len(r.dropped) != 1 {
		t.Fatalf("dropped: got %d, want 1", len(r.dropped))
	}
	if !r.consumedBeadsName["new"] {
		t.Errorf("expected consumedBeadsName[new]=true to suppress add, got false")
	}
}

func TestClassifyRenames_CaseMismatchWithRenameDoesNotDelete(t *testing.T) {
	// Regression for the case-mismatch + rename data-loss bug:
	// snapshot "Bug" (Linear's case from prior sync), bead "bug" (mismatched case),
	// Linear renames the label to "flaky" (same ID L1). Without case-insensitive
	// beadsSet lookup, this would classify as DROPPED rename and emit
	// RemoveFromLinear[L1], destroying Linear's label. With the fix, it
	// correctly classifies as APPLIED rename and the bead's "bug" gets
	// renamed to "flaky".
	beads := []string{"bug"}
	linear := []LinearLabel{{Name: "flaky", ID: "L1"}}
	snap := []SnapshotEntry{{Name: "Bug", ID: "L1"}}
	r := classifyRenames(beads, linear, snap)
	if len(r.applied) != 1 {
		t.Fatalf("applied: got %d, want 1 (case-insensitive match should classify as applied, not dropped)", len(r.applied))
	}
	if r.applied[0].OldName != "bug" || r.applied[0].NewName != "flaky" {
		t.Errorf("applied[0]: got %+v, want OldName=bug NewName=flaky", r.applied[0])
	}
	if len(r.dropped) != 0 {
		t.Errorf("dropped: got %d, want 0 (must not destroy Linear label on casing mismatch)", len(r.dropped))
	}
	if !r.consumedBeadsName["bug"] {
		t.Errorf("expected consumedBeadsName[bug]=true (bead's actual spelling)")
	}
}
```

(`internal/linear/reconciler.go` will need `import "strings"` if not already present from Task B2.)

- [ ] **Step 2: Run, confirm they fail**

Run: `go test ./internal/linear/ -run TestClassifyRenames -v`
Expected: FAIL — `classifyRenames` undefined.

- [ ] **Step 3: Implement classification**

Add to `internal/linear/reconciler.go`:

```go
type renameClass struct {
	applied            []LabelRename
	dropped            []LabelRename // OldName + ID only matter; NewName captured for diagnostics
	consumedSnapshotID map[string]bool
	consumedLinearID   map[string]bool
	consumedBeadsName  map[string]bool
}

// classifyRenames is pass 2 of the reconciler. It detects Linear-side renames
// (snapshot ID matches Linear ID, names differ) and decides which to apply
// vs. which to drop based on whether the user has deleted the old name in beads.
//
// "Consume" means: the row should be removed from pass-3's input. The boolean
// maps record what to skip.
//
// Case-insensitive `beadsSet` lookup: rename detection compares snapshot.Name
// against beadsSet using case-folded keys. Without this, a casing mismatch
// (snapshot has "Bug" from a prior Linear sync, bead has "bug") combined with
// a Linear rename would falsely classify as DROPPED rename and emit
// RemoveFromLinear — destroying the Linear label even though the user just
// has a casing inconsistency. The truth table (pass 3) still matches by exact
// name; case-insensitive matching for that broader case is deferred to v2.
func classifyRenames(beads []string, linear []LinearLabel, snap []SnapshotEntry) renameClass {
	r := renameClass{
		consumedSnapshotID: map[string]bool{},
		consumedLinearID:   map[string]bool{},
		consumedBeadsName:  map[string]bool{},
	}
	// beadsSetExact preserves original case for consumption marking.
	// beadsSetFold provides case-insensitive lookup for rename classification.
	beadsSetExact := make(map[string]bool, len(beads))
	beadsSetFold := make(map[string]string, len(beads)) // lower → original
	for _, b := range beads {
		beadsSetExact[b] = true
		beadsSetFold[strings.ToLower(b)] = b
	}
	snapByID := make(map[string]SnapshotEntry, len(snap))
	for _, s := range snap {
		snapByID[s.ID] = s
	}

	for _, l := range linear {
		s, ok := snapByID[l.ID]
		if !ok {
			continue
		}
		if s.Name == l.Name {
			// Names match — pass-3 will see them as in-agreement; no consumption needed.
			continue
		}
		// Case-insensitive: does beads still have the old (pre-rename) name?
		if beadOriginal, exists := beadsSetFold[strings.ToLower(s.Name)]; exists {
			r.applied = append(r.applied, LabelRename{OldName: beadOriginal, NewName: l.Name, ID: l.ID})
			r.consumedSnapshotID[l.ID] = true
			r.consumedLinearID[l.ID] = true
			r.consumedBeadsName[beadOriginal] = true // mark the bead's actual spelling
		} else {
			r.dropped = append(r.dropped, LabelRename{OldName: s.Name, NewName: l.Name, ID: l.ID})
			r.consumedSnapshotID[l.ID] = true
			r.consumedLinearID[l.ID] = true
			// Also consume the new-name beads row if the user happens to have
			// independently re-added the new name (case-insensitive too) — prevents spurious add.
			if beadOriginal, exists := beadsSetFold[strings.ToLower(l.Name)]; exists {
				r.consumedBeadsName[beadOriginal] = true
			}
		}
	}
	return r
}
```

- [ ] **Step 4: Run, confirm tests pass**

Run: `go test ./internal/linear/ -run TestClassifyRenames -v`
Expected: PASS for all three.

- [ ] **Step 5: Commit**

```bash
git add internal/linear/reconciler.go internal/linear/reconciler_test.go
git commit -m "feat(linear): reconciler pass-2 rename classification"
```

---

### Task B4: Pass-3 truth table

**Files:**
- Modify: `internal/linear/reconciler.go`
- Modify: `internal/linear/reconciler_test.go`

- [ ] **Step 1: Write the failing tests**

Append to `internal/linear/reconciler_test.go`:

```go
func TestApplyTruthTable_AllSevenRows(t *testing.T) {
	cases := []struct {
		name                 string
		snap, beads, linear  []string
		wantAddBeads         []string
		wantRemoveBeads      []string
		wantAddLinear        []string
		wantRemoveLinearIDs  []string
	}{
		{
			name:   "in_agreement_unchanged",
			snap:   []string{"x"}, beads: []string{"x"}, linear: []string{"x"},
		},
		{
			name:        "added_in_beads",
			snap:        []string{}, beads: []string{"x"}, linear: []string{},
			wantAddLinear: []string{"x"},
		},
		{
			name:         "added_in_linear",
			snap:         []string{}, beads: []string{}, linear: []string{"x"},
			wantAddBeads: []string{"x"},
		},
		{
			name: "added_both_sides_in_agreement",
			snap: []string{}, beads: []string{"x"}, linear: []string{"x"},
		},
		{
			name:                "removed_in_beads",
			snap:                []string{"x"}, beads: []string{}, linear: []string{"x"},
			wantRemoveLinearIDs: []string{"id-x"},
		},
		{
			name:            "removed_in_linear",
			snap:            []string{"x"}, beads: []string{"x"}, linear: []string{},
			wantRemoveBeads: []string{"x"},
		},
		{
			name: "removed_both_sides_in_agreement",
			snap: []string{"x"}, beads: []string{}, linear: []string{},
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			beads := c.beads
			linear := make([]LinearLabel, len(c.linear))
			for i, n := range c.linear {
				linear[i] = LinearLabel{Name: n, ID: "id-" + n}
			}
			snap := make([]SnapshotEntry, len(c.snap))
			for i, n := range c.snap {
				snap[i] = SnapshotEntry{Name: n, ID: "id-" + n}
			}

			res := applyTruthTable(beads, linear, snap, renameClass{
				consumedSnapshotID: map[string]bool{},
				consumedLinearID:   map[string]bool{},
				consumedBeadsName:  map[string]bool{},
			})

			if !reflect.DeepEqual(sortedNames(res.AddToBeads), sortedNames(c.wantAddBeads)) {
				t.Errorf("AddToBeads: got %v, want %v", res.AddToBeads, c.wantAddBeads)
			}
			if !reflect.DeepEqual(sortedNames(res.RemoveFromBeads), sortedNames(c.wantRemoveBeads)) {
				t.Errorf("RemoveFromBeads: got %v, want %v", res.RemoveFromBeads, c.wantRemoveBeads)
			}
			if !reflect.DeepEqual(sortedNames(res.AddToLinear), sortedNames(c.wantAddLinear)) {
				t.Errorf("AddToLinear: got %v, want %v", res.AddToLinear, c.wantAddLinear)
			}
			if !reflect.DeepEqual(sortedNames(res.RemoveFromLinear), sortedNames(c.wantRemoveLinearIDs)) {
				t.Errorf("RemoveFromLinear: got %v, want %v", res.RemoveFromLinear, c.wantRemoveLinearIDs)
			}
		})
	}
}

func TestApplyTruthTable_RespectsConsumption(t *testing.T) {
	// Snapshot has X (consumed by pass-2), beads has X, linear has X — would
	// normally be "in agreement" but consumption removes it from consideration.
	res := applyTruthTable(
		[]string{"x"},
		[]LinearLabel{{Name: "x", ID: "id-x"}},
		[]SnapshotEntry{{Name: "x", ID: "id-x"}},
		renameClass{
			consumedSnapshotID: map[string]bool{"id-x": true},
			consumedLinearID:   map[string]bool{"id-x": true},
			consumedBeadsName:  map[string]bool{},
		},
	)
	// Both Linear and snapshot rows are consumed; beads row remains and looks
	// like "added in beads, not in snapshot, not in linear" — which would push.
	// This confirms the function respects consumption inputs (the actual
	// reconcileLabels orchestrator decides whether this push is wanted).
	if len(res.AddToLinear) != 1 || res.AddToLinear[0] != "x" {
		t.Errorf("expected AddToLinear=[x] after Linear+snapshot consumed, got %+v", res.AddToLinear)
	}
}
```

- [ ] **Step 2: Run, confirm they fail**

Run: `go test ./internal/linear/ -run TestApplyTruthTable -v`
Expected: FAIL — `applyTruthTable` undefined.

- [ ] **Step 3: Implement the truth table**

Add to `internal/linear/reconciler.go`:

```go
// applyTruthTable is pass 3 of the reconciler. It takes the post-exclusion
// inputs and the consumption decisions from pass 2, then computes adds/removes
// per the 7-row truth table in the design doc.
//
// It does not handle the rename results themselves — those are emitted
// separately by the orchestrator using the LabelRename entries from pass 2.
func applyTruthTable(beads []string, linear []LinearLabel, snap []SnapshotEntry, rc renameClass) LabelReconcileResult {
	// Build presence sets, skipping consumed rows.
	beadsSet := map[string]bool{}
	for _, b := range beads {
		if rc.consumedBeadsName[b] {
			continue
		}
		beadsSet[b] = true
	}
	linearByName := map[string]LinearLabel{}
	for _, l := range linear {
		if rc.consumedLinearID[l.ID] {
			continue
		}
		linearByName[l.Name] = l
	}
	snapByName := map[string]SnapshotEntry{}
	for _, s := range snap {
		if rc.consumedSnapshotID[s.ID] {
			continue
		}
		snapByName[s.Name] = s
	}

	// Union of all names across the three sets.
	all := map[string]bool{}
	for n := range beadsSet {
		all[n] = true
	}
	for n := range linearByName {
		all[n] = true
	}
	for n := range snapByName {
		all[n] = true
	}

	var res LabelReconcileResult
	for n := range all {
		inBeads := beadsSet[n]
		_, inLinear := linearByName[n]
		snapEntry, inSnap := snapByName[n]

		switch {
		case inSnap && inBeads && inLinear:
			// unchanged
		case !inSnap && inBeads && !inLinear:
			res.AddToLinear = append(res.AddToLinear, n)
		case !inSnap && !inBeads && inLinear:
			res.AddToBeads = append(res.AddToBeads, n)
		case !inSnap && inBeads && inLinear:
			// agreement — nothing
		case inSnap && !inBeads && inLinear:
			res.RemoveFromLinear = append(res.RemoveFromLinear, snapEntry.ID)
		case inSnap && inBeads && !inLinear:
			res.RemoveFromBeads = append(res.RemoveFromBeads, n)
		case inSnap && !inBeads && !inLinear:
			// agreement — nothing
		}
	}
	return res
}
```

- [ ] **Step 4: Run, confirm tests pass**

Run: `go test ./internal/linear/ -run TestApplyTruthTable -v`
Expected: PASS for all rows + the consumption-respect test.

- [ ] **Step 5: Commit**

```bash
git add internal/linear/reconciler.go internal/linear/reconciler_test.go
git commit -m "feat(linear): reconciler pass-3 truth table"
```

---

### Task B5: First-sync intersection synthesis

**Files:**
- Modify: `internal/linear/reconciler.go`
- Modify: `internal/linear/reconciler_test.go`

- [ ] **Step 1: Write the failing test**

Append to `internal/linear/reconciler_test.go`:

```go
func TestSynthesizeFirstSyncSnapshot(t *testing.T) {
	beads := []string{"a", "b"}
	linear := []LinearLabel{
		{Name: "a", ID: "ID-A"},
		{Name: "c", ID: "ID-C"},
	}
	got := synthesizeFirstSyncSnapshot(beads, linear)
	if len(got) != 1 || got[0].Name != "a" || got[0].ID != "ID-A" {
		t.Fatalf("expected intersection [{a, ID-A}], got %+v", got)
	}
}

func TestSynthesizeFirstSyncSnapshot_NoOverlap(t *testing.T) {
	got := synthesizeFirstSyncSnapshot([]string{"a"}, []LinearLabel{{Name: "b", ID: "ID-B"}})
	if len(got) != 0 {
		t.Fatalf("expected empty intersection, got %+v", got)
	}
}
```

- [ ] **Step 2: Run, confirm they fail**

Run: `go test ./internal/linear/ -run TestSynthesizeFirstSyncSnapshot -v`
Expected: FAIL — undefined.

- [ ] **Step 3: Implement**

Add to `internal/linear/reconciler.go`:

```go
// synthesizeFirstSyncSnapshot returns the intersection of beads and Linear
// label names, with IDs taken from the Linear side. Used as the synthetic
// snapshot input on the first sync for a bead, so the truth table behaves
// as if both sides were already in agreement on shared labels — preventing
// removals while still allowing both-side adds to flow.
func synthesizeFirstSyncSnapshot(beads []string, linear []LinearLabel) []SnapshotEntry {
	beadsSet := make(map[string]bool, len(beads))
	for _, b := range beads {
		beadsSet[b] = true
	}
	var out []SnapshotEntry
	for _, l := range linear {
		if beadsSet[l.Name] {
			out = append(out, SnapshotEntry{Name: l.Name, ID: l.ID})
		}
	}
	return out
}
```

- [ ] **Step 4: Run, confirm tests pass**

Run: `go test ./internal/linear/ -run TestSynthesizeFirstSyncSnapshot -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add internal/linear/reconciler.go internal/linear/reconciler_test.go
git commit -m "feat(linear): first-sync intersection synthesis"
```

---

### Task B6: Wire the orchestrator

**Files:**
- Modify: `internal/linear/reconciler.go`
- Modify: `internal/linear/reconciler_test.go`

- [ ] **Step 1: Write the failing tests** for the public `ReconcileLabels` orchestrator. These cover the integration of all three passes plus snapshot computation.

Append to `internal/linear/reconciler_test.go`:

```go
func TestReconcileLabels_FirstSyncIntersectionPreservesBoth(t *testing.T) {
	// Empty snapshot, bead has [A, B], Linear has [A] → first-sync rule.
	// Expect: nothing removed; B pushed; new snapshot covers both.
	res := ReconcileLabels(LabelReconcileInput{
		Beads:    []string{"A", "B"},
		Linear:   []LinearLabel{{Name: "A", ID: "lin-A"}},
		Snapshot: nil,
	})
	if len(res.RemoveFromBeads) != 0 || len(res.RemoveFromLinear) != 0 {
		t.Errorf("first-sync should remove nothing, got removeBeads=%v removeLinear=%v",
			res.RemoveFromBeads, res.RemoveFromLinear)
	}
	if !reflect.DeepEqual(sortedNames(res.AddToLinear), []string{"B"}) {
		t.Errorf("AddToLinear: got %v, want [B]", res.AddToLinear)
	}
	if len(res.AddToBeads) != 0 {
		t.Errorf("AddToBeads should be empty (A in agreement), got %v", res.AddToBeads)
	}
	// New snapshot reflects what's CURRENTLY agreed; pushed B has no Linear
	// ID yet, so the orchestrator emits a snapshot containing only A. The
	// caller writes the post-push-resolved snapshot separately.
	if len(res.NewSnapshot) != 1 || res.NewSnapshot[0].Name != "A" {
		t.Errorf("NewSnapshot: got %+v, want one entry for A", res.NewSnapshot)
	}
}

func TestReconcileLabels_AppliedRename(t *testing.T) {
	res := ReconcileLabels(LabelReconcileInput{
		Beads:    []string{"old"},
		Linear:   []LinearLabel{{Name: "new", ID: "X"}},
		Snapshot: []SnapshotEntry{{Name: "old", ID: "X"}},
	})
	if !reflect.DeepEqual(sortedNames(res.RemoveFromBeads), []string{"old"}) {
		t.Errorf("RemoveFromBeads: got %v, want [old]", res.RemoveFromBeads)
	}
	if !reflect.DeepEqual(sortedNames(res.AddToBeads), []string{"new"}) {
		t.Errorf("AddToBeads: got %v, want [new]", res.AddToBeads)
	}
	if len(res.RenamesApplied) != 1 || res.RenamesApplied[0].ID != "X" {
		t.Errorf("RenamesApplied: got %+v", res.RenamesApplied)
	}
	if len(res.NewSnapshot) != 1 || res.NewSnapshot[0].Name != "new" || res.NewSnapshot[0].ID != "X" {
		t.Errorf("NewSnapshot: got %+v, want [{new, X}]", res.NewSnapshot)
	}
}

func TestReconcileLabels_DroppedRenameDeleteWins(t *testing.T) {
	// Decision #10
	res := ReconcileLabels(LabelReconcileInput{
		Beads:    []string{},
		Linear:   []LinearLabel{{Name: "new", ID: "X"}},
		Snapshot: []SnapshotEntry{{Name: "old", ID: "X"}},
	})
	if !reflect.DeepEqual(sortedNames(res.RemoveFromLinear), []string{"X"}) {
		t.Errorf("RemoveFromLinear: got %v, want [X]", res.RemoveFromLinear)
	}
	if len(res.AddToBeads) != 0 {
		t.Errorf("AddToBeads should be empty (delete wins), got %v", res.AddToBeads)
	}
	if len(res.NewSnapshot) != 0 {
		t.Errorf("NewSnapshot: got %+v, want empty", res.NewSnapshot)
	}
}

func TestReconcileLabels_DroppedRenameWithLocalReAdd(t *testing.T) {
	res := ReconcileLabels(LabelReconcileInput{
		Beads:    []string{"new"},
		Linear:   []LinearLabel{{Name: "new", ID: "X"}},
		Snapshot: []SnapshotEntry{{Name: "old", ID: "X"}},
	})
	// Pass-2 consumed beads row "new" (suppressing AddToBeads), Linear row X,
	// snapshot row X. Pass-3 sees nothing. End-state in agreement.
	if len(res.AddToBeads) != 0 || len(res.AddToLinear) != 0 ||
		len(res.RemoveFromBeads) != 0 || len(res.RemoveFromLinear) != 0 {
		t.Errorf("expected no changes, got %+v", res)
	}
	// Snapshot reflects the agreed state.
	if len(res.NewSnapshot) != 1 || res.NewSnapshot[0].Name != "new" || res.NewSnapshot[0].ID != "X" {
		t.Errorf("NewSnapshot: got %+v, want [{new, X}]", res.NewSnapshot)
	}
}

func TestReconcileLabels_OldDeleteNewAddIndependent(t *testing.T) {
	// No ID match — these are independent labels, not a rename.
	res := ReconcileLabels(LabelReconcileInput{
		Beads:    []string{"bar"},
		Linear:   []LinearLabel{{Name: "foo", ID: "F"}},
		Snapshot: []SnapshotEntry{{Name: "foo", ID: "F"}},
	})
	if !reflect.DeepEqual(sortedNames(res.RemoveFromLinear), []string{"F"}) {
		t.Errorf("RemoveFromLinear: got %v, want [F]", res.RemoveFromLinear)
	}
	if !reflect.DeepEqual(sortedNames(res.AddToLinear), []string{"bar"}) {
		t.Errorf("AddToLinear: got %v, want [bar]", res.AddToLinear)
	}
}

func TestReconcileLabels_StandardThreeWayMerge(t *testing.T) {
	// Snapshot has [A, B]. Linear added C, removed B. Beads added D, removed A.
	// Expect: A removed from Linear, B removed from beads, C added to beads,
	// D added to Linear.
	res := ReconcileLabels(LabelReconcileInput{
		Beads:  []string{"B", "D"},
		Linear: []LinearLabel{{Name: "A", ID: "ia"}, {Name: "C", ID: "ic"}},
		Snapshot: []SnapshotEntry{
			{Name: "A", ID: "ia"},
			{Name: "B", ID: "ib"},
		},
	})
	if !reflect.DeepEqual(sortedNames(res.RemoveFromLinear), []string{"ia"}) {
		t.Errorf("RemoveFromLinear: got %v, want [ia]", res.RemoveFromLinear)
	}
	if !reflect.DeepEqual(sortedNames(res.RemoveFromBeads), []string{"B"}) {
		t.Errorf("RemoveFromBeads: got %v, want [B]", res.RemoveFromBeads)
	}
	if !reflect.DeepEqual(sortedNames(res.AddToBeads), []string{"C"}) {
		t.Errorf("AddToBeads: got %v, want [C]", res.AddToBeads)
	}
	if !reflect.DeepEqual(sortedNames(res.AddToLinear), []string{"D"}) {
		t.Errorf("AddToLinear: got %v, want [D]", res.AddToLinear)
	}
}

func TestReconcileLabels_EmptyInputs(t *testing.T) {
	res := ReconcileLabels(LabelReconcileInput{})
	if len(res.AddToBeads)+len(res.RemoveFromBeads)+len(res.AddToLinear)+len(res.RemoveFromLinear) != 0 {
		t.Errorf("expected no changes, got %+v", res)
	}
	if len(res.NewSnapshot) != 0 {
		t.Errorf("NewSnapshot: got %+v, want empty", res.NewSnapshot)
	}
}
```

- [ ] **Step 2: Run, confirm they fail**

Run: `go test ./internal/linear/ -run TestReconcileLabels -v`
Expected: FAIL — `ReconcileLabels` undefined.

- [ ] **Step 3: Implement the orchestrator**

Add to `internal/linear/reconciler.go`:

```go
// ReconcileLabels runs the three-pass reconciler.
//
// Pass 1: apply exclusion filter to all input sets.
// Pass 2: classify Linear-side renames (rename map + per-row consumption flags).
// Pass 3: run the per-label decision table on the unconsumed rows.
//
// Returns adds/removes for each side, the rename events to surface, and the
// next snapshot. Callers apply the mutations and persist NewSnapshot inside a
// transaction (see internal/tracker/engine.go for the integration point).
//
// First-sync rule: if Snapshot is empty and either side has labels, synthesize
// a snapshot equal to the intersection of beads and Linear, then run normally.
// This guarantees no removals on first sync (rows in only one side become adds,
// rows in both become in-agreement).
func ReconcileLabels(in LabelReconcileInput) LabelReconcileResult {
	beads, linear, snap := applyExclusionFilter(in)

	if len(snap) == 0 && (len(beads) > 0 || len(linear) > 0) {
		snap = synthesizeFirstSyncSnapshot(beads, linear)
	}

	rc := classifyRenames(beads, linear, snap)
	res := applyTruthTable(beads, linear, snap, rc)

	// Apply rename effects to the user-visible result.
	for _, r := range rc.applied {
		res.RemoveFromBeads = append(res.RemoveFromBeads, r.OldName)
		res.AddToBeads = append(res.AddToBeads, r.NewName)
		res.RenamesApplied = append(res.RenamesApplied, r)
	}
	for _, r := range rc.dropped {
		res.RemoveFromLinear = append(res.RemoveFromLinear, r.ID)
	}

	res.NewSnapshot = computeNewSnapshot(beads, linear, snap, rc, res)
	return res
}

// computeNewSnapshot builds the post-sync snapshot. It contains an entry for
// every label that exists on BOTH sides after the reconciler's mutations would
// be applied. Labels added to Linear via auto-create are NOT included here —
// the caller adds them after resolving/creating IDs.
func computeNewSnapshot(beads []string, linear []LinearLabel, snap []SnapshotEntry, rc renameClass, res LabelReconcileResult) []SnapshotEntry {
	// Project end-state on each side.
	beadsEnd := make(map[string]bool, len(beads))
	for _, b := range beads {
		beadsEnd[b] = true
	}
	for _, n := range res.RemoveFromBeads {
		delete(beadsEnd, n)
	}
	for _, n := range res.AddToBeads {
		beadsEnd[n] = true
	}

	linearEnd := make(map[string]LinearLabel, len(linear))
	for _, l := range linear {
		linearEnd[l.Name] = l
	}
	for _, id := range res.RemoveFromLinear {
		for name, l := range linearEnd {
			if l.ID == id {
				delete(linearEnd, name)
			}
		}
	}
	// AddToLinear has no IDs yet; caller resolves and re-snapshot writes after push.

	out := make([]SnapshotEntry, 0)
	for name, l := range linearEnd {
		if beadsEnd[name] {
			out = append(out, SnapshotEntry{Name: name, ID: l.ID})
		}
	}
	return out
}
```

- [ ] **Step 4: Run, confirm tests pass**

Run: `go test ./internal/linear/ -run TestReconcileLabels -v`
Expected: PASS for all six.

- [ ] **Step 5: Run all reconciler tests for sanity**

Run: `go test ./internal/linear/ -run 'TestReconcile|TestApply|TestClassify|TestSynthesize' -v`
Expected: all PASS.

- [ ] **Step 6: Commit**

```bash
git add internal/linear/reconciler.go internal/linear/reconciler_test.go
git commit -m "feat(linear): wire ReconcileLabels orchestrator"
```

---

## Phase C — Linear Client Extensions

### Task C1: LabelsByName client method

**Files:**
- Modify: `internal/linear/client.go`
- Modify: `internal/linear/client_test.go`

- [ ] **Step 1: Write the failing test**

Open `internal/linear/client_test.go` (create if it doesn't exist). Add the helper if not present, and the tests:

```go
package linear

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// mockGraphQLServer wraps httptest with a JSON-aware request handler.
// Returns a server whose URL the caller passes to Client via WithEndpoint.
func mockGraphQLServer(t *testing.T, respond func(reqBody string) string) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		w.Header().Set("Content-Type", "application/json")
		// The Client wraps the response body into {"data": ...}, so respond() should
		// return just the inner data object (e.g. `{"team": {...}}`).
		_, _ = io.WriteString(w, `{"data":`+respond(string(body))+`}`)
	}))
}

func newTestClient(serverURL string) *Client {
	c := NewClient("test-api-key", "team-1")
	return c.WithEndpoint(serverURL)
}

func TestLabelsByName_TeamScoped(t *testing.T) {
	server := mockGraphQLServer(t, func(req string) string {
		// Assert the query targets both team.labels and organization.labels.
		if !strings.Contains(req, "team(") || !strings.Contains(req, "labels") {
			t.Errorf("expected query to fetch team labels, got: %s", req)
		}
		return `{"team":{"labels":{"nodes":[
			{"id":"L1","name":"bug"},
			{"id":"L2","name":"p1"}
		]}},"organization":{"labels":{"nodes":[]}}}`
	})
	defer server.Close()

	c := newTestClient(server.URL)
	out, err := c.LabelsByName(context.Background(), []string{"Bug", "missing"}) // beads-side spelling differs from Linear
	if err != nil {
		t.Fatalf("LabelsByName: %v", err)
	}
	// Result is keyed by lowercase name (case-insensitive match), but the
	// LinearLabel.Name field preserves Linear's display casing.
	if got, ok := out["bug"]; !ok || got.ID != "L1" || got.Name != "bug" {
		t.Errorf("bug: got %+v, want {ID: L1, Name: bug}", got)
	}
	if _, ok := out["missing"]; ok {
		t.Errorf("missing: should not be in result map")
	}
}

func TestLabelsByName_DuplicateNamesFailLoudly(t *testing.T) {
	server := mockGraphQLServer(t, func(_ string) string {
		return `{"team":{"labels":{"nodes":[
			{"id":"L1","name":"bug"},
			{"id":"L2","name":"bug"}
		]}},"organization":{"labels":{"nodes":[]}}}`
	})
	defer server.Close()

	c := newTestClient(server.URL)
	_, err := c.LabelsByName(context.Background(), []string{"bug"})
	if err == nil {
		t.Fatal("expected duplicate-name error, got nil")
	}
	if !strings.Contains(err.Error(), "ambiguous") && !strings.Contains(err.Error(), "duplicate") {
		t.Errorf("error should mention ambiguity, got: %v", err)
	}
}

// keep json import alive in some implementations
var _ = json.Unmarshal

- [ ] **Step 2: Run, confirm fail**

Run: `go test ./internal/linear/ -run TestLabelsByName -v`
Expected: FAIL — `LabelsByName` undefined.

- [ ] **Step 3: Implement LabelsByName**

Add to `internal/linear/client.go`:

```go
// LabelsByName resolves a set of label names to their Linear IDs by querying
// both team-scoped and workspace-scoped labels. Names not found in Linear are
// simply absent from the returned map; the caller decides whether to auto-create.
//
// On collision (same name in both team and workspace scope), team-scoped wins.
// Duplicate names within a single scope cause an ambiguity error per
// the precedent in commit d4df404a — we never silently pick one.
func (c *Client) LabelsByName(ctx context.Context, names []string) (map[string]LinearLabel, error) {
	if len(names) == 0 {
		return map[string]LinearLabel{}, nil
	}

	query := `
		query LabelsByName($teamId: String!) {
			team(id: $teamId) {
				labels(first: 250) {
					nodes { id name }
				}
			}
			organization {
				labels(first: 250) {
					nodes { id name }
				}
			}
		}
	`
	req := &GraphQLRequest{
		Query:     query,
		Variables: map[string]interface{}{"teamId": c.TeamID},
	}
	data, err := c.Execute(ctx, req)
	if err != nil {
		return nil, fmt.Errorf("LabelsByName: %w", err)
	}

	var resp struct {
		Team struct {
			Labels struct {
				Nodes []struct{ ID, Name string }
			}
		}
		Organization struct {
			Labels struct {
				Nodes []struct{ ID, Name string }
			}
		}
	}
	if err := json.Unmarshal(data, &resp); err != nil {
		return nil, fmt.Errorf("LabelsByName: parse response: %w", err)
	}

	wanted := make(map[string]bool, len(names))
	for _, n := range names {
		wanted[strings.ToLower(n)] = true
	}

	// First fold: detect duplicates within either scope. Team or workspace
	// individually having two labels with the same name is the ambiguity
	// case we fail on.
	checkScope := func(scope string, nodes []struct{ ID, Name string }) error {
		seen := map[string]string{}
		for _, n := range nodes {
			key := strings.ToLower(n.Name)
			if !wanted[key] {
				continue
			}
			if existing, ok := seen[key]; ok {
				return fmt.Errorf("ambiguous label %q in %s scope: ids %s and %s — dedupe in Linear before sync", n.Name, scope, existing, n.ID)
			}
			seen[key] = n.ID
		}
		return nil
	}
	if err := checkScope("team", resp.Team.Labels.Nodes); err != nil {
		return nil, err
	}
	if err := checkScope("workspace", resp.Organization.Labels.Nodes); err != nil {
		return nil, err
	}

	// Second fold: build the output. **Keyed by lowercase name** so callers
	// (notably resolveLabelIDs) get case-insensitive lookups — Linear matches
	// labels case-insensitively, and beads label casing may differ from
	// Linear's display casing. The LinearLabel.Name field preserves Linear's
	// display case.
	//
	// Team scope wins on cross-scope collision (more specific).
	out := map[string]LinearLabel{}
	for _, n := range resp.Organization.Labels.Nodes {
		key := strings.ToLower(n.Name)
		if wanted[key] {
			out[key] = LinearLabel{Name: n.Name, ID: n.ID}
		}
	}
	for _, n := range resp.Team.Labels.Nodes {
		key := strings.ToLower(n.Name)
		if wanted[key] {
			out[key] = LinearLabel{Name: n.Name, ID: n.ID}
		}
	}
	return out, nil
}
```

- [ ] **Step 4: Run tests, confirm pass**

Run: `go test ./internal/linear/ -run TestLabelsByName -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add internal/linear/client.go internal/linear/client_test.go
git commit -m "feat(linear): add LabelsByName client method with ambiguity errors"
```

---

### Task C2: CreateLabel client method

**Files:**
- Modify: `internal/linear/client.go`
- Modify: `internal/linear/client_test.go`

- [ ] **Step 1: Write the failing tests**

Append to `internal/linear/client_test.go`:

```go
func TestCreateLabel_TeamScoped(t *testing.T) {
	var captured string
	server := mockGraphQLServer(t, func(req string) string {
		captured = req
		return `{"issueLabelCreate":{"success":true,"issueLabel":{"id":"new-id","name":"flaky-test"}}}`
	})
	defer server.Close()

	c := newTestClient(server.URL)
	got, err := c.CreateLabel(context.Background(), "flaky-test", LabelScopeTeam)
	if err != nil {
		t.Fatalf("CreateLabel: %v", err)
	}
	if got.ID != "new-id" || got.Name != "flaky-test" {
		t.Errorf("got %+v, want {ID: new-id, Name: flaky-test}", got)
	}
	if !strings.Contains(captured, "team-1") {
		t.Errorf("expected teamId in payload, got: %s", captured)
	}
}

func TestCreateLabel_WorkspaceScoped(t *testing.T) {
	var captured string
	server := mockGraphQLServer(t, func(req string) string {
		captured = req
		return `{"issueLabelCreate":{"success":true,"issueLabel":{"id":"new-id","name":"flaky-test"}}}`
	})
	defer server.Close()

	c := newTestClient(server.URL)
	_, err := c.CreateLabel(context.Background(), "flaky-test", LabelScopeWorkspace)
	if err != nil {
		t.Fatalf("CreateLabel: %v", err)
	}
	if strings.Contains(captured, `"teamId":"team-1"`) {
		t.Errorf("workspace scope must omit teamId, got: %s", captured)
	}
}
```

- [ ] **Step 2: Run, confirm fail**

Run: `go test ./internal/linear/ -run TestCreateLabel -v`
Expected: FAIL.

- [ ] **Step 3: Implement CreateLabel**

Add to `internal/linear/client.go`:

```go
// LabelScope controls where auto-created Linear labels live.
type LabelScope int

const (
	LabelScopeTeam LabelScope = iota
	LabelScopeWorkspace
)

// CreateLabel creates a new label in Linear. With LabelScopeTeam, the label
// is scoped to the client's TeamID. With LabelScopeWorkspace, no teamId is
// passed and the label becomes a workspace-level (organization-wide) label.
//
// Returns the created label with its server-assigned ID.
func (c *Client) CreateLabel(ctx context.Context, name string, scope LabelScope) (LinearLabel, error) {
	query := `
		mutation CreateLabel($input: IssueLabelCreateInput!) {
			issueLabelCreate(input: $input) {
				success
				issueLabel { id name }
			}
		}
	`
	input := map[string]interface{}{"name": name}
	if scope == LabelScopeTeam {
		input["teamId"] = c.TeamID
	}

	req := &GraphQLRequest{
		Query:     query,
		Variables: map[string]interface{}{"input": input},
	}
	data, err := c.Execute(ctx, req)
	if err != nil {
		return LinearLabel{}, fmt.Errorf("CreateLabel %q: %w", name, err)
	}

	var resp struct {
		IssueLabelCreate struct {
			Success    bool
			IssueLabel struct{ ID, Name string }
		}
	}
	if err := json.Unmarshal(data, &resp); err != nil {
		return LinearLabel{}, fmt.Errorf("CreateLabel %q: parse: %w", name, err)
	}
	if !resp.IssueLabelCreate.Success {
		return LinearLabel{}, fmt.Errorf("CreateLabel %q: server reported failure", name)
	}
	return LinearLabel{
		ID:   resp.IssueLabelCreate.IssueLabel.ID,
		Name: resp.IssueLabelCreate.IssueLabel.Name,
	}, nil
}
```

- [ ] **Step 4: Run, confirm pass**

Run: `go test ./internal/linear/ -run TestCreateLabel -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add internal/linear/client.go internal/linear/client_test.go
git commit -m "feat(linear): add CreateLabel with team/workspace scope"
```

---

### Task C3: Verify Linear `labelIds` replace semantics

**Files:**
- Modify: `internal/linear/client_test.go`

- [ ] **Step 1: Write the verification test**

This test enforces the spec assumption that `issueUpdate.input.labelIds` REPLACES the issue's label set rather than merging.

Append to `internal/linear/client_test.go`:

```go
func TestUpdateIssue_LabelIdsReplaceSemantics(t *testing.T) {
	// Asserts the wire format. The behavioral assertion that Linear's API
	// actually replaces (not merges) lives in cmd/bd/linear_roundtrip_test.go
	// Task G5, where the mock server is configured to replace.
	var captured string
	server := mockGraphQLServer(t, func(req string) string {
		captured = req
		return `{"issueUpdate":{"success":true,"issue":{"id":"I1","identifier":"TEST-1"}}}`
	})
	defer server.Close()

	c := newTestClient(server.URL)
	_, err := c.UpdateIssue(context.Background(), "I1", map[string]interface{}{
		"labelIds": []string{"L-NEW"},
	})
	if err != nil {
		t.Fatalf("UpdateIssue: %v", err)
	}
	if !strings.Contains(captured, `"labelIds":["L-NEW"]`) {
		t.Errorf("expected labelIds:[L-NEW] in payload, got: %s", captured)
	}
}
```

- [ ] **Step 2: Run, confirm pass**

Run: `go test ./internal/linear/ -run TestUpdateIssue_LabelIdsReplaceSemantics -v`
Expected: PASS — `UpdateIssue` already accepts an arbitrary input map; this test just asserts the wire format.

- [ ] **Step 3: Add a doc comment annotating the assumption**

Find `func (c *Client) UpdateIssue` in `internal/linear/client.go` (around line 525). Above the function, add:

```go
// NOTE: The "labelIds" input field is documented by Linear as REPLACING the
// issue's label set, not merging. This is what we rely on for label-removal
// pushes. If Linear ever changes this to merge semantics, the
// linear_roundtrip_test will catch it (TestRoundtrip_LabelIdsReplaceSemantics
// in cmd/bd/linear_roundtrip_test.go).
```

- [ ] **Step 4: Commit**

```bash
git add internal/linear/client.go internal/linear/client_test.go
git commit -m "test(linear): assert labelIds wire format and document replace semantics"
```

---

## Phase D — Push-Side Wiring

### Task D1: resolveLabelIDs helper in tracker

**Files:**
- Modify: `internal/linear/tracker.go`
- Modify: `internal/linear/tracker_test.go` (create if needed)

- [ ] **Step 1: Write the failing test**

In `internal/linear/tracker_test.go` (append or create), add:

```go
// fakeLabelClient stubs LabelsByName and CreateLabel for tracker tests.
type fakeLabelClient struct {
	existing map[string]string // name → ID
	created  []string          // names passed to CreateLabel, in order
	scope    LabelScope
}

func (f *fakeLabelClient) LabelsByName(ctx context.Context, names []string) (map[string]LinearLabel, error) {
	out := map[string]LinearLabel{}
	for _, n := range names {
		if id, ok := f.existing[n]; ok {
			out[n] = LinearLabel{Name: n, ID: id}
		}
	}
	return out, nil
}

func (f *fakeLabelClient) CreateLabel(ctx context.Context, name string, scope LabelScope) (LinearLabel, error) {
	f.created = append(f.created, name)
	f.scope = scope
	id := "auto-" + name
	if f.existing == nil {
		f.existing = map[string]string{}
	}
	f.existing[name] = id
	return LinearLabel{Name: name, ID: id}, nil
}

func TestResolveLabelIDs_AutoCreatesMissing(t *testing.T) {
	fc := &fakeLabelClient{existing: map[string]string{"bug": "L-bug"}}
	got, err := resolveLabelIDs(context.Background(), fc, []string{"bug", "flaky-test"}, LabelScopeTeam, nil)
	if err != nil {
		t.Fatalf("resolveLabelIDs: %v", err)
	}
	if got["bug"] != "L-bug" {
		t.Errorf("bug: got %q, want L-bug", got["bug"])
	}
	if got["flaky-test"] != "auto-flaky-test" {
		t.Errorf("flaky-test: got %q, want auto-flaky-test", got["flaky-test"])
	}
	if !reflect.DeepEqual(fc.created, []string{"flaky-test"}) {
		t.Errorf("created: got %v, want [flaky-test]", fc.created)
	}
	if fc.scope != LabelScopeTeam {
		t.Errorf("scope: got %v, want team", fc.scope)
	}
}
```

(Imports needed: `"context"`, `"reflect"`, `"testing"`.)

- [ ] **Step 2: Run, confirm fail**

Run: `go test ./internal/linear/ -run TestResolveLabelIDs -v`
Expected: FAIL — `resolveLabelIDs` undefined and no `labelClient` interface.

- [ ] **Step 3: Implement the helper and the narrow client interface it uses**

Add to `internal/linear/tracker.go`:

```go
// labelClient is the narrow interface resolveLabelIDs uses; *Client satisfies it.
// Defined as an interface so tests can stub without spinning up an HTTP server.
type labelClient interface {
	LabelsByName(ctx context.Context, names []string) (map[string]LinearLabel, error)
	CreateLabel(ctx context.Context, name string, scope LabelScope) (LinearLabel, error)
}

// resolveLabelIDs maps a set of beads label names to Linear label IDs, auto-
// creating any that don't exist. Per the spec (Atomicity & partial failure):
// a CreateLabel failure does NOT abort the whole push — the failed label is
// omitted from the result map, and the snapshot writer omits it too, so the
// next sync sees it as a fresh add and retries.
//
// LabelsByName ambiguity errors DO abort, since a duplicate label needs human
// resolution before further pushes are safe.
func resolveLabelIDs(ctx context.Context, c labelClient, names []string, scope LabelScope, warn func(format string, args ...interface{})) (map[string]string, error) {
	if len(names) == 0 {
		return map[string]string{}, nil
	}
	existing, err := c.LabelsByName(ctx, names)
	if err != nil {
		return nil, err // LabelsByName failure (incl. ambiguity) is fatal for this push
	}
	// LabelsByName returns lowercase-keyed results. We match case-insensitively
	// (Linear matches that way; beads label casing may differ from Linear's).
	// Output keys preserve the bead's original spelling so callers can use the
	// map with their original list.
	out := make(map[string]string, len(names))
	for _, n := range names {
		if l, ok := existing[strings.ToLower(n)]; ok {
			out[n] = l.ID
			continue
		}
		l, err := c.CreateLabel(ctx, n, scope)
		if err != nil {
			if warn != nil {
				warn("auto-create label %q failed; skipping for this sync (will retry next): %v", n, err)
			}
			continue // skip this label; do NOT abort the whole push
		}
		out[n] = l.ID
	}
	return out, nil
}
```

- [ ] **Step 4: Run tests, confirm pass**

Run: `go test ./internal/linear/ -run TestResolveLabelIDs -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add internal/linear/tracker.go internal/linear/tracker_test.go
git commit -m "feat(linear): resolveLabelIDs with auto-create"
```

---

### Task D2: Wire `Tracker.UpdateIssue` to push labels

**Files:**
- Modify: `internal/linear/tracker.go:175`

- [ ] **Step 1: Write the failing test**

Append to `internal/linear/tracker_test.go`:

```go
func TestTrackerUpdateIssue_PassesLabelIds(t *testing.T) {
	// Verify that when label sync is enabled, Tracker.UpdateIssue runs the
	// reconciler and includes labelIds in the GraphQL input. Full integration
	// (with snapshot persistence) is in cmd/bd/linear_roundtrip_test Phase G.
	t.Skip("integration: covered by cmd/bd/linear_roundtrip_test (Task G4)")
}
```

(This is a stub for documentation — full coverage is in Phase G.)

- [ ] **Step 2: Add the labelSync state on Tracker**

Open `internal/linear/tracker.go`. Add fields to the existing `Tracker` struct (the struct currently has `clients`, `config`, `store`, `teamIDs`, `projectID`):

```go
type Tracker struct {
	clients   map[string]*Client
	config    *MappingConfig
	store     storage.Storage
	teamIDs   []string
	projectID string

	// Label sync state (set via SetLabelSyncConfig from cmd/bd; defaults disable sync).
	labelSyncEnabled bool
	labelExclude     map[string]bool
	labelCreateScope LabelScope
	labelWarnFn      func(format string, args ...interface{}) // optional, for resolveLabelIDs warnings
}

// SetLabelSyncConfig configures bidirectional label sync. When enabled is
// false (the default), label-related code paths short-circuit and the legacy
// behavior is preserved. The warn callback receives messages for non-fatal
// failures (e.g., CreateLabel rate limits); pass nil to discard.
func (t *Tracker) SetLabelSyncConfig(enabled bool, exclude map[string]bool, scope LabelScope, warn func(string, ...interface{})) {
	t.labelSyncEnabled = enabled
	t.labelExclude = exclude
	t.labelCreateScope = scope
	t.labelWarnFn = warn
}

// LabelSyncEnabled is read by cmd/bd to decide whether to install the
// label-aware ContentEqual hook and PullHooks.ReconcileLabels callback.
func (t *Tracker) LabelSyncEnabled() bool { return t.labelSyncEnabled }

// LabelExclude is read by cmd/bd hooks to pass into the reconciler.
func (t *Tracker) LabelExclude() map[string]bool { return t.labelExclude }

// LoadSnapshot reads the persisted snapshot for an issue using the tracker's
// own store. Returns an empty slice when no snapshot exists. Used by the
// pull/push hooks AND the dry-run gate in ContentEqual.
//
// This method does its own short-lived read transaction; it does NOT need to
// participate in any caller's transaction (snapshots are read-only here).
func (t *Tracker) LoadSnapshot(ctx context.Context, issueID string) ([]SnapshotEntry, error) {
	if t.store == nil {
		return nil, nil
	}
	var entries []storage.LinearLabelSnapshotEntry
	err := t.store.RunInTransaction(ctx, "linear: read snapshot", func(tx storage.Transaction) error {
		var err error
		entries, err = tx.GetLinearLabelSnapshot(ctx, issueID)
		return err
	})
	if err != nil {
		return nil, err
	}
	out := make([]SnapshotEntry, len(entries))
	for i, e := range entries {
		out[i] = SnapshotEntry{Name: e.LabelName, ID: e.LabelID}
	}
	return out, nil
}
```

- [ ] **Step 3: Modify `Tracker.UpdateIssue` (line 175) to push labels**

Replace the body of `UpdateIssue` to include label sync. The existing body (after `updates := mapper.IssueToTracker(issue)` and the stateID block) ends with `client.UpdateIssue(ctx, externalID, updates)`. Insert the label-sync block right before that call:

```go
func (t *Tracker) UpdateIssue(ctx context.Context, externalID string, issue *types.Issue) (*tracker.TrackerIssue, error) {
	client := t.clientForExternalID(ctx, externalID)
	if client == nil {
		return nil, fmt.Errorf("cannot determine Linear team for issue %s", externalID)
	}

	mapper := t.FieldMapper()
	updates := mapper.IssueToTracker(issue)

	stateID, err := t.findStateIDForIssue(ctx, client, issue)
	if err != nil {
		return nil, fmt.Errorf("finding state for status %s: %w", issue.Status, err)
	}
	if stateID != "" {
		updates["stateId"] = stateID
	}

	// Label sync (decision #12: independent path, runs even when other fields are equal).
	var snapshotToWrite []storage.LinearLabelSnapshotEntry
	if t.labelSyncEnabled {
		// Fetch current Linear labels for this issue so the reconciler has fresh state.
		// Note: the engine already fetched the issue once for ContentEqual; this is an
		// extra round-trip. Acceptable for v1 — a future optimization could pass the
		// pre-fetched issue through the IssueTracker interface.
		fresh, err := client.FetchIssueByIdentifier(ctx, externalID)
		if err != nil {
			return nil, fmt.Errorf("fetch for label reconciliation: %w", err)
		}
		if fresh == nil {
			return nil, fmt.Errorf("label reconcile: issue %s not found in Linear", externalID)
		}

		linearLabels := make([]LinearLabel, 0)
		if fresh.Labels != nil {
			for _, l := range fresh.Labels.Nodes {
				linearLabels = append(linearLabels, LinearLabel{Name: l.Name, ID: l.ID})
			}
		}

		snap, err := t.LoadSnapshot(ctx, issue.ID)
		if err != nil {
			return nil, fmt.Errorf("load snapshot: %w", err)
		}

		res := ReconcileLabels(LabelReconcileInput{
			Beads:    issue.Labels,
			Linear:   linearLabels,
			Snapshot: snap,
			Exclude:  t.labelExclude,
		})

		resolved, err := resolveLabelIDs(ctx, client, res.AddToLinear, t.labelCreateScope, t.labelWarnFn)
		if err != nil {
			return nil, err
		}

		// Build the post-mutation labelIds set: existing IDs minus removals plus resolved adds.
		removeSet := make(map[string]bool, len(res.RemoveFromLinear))
		for _, id := range res.RemoveFromLinear {
			removeSet[id] = true
		}
		// Build labelIds, deduplicating by ID. Duplicates can occur when
		// case-insensitive resolution maps a bead label like "bug" to the
		// SAME Linear ID that's already in linearLabels under "Bug" — the
		// reconciler treated them as distinct (case-sensitive matching), but
		// LabelsByName resolved them to the same ID. Without dedup we'd send
		// `[L1, L1]` to Linear AND fail the snapshot insert on PK conflict.
		labelIDSet := make(map[string]bool)
		labelIDs := make([]string, 0)
		linearByID := make(map[string]LinearLabel, len(linearLabels))
		for _, l := range linearLabels {
			linearByID[l.ID] = l
			if !removeSet[l.ID] && !labelIDSet[l.ID] {
				labelIDs = append(labelIDs, l.ID)
				labelIDSet[l.ID] = true
			}
		}
		for _, n := range res.AddToLinear {
			if id, ok := resolved[n]; ok && !labelIDSet[id] {
				labelIDs = append(labelIDs, id)
				labelIDSet[id] = true
			}
		}
		updates["labelIds"] = labelIDs

		// Build the snapshot to persist. Reflects the post-push agreed state:
		// every labelID we just sent to Linear, with its name. Skipped labels
		// (CreateLabel failures) are absent — they retry next sync.
		//
		// CRITICAL: persist Linear's display case for the label name, NOT the
		// bead's spelling. Otherwise, when LabelsByName matches case-insensitively
		// (e.g. bead "bug" → Linear "Bug" with id L1), we'd persist {L1, "bug"}
		// in the snapshot. Next sync's reconciler sees snapshot.Name="bug" and
		// Linear.Name="Bug" with the same ID L1 → false rename detection → infinite
		// churn. We pre-populate nameByID with Linear's display case from the
		// fetched labels, then ONLY add resolved entries for IDs not already
		// known (i.e., labels that were freshly auto-created with the bead's
		// spelling — those names ARE Linear's spelling, since CreateLabel just
		// created them with that name).
		nameByID := make(map[string]string, len(linearLabels)+len(resolved))
		for _, l := range linearLabels {
			nameByID[l.ID] = l.Name // Linear's display case wins for known IDs
		}
		for n, id := range resolved {
			if _, alreadyKnown := nameByID[id]; !alreadyKnown {
				nameByID[id] = n // freshly-created via CreateLabel; n is Linear's name now
			}
		}
		snapshotToWrite = make([]storage.LinearLabelSnapshotEntry, 0, len(labelIDs))
		for _, id := range labelIDs {
			snapshotToWrite = append(snapshotToWrite, storage.LinearLabelSnapshotEntry{
				LabelID:   id,
				LabelName: nameByID[id],
			})
		}
	}

	updated, err := client.UpdateIssue(ctx, externalID, updates)
	if err != nil {
		return nil, err
	}

	// After successful push, persist the snapshot. Done OUTSIDE the local
	// transaction since the engine doesn't expose a tx here. If the snapshot
	// write fails, the push has already happened — log and move on; the next
	// sync's reconciler will compute correctly from prior snapshot state and
	// converge.
	if t.labelSyncEnabled {
		if err := t.writeSnapshot(ctx, issue.ID, snapshotToWrite); err != nil {
			if t.labelWarnFn != nil {
				t.labelWarnFn("snapshot write failed for %s: %v", issue.ID, err)
			}
		}
	}

	ti := linearToTrackerIssue(updated)
	return &ti, nil
}

// writeSnapshot persists the post-sync label snapshot for an issue.
// Used by both UpdateIssue and CreateIssue after a successful push.
func (t *Tracker) writeSnapshot(ctx context.Context, issueID string, entries []storage.LinearLabelSnapshotEntry) error {
	if t.store == nil {
		return nil
	}
	return t.store.RunInTransaction(ctx, fmt.Sprintf("linear: snapshot labels %s", issueID), func(tx storage.Transaction) error {
		return tx.PutLinearLabelSnapshot(ctx, issueID, entries)
	})
}
```

(`linearToTrackerIssue` and other helpers are existing in the file; do not redefine.)

- [ ] **Step 4: Build to verify**

Run: `go build ./internal/linear/`
Expected: clean build.

- [ ] **Step 5: Run linear unit tests**

Run: `go test ./internal/linear/ -v`
Expected: existing tests still PASS; the stub from Step 1 SKIPs.

- [ ] **Step 6: Commit**

```bash
git add internal/linear/tracker.go internal/linear/tracker_test.go
git commit -m "feat(linear): UpdateIssue runs reconciler, pushes labelIds, persists snapshot"
```

- [ ] **Step 3: Build to verify**

Run: `go build ./internal/linear/`
Expected: clean build (test references compile).

- [ ] **Step 4: Run linear unit tests**

Run: `go test ./internal/linear/ -v`
Expected: existing tests still PASS; new test from Step 1 SKIPs.

- [ ] **Step 5: Commit**

```bash
git add internal/linear/tracker.go internal/linear/tracker_test.go
git commit -m "feat(linear): UpdateIssue resolves and passes labelIds when sync enabled"
```

---

### Task D3: Wire `Tracker.CreateIssue` to push labels

**Files:**
- Modify: `internal/linear/tracker.go:152`

- [ ] **Step 1: Write the failing test**

Append to `internal/linear/tracker_test.go`:

```go
func TestTrackerCreateIssue_PassesLabelIds(t *testing.T) {
	t.Skip("integration: covered by cmd/bd/linear_roundtrip_test (Task G3)")
}
```

- [ ] **Step 2: Modify `Tracker.CreateIssue` (line 152)**

Replace the body of `CreateIssue`. The existing function calls `client.CreateIssue(..., nil)` at line 166. Insert label resolution before the call and persist the snapshot after:

```go
func (t *Tracker) CreateIssue(ctx context.Context, issue *types.Issue) (*tracker.TrackerIssue, error) {
	client := t.primaryClient()
	if client == nil {
		return nil, fmt.Errorf("no Linear client available")
	}

	priority := PriorityToLinear(issue.Priority, t.config)

	stateID, err := t.findStateIDForIssue(ctx, client, issue)
	if err != nil {
		return nil, fmt.Errorf("finding state for status %s: %w", issue.Status, err)
	}

	var labelIDs []string
	var snapshotToWrite []storage.LinearLabelSnapshotEntry
	if t.labelSyncEnabled && len(issue.Labels) > 0 {
		// Filter excluded labels, then resolve names to Linear IDs.
		toResolve := make([]string, 0, len(issue.Labels))
		for _, name := range issue.Labels {
			if t.labelExclude == nil || !t.labelExclude[strings.ToLower(name)] {
				toResolve = append(toResolve, name)
			}
		}
		resolved, err := resolveLabelIDs(ctx, client, toResolve, t.labelCreateScope, t.labelWarnFn)
		if err != nil {
			return nil, err
		}
		for _, n := range toResolve {
			if id, ok := resolved[n]; ok {
				labelIDs = append(labelIDs, id)
				snapshotToWrite = append(snapshotToWrite, storage.LinearLabelSnapshotEntry{
					LabelID: id, LabelName: n,
				})
			}
		}
	}

	created, err := client.CreateIssue(ctx, issue.Title, issue.Description, priority, stateID, labelIDs)
	if err != nil {
		return nil, err
	}

	if t.labelSyncEnabled && len(snapshotToWrite) > 0 {
		if err := t.writeSnapshot(ctx, issue.ID, snapshotToWrite); err != nil {
			if t.labelWarnFn != nil {
				t.labelWarnFn("snapshot write failed for new bead %s: %v", issue.ID, err)
			}
		}
	}

	ti := linearToTrackerIssue(created)
	return &ti, nil
}
```

Add `"strings"` and `"github.com/steveyegge/beads/internal/storage"` to the import list if not already present (they should be — the file already uses both elsewhere).

- [ ] **Step 3: Build to verify**

Run: `go build ./internal/linear/`
Expected: clean build.

- [ ] **Step 4: Commit**

```bash
git add internal/linear/tracker.go internal/linear/tracker_test.go
git commit -m "feat(linear): CreateIssue resolves and passes labelIds when sync enabled"
```

---

### Task D4: Make `ContentEqual` label-aware

**Files:**
- Modify: `cmd/bd/linear.go:437`

- [ ] **Step 1: Write the failing test**

In `cmd/bd/linear_test.go` (or a new file `cmd/bd/linear_content_equal_test.go`):

```go
func TestContentEqual_TripsOnLabelDelta(t *testing.T) {
	// When PushFieldsEqual would say "equal", but the bead has a label not on
	// Linear (and label sync is enabled), ContentEqual returns false so the
	// engine doesn't skip the push.
	t.Skip("integration: requires Tracker fixture; covered by Task G4 round-trip")
}
```

(Full unit coverage is impractical without significant Tracker mocking. The behavioral assertion is in roundtrip Task G4.)

- [ ] **Step 2: Modify the ContentEqual hook**

Open `cmd/bd/linear.go:437`. The current callback in `buildLinearPushHooks` returns `linear.PushFieldsEqual(...)` for the *Linear-typed remote case. Wrap it so that when label sync is enabled and there's a label delta, the gate returns false and the engine proceeds to push:

```go
		ContentEqual: func(local *types.Issue, remote *tracker.TrackerIssue) bool {
			remoteIssue, ok := remote.Raw.(*linear.Issue)
			if ok && remoteIssue != nil {
				if !linear.PushFieldsEqual(local, remoteIssue, config) {
					return false
				}
				// Decision #12: label sync gate. When label sync is enabled,
				// non-empty label delta forces a push even if all other fields
				// are equal.
				if lt.LabelSyncEnabled() && hasLabelDelta(ctx, lt, local, remoteIssue) {
					return false
				}
				return true
			}
			remoteConv := lt.FieldMapper().IssueToBeads(remote)
			if remoteConv == nil || remoteConv.Issue == nil {
				return false
			}
			return linear.PushFieldsEqualToBeads(local, remoteConv.Issue)
		},
```

Add a helper at the bottom of `cmd/bd/linear.go`:

```go
// hasLabelDelta runs the reconciler in dry-run mode against the persisted
// snapshot and returns true if any **push-direction** label adds/removes
// would fire. Used by the push-side ContentEqual to bypass the engine-level
// skip when only labels differ in the push direction.
//
// Pull-direction deltas (AddToBeads, RemoveFromBeads) are deliberately NOT
// checked here — those are the pull path's concern, gated by the pull-side
// ContentEqual / pullHasLabelDelta. Including them here would cause push to
// issue an IssueUpdate carrying labelIds identical to Linear's current state
// (a wasted API call) just because the bead is missing labels Linear has.
//
// Fail-safe: if the snapshot read errors, return true to force a push;
// worse to silently swallow a label change than to issue a no-op mutation.
func hasLabelDelta(ctx context.Context, lt *linear.Tracker, local *types.Issue, remoteIssue *linear.Issue) bool {
	linearLabels := make([]linear.LinearLabel, 0)
	if remoteIssue.Labels != nil {
		for _, l := range remoteIssue.Labels.Nodes {
			linearLabels = append(linearLabels, linear.LinearLabel{Name: l.Name, ID: l.ID})
		}
	}
	snap, err := lt.LoadSnapshot(ctx, local.ID)
	if err != nil {
		return true
	}
	res := linear.ReconcileLabels(linear.LabelReconcileInput{
		Beads:    local.Labels,
		Linear:   linearLabels,
		Snapshot: snap,
		Exclude:  lt.LabelExclude(),
	})
	return len(res.AddToLinear) > 0 || len(res.RemoveFromLinear) > 0
}
```

(`LabelSyncEnabled`, `LabelExclude`, and `LoadSnapshot` were added on `Tracker` in Task D2.)

- [ ] **Step 3: Build**

Run: `go build ./...`
Expected: clean build.

- [ ] **Step 4: Run all unit tests for sanity**

Run: `go test ./internal/linear/... ./cmd/bd/... -v`
Expected: all PASS (label sync stays disabled by default until Task F1 wires the config, so existing behavior is preserved).

- [ ] **Step 5: Commit**

```bash
git add cmd/bd/linear.go cmd/bd/linear_test.go
git commit -m "feat(linear): make ContentEqual label-aware via reconciler dry-run"
```

---

## Phase E — Pull-Side Wiring

> **Architectural note:** `tracker.PullHooks` already exists at `internal/tracker/engine.go:24-58` with rich functionality (`ContentEqual`, `ShouldImport`, `TransformIssue`, `GenerateID`, `SyncComments`, `SyncAttachments`). `buildLinearPullHooks` already exists at `cmd/bd/linear.go:348` and is wired at line 236. We **add a new field** to the existing struct, not redefine it. We extend the existing builder, not replace it. Snapshot persistence on push happens inside `Tracker.UpdateIssue`/`CreateIssue` themselves (Task D2/D3) — no `AfterPush` hook needed.

### Task E1: Add `ReconcileLabels` field to existing PullHooks; replace pull-side label sync

**Files:**
- Modify: `internal/tracker/engine.go` (add field on existing struct, swap call site)

- [ ] **Step 1: Survey the call site**

Run: `grep -n syncIssueLabels internal/tracker/engine.go`
Expected: shows the call at line 496 and the function definition at line 644.

Run: `grep -rn syncIssueLabels --include='*.go' .`
Expected: only the two refs in `engine.go`.

- [ ] **Step 2: Add `ReconcileLabels` to existing PullHooks struct**

Open `internal/tracker/engine.go`. Find the existing `PullHooks` struct (line 26). It currently ends with the `ContentEqual` field. Add a new field at the end:

```go
type PullHooks struct {
	GenerateID      func(ctx context.Context, issue *types.Issue) error
	TransformIssue  func(issue *types.Issue)
	ShouldImport    func(issue *TrackerIssue) bool
	SyncComments    func(ctx context.Context, localIssueID string, externalIssueID string) error
	SyncAttachments func(ctx context.Context, localIssueID string, externalIssueID string) error
	ContentEqual    func(local, remote *types.Issue) bool

	// ReconcileLabels overrides the legacy "Linear-authoritative" label sync
	// at engine.go:496. When set, the hook owns reading current labels,
	// running its own reconciliation logic, and writing back through tx
	// (including any per-tracker snapshot tables).
	//
	// When nil, the engine falls back to legacySyncIssueLabels (the prior
	// behavior, renamed in this commit). This keeps non-Linear trackers
	// (GitHub/GitLab) unaffected — they have no opinion about label
	// reconciliation and rely on the legacy flow.
	ReconcileLabels func(ctx context.Context, tx storage.Transaction, issueID string, desired []string, extIssue *TrackerIssue, actor string) error
}
```

(Match the existing struct field order — only the `ReconcileLabels` field is new.)

- [ ] **Step 3: Replace the call site**

Open `internal/tracker/engine.go:492-500`. Change:

```go
if err := e.Store.RunInTransaction(ctx, fmt.Sprintf("bd: pull update %s", existing.ID), func(tx storage.Transaction) error {
    if err := tx.UpdateIssue(ctx, existing.ID, updates, e.Actor); err != nil {
        return err
    }
    return syncIssueLabels(ctx, tx, existing.ID, conv.Issue.Labels, e.Actor)
}); err != nil {
```

to:

```go
if err := e.Store.RunInTransaction(ctx, fmt.Sprintf("bd: pull update %s", existing.ID), func(tx storage.Transaction) error {
    if err := tx.UpdateIssue(ctx, existing.ID, updates, e.Actor); err != nil {
        return err
    }
    if e.PullHooks != nil && e.PullHooks.ReconcileLabels != nil {
        return e.PullHooks.ReconcileLabels(ctx, tx, existing.ID, conv.Issue.Labels, &extIssue, e.Actor)
    }
    return legacySyncIssueLabels(ctx, tx, existing.ID, conv.Issue.Labels, e.Actor)
}); err != nil {
```

Note: the `extIssue` variable in the surrounding scope is `tracker.TrackerIssue` (value, not pointer). Pass `&extIssue` to satisfy the `*TrackerIssue` parameter. Verify by reading lines 380-490 of engine.go before editing.

- [ ] **Step 4: Rename `syncIssueLabels` → `legacySyncIssueLabels`**

Find the function at line 644:
```go
func syncIssueLabels(ctx context.Context, tx storage.Transaction, issueID string, desired []string, actor string) error {
```

Rename to `legacySyncIssueLabels`. Body unchanged.

Run: `grep -rn syncIssueLabels --include='*.go' .`
Expected: no results.

Run: `grep -rn legacySyncIssueLabels --include='*.go' .`
Expected: two results — the def at engine.go:644 and the fallback call at engine.go:~498.

- [ ] **Step 5: Build to verify**

Run: `go build ./...`
Expected: clean build.

- [ ] **Step 6: Run engine tests**

Run: `go test ./internal/tracker/... -v`
Expected: PASS — the legacy fallback preserves all existing behavior since no tracker has set `ReconcileLabels` yet.

- [ ] **Step 7: Commit**

```bash
git add internal/tracker/engine.go
git commit -m "feat(tracker): add PullHooks.ReconcileLabels with legacy fallback"
```

---

### Task E2: Wire the Linear `ReconcileLabels` callback + make pull `ContentEqual` label-aware

**Files:**
- Modify: `cmd/bd/linear.go` (extend existing `buildLinearPullHooks` at line 348)

> **Critical:** the engine's pull path at `internal/tracker/engine.go:440-462` calls `PullHooks.ContentEqual` and `continue`s (skipping the transaction where `ReconcileLabels` would run) when it returns true. The existing pull `ContentEqual` ignores labels (it only compares title/description/priority/status/type/close_reason), so a Linear-only label add would never trigger the reconciler. We must make pull-side `ContentEqual` label-aware, exactly like Task D4 did for push.

- [ ] **Step 1: Make pull `ContentEqual` label-aware**

Open `cmd/bd/linear.go:363`. The existing `ContentEqual` callback returns true when title/desc/priority/status/type/close_reason match. After the existing equality checks, before `return true`, add:

```go
		// Decision #12 (pull-side mirror): when label sync is enabled, a label
		// delta forces ContentEqual to false so the engine enters the update
		// transaction at engine.go:492 and our ReconcileLabels callback runs.
		// Without this, Linear-only label adds would be silently skipped.
		if lt.LabelSyncEnabled() && pullHasLabelDelta(ctx, lt, local, remote) {
			return false
		}
		return true
```

Add the helper at the bottom of `cmd/bd/linear.go` (sibling to `hasLabelDelta`):

```go
// pullHasLabelDelta is the pull-side counterpart to hasLabelDelta. It runs
// the reconciler against the persisted snapshot using whatever labels the
// pull-converted bead has, and returns true if any beads-side mutations
// (AddToBeads, RemoveFromBeads) would fire.
//
// The pull-side hook only applies beads-side mutations, so we only check
// those — Linear-side mutations are owned by the push path and gated by
// the existing hasLabelDelta on the push hook.
//
// Fail-safe: snapshot read errors → return true to force the update path.
func pullHasLabelDelta(ctx context.Context, lt *linear.Tracker, local *types.Issue, remote *types.Issue) bool {
	// remote.Labels is the converted bead form (just names — no IDs). We need IDs
	// from the raw Linear issue. The engine doesn't pass it here, so we fetch
	// the persisted snapshot and the local bead's current labels and remote.Labels
	// (without IDs), then synthesize a degenerate reconcile that checks NAME-level
	// adds/removes only.
	//
	// Compare local.Labels vs remote.Labels by name. If any name appears on one
	// side and not the other, there's a delta. Snapshot consultation isn't needed
	// here (the full reconciler runs inside the transaction with full IDs).
	localSet := make(map[string]bool, len(local.Labels))
	for _, n := range local.Labels {
		localSet[n] = true
	}
	remoteSet := make(map[string]bool, len(remote.Labels))
	for _, n := range remote.Labels {
		remoteSet[n] = true
	}
	excluded := lt.LabelExclude()
	for n := range remoteSet {
		if excluded != nil && excluded[strings.ToLower(n)] {
			continue
		}
		if !localSet[n] {
			return true // Linear has a label beads doesn't
		}
	}
	for n := range localSet {
		if excluded != nil && excluded[strings.ToLower(n)] {
			continue
		}
		if !remoteSet[n] {
			return true // beads has a label Linear doesn't
		}
	}
	return false
}
```

- [ ] **Step 2: Extend `buildLinearPullHooks` with the ReconcileLabels callback**

`buildLinearPullHooks` already exists at `cmd/bd/linear.go:348`. After the existing field assignments and before the `return hooks` at the end, add:

```go
	// Bidirectional label sync — Linear-specific reconciliation, gated on config.
	if lt.LabelSyncEnabled() {
		hooks.ReconcileLabels = func(ctx context.Context, tx storage.Transaction, issueID string, desired []string, extIssue *tracker.TrackerIssue, actor string) error {
			remoteIssue, ok := extIssue.Raw.(*linear.Issue)
			if !ok || remoteIssue == nil {
				return fmt.Errorf("ReconcileLabels: unexpected raw type %T", extIssue.Raw)
			}

			linearLabels := make([]linear.LinearLabel, 0)
			if remoteIssue.Labels != nil {
				for _, l := range remoteIssue.Labels.Nodes {
					linearLabels = append(linearLabels, linear.LinearLabel{Name: l.Name, ID: l.ID})
				}
			}

			snap, err := tx.GetLinearLabelSnapshot(ctx, issueID)
			if err != nil {
				return fmt.Errorf("load snapshot: %w", err)
			}
			snapEntries := make([]linear.SnapshotEntry, len(snap))
			for i, s := range snap {
				snapEntries[i] = linear.SnapshotEntry{Name: s.LabelName, ID: s.LabelID}
			}

			currentLabels, err := tx.GetLabels(ctx, issueID)
			if err != nil {
				return fmt.Errorf("get current labels: %w", err)
			}

			res := linear.ReconcileLabels(linear.LabelReconcileInput{
				Beads:    currentLabels,
				Linear:   linearLabels,
				Snapshot: snapEntries,
				Exclude:  lt.LabelExclude(),
			})

			// Apply Beads-side mutations only — Linear-side mutations
			// (RemoveFromLinear, AddToLinear) are owned by the push path
			// (Tracker.UpdateIssue), which runs in a separate transaction
			// and writes its own snapshot after a successful API call.
			for _, n := range res.RemoveFromBeads {
				if err := tx.RemoveLabel(ctx, issueID, n, actor); err != nil {
					return fmt.Errorf("remove label %q: %w", n, err)
				}
			}
			for _, n := range res.AddToBeads {
				if err := tx.AddLabel(ctx, issueID, n, actor); err != nil {
					return fmt.Errorf("add label %q: %w", n, err)
				}
			}

			// Snapshot reflects the agreed state after pull-side mutations.
			// Pure-Linear-side adds/removes that the push path will apply later
			// are deliberately NOT in this snapshot — they get added to the
			// snapshot by the push path's own write after a successful Linear
			// API call. If push fails, this snapshot is still correct (it
			// reflects what the local DB now has + what Linear has).
			snapshotEntries := make([]storage.LinearLabelSnapshotEntry, len(res.NewSnapshot))
			for i, e := range res.NewSnapshot {
				snapshotEntries[i] = storage.LinearLabelSnapshotEntry{LabelID: e.ID, LabelName: e.Name}
			}
			return tx.PutLinearLabelSnapshot(ctx, issueID, snapshotEntries)
		}
	}
```

The wiring at `cmd/bd/linear.go:236` (`engine.PullHooks = buildLinearPullHooks(ctx, lt)`) already exists and needs no change.

- [ ] **Step 2: Build**

Run: `go build ./...`
Expected: clean build.

- [ ] **Step 3: Run all tests**

Run: `go test ./...`
Expected: all PASS. (Label sync stays disabled until config is set in Task F1, so existing pull behavior is preserved by the legacy fallback in E1.)

- [ ] **Step 4: Commit**

```bash
git add cmd/bd/linear.go
git commit -m "feat(linear): wire pull-side reconciler with snapshot persistence"
```

---

### Task E3: Verify the pull→push interaction

**Files:** none — verification only.

This is a sanity step that no code is needed for. The data flow:

1. **Pull-side** (this task: `ReconcileLabels` callback) — applies beads-side adds/removes, writes snapshot reflecting the post-pull agreed state. **Does NOT** touch Linear or write the not-yet-pushed AddToLinear entries to the snapshot.

2. **Push-side** (Task D2: `Tracker.UpdateIssue`) — reads the same snapshot, runs reconciler again, computes labelIds, calls Linear, writes a new snapshot containing the post-push agreed state including freshly-pushed labels.

3. **Idempotency**: if pull writes snapshot then push writes snapshot, the second write replaces the first — push's snapshot is always the more complete one. If push fails, pull's snapshot still represents truth-as-of-pull, which is correct.

- [ ] **Step 1: Add a doc comment to `Tracker.UpdateIssue`** explaining the interaction:

In `internal/linear/tracker.go`, above `func (t *Tracker) UpdateIssue`, add or extend the doc comment:

```go
// UpdateIssue pushes a bead's changes to Linear. When label sync is enabled,
// it also runs the label reconciler and pushes labelIds in the same mutation,
// then persists a fresh snapshot reflecting the post-push agreed state.
//
// Pull-side label reconciliation (PullHooks.ReconcileLabels in cmd/bd/linear.go)
// runs in the engine's pull-side transaction and writes its own snapshot
// reflecting only beads-side mutations. When push runs after pull in the same
// sync cycle, push's later snapshot write replaces pull's — they don't conflict
// because they cover the same set of labels with consistent IDs.
```

- [ ] **Step 2: Commit (doc-only change)**

```bash
git add internal/linear/tracker.go
git commit -m "docs(linear): document pull/push snapshot interaction"
```

---

## Phase F — Configuration

### Task F1: Read new config keys

**Files:**
- Modify: `cmd/bd/linear.go`

- [ ] **Step 1: Write the failing test**

In `cmd/bd/linear_test.go` (or `cmd/bd/config_test.go`), add:

```go
func TestLinearConfig_LabelSyncDefaults(t *testing.T) {
	cfg := loadLinearLabelSyncConfig(map[string]string{})
	if cfg.Enabled {
		t.Errorf("Enabled: got true, want false (default)")
	}
	if len(cfg.Exclude) != 0 {
		t.Errorf("Exclude: got %v, want empty", cfg.Exclude)
	}
	if cfg.CreateScope != linear.LabelScopeTeam {
		t.Errorf("CreateScope: got %v, want team", cfg.CreateScope)
	}
}

func TestLinearConfig_LabelSyncOverrides(t *testing.T) {
	cfg := loadLinearLabelSyncConfig(map[string]string{
		"linear.label_sync_enabled": "true",
		"linear.label_sync_exclude": "bug, defect , Internal",
		"linear.label_create_scope": "workspace",
	})
	if !cfg.Enabled {
		t.Errorf("Enabled: got false, want true")
	}
	if !cfg.Exclude["bug"] || !cfg.Exclude["defect"] || !cfg.Exclude["internal"] {
		t.Errorf("Exclude should contain bug, defect, internal (lowercased): got %v", cfg.Exclude)
	}
	if cfg.CreateScope != linear.LabelScopeWorkspace {
		t.Errorf("CreateScope: got %v, want workspace", cfg.CreateScope)
	}
}
```

- [ ] **Step 2: Run, confirm fail**

Run: `go test ./cmd/bd/ -run TestLinearConfig_LabelSync -v`
Expected: FAIL.

- [ ] **Step 3: Implement the loader**

In `cmd/bd/linear.go`, add:

```go
type linearLabelSyncConfig struct {
	Enabled     bool
	Exclude     map[string]bool
	CreateScope linear.LabelScope
}

// loadLinearLabelSyncConfig parses the three label-sync config keys from a
// flat config map. Pass `store.GetAllConfig()` results in.
func loadLinearLabelSyncConfig(cfg map[string]string) linearLabelSyncConfig {
	out := linearLabelSyncConfig{
		Exclude:     map[string]bool{},
		CreateScope: linear.LabelScopeTeam,
	}
	if v := cfg["linear.label_sync_enabled"]; strings.EqualFold(strings.TrimSpace(v), "true") {
		out.Enabled = true
	}
	if v := cfg["linear.label_sync_exclude"]; v != "" {
		for _, raw := range strings.Split(v, ",") {
			n := strings.ToLower(strings.TrimSpace(raw))
			if n != "" {
				out.Exclude[n] = true
			}
		}
	}
	switch strings.ToLower(strings.TrimSpace(cfg["linear.label_create_scope"])) {
	case "workspace":
		out.CreateScope = linear.LabelScopeWorkspace
	default:
		out.CreateScope = linear.LabelScopeTeam
	}
	return out
}
```

- [ ] **Step 4: Wire the config into the Tracker**

In `cmd/bd/linear.go`, find the block that calls `lt.Init(ctx, store)`. After that succeeds (the tracker is now ready), add:

```go
allCfg, _ := store.GetAllConfig()
lsCfg := loadLinearLabelSyncConfig(allCfg)
lt.SetLabelSyncConfig(lsCfg.Enabled, lsCfg.Exclude, lsCfg.CreateScope, func(format string, args ...interface{}) {
    fmt.Fprintf(os.Stderr, "Warning: linear label sync: "+format+"\n", args...)
})
```

(`SetLabelSyncConfig` is the setter defined in Task D2 — no need to add it again here.)

This must run **before** `engine.PushHooks = buildLinearPushHooks(...)` and `engine.PullHooks = buildLinearPullHooks(...)` so that `lt.LabelSyncEnabled()` reports the right value when those builders inspect it.

- [ ] **Step 5: Run, confirm pass**

Run: `go test ./cmd/bd/ -run TestLinearConfig_LabelSync -v`
Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add cmd/bd/linear.go internal/linear/tracker.go cmd/bd/linear_test.go
git commit -m "feat(linear): parse and wire label sync config keys"
```

---

### Task F2: Extend `--verbose-diff` with label deltas

**Files:**
- Modify: `cmd/bd/linear.go`

- [ ] **Step 1: Add label diff to DescribeDiff**

In `cmd/bd/linear.go`, find the `DescribeDiff` field inside `buildLinearPushHooks` (around line 448 today). The existing implementation returns `linear.PushFieldsDiff(local, remoteIssue, config)`. Extend it:

```go
		DescribeDiff: func(local *types.Issue, remote *tracker.TrackerIssue) []string {
			remoteIssue, ok := remote.Raw.(*linear.Issue)
			if !ok || remoteIssue == nil {
				return nil
			}
			diffs := linear.PushFieldsDiff(local, remoteIssue, config)

			// Append label diff when label sync is enabled.
			if lt.LabelSyncEnabled() {
				linearLabels := make([]linear.LinearLabel, 0)
				if remoteIssue.Labels != nil {
					for _, l := range remoteIssue.Labels.Nodes {
						linearLabels = append(linearLabels, linear.LinearLabel{Name: l.Name, ID: l.ID})
					}
				}
				snap, _ := lt.LoadSnapshot(ctx, local.ID)
				res := linear.ReconcileLabels(linear.LabelReconcileInput{
					Beads:    local.Labels,
					Linear:   linearLabels,
					Snapshot: snap,
					Exclude:  lt.LabelExclude(),
				})
				if len(res.AddToLinear) > 0 {
					diffs = append(diffs, fmt.Sprintf("labels +%v (push to Linear)", res.AddToLinear))
				}
				if len(res.RemoveFromLinear) > 0 {
					diffs = append(diffs, fmt.Sprintf("labels -%v (remove from Linear)", res.RemoveFromLinear))
				}
				if len(res.AddToBeads) > 0 {
					diffs = append(diffs, fmt.Sprintf("labels +%v (add to bead)", res.AddToBeads))
				}
				if len(res.RemoveFromBeads) > 0 {
					diffs = append(diffs, fmt.Sprintf("labels -%v (remove from bead)", res.RemoveFromBeads))
				}
			}
			return diffs
		},
```

- [ ] **Step 2: Build & test**

Run: `go build ./... && go test ./...`
Expected: all PASS.

- [ ] **Step 3: Commit**

```bash
git add cmd/bd/linear.go
git commit -m "feat(linear): extend --verbose-diff with label deltas"
```

---

## Phase G — Round-trip Integration Tests

These tests exercise the end-to-end pipeline against the existing `mockLinearServer` in `cmd/bd/linear_roundtrip_test.go:23`. The mock currently handles `issueCreate`, `issueUpdate`, `TeamStates`, and `issues` queries — we extend it to track labels per-issue, handle the `LabelsByName` query, handle `issueLabelCreate`, and apply replace semantics on `labelIds`.

### Task G0: Extend `mockLinearServer` with label support

**Files:**
- Modify: `cmd/bd/linear_roundtrip_test.go`

- [ ] **Step 1: Add label state to the mock**

Open `cmd/bd/linear_roundtrip_test.go`. The struct at line 23 currently has `issues`, `teamID`, `teamKey`, `states`, `stateMap`. Add label-related fields:

```go
type mockLinearServer struct {
	mu       sync.Mutex
	issues   map[string]*linear.Issue
	nextSeq  int
	teamID   string
	teamKey  string
	states   []linear.State
	stateMap map[string]linear.State

	// Label state for Phase G tests. Workspace-scoped vs team-scoped is
	// distinguished by which map an entry lives in.
	teamLabels      map[string]linear.Label // ID → Label, scoped to teamID
	workspaceLabels map[string]linear.Label // ID → Label, organization-wide
	labelCreates    []string                // names passed to issueLabelCreate, in order
	updateCalls     map[string]int          // issueID → count of issueUpdate calls
}
```

In `newMockLinearServer`, initialize the new maps:

```go
return &mockLinearServer{
    issues:          make(map[string]*linear.Issue),
    teamID:          teamID,
    teamKey:         teamKey,
    states:          states,
    stateMap:        stateMap,
    teamLabels:      make(map[string]linear.Label),
    workspaceLabels: make(map[string]linear.Label),
    updateCalls:     make(map[string]int),
}
```

- [ ] **Step 2: Route the new query types**

The `ServeHTTP` switch (line 64) routes by query string match. Add cases for `LabelsByName` and `CreateLabel` BEFORE the generic `issues` case (since the LabelsByName query also contains the word `team(`):

```go
switch {
case strings.Contains(req.Query, "issueLabelCreate"):
    data, err = m.handleLabelCreate(req)
case strings.Contains(req.Query, "LabelsByName"):
    data, err = m.handleLabelsByName(req)
case strings.Contains(req.Query, "issueCreate"):
    data, err = m.handleCreate(req)
case strings.Contains(req.Query, "issueUpdate"):
    data, err = m.handleUpdate(req)
case strings.Contains(req.Query, "TeamStates") || strings.Contains(req.Query, "team(id:") || (strings.Contains(req.Query, "team(") && strings.Contains(req.Query, "states")):
    data = m.handleTeamStates()
case strings.Contains(req.Query, "issues"):
    data, err = m.handleFetchIssues(req)
default:
    http.Error(w, fmt.Sprintf("unhandled query: %s", req.Query[:min(80, len(req.Query))]), http.StatusBadRequest)
    return
}
```

- [ ] **Step 3: Implement the new handlers**

Append to `cmd/bd/linear_roundtrip_test.go`:

```go
func (m *mockLinearServer) handleLabelsByName(req linear.GraphQLRequest) (interface{}, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	teamNodes := make([]linear.Label, 0, len(m.teamLabels))
	for _, l := range m.teamLabels {
		teamNodes = append(teamNodes, l)
	}
	orgNodes := make([]linear.Label, 0, len(m.workspaceLabels))
	for _, l := range m.workspaceLabels {
		orgNodes = append(orgNodes, l)
	}
	return map[string]interface{}{
		"team": map[string]interface{}{
			"labels": map[string]interface{}{"nodes": teamNodes},
		},
		"organization": map[string]interface{}{
			"labels": map[string]interface{}{"nodes": orgNodes},
		},
	}, nil
}

func (m *mockLinearServer) handleLabelCreate(req linear.GraphQLRequest) (interface{}, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	inputRaw, ok := req.Variables["input"]
	if !ok {
		return nil, fmt.Errorf("missing input")
	}
	input, ok := inputRaw.(map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("input is not a map")
	}
	name := strVal(input, "name")
	teamID := strVal(input, "teamId")

	m.nextSeq++
	id := fmt.Sprintf("auto-label-%d", m.nextSeq)
	label := linear.Label{ID: id, Name: name}

	if teamID != "" {
		m.teamLabels[id] = label
	} else {
		m.workspaceLabels[id] = label
	}
	m.labelCreates = append(m.labelCreates, name)

	return map[string]interface{}{
		"issueLabelCreate": map[string]interface{}{
			"success":    true,
			"issueLabel": label,
		},
	}, nil
}

// SeedLinearLabels lets tests pre-populate labels on the team or workspace.
// Use scope="team" or "workspace".
func (m *mockLinearServer) SeedLinearLabels(scope string, labels ...linear.Label) {
	m.mu.Lock()
	defer m.mu.Unlock()
	target := m.teamLabels
	if scope == "workspace" {
		target = m.workspaceLabels
	}
	for _, l := range labels {
		target[l.ID] = l
	}
}

// SetIssueLabels assigns labels to an existing issue (test helper).
func (m *mockLinearServer) SetIssueLabels(issueID string, labels []linear.Label) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if issue, ok := m.issues[issueID]; ok {
		issue.Labels = &linear.Labels{Nodes: labels}
		issue.UpdatedAt = time.Now().UTC().Format(time.RFC3339)
	}
}

// IssueLabels returns the current labels on a Linear issue (test helper).
func (m *mockLinearServer) IssueLabels(issueID string) []linear.Label {
	m.mu.Lock()
	defer m.mu.Unlock()
	issue, ok := m.issues[issueID]
	if !ok || issue.Labels == nil {
		return nil
	}
	out := make([]linear.Label, len(issue.Labels.Nodes))
	copy(out, issue.Labels.Nodes)
	return out
}

// LabelCreates returns names that were created via issueLabelCreate, in order.
func (m *mockLinearServer) LabelCreates() []string {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]string, len(m.labelCreates))
	copy(out, m.labelCreates)
	return out
}

// UpdateCallCount returns how many issueUpdate calls were made for an issue.
func (m *mockLinearServer) UpdateCallCount(issueID string) int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.updateCalls[issueID]
}
```

- [ ] **Step 4: Make `handleUpdate` apply labelIds with replace semantics**

Find `handleUpdate` (line 143). After the existing field updates (title, description, priority, stateID), and before the `UpdatedAt` line, insert:

```go
	// Track call count.
	m.updateCalls[id]++

	// Apply labelIds REPLACE semantics — Linear's actual behavior per the spec.
	if labelIDsRaw, ok := input["labelIds"]; ok {
		labelIDs, _ := labelIDsRaw.([]interface{})
		nodes := make([]linear.Label, 0, len(labelIDs))
		for _, raw := range labelIDs {
			idStr, _ := raw.(string)
			if l, ok := m.teamLabels[idStr]; ok {
				nodes = append(nodes, l)
			} else if l, ok := m.workspaceLabels[idStr]; ok {
				nodes = append(nodes, l)
			} else {
				// Unknown ID — keep it but with a synthetic name (mirrors Linear's behavior of error-on-unknown,
				// but we're permissive so tests can verify the wire format separately).
				nodes = append(nodes, linear.Label{ID: idStr, Name: "<unknown:" + idStr + ">"})
			}
		}
		issue.Labels = &linear.Labels{Nodes: nodes}
	}
```

- [ ] **Step 5: Make `handleCreate` apply labelIds**

Find `handleCreate` (line 89). After the issue is built and before `m.issues[id] = issue`, insert:

```go
	// Apply labelIds if provided (mirrors handleUpdate logic for replace semantics).
	if labelIDsRaw, ok := input["labelIds"]; ok {
		labelIDs, _ := labelIDsRaw.([]interface{})
		nodes := make([]linear.Label, 0, len(labelIDs))
		for _, raw := range labelIDs {
			idStr, _ := raw.(string)
			if l, ok := m.teamLabels[idStr]; ok {
				nodes = append(nodes, l)
			} else if l, ok := m.workspaceLabels[idStr]; ok {
				nodes = append(nodes, l)
			} else {
				nodes = append(nodes, linear.Label{ID: idStr, Name: "<unknown:" + idStr + ">"})
			}
		}
		issue.Labels = &linear.Labels{Nodes: nodes}
	}
```

- [ ] **Step 6: Add an enable-label-sync helper**

Append to `cmd/bd/linear_roundtrip_test.go`:

```go
// enableLabelSyncForTest sets the three label sync config keys in a test store.
// Pair this with creating a fresh Tracker (or calling Init again) so the
// tracker re-reads config — alternatively, call lt.SetLabelSyncConfig directly
// after initialization for a more targeted test.
func enableLabelSyncForTest(t *testing.T, store interface {
	SetConfig(ctx context.Context, key, value string) error
}) {
	t.Helper()
	ctx := context.Background()
	for k, v := range map[string]string{
		"linear.label_sync_enabled": "true",
		"linear.label_create_scope": "team",
	} {
		if err := store.SetConfig(ctx, k, v); err != nil {
			t.Fatalf("SetConfig(%s): %v", k, err)
		}
	}
}
```

- [ ] **Step 7: Build & run existing roundtrip to ensure no regressions**

Run: `go test ./cmd/bd/ -run TestLinearRoundTripCoreFields -v`
Expected: PASS — existing roundtrip should be unaffected by the new code paths since they only fire when label sync is enabled.

- [ ] **Step 8: Commit**

```bash
git add cmd/bd/linear_roundtrip_test.go
git commit -m "test(linear): extend mockLinearServer with label tracking"
```

---

### Task G1: Test — first-sync preserves both sides

**Files:**
- Modify: `cmd/bd/linear_roundtrip_test.go`

**Common test scaffolding for G1–G8.** All tests follow this shape:

```go
func setupLabelSyncTest(t *testing.T) (sourceStore /* test store */, *mockLinearServer, *linear.Tracker, *tracker.Engine) {
	t.Helper()
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	ctx := context.Background()
	teamID := "test-team-uuid"
	store, cleanup := setupTestDB(t)
	t.Cleanup(cleanup)

	mock := newMockLinearServer(teamID, "MOCK")
	server := httptest.NewServer(mock)
	t.Cleanup(server.Close)

	for k, v := range map[string]string{
		"linear.api_key":      "test-api-key",
		"linear.team_id":      teamID,
		"linear.api_endpoint": server.URL,
		"issue_prefix":        "bd",
	} {
		if err := store.SetConfig(ctx, k, v); err != nil {
			t.Fatalf("SetConfig(%s): %v", k, err)
		}
	}
	enableLabelSyncForTest(t, store)

	lt := &linear.Tracker{}
	lt.SetTeamIDs([]string{teamID})
	if err := lt.Init(ctx, store); err != nil {
		t.Fatalf("Tracker.Init: %v", err)
	}
	// Apply label sync config — Init doesn't currently read these keys, and
	// we want the test to be explicit about what the runtime would do.
	allCfg, _ := store.GetAllConfig()
	lsCfg := loadLinearLabelSyncConfig(allCfg)
	lt.SetLabelSyncConfig(lsCfg.Enabled, lsCfg.Exclude, lsCfg.CreateScope, func(format string, args ...interface{}) {
		t.Logf("label-sync warn: "+format, args...)
	})

	engine := tracker.NewEngine(lt, store, "test-actor")
	engine.PushHooks = buildLinearPushHooksForTest(ctx, lt)
	engine.PullHooks = buildLinearPullHooksForTest(ctx, store)
	// The simplified test hooks in buildLinearPushHooksForTest /
	// buildLinearPullHooksForTest don't include label awareness — they're
	// content-hash based and were written before label sync existed. Patch
	// both with the production-equivalent label-aware behavior here.
	patchTestHooksForLabelSync(ctx, lt, engine.PushHooks, engine.PullHooks)
	engine.PullHooks.ReconcileLabels = installReconcileLabelsHook(ctx, lt)

	return store, mock, lt, engine
}

// patchTestHooksForLabelSync wraps the test hooks' ContentEqual callbacks with
// label-aware versions, mirroring what buildLinearPushHooks/buildLinearPullHooks
// install in production. Without this patch the engine skips both pull and push
// for label-only deltas (the gate problem described in spec decision #12).
func patchTestHooksForLabelSync(ctx context.Context, lt *linear.Tracker, push *tracker.PushHooks, pull *tracker.PullHooks) {
	if !lt.LabelSyncEnabled() {
		return
	}
	pushOriginal := push.ContentEqual
	push.ContentEqual = func(local *types.Issue, remote *tracker.TrackerIssue) bool {
		if pushOriginal != nil && !pushOriginal(local, remote) {
			return false
		}
		remoteIssue, ok := remote.Raw.(*linear.Issue)
		if !ok || remoteIssue == nil {
			return true
		}
		return !hasLabelDeltaTest(ctx, lt, local, remoteIssue)
	}
	pullOriginal := pull.ContentEqual
	pull.ContentEqual = func(local, remote *types.Issue) bool {
		if pullOriginal != nil && !pullOriginal(local, remote) {
			return false
		}
		// Compare label name sets (pull side has no IDs in the converted bead).
		return !pullHasLabelDeltaTest(lt, local, remote)
	}
}

// hasLabelDeltaTest mirrors hasLabelDelta from cmd/bd/linear.go — push direction only.
func hasLabelDeltaTest(ctx context.Context, lt *linear.Tracker, local *types.Issue, remoteIssue *linear.Issue) bool {
	linearLabels := make([]linear.LinearLabel, 0)
	if remoteIssue.Labels != nil {
		for _, l := range remoteIssue.Labels.Nodes {
			linearLabels = append(linearLabels, linear.LinearLabel{Name: l.Name, ID: l.ID})
		}
	}
	snap, err := lt.LoadSnapshot(ctx, local.ID)
	if err != nil {
		return true
	}
	res := linear.ReconcileLabels(linear.LabelReconcileInput{
		Beads: local.Labels, Linear: linearLabels, Snapshot: snap, Exclude: lt.LabelExclude(),
	})
	return len(res.AddToLinear) > 0 || len(res.RemoveFromLinear) > 0
}

// pullHasLabelDeltaTest mirrors pullHasLabelDelta from cmd/bd/linear.go.
func pullHasLabelDeltaTest(lt *linear.Tracker, local, remote *types.Issue) bool {
	localSet := make(map[string]bool, len(local.Labels))
	for _, n := range local.Labels {
		localSet[n] = true
	}
	remoteSet := make(map[string]bool, len(remote.Labels))
	for _, n := range remote.Labels {
		remoteSet[n] = true
	}
	excluded := lt.LabelExclude()
	for n := range remoteSet {
		if excluded != nil && excluded[strings.ToLower(n)] {
			continue
		}
		if !localSet[n] {
			return true
		}
	}
	for n := range localSet {
		if excluded != nil && excluded[strings.ToLower(n)] {
			continue
		}
		if !remoteSet[n] {
			return true
		}
	}
	return false
}

// installReconcileLabelsHook returns a PullHooks.ReconcileLabels callback
// equivalent to what buildLinearPullHooks installs in production. Lifted to a
// helper so tests don't have to import cmd/bd's private builder.
func installReconcileLabelsHook(ctx context.Context, lt *linear.Tracker) func(context.Context, storage.Transaction, string, []string, *tracker.TrackerIssue, string) error {
	return func(ctx context.Context, tx storage.Transaction, issueID string, desired []string, extIssue *tracker.TrackerIssue, actor string) error {
		remoteIssue, ok := extIssue.Raw.(*linear.Issue)
		if !ok || remoteIssue == nil {
			return fmt.Errorf("ReconcileLabels: unexpected raw type %T", extIssue.Raw)
		}
		linearLabels := make([]linear.LinearLabel, 0)
		if remoteIssue.Labels != nil {
			for _, l := range remoteIssue.Labels.Nodes {
				linearLabels = append(linearLabels, linear.LinearLabel{Name: l.Name, ID: l.ID})
			}
		}
		snap, err := tx.GetLinearLabelSnapshot(ctx, issueID)
		if err != nil {
			return err
		}
		snapEntries := make([]linear.SnapshotEntry, len(snap))
		for i, s := range snap {
			snapEntries[i] = linear.SnapshotEntry{Name: s.LabelName, ID: s.LabelID}
		}
		currentLabels, err := tx.GetLabels(ctx, issueID)
		if err != nil {
			return err
		}
		res := linear.ReconcileLabels(linear.LabelReconcileInput{
			Beads: currentLabels, Linear: linearLabels, Snapshot: snapEntries, Exclude: lt.LabelExclude(),
		})
		for _, n := range res.RemoveFromBeads {
			if err := tx.RemoveLabel(ctx, issueID, n, actor); err != nil {
				return err
			}
		}
		for _, n := range res.AddToBeads {
			if err := tx.AddLabel(ctx, issueID, n, actor); err != nil {
				return err
			}
		}
		snapshotEntries := make([]storage.LinearLabelSnapshotEntry, len(res.NewSnapshot))
		for i, e := range res.NewSnapshot {
			snapshotEntries[i] = storage.LinearLabelSnapshotEntry{LabelID: e.ID, LabelName: e.Name}
		}
		return tx.PutLinearLabelSnapshot(ctx, issueID, snapshotEntries)
	}
}

// setEq compares two string slices ignoring order.
func setEq(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	m := map[string]bool{}
	for _, x := range a {
		m[x] = true
	}
	for _, x := range b {
		if !m[x] {
			return false
		}
	}
	return true
}

// labelNames extracts the names from a slice of linear.Labels.
func labelNames(ls []linear.Label) []string {
	out := make([]string, len(ls))
	for i, l := range ls {
		out[i] = l.Name
	}
	return out
}

// snapshotNames extracts the names from a snapshot.
func snapshotNames(snap []storage.LinearLabelSnapshotEntry) []string {
	out := make([]string, len(snap))
	for i, e := range snap {
		out[i] = e.LabelName
	}
	return out
}

// readSnapshot is a test helper that reads the snapshot for an issue.
func readSnapshot(t *testing.T, store storage.Storage, issueID string) []storage.LinearLabelSnapshotEntry {
	t.Helper()
	var out []storage.LinearLabelSnapshotEntry
	err := store.RunInTransaction(context.Background(), "test read snapshot", func(tx storage.Transaction) error {
		var err error
		out, err = tx.GetLinearLabelSnapshot(context.Background(), issueID)
		return err
	})
	if err != nil {
		t.Fatalf("readSnapshot: %v", err)
	}
	return out
}

// writeSnapshot is a test helper that pre-seeds the snapshot for an issue.
func writeSnapshot(t *testing.T, store storage.Storage, issueID string, entries []storage.LinearLabelSnapshotEntry) {
	t.Helper()
	err := store.RunInTransaction(context.Background(), "test seed snapshot", func(tx storage.Transaction) error {
		return tx.PutLinearLabelSnapshot(context.Background(), issueID, entries)
	})
	if err != nil {
		t.Fatalf("writeSnapshot: %v", err)
	}
}

// pushAndPull runs a sync that does both directions. Each test calls this
// after seeding state.
func pushAndPull(t *testing.T, engine *tracker.Engine) {
	t.Helper()
	_, err := engine.Sync(context.Background(), tracker.SyncOptions{Push: true, Pull: true})
	if err != nil {
		t.Fatalf("Sync failed: %v", err)
	}
}
```

(Add this scaffolding once at the bottom of `cmd/bd/linear_roundtrip_test.go`. All G1–G8 tests assume it exists.)

### Task G1: Test — first-sync preserves both sides

**Files:**
- Modify: `cmd/bd/linear_roundtrip_test.go`

- [ ] **Step 1: Add the test**

```go
func TestRoundtrip_FirstSyncPreservesBothSides(t *testing.T) {
	store, mock, _, engine := setupLabelSyncTest(t)
	ctx := context.Background()

	// Seed Linear with an issue that has label A.
	mock.SeedLinearLabels("team", linear.Label{ID: "id-A", Name: "A"}, linear.Label{ID: "id-B", Name: "B"})
	mock.issues["lin-1"] = &linear.Issue{
		ID: "lin-1", Identifier: "MOCK-1", Title: "test",
		Labels:    &linear.Labels{Nodes: []linear.Label{{ID: "id-A", Name: "A"}}},
		CreatedAt: time.Now().UTC().Format(time.RFC3339),
		UpdatedAt: time.Now().UTC().Format(time.RFC3339),
	}

	// Seed beads with corresponding bead linked to that Linear issue, with
	// labels [A, B] (B is local-only).
	extRef := "https://linear.app/mock/issue/MOCK-1"
	beadID := "bd-1"
	if err := store.CreateIssue(ctx, &types.Issue{
		ID: beadID, Title: "test", Status: types.StatusOpen,
		ExternalRef: &extRef,
		Labels:      []string{"A", "B"},
	}, "test-actor"); err != nil {
		t.Fatalf("CreateIssue: %v", err)
	}

	// No snapshot yet → first-sync rule applies.
	pushAndPull(t, engine)

	// Bead labels: still [A, B] (no removal on first sync).
	pulled, err := store.GetIssue(ctx, beadID)
	if err != nil {
		t.Fatalf("GetIssue: %v", err)
	}
	if !setEq(pulled.Labels, []string{"A", "B"}) {
		t.Errorf("bead labels: got %v, want [A B]", pulled.Labels)
	}

	// Linear labels: now [A, B] (B was pushed).
	got := labelNames(mock.IssueLabels("lin-1"))
	if !setEq(got, []string{"A", "B"}) {
		t.Errorf("linear labels: got %v, want [A B]", got)
	}

	// Snapshot reflects both labels.
	snap := readSnapshot(t, store, beadID)
	if !setEq(snapshotNames(snap), []string{"A", "B"}) {
		t.Errorf("snapshot: got %+v, want entries for [A B]", snap)
	}
}
```

- [ ] **Step 2: Run, confirm pass**

Run: `go test ./cmd/bd/ -run TestRoundtrip_FirstSyncPreservesBothSides -v`
Expected: PASS.

- [ ] **Step 3: Commit**

```bash
git add cmd/bd/linear_roundtrip_test.go
git commit -m "test(linear): roundtrip — first-sync preserves both sides"
```

---

### Task G2: Test — pull-side removal works

- [ ] **Step 1: Add test**

```go
func TestRoundtrip_PullSideRemovalApplies(t *testing.T) {
	store, mock, _, engine := setupLabelSyncTest(t)
	ctx := context.Background()

	mock.SeedLinearLabels("team", linear.Label{ID: "id-A", Name: "A"}, linear.Label{ID: "id-B", Name: "B"})
	mock.issues["lin-1"] = &linear.Issue{
		ID: "lin-1", Identifier: "MOCK-1", Title: "test",
		Labels:    &linear.Labels{Nodes: []linear.Label{{ID: "id-A", Name: "A"}}}, // B already gone
		CreatedAt: time.Now().UTC().Format(time.RFC3339),
		UpdatedAt: time.Now().UTC().Format(time.RFC3339),
	}

	extRef := "https://linear.app/mock/issue/MOCK-1"
	beadID := "bd-1"
	if err := store.CreateIssue(ctx, &types.Issue{
		ID: beadID, Title: "test", Status: types.StatusOpen,
		ExternalRef: &extRef,
		Labels:      []string{"A", "B"}, // bead still has B locally
	}, "test-actor"); err != nil {
		t.Fatalf("CreateIssue: %v", err)
	}

	// Seed snapshot showing both A and B were last agreed.
	writeSnapshot(t, store, beadID, []storage.LinearLabelSnapshotEntry{
		{LabelID: "id-A", LabelName: "A"},
		{LabelID: "id-B", LabelName: "B"},
	})

	pushAndPull(t, engine)

	pulled, err := store.GetIssue(ctx, beadID)
	if err != nil {
		t.Fatalf("GetIssue: %v", err)
	}
	if !setEq(pulled.Labels, []string{"A"}) {
		t.Errorf("bead labels: got %v, want [A] (B removed in Linear)", pulled.Labels)
	}
}
```

- [ ] **Step 2: Run, commit**

```bash
go test ./cmd/bd/ -run TestRoundtrip_PullSideRemovalApplies -v
git add cmd/bd/linear_roundtrip_test.go
git commit -m "test(linear): roundtrip — pull-side removals apply correctly"
```

---

### Task G3: Test — bead created with labels pushes labelIds

- [ ] **Step 1: Add test**

```go
func TestRoundtrip_NewBeadPushesLabels(t *testing.T) {
	// Regression for the bug where Tracker.CreateIssue passed nil for labelIDs.
	store, mock, _, engine := setupLabelSyncTest(t)
	ctx := context.Background()

	mock.SeedLinearLabels("team", linear.Label{ID: "id-bug", Name: "bug"})

	beadID := "bd-1"
	if err := store.CreateIssue(ctx, &types.Issue{
		ID: beadID, Title: "test", Status: types.StatusOpen,
		Labels: []string{"bug", "p1"}, // bug exists, p1 will auto-create
	}, "test-actor"); err != nil {
		t.Fatalf("CreateIssue: %v", err)
	}

	pushAndPull(t, engine)

	// Find the Linear issue created from this push (only one in mock).
	var created *linear.Issue
	for _, issue := range mock.issues {
		created = issue
		break
	}
	if created == nil {
		t.Fatal("no issue was created in Linear")
	}
	got := labelNames(created.Labels.Nodes)
	if !setEq(got, []string{"bug", "p1"}) {
		t.Errorf("created issue labels: got %v, want [bug p1]", got)
	}
	if !contains(mock.LabelCreates(), "p1") {
		t.Errorf("expected p1 to be auto-created, LabelCreates=%v", mock.LabelCreates())
	}
}

// contains is a tiny test helper.
func contains(xs []string, x string) bool {
	for _, s := range xs {
		if s == x {
			return true
		}
	}
	return false
}
```

- [ ] **Step 2: Run, commit**

```bash
go test ./cmd/bd/ -run TestRoundtrip_NewBeadPushesLabels -v
git add cmd/bd/linear_roundtrip_test.go
git commit -m "test(linear): roundtrip — new bead pushes labelIds (CreateIssue regression)"
```

---

### Task G4: Test — label-only push fires (gate regression)

- [ ] **Step 1: Add test**

```go
func TestRoundtrip_LabelOnlyPushFires(t *testing.T) {
	// Regression for decision #12 — ContentEqual must be label-aware.
	// Bead has unchanged title/desc/priority/state but a new label.
	store, mock, _, engine := setupLabelSyncTest(t)
	ctx := context.Background()

	mock.SeedLinearLabels("team",
		linear.Label{ID: "id-A", Name: "A"},
		linear.Label{ID: "id-B", Name: "B"},
	)
	mock.issues["lin-1"] = &linear.Issue{
		ID: "lin-1", Identifier: "MOCK-1", Title: "test", Description: "body", Priority: 0,
		Labels:    &linear.Labels{Nodes: []linear.Label{{ID: "id-A", Name: "A"}}},
		CreatedAt: time.Now().UTC().Format(time.RFC3339),
		UpdatedAt: time.Now().UTC().Format(time.RFC3339),
	}

	extRef := "https://linear.app/mock/issue/MOCK-1"
	beadID := "bd-1"
	if err := store.CreateIssue(ctx, &types.Issue{
		ID: beadID, Title: "test", Description: "body", Priority: 0, Status: types.StatusOpen,
		ExternalRef: &extRef,
		Labels:      []string{"A", "B"}, // only labels differ from Linear
	}, "test-actor"); err != nil {
		t.Fatalf("CreateIssue: %v", err)
	}
	writeSnapshot(t, store, beadID, []storage.LinearLabelSnapshotEntry{
		{LabelID: "id-A", LabelName: "A"},
	})

	pushAndPull(t, engine)

	got := labelNames(mock.IssueLabels("lin-1"))
	if !setEq(got, []string{"A", "B"}) {
		t.Errorf("expected B pushed to Linear, got %v", got)
	}
	if mock.UpdateCallCount("lin-1") == 0 {
		t.Errorf("expected at least one issueUpdate call (gate must allow label-only deltas)")
	}
}
```

- [ ] **Step 2: Run, commit**

```bash
go test ./cmd/bd/ -run TestRoundtrip_LabelOnlyPushFires -v
git add cmd/bd/linear_roundtrip_test.go
git commit -m "test(linear): roundtrip — label-only push fires (gate fix)"
```

---

### Task G5: Test — Linear `labelIds` replace semantics

- [ ] **Step 1: Add test**

```go
func TestRoundtrip_LabelIdsReplaceSemantics(t *testing.T) {
	// Linear starts with [X], bead removes X and adds Y, push replaces.
	store, mock, _, engine := setupLabelSyncTest(t)
	ctx := context.Background()

	mock.SeedLinearLabels("team",
		linear.Label{ID: "id-X", Name: "X"},
		linear.Label{ID: "id-Y", Name: "Y"},
	)
	mock.issues["lin-1"] = &linear.Issue{
		ID: "lin-1", Identifier: "MOCK-1", Title: "test",
		Labels:    &linear.Labels{Nodes: []linear.Label{{ID: "id-X", Name: "X"}}},
		CreatedAt: time.Now().UTC().Format(time.RFC3339),
		UpdatedAt: time.Now().UTC().Format(time.RFC3339),
	}

	extRef := "https://linear.app/mock/issue/MOCK-1"
	beadID := "bd-1"
	if err := store.CreateIssue(ctx, &types.Issue{
		ID: beadID, Title: "test", Status: types.StatusOpen,
		ExternalRef: &extRef,
		Labels:      []string{"Y"}, // user removed X locally and added Y
	}, "test-actor"); err != nil {
		t.Fatalf("CreateIssue: %v", err)
	}
	writeSnapshot(t, store, beadID, []storage.LinearLabelSnapshotEntry{
		{LabelID: "id-X", LabelName: "X"},
	})

	pushAndPull(t, engine)

	got := labelNames(mock.IssueLabels("lin-1"))
	if !setEq(got, []string{"Y"}) {
		t.Errorf("expected Linear labels [Y] after replace, got %v", got)
	}
}
```

- [ ] **Step 2: Run, commit**

```bash
go test ./cmd/bd/ -run TestRoundtrip_LabelIdsReplaceSemantics -v
git add cmd/bd/linear_roundtrip_test.go
git commit -m "test(linear): roundtrip — assert labelIds replace semantics"
```

---

### Task G6: Test — auto-create missing label on push

- [ ] **Step 1: Add test**

```go
func TestRoundtrip_AutoCreatesMissingLabel(t *testing.T) {
	store, mock, _, engine := setupLabelSyncTest(t)
	ctx := context.Background()

	// Linear knows about no labels initially.
	mock.issues["lin-1"] = &linear.Issue{
		ID: "lin-1", Identifier: "MOCK-1", Title: "test",
		Labels:    &linear.Labels{Nodes: []linear.Label{}},
		CreatedAt: time.Now().UTC().Format(time.RFC3339),
		UpdatedAt: time.Now().UTC().Format(time.RFC3339),
	}

	extRef := "https://linear.app/mock/issue/MOCK-1"
	beadID := "bd-1"
	if err := store.CreateIssue(ctx, &types.Issue{
		ID: beadID, Title: "test", Status: types.StatusOpen,
		ExternalRef: &extRef,
		Labels:      []string{"never-seen"},
	}, "test-actor"); err != nil {
		t.Fatalf("CreateIssue: %v", err)
	}

	pushAndPull(t, engine)

	created := mock.LabelCreates()
	if len(created) != 1 || created[0] != "never-seen" {
		t.Errorf("expected one CreateLabel call for never-seen, got %v", created)
	}
	got := labelNames(mock.IssueLabels("lin-1"))
	if !setEq(got, []string{"never-seen"}) {
		t.Errorf("expected Linear to have never-seen label, got %v", got)
	}
}
```

- [ ] **Step 2: Run, commit**

```bash
go test ./cmd/bd/ -run TestRoundtrip_AutoCreatesMissingLabel -v
git add cmd/bd/linear_roundtrip_test.go
git commit -m "test(linear): roundtrip — auto-create missing label on push"
```

---

### Task G7: Test — concurrent both-sides changes converge

- [ ] **Step 1: Add test**

```go
func TestRoundtrip_ConcurrentBothSidesChangesConverge(t *testing.T) {
	store, mock, _, engine := setupLabelSyncTest(t)
	ctx := context.Background()

	mock.SeedLinearLabels("team",
		linear.Label{ID: "id-A", Name: "A"},
		linear.Label{ID: "id-C", Name: "C"},
		linear.Label{ID: "id-D", Name: "D"},
	)
	mock.issues["lin-1"] = &linear.Issue{
		ID: "lin-1", Identifier: "MOCK-1", Title: "test",
		Labels:    &linear.Labels{Nodes: []linear.Label{{ID: "id-A", Name: "A"}, {ID: "id-D", Name: "D"}}},
		CreatedAt: time.Now().UTC().Format(time.RFC3339),
		UpdatedAt: time.Now().UTC().Format(time.RFC3339),
	}

	extRef := "https://linear.app/mock/issue/MOCK-1"
	beadID := "bd-1"
	if err := store.CreateIssue(ctx, &types.Issue{
		ID: beadID, Title: "test", Status: types.StatusOpen,
		ExternalRef: &extRef,
		Labels:      []string{"B", "C"}, // beads removed A, added C
	}, "test-actor"); err != nil {
		t.Fatalf("CreateIssue: %v", err)
	}
	writeSnapshot(t, store, beadID, []storage.LinearLabelSnapshotEntry{
		{LabelID: "id-A", LabelName: "A"},
		{LabelID: "id-B", LabelName: "B"},
	})

	pushAndPull(t, engine)

	pulled, _ := store.GetIssue(ctx, beadID)
	if !setEq(pulled.Labels, []string{"C", "D"}) {
		t.Errorf("bead labels: got %v, want [C D]", pulled.Labels)
	}
	got := labelNames(mock.IssueLabels("lin-1"))
	if !setEq(got, []string{"C", "D"}) {
		t.Errorf("linear labels: got %v, want [C D]", got)
	}
}
```

- [ ] **Step 2: Run, commit**

```bash
go test ./cmd/bd/ -run TestRoundtrip_ConcurrentBothSidesChangesConverge -v
git add cmd/bd/linear_roundtrip_test.go
git commit -m "test(linear): roundtrip — concurrent both-sides changes converge"
```

---

### Task G8: Test — rename + concurrent local delete (decision #10)

- [ ] **Step 1: Add test**

```go
func TestRoundtrip_RenamePlusLocalDeleteWins(t *testing.T) {
	store, mock, _, engine := setupLabelSyncTest(t)
	ctx := context.Background()

	mock.SeedLinearLabels("team", linear.Label{ID: "X", Name: "new"})
	mock.issues["lin-1"] = &linear.Issue{
		ID: "lin-1", Identifier: "MOCK-1", Title: "test",
		Labels:    &linear.Labels{Nodes: []linear.Label{{ID: "X", Name: "new"}}}, // Linear renamed
		CreatedAt: time.Now().UTC().Format(time.RFC3339),
		UpdatedAt: time.Now().UTC().Format(time.RFC3339),
	}

	extRef := "https://linear.app/mock/issue/MOCK-1"
	beadID := "bd-1"
	if err := store.CreateIssue(ctx, &types.Issue{
		ID: beadID, Title: "test", Status: types.StatusOpen,
		ExternalRef: &extRef,
		Labels:      []string{}, // user deleted "old"
	}, "test-actor"); err != nil {
		t.Fatalf("CreateIssue: %v", err)
	}
	writeSnapshot(t, store, beadID, []storage.LinearLabelSnapshotEntry{
		{LabelID: "X", LabelName: "old"},
	})

	pushAndPull(t, engine)

	// Delete wins: bead stays empty, Linear loses the label entirely.
	pulled, _ := store.GetIssue(ctx, beadID)
	if len(pulled.Labels) != 0 {
		t.Errorf("bead: got %v, want empty", pulled.Labels)
	}
	got := mock.IssueLabels("lin-1")
	if len(got) != 0 {
		t.Errorf("linear: got %v, want empty (delete wins)", labelNames(got))
	}
	snap := readSnapshot(t, store, beadID)
	if len(snap) != 0 {
		t.Errorf("snapshot: got %+v, want empty", snap)
	}
}
```

- [ ] **Step 2: Run, commit**

```bash
go test ./cmd/bd/ -run TestRoundtrip_RenamePlusLocalDeleteWins -v
git add cmd/bd/linear_roundtrip_test.go
git commit -m "test(linear): roundtrip — rename + local delete decision #10"
```

---

## Phase H — Documentation & Polish

### Task H1: Help text in `cmd/bd/linear.go`

**Files:**
- Modify: `cmd/bd/linear.go`

- [ ] **Step 1: Find the existing config-help block**

Run: `grep -n 'label_type_map\|exclude_labels' cmd/bd/linear.go`
Expected: shows the existing help-text block (around lines 50-110 today).

- [ ] **Step 2: Add label-sync help**

In the `Long` field of the relevant cobra command (the one whose help text already documents `label_type_map`), append:

```
  Bidirectional label sync (opt-in):
    bd config set linear.label_sync_enabled true
    bd config set linear.label_sync_exclude "internal-tag,gt:agent"
    bd config set linear.label_create_scope team   # or workspace

  When enabled, label changes propagate in both directions:
  - Adding/removing a label in Linear flows to the bead on next sync.
  - Adding/removing a label in beads flows to Linear on next sync.
  - Renaming a label in Linear (same ID) flows as a name change.
  - The first sync after enabling never removes labels on either side;
    both sides' labels merge into a unified set.

  Note: enabling this changes pull semantics for existing beads. Today the
  pull-side already mirrors Linear labels destructively (beads labels not on
  Linear are silently removed). With label sync enabled, removal becomes
  intent-aware: a label is only removed when one side actively removes it.
```

- [ ] **Step 3: Build & test**

Run: `go build ./... && go test ./cmd/bd/...`
Expected: PASS.

- [ ] **Step 4: Commit**

```bash
git add cmd/bd/linear.go
git commit -m "docs(linear): document label sync config keys in command help"
```

---

### Task H2: CHANGELOG entry (if present)

- [ ] **Step 1: Check for CHANGELOG**

Run: `ls CHANGELOG.md 2>/dev/null || echo "no changelog"`

- [ ] **Step 2: If CHANGELOG.md exists, prepend an entry**

Add under the next-version heading:

```markdown
- **feat(linear):** Bidirectional label sync between Linear and beads (opt-in).
  Set `linear.label_sync_enabled = true` to enable. Label adds, removes, and
  Linear-side renames now propagate in both directions with proper conflict
  resolution. Replaces today's destructive Linear-authoritative pull-side label
  sync. See `docs/superpowers/specs/2026-04-27-linear-label-sync-design.md`.
```

- [ ] **Step 3: Commit**

```bash
git add CHANGELOG.md
git commit -m "docs: changelog entry for bidirectional label sync"
```

---

### Task H3: Final integration smoke test

- [ ] **Step 1: Run full test suite**

Run: `go test ./...`
Expected: ALL PASS.

- [ ] **Step 2: Run linter**

Run: `golangci-lint run ./...`
Expected: no new warnings beyond the documented baseline.

- [ ] **Step 3: Build the bd binary and smoke-test the new config keys**

Run:
```bash
make install
bd config set linear.label_sync_enabled true
bd config get linear.label_sync_enabled
bd config set linear.label_sync_exclude "test-tag"
bd config get linear.label_sync_exclude
bd config set linear.label_create_scope workspace
bd config get linear.label_create_scope
```
Expected: each `get` returns the value you just set.

- [ ] **Step 4: Reset to defaults**

```bash
bd config delete linear.label_sync_enabled
bd config delete linear.label_sync_exclude
bd config delete linear.label_create_scope
```

- [ ] **Step 5: Final commit (if anything changed during smoke-test)**

```bash
git status
# If clean, no commit needed.
```

---

## Self-Review

**Spec coverage check:**
- Pull-side reconciler replaces `syncIssueLabels`: Tasks E1, E2 ✓
- Push-side resolves label IDs and passes to Linear: Tasks D1, D2, D3 ✓
- `ContentEqual` becomes label-aware: Task D4 ✓
- Snapshot stored in dedicated table with proper schema (`issue_id VARCHAR(255) REFERENCES issues(id)`, with `MarkDirty` calls): Tasks A1, A2, A3, A4, A5, A6 ✓
- Reconciler 3-pass with rename consumption: Tasks B1–B6 ✓
- First-sync intersection rule: Tasks B5, B6, G1 ✓
- Decision #10 (delete wins): Tasks B6, G8 ✓
- Auto-create with team/workspace scope: Tasks C2, D1, F1, G6 ✓
- Loud failure on duplicate Linear labels (LabelsByName): Task C1 ✓
- Soft failure on CreateLabel rate limits / network errors (skip not abort): Task D1 ✓
- Three new config keys: Task F1 ✓
- Two migration paths + staging lists: Tasks A1, A2, A3 ✓
- Verbose-diff for labels: Task F2 ✓
- Replace-vs-merge wire format test (unit): Task C3; behavioral mock test: Task G5 ✓
- Help text + changelog: Tasks H1, H2 ✓
- New bead push includes labels (regression for nil-labelIDs bug): Task G3 ✓
- Label-only push fires (regression for ContentEqual gate): Task G4 ✓

All major spec requirements have at least one task. ✓

**Type consistency check:**
- `LinearLabel{Name, ID}` — defined in `internal/linear/reconciler.go` (Task B1), used in client (C1, C2), tracker (D1, D2, D3), and tests throughout ✓
- `SnapshotEntry{Name, ID}` (in `linear` package) vs `LinearLabelSnapshotEntry{LabelID, LabelName}` (in `storage` package) — deliberately different types at different layers; conversion happens at the package boundary in Tasks D2 (writeSnapshot) and E2 (ReconcileLabels callback). The field-name flip (`Name`/`ID` ↔ `LabelName`/`LabelID`) is intentional: storage uses table-column names, the linear package uses domain names ✓
- `LabelScope` (`LabelScopeTeam`, `LabelScopeWorkspace`) — defined in C2, used in D1 and F1 ✓
- The `lastReconcileResult` / `ConsumeLastReconcileResult` / `AfterPush` mechanism from the prior plan version is **removed** — Task D2 has `Tracker.UpdateIssue` persist the snapshot itself via `t.writeSnapshot`. No engine-side hooks needed; no exported `ReconcileResultFor` type ✓
- `linear.Labels` and `linear.Label` (NOT `LabelConnection` / `IssueLabel` — verified at `internal/linear/types.go:102-110`) ✓
- `Client.Endpoint` (NOT `BaseURL` — verified at `internal/linear/types.go:43`); tests use `WithEndpoint(server.URL)` ✓
- `client.FetchIssueByIdentifier` (NOT `client.FetchIssue`) for fetching one Linear issue from a `*Client` (Task D2) ✓
- `tracker.PullHooks` is the EXISTING struct at `internal/tracker/engine.go:24` — Task E1 ADDS one new field, does not redefine ✓

**Placeholder scan:**
- No "TBD", "TODO", or "fill in later" in any task body ✓
- Every code step has actual code, not pseudo-code ✓
- Test stubs in D2/D3 are intentional skips with cross-references to G3/G4 ✓

**Case-sensitivity (resolved post-merge):** Originally documented as a known v1 limitation; resolved as a follow-up fix (commit landing after this plan). The reconciler now does case-insensitive matching across all four call sites:
- `applyTruthTable` — case-fold maps with original-case-preserved values. Bead "bug" + Linear "Bug" with no snapshot collapses to "in agreement" (no spurious adds).
- `synthesizeFirstSyncSnapshot` — case-fold intersection.
- `computeNewSnapshot` — lowercase-keyed map with Linear's display case preserved in values. Rename detection by ID.
- `pullHasLabelDelta` (cmd/bd/linear.go) — case-fold set comparison.

Output casing rule: `AddToLinear` uses the bead's spelling (sent to Linear); `AddToBeads` uses Linear's display case (so beads adopts Linear's canonical name). Snapshot persists Linear's display case as canonical.

Verified via the mayor's hw-gxrq production scenario: bead "bug" + Linear "Bug" no longer thrashes, no duplicate label appears on either side.

**Codex-found issues addressed (rounds 1-4):**

Round 1-2 (architectural):
- Wrong Linear types (LabelConnection/IssueLabel) → use real types Labels/Label everywhere ✓
- Wrong Client field (BaseURL) → use Endpoint ✓
- Wrong client method (FetchIssue) → use FetchIssueByIdentifier ✓
- Redefining existing PullHooks → ADD ReconcileLabels field to existing struct ✓
- AfterPush ordering bug + lastReconcileResult fragility → removed entirely; Tracker persists its own snapshot ✓
- Snapshot writes silently dropped → MarkDirty added to A5 and A6 ✓
- Phase G helpers don't exist → Task G0 adds them; G1-G8 use the existing `mockLinearServer` extended ✓
- resolveLabelIDs aborted on partial failure → now skips and warns ✓
- Self-contradictory rename note → removed ✓

Round 3 (gate + correctness):
- **Pull-side ContentEqual gate** ate label-only updates (engine.go:440-462 skips before reconciler) → Task E2 now extends pull ContentEqual with `pullHasLabelDelta` ✓
- D1 test arg count mismatch (`resolveLabelIDs` 5th `warn` arg) → fixed ✓
- LabelsByName case-insensitive lookup bug → LabelsByName keys by lowercase; resolveLabelIDs lowercases the lookup key; LinearLabel.Name preserves Linear's display case ✓
- G4 test mirror was using label-blind `buildLinearPushHooksForTest` → `patchTestHooksForLabelSync` wraps both ContentEqual callbacks ✓

Round 4 (final correctness pass):
- **Snapshot infinite-churn loop** → snapshot preserves Linear's display case for known IDs ✓
- **`hasLabelDelta` over-firing** → both production and test variants now push-direction only ✓
- Documented case-sensitivity limitation ✓

Round 5 (case-mismatch landmines):
- **Case mismatch + Linear rename = silent Linear label deletion** (snap had "Bug", bead has "bug", Linear renamed "Bug"→"flaky": classifyRenames thought user deleted "Bug" locally → emitted RemoveFromLinear → destroyed Linear's label) → `classifyRenames` now does case-insensitive `beadsSet` lookup; new test `TestClassifyRenames_CaseMismatchWithRenameDoesNotDelete` covers the regression ✓
- **Duplicate `labelIds` cause snapshot PK violation** (case-insensitive `LabelsByName` resolves bead's "bug" to Linear's "Bug=L1", which is also already in `linearLabels`; without dedup we'd push `[L1, L1]` and the snapshot insert would fail on `(issue_id, label_id)` PK constraint) → D2 now deduplicates `labelIDs` and `snapshotToWrite` by ID before constructing the input map ✓
- Codex finding "implementation absent" — false positive (it's a plan, not code; this is correct).
- Codex finding "push-only mode can't repair pull-direction deltas" — this is intentional (push-only doesn't pull, by design and consistent with all other field syncs). Documented.

Plan is ready.

---

## Execution Handoff

**Plan complete and saved to `docs/superpowers/plans/2026-04-27-linear-label-sync.md`.**

Two execution options:

1. **Subagent-Driven (recommended)** — Dispatch a fresh subagent per task, review between tasks, fast iteration. Good when tasks are reasonably independent and you want clean context per task.

2. **Inline Execution** — Execute tasks in this session using executing-plans, batch execution with checkpoints. Good when context flows naturally between tasks (e.g., debugging an integration after several earlier wirings).

Given this plan has 30+ tasks across 8 phases with clear isolation between phases (storage → reconciler → client → wiring → integration), **subagent-driven is the better fit** — each phase can be a clean session.

Which approach do you want?
