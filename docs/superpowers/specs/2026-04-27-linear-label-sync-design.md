# Bidirectional Linear ↔ beads label sync

**Status:** Design
**Date:** 2026-04-27
**Branch:** `feat/linear-bead-sync-upstream-pr`

## Problem

Today's Linear sync handles labels asymmetrically:

- **Pull side already exists, but is destructive.** `internal/tracker/engine.go:496` calls `syncIssueLabels` (`engine.go:644`) on every pull, which forces beads' label set to match Linear's: any beads label not present on the Linear issue is removed, any Linear label not on the bead is added. This treats Linear as authoritative on every sync, with no awareness of intent or prior state.
- **Push side does not exist.** `internal/linear/mapping.go:608-638` defines `PushFieldsEqual`, which deliberately excludes labels (per the comment at `mapping.go:609-610`). A beads-only label change (add or remove) does not change the push gate's verdict, so no `IssueUpdate` mutation is sent for label-only deltas. `Tracker.UpdateIssue` (`internal/linear/tracker.go:175`) never passes `labelIds` to Linear.
- **Linear label names ARE consumed by `linear.label_type_map`** for type inference (e.g. `bug` label → `type=bug`). That logic is orthogonal to label storage and stays unchanged.
- **Bead-level filtering** via `linear.exclude_labels` is a different concern entirely — it skips whole beads from sync — and stays unchanged.

The user-visible result: a label added in beads silently fails to reach Linear, and a label removed in either system has different semantics depending on which side removed it (Linear-side removal flows through; beads-side removal is reverted on the next pull).

We want symmetric, intent-aware bidirectional sync: a label added or removed in either system propagates to the other on the next sync, with proper handling of concurrent changes.

## Goals

- Every Linear label on a synced issue appears as a beads label on the corresponding bead.
- Every beads label on a synced bead appears as a Linear label on the corresponding issue.
- Removing a label in one system removes it from the other on the next sync.
- Renaming a Linear label flows through as a name update (not a destroy + recreate).
- Per-label exclusion: users can mark specific label names as "do not sync".
- The `label_type_map` keyword consumption stays orthogonal — type inference still runs, and labels still mirror by default. Users who want the consume behavior add the type-map keywords to `label_sync_exclude`.

## Non-goals

- **Linear label groups.** Linear lets users organize labels into mutually-exclusive groups (e.g., a "Type" group containing `bug`/`feature`). For v1, group membership is flattened on pull (we take the leaf name). Auto-created labels on push are always ungrouped. Existing grouped labels still sync as plain names.
- **Linear label colors.** Beads has no color metadata. Colors are ignored in both directions.
- **Beads-side rename detection.** Beads labels are flat strings with no stable ID. A user rename in beads manifests as remove + add through the reconciler — which is the intended behavior.
- **Eager label creation on Linear.** A beads-only label is only created on Linear when it is attached to a synced issue, not the moment it is created.
- **Webhook-driven sync.** Sync remains batch (`bd sync` and friends), consistent with the rest of the pipeline.
- **The existing bead-level `linear.exclude_labels` filter.** That key continues to filter whole beads from syncing; it is unchanged and unrelated to this feature.

## Decisions log

These were resolved during brainstorming and are referenced throughout the design.

| # | Decision | Choice |
|---|---|---|
| 1 | Mirroring vs. consuming type-map keywords | **Mirror** — every Linear label becomes a beads label. Type-map runs in addition. |
| 2 | Push direction symmetry | **Mirror what exists, never invent.** Type does *not* generate labels on push. |
| 3 | Missing labels on Linear at push time | **Auto-create.** |
| 4 | Conflict resolution | **3-way merge** with stored prior state per bead. |
| 5 | Auto-create scope | **Configurable**, default `team`. |
| 6 | Per-label exclusion | **One config key**: `linear.label_sync_exclude`. No implicit coupling to `label_type_map`. |
| 7 | Duplicate Linear labels with same name | **Fail loudly** — surface ambiguity, block this issue's push. |
| 8 | Snapshot stores names only vs. names + IDs | **Names + IDs**, to detect Linear-side renames. |
| 9 | First sync (no snapshot) | **Linear-authoritative on first encounter**: pull all Linear labels into beads, push only beads labels not yet on Linear. Never delete on first sync. |
| 10 | Rename + concurrent local delete | **Delete wins.** If snapshot has `{name:old, id:X}`, beads removed `old`, Linear renamed to `{name:new, id:X}` — the rename is moot because the user expressed intent to remove. Result: no label on bead; `old` removed from Linear (which Linear has already done as part of the rename). |
| 11 | Snapshot storage | **Dedicated table** (`linear_label_snapshots`), keyed by issue UUID. Not the global config k/v store. |
| 12 | Push gate for labels | **Make `ContentEqual` label-aware.** The engine-level skip (`internal/tracker/engine.go:908`) calls the Linear-specific `ContentEqual` hook (`cmd/bd/linear.go:437`), which today wraps `PushFieldsEqual` and ignores labels. We make `ContentEqual` also consult the label reconciler: if there is a label delta to push, `ContentEqual` returns false and the engine proceeds into the push path. `PushFieldsEqual` stays unchanged — it remains a *content* equality check; the label-aware behavior lives in the wrapping hook. The push path then issues a single `IssueUpdate` carrying both field changes and `labelIds`. |

## Architecture

Three integration points, all on existing scaffolding plus one new persistence type.

### Components

```
                              ┌─────────────────────────────┐
                              │       reconcileLabels()     │
                              │       (pure function)       │
                              └──────────┬──────────────────┘
                                         │
        ┌───────────────────┬────────────┼────────────┬───────────────────────┐
        │                   │            │            │                       │
   pull labels        load snapshot      │      apply mutations        write snapshot
   (Linear → mem)   (linear_label_       │     ┌──────┴──────┐         (linear_label_
                     snapshots table)    │     │             │          snapshots table)
                                         │  push to Linear   update bead
                                         │ (auto-create      (tx.AddLabel /
                                         │  if needed)        tx.RemoveLabel)
                                         │
                                         └── all writes inside engine.go RunInTransaction ──
```

### New / changed code

| File | Change |
|---|---|
| `internal/linear/reconciler.go` (new) | Pure `reconcileLabels()` function and types. |
| `internal/linear/reconciler_test.go` (new) | Exhaustive table-driven unit tests. |
| `internal/tracker/engine.go` | Replace the `syncIssueLabels` call at line 496 with the reconciler-driven flow. Snapshot read/write happens inside the existing `RunInTransaction` block (line 492) so label mutations and snapshot updates are atomic. |
| `internal/linear/tracker.go` | Pass resolved `labelIds` in both `CreateIssue` (line 152) and `UpdateIssue` (line 175). Apply exclusion filter at the boundary. |
| `internal/linear/client.go` | Add `LabelsByName` (with team + workspace lookup) and `CreateLabel`. Extend `UpdateIssue` to accept `labelIds` in the input map. `CreateIssue` already accepts `labelIDs` (line 453) — start passing it. |
| `internal/linear/mapping.go` | `PushFieldsEqual` unchanged — labels stay outside that function by design (decision #12). |
| `cmd/bd/linear.go` | Make `ContentEqual` (line 437) label-aware: when label sync is enabled and the reconciler reports a non-empty delta, return false. Read new config keys. Extend `--verbose-diff` output to show label deltas. |
| `internal/storage/storage.go` | Extend the `Transaction` interface with snapshot CRUD methods (read all snapshots for an issue, write the full snapshot set, delete on bead removal — though FK cascade handles delete). |
| `internal/storage/schema/migrations/0033_create_linear_label_snapshots.up.sql` (new) | Canonical schema migration. Schema in [Persistence](#persistence). |
| `internal/storage/schema/migrations/0033_create_linear_label_snapshots.down.sql` (new) | Companion down migration. |
| `internal/storage/dolt/migrations/018_linear_label_snapshots.go` (new) | Compat migration that mirrors 0033 for older databases that bypass the canonical path. |
| `internal/storage/dolt/store.go` | Add `linear_label_snapshots` to the staging lists at lines 1467-1475. |
| `internal/storage/dolt/migrations/runner.go` | Add `linear_label_snapshots` to the staging list at lines 68-75. |
| `internal/storage/dolt/transaction.go` | Implement the new `Transaction` interface methods on `doltTransaction`. |
| `internal/storage/embeddeddolt/transaction.go` | Implement the same on `embeddedTransaction`. |
| `internal/storage/dolt/linear_label_snapshots.go` (new) | Helper SQL builders shared by both transaction implementations. |
| `cmd/bd/linear_roundtrip_test.go` | Extend with label scenarios including rename via ID match. |

## Reconciler logic

The reconciler is a pure function. All sync state passes through it; pull and push become dumb appliers.

### Types

```go
type SnapshotEntry struct {
    Name string `json:"name"`
    ID   string `json:"id"` // Linear label ID, never empty for confirmed entries
}

type LabelReconcileInput struct {
    Beads    []string                  // current beads labels (post-exclusion)
    Linear   []LinearLabel             // current Linear labels with IDs (post-exclusion)
    Snapshot []SnapshotEntry           // last-synced state (post-exclusion)
    Exclude  map[string]bool           // labels to ignore entirely (lowercased)
}

type LinearLabel struct {
    Name string
    ID   string
}

type LabelReconcileResult struct {
    AddToBeads       []string         // names to add to bead
    RemoveFromBeads  []string         // names to remove from bead
    AddToLinear      []string         // names to push (auto-create if missing)
    RemoveFromLinear []string         // Linear label IDs to detach
    NewSnapshot      []SnapshotEntry  // becomes next call's Snapshot
    RenamesApplied   []LabelRename    // for verbose-diff output
}

type LabelRename struct {
    OldName string
    NewName string
    ID      string
}
```

### Reconciliation order

The reconciler proceeds in three passes. **Order is significant** — earlier passes can consume rows that later passes would otherwise process.

1. **Apply exclusion filter** to all three input sets. Excluded labels become invisible to subsequent passes.

2. **Classify each Linear-snapshot ID match.** For every Linear label whose ID also appears in the snapshot:
   - **Names equal** → mark as "in agreement"; consumed by *both* the snapshot row and the Linear row from pass-3 input.
   - **Names differ AND snapshot's old name is still in beads** → **applied rename**: emit `RemoveFromBeads(oldName) + AddToBeads(newName)` and `RenamesApplied`. Consume the snapshot row, the Linear row, AND the beads row for `oldName`. The new snapshot entry has `{name: newName, id: sameID}`.
   - **Names differ AND snapshot's old name is NOT in beads** (user deleted) → **dropped rename**: emit `RemoveFromLinear(sameID)`. Consume the snapshot row, the Linear row, and any beads row for `newName` (i.e., do *not* later add `newName` to anything — Linear has it, but the user just deleted the same logical label, so we honor the delete). The snapshot entry is dropped.

   "Consume" means: the row is removed from the inputs to pass 3 so the truth table never sees it. This avoids both the "double add" bug and the "ghost rename" bug.

3. **Run per-label decision table** on whatever rows remain after pass 2 consumed renames. Match by name across the three remaining sets. Every cell in the table has an unambiguous answer (see [Per-label decision table](#per-label-decision-table)).

The pass-2 row consumption is the rule that makes #10 (delete wins) work. The applied-rename case mirrors what the user expects (the name updates and follows the issue); the dropped-rename case honors the user's local delete by removing the label from Linear and discarding the snapshot row entirely.

### Per-label decision table

For each label that appears in *any* of `Beads`, `Linear`, `Snapshot` (after exclusion), match by name (with rename remapping per the rule above).

| Snapshot | Beads | Linear | Meaning | Action |
|:--------:|:-----:|:------:|---------|--------|
| ✓ | ✓ | ✓ | Unchanged | nothing |
| ✗ | ✓ | ✗ | Added in beads | push to Linear |
| ✗ | ✗ | ✓ | Added in Linear | add to bead |
| ✗ | ✓ | ✓ | Added on both sides (already in agreement) | nothing |
| ✓ | ✗ | ✓ | Removed in beads | remove from Linear |
| ✓ | ✓ | ✗ | Removed in Linear | remove from bead |
| ✓ | ✗ | ✗ | Removed on both sides (already in agreement) | nothing |

There are no genuine label-level conflicts in the table — every cell has an unambiguous answer. Apparent "conflicts" at the bead level (e.g., user removes `bug` in beads while someone in Linear removes `bug` and adds `regression`) decompose into per-label rows that all agree.

### First-sync rule (no snapshot)

If `Snapshot` is empty for a bead — meaning sync was just enabled, or this is a brand-new bead — we deviate from the pure 3-way merge:

- **Treat snapshot as equal to the intersection of `Beads` and `Linear`** (matched by name; IDs come from Linear for the matched labels). Then run the standard truth table.
- Effect on each label class:
  - Label in both sides: snapshot:✓, beads:✓, linear:✓ → nothing.
  - Label only in Linear: snapshot:✗, beads:✗, linear:✓ → add to bead.
  - Label only in beads: snapshot:✗, beads:✓, linear:✗ → push to Linear (auto-create).
  - **Nothing is removed on either side**, because the "removed" rows of the truth table all require snapshot:✓, and the synthesized snapshot only contains labels present on both sides.
- After this sync completes, snapshot is written normally (now the union of both sides plus IDs) and subsequent syncs use the standard 3-way merge.

This avoids accidentally nuking either side's label set the moment the feature is turned on.

### Rename detection

Renames flow Linear → beads only. Detection and application both happen in pass 2 of the reconciliation order — see the classification rules there. Summary:

- **Applied rename** (user kept the old name locally): emit `RemoveFromBeads(oldName) + AddToBeads(newName)` and append a `RenamesApplied` entry for verbose-diff output. Update the snapshot entry's name; ID stays. The result is one `label_removed` + one `label_added` event in the audit log; acceptable for v1 (can be optimized later if `Storage.RenameLabel` and a rename event get added).
- **Dropped rename** (user deleted the old name locally): emit `RemoveFromLinear(sameID)`, consume the new-name beads row to suppress any spurious `AddToBeads(newName)`, and drop the snapshot entry. The user's delete intent wins (decision #10).

Beads-side renames are not detectable. A user removing `p1` and adding `priority-1` in beads manifests as a Linear-side delete + create, which is the correct semantic for what the user did.

### Atomicity & partial failure

The snapshot, the bead's label mutations, and the audit-log events are all written in the **same transaction** as the rest of the pull-side issue update. The natural place is inside the `RunInTransaction` block at `internal/tracker/engine.go:492`. If any local mutation fails, the transaction rolls back and the snapshot is unchanged — the next sync re-evaluates from the prior snapshot state.

Push-side failures are different (they cross a network boundary and can't be transactional with the local store):

| Failure case | Behavior |
|---|---|
| `CreateLabel` fails for one label name | Skip that label in this push. Compute the snapshot from labels that *did* succeed. The omitted label stays in beads but not in snapshot, so next sync sees it as a fresh add and retries `CreateLabel`. |
| `IssueUpdate` fails after labels were resolved/created | Do not write the snapshot. Pull-side mutations also roll back if the same transaction was attempted. Next sync re-runs the reconciler from the prior snapshot. |
| Local mutation (`AddLabel`/`RemoveLabel`) fails inside the transaction | Whole transaction rolls back — snapshot, bead labels, and any push side-effects already issued are out of sync. Push side-effects can't be undone, so this case results in a "snapshot lags push" state. Next sync sees Linear's new state but old snapshot, and reconciles correctly because the truth table handles divergence. |

## Push path

Push translates label *names* (beads' representation) to Linear label *IDs* (the GraphQL mutation's representation). The push fires for two reasons:

1. **`ContentEqual` becomes label-aware** so the engine-level skip at `internal/tracker/engine.go:908` no longer eats label-only changes. The Linear `ContentEqual` callback at `cmd/bd/linear.go:437` is extended:

   ```go
   ContentEqual: func(local *types.Issue, remote *tracker.TrackerIssue) bool {
       if !linear.PushFieldsEqual(local, remoteIssue, config) {
           return false
       }
       if labelSyncEnabled && hasLabelDelta(local, remote) {
           return false
       }
       return true
   },
   ```

   `hasLabelDelta` does a lightweight reconciler dry-run using the cached snapshot — it returns `true` if the reconciler would emit any `AddToLinear` or `RemoveFromLinear`. We do *not* run the full reconciler here; that runs once inside the push path and produces the same result. The `hasLabelDelta` check exists only to flip the gate; the cached snapshot is read once per sync per bead.

2. **`Tracker.UpdateIssue` and `Tracker.CreateIssue` resolve and pass `labelIds`** to the underlying mutations.

   `UpdateIssue` (`internal/linear/tracker.go:175`):
   ```go
   func (t *Tracker) UpdateIssue(ctx, externalID, issue) {
       client := t.clientForExternalID(ctx, externalID)
       reconciled := reconcileLabels(...)  // full reconciler run
       labelIDs := t.resolveLabelIDs(ctx, client, reconciled.AddToLinear, currentLinear)

       // One IssueUpdate carries both field changes and labelIds.
       updates := buildIssueUpdateInput(local, labelIDs)
       return client.UpdateIssue(ctx, externalID, updates)
   }
   ```

   `CreateIssue` (`internal/linear/tracker.go:152`) — currently passes `nil` for `labelIDs`. We change it to:
   ```go
   labelIDs := t.resolveLabelIDs(ctx, client, issue.Labels, nil /* no current */)
   created, err := client.CreateIssue(ctx, issue.Title, issue.Description, priority, stateID, labelIDs)
   ```
   For a brand-new bead being mirrored to Linear, every bead label is a fresh add; resolution just means `LabelsByName` + `CreateLabel` for the missing ones. After creation, write the snapshot reflecting the new state.

This keeps the network footprint at one mutation per bead per sync regardless of which fields changed.

### Step 1 — Resolve existing labels

New client method:

```go
func (c *Client) LabelsByName(ctx context.Context, names []string) (map[string]LinearLabel, error)
```

Queries both `team.labels` (team-scoped) and `organization.labels` (workspace-scoped). On collision (same name in both scopes), team-scoped wins (more specific). Case-insensitive name matching; preserves Linear's display case.

The result is cached for the duration of a sync run via a per-`Tracker` map. Label sets do not change frequently, so a single fetch per sync per team is sufficient.

### Step 2 — Auto-create missing labels

For every name in `AddToLinear` not resolved in step 1:

```go
client.CreateLabel(ctx, name, scope)  // scope from linear.label_create_scope
```

GraphQL: `issueLabelCreate { input: { name, teamId? } }`. `teamId` is omitted for workspace scope. On success, add to the per-run cache.

For multi-team setups with `scope=team`: each bead is associated with exactly one Linear team (via `clientForExternalID` in `tracker.go:177`), so a single bead's push creates the missing label exactly once on that bead's owning team. If the same label name appears on beads belonging to different Linear teams, the label is created independently on each team across separate pushes. Choosing `team` scope means accepting this per-team duplication; users who want a single shared label switch to `scope=workspace`.

### Step 3 — Apply to issue

Call the update mutation, passing the full intended `labelIds` set. The intended set = current Linear labels − `RemoveFromLinear` IDs + resolved IDs for `AddToLinear` names.

**Verify before implementation:** This design assumes Linear's `issueUpdate.input.labelIds` **replaces** the issue's label set (not merges). If the actual semantics are merge-only, removal from Linear silently fails and we'd need a separate `issueRemoveLabel` mutation per removed label. The Linear GraphQL pattern in `client.go:524-553` doesn't prove either way. **Implementation must:** (a) check the current Linear API docs; (b) add an integration test that puts label `X` on an issue, calls `IssueUpdate` with `labelIds: [Y_id]`, and asserts the issue ends up with only `[Y]`. If that test fails, switch to per-label add/remove mutations and update this design.

### Failure handling

| Failure | Behavior |
|---|---|
| `CreateLabel` fails (rate limit, permission, network) | Log warning, skip this label for this push, omit from snapshot. Other labels for the issue still apply. Retry next sync. |
| `LabelsByName` returns multiple IDs for the same name (duplicate Linear labels exist) | **Fail this issue's push loudly.** Surface the ambiguity per the precedent in commit `d4df404a`. User must dedupe in Linear before sync resumes for that issue. |
| `IssueUpdate` fails after labels resolved | Log error, omit this issue from snapshot writes, retry next sync. |

## Pull path

The pull path today calls `syncIssueLabels` (`internal/tracker/engine.go:644`), a destructive Linear-authoritative replace. We **replace that call** with the reconciler-driven flow.

Concrete changes:

1. **Lift label IDs through the conversion boundary.** `linearToTrackerIssue` returns `tracker.TrackerIssue` whose `Labels` field is `[]string` of names. For rename detection, the reconciler also needs IDs. Two options:
   - Extend `tracker.TrackerIssue.Labels` to a struct (`{Name, ID}`). Cleanest, but touches the cross-tracker abstraction (GitHub/GitLab trackers also use this field; they'd need to populate ID = empty string or similar).
   - Have the Linear-specific glue read IDs separately from `ti.Raw.(*linear.Issue).Labels.Nodes`. Less invasive but couples the reconciler glue to the concrete Linear type.

   **Recommendation:** start with option 2 (raw access in the Linear glue) and revisit if other trackers gain rename semantics. Document the coupling clearly.

2. **Apply exclusion filter at the reconciler boundary.** Before the reconciler runs, both the Linear-side and beads-side label sets are filtered by `linear.label_sync_exclude` (case-insensitive). The snapshot stores only post-exclusion entries.

3. **Apply mutations to the bead** via existing transaction-scoped methods (`tx.AddLabel(ctx, issueID, name, actor)` / `tx.RemoveLabel(ctx, issueID, name, actor)` — see `internal/tracker/engine.go:655-663` for the established signature pattern). These already emit `EventLabelAdded` / `EventLabelRemoved` events, picked up for free by the audit log. No new event types. The mutations and the snapshot write happen inside the same `RunInTransaction` block (`engine.go:492`) for atomicity.

4. **Replace `syncIssueLabels`.** The old function (`engine.go:644`) is dead after this change. Remove it. Any other callers (search before deleting) get migrated to the reconciler-driven flow or have their own narrower replacement defined.

## Persistence

Snapshot is stored in a **dedicated table**, not in the global config k/v store. Per-bead state belongs in its own table — both for scaling (one row per snapshotted label per bead, instead of one config key per bead) and for namespace hygiene (`cmd/bd/kv.go:31` reserves the `linear.*` prefix from user access, and snapshots are not user-facing config).

### Schema

The table follows the conventions of existing per-issue tables (see `internal/storage/schema/migrations/0003_create_labels.up.sql:1-7` for the `labels` precedent — same `issues.id` FK, same column types):

```sql
CREATE TABLE linear_label_snapshots (
    issue_id     VARCHAR(255) NOT NULL,    -- beads issue ID (matches issues.id)
    label_id     VARCHAR(64)  NOT NULL,    -- Linear label UUID
    label_name   VARCHAR(255) NOT NULL,    -- canonical name as last seen on Linear
    synced_at    DATETIME     NOT NULL,
    PRIMARY KEY (issue_id, label_id),
    INDEX idx_linear_label_snapshots_issue (issue_id),
    CONSTRAINT fk_linear_label_snapshots_issue
        FOREIGN KEY (issue_id) REFERENCES issues(id) ON DELETE CASCADE
);
```

Notes:

- `issue_id VARCHAR(255)` and `REFERENCES issues(id)` — matches the existing `labels`, `dependencies`, `comments`, `events` precedent. The `issues` PK is `id`, not `uuid`.
- Primary key on `(issue_id, label_id)` — a bead can have many labels; a Linear label ID is unique within a Linear workspace.
- `ON DELETE CASCADE` on the FK so beads deletion cleans up snapshots automatically (no application-level delete needed).
- `synced_at` not strictly needed for correctness, but useful for diagnostics.

### Migration paths

Beads has **two migration paths** that both need updating:

1. **Canonical schema migration** at `internal/storage/schema/migrations/0033_create_linear_label_snapshots.up.sql` (next free number after 0032). Plus matching `.down.sql`.
2. **Compat dolt migration** at `internal/storage/dolt/migrations/018_linear_label_snapshots.go` (next free number after 017). This mirrors 0033 for code paths that bypass the canonical migrator.
3. **Staging lists**: add `"linear_label_snapshots"` to the lists at `internal/storage/dolt/store.go:1467-1475` and `internal/storage/dolt/migrations/runner.go:68-75`. These lists drive table-level operations like compaction and migration ordering.

### Transaction interface

The `storage.Transaction` interface (`internal/storage/storage.go:262-300`) gains snapshot methods:

```go
type Transaction interface {
    // ... existing methods ...

    // GetLinearLabelSnapshot returns the last-synced label snapshot for an issue.
    // Returns an empty slice (not nil) when no snapshot exists.
    GetLinearLabelSnapshot(ctx context.Context, issueID string) ([]LinearLabelSnapshotEntry, error)

    // PutLinearLabelSnapshot replaces the snapshot for an issue with the given set.
    // Implementation: DELETE all rows for issue_id, then bulk INSERT the new set.
    PutLinearLabelSnapshot(ctx context.Context, issueID string, entries []LinearLabelSnapshotEntry) error
}

type LinearLabelSnapshotEntry struct {
    LabelID   string
    LabelName string
}
```

Both implementations need the same methods:

- `internal/storage/dolt/transaction.go` — `doltTransaction` implementation.
- `internal/storage/embeddeddolt/transaction.go` — `embeddedTransaction` implementation.

Shared SQL builders live in a new file `internal/storage/dolt/linear_label_snapshots.go` so both implementations stay aligned. Tests cover both transaction types.

Per-bead delete is handled by the FK cascade at the schema level — no application-level delete method needed.

## Configuration

Three new keys under the existing `linear.` namespace:

| Key | Type | Default | Meaning |
|---|---|---|---|
| `linear.label_sync_enabled` | bool | `false` | Master gate. Opt-in for v1 since enabling changes sync semantics for every existing bead. |
| `linear.label_sync_exclude` | comma-separated string | `""` | Label names (case-insensitive) to skip in both directions. Examples: agent-system labels you want kept local, or the `label_type_map` keywords if you want consume behavior. |
| `linear.label_create_scope` | enum: `team` \| `workspace` | `team` | Where auto-created labels live in Linear. |

Existing keys are unchanged:

- `linear.label_type_map.<name> = <type>` — type inference on pull, orthogonal to label sync.
- `linear.exclude_labels = "..."` — bead-level filter, different concern.

## Rollout

Two distinct behavior changes happen when `linear.label_sync_enabled` flips to `true` on an existing project:

1. **Pull semantics change.** Today (`engine.go:496`), the pull does a destructive Linear-authoritative replace: any beads label not on the Linear issue is silently removed. With this design, the reconciler runs instead. On the very first sync after enabling, the snapshot table is empty for every bead, so the first-sync rule kicks in — no removals on either side, and Linear's labels flow into beads (which is what already happens, minus the removal). After this first sync, snapshots are populated and subsequent syncs use the standard 3-way merge. **Net effect for users: removals stop being silently lost. Pull-side adds are unchanged.**

2. **Push side starts firing.** Beads-only label changes that previously did nothing now produce `IssueUpdate` mutations against Linear. Beads labels not yet on Linear get auto-created. For large projects with many local-only labels, the first sync after enable issues many `issueLabelCreate` mutations. Sync is sequential per issue, so rate limits are mostly self-throttling, but users may want to enable during off-hours.

The flag default is `false` so existing projects keep current behavior until the user opts in. We document this clearly in the config description, `cmd/bd/linear.go` help text, and a `CHANGELOG` entry.

## Testing

Three layers, mirroring the existing test structure under `internal/linear/` and `cmd/bd/`.

### 1. Reconciler unit tests (`internal/linear/reconciler_test.go`)

Pure function, exhaustive table-driven. Coverage targets:

- All 7 truth-table rows (the 8th — all absent — won't appear in input).
- First-sync (empty snapshot) for each input shape.
- **Applied rename** (user kept old name): snapshot `{name:old, id:X}`, beads has `[old]`, Linear has `{name:new, id:X}`. Assert: `RemoveFromBeads=[old]`, `AddToBeads=[new]`, `RenamesApplied=[{old, new, X}]`, snapshot becomes `{name:new, id:X}`.
- **Dropped rename — user deleted locally** (decision #10): snapshot `{name:old, id:X}`, beads `[]`, Linear has `{name:new, id:X}`. Assert: `RemoveFromLinear=[X]`, `AddToBeads=[]`, `AddToLinear=[]`, snapshot row removed.
- **Dropped rename + local re-add of new name**: snapshot `{name:old, id:X}`, beads has `[new]` (user deleted `old` and independently added `new`), Linear has `{name:new, id:X}`. Assert: `AddToBeads=[]` (suppressed by pass-2 consumption), `RemoveFromLinear=[]` (Linear already has `new`), `AddToLinear=[]`, snapshot becomes `{name:new, id:X}` — converging to in-agreement state without churn.
- **Local old-delete + local new-add as independent labels** (no rename, just two operations): snapshot `[{name:foo, id:F}]`, beads has `[bar]`, Linear has `[foo]`. Assert: `RemoveFromLinear=[F]`, `AddToLinear=[bar]`, `AddToBeads=[]`, `RemoveFromBeads=[]`. Confirms pass-3 truth table runs independently when no ID match exists.
- Exclusion filter: excluded labels never appear in any output set or new snapshot, regardless of which side has them.
- Beads-side "rename" (remove + add of different names) flows correctly to Linear as remove + add.
- Empty inputs (no labels anywhere): no-op, empty result.

Aim for ~100% line coverage on `reconcileLabels`.

### 2. Linear client tests (`internal/linear/client_test.go`)

Mock GraphQL transport for:

- `LabelsByName` happy path (single team, multi-team, workspace-scoped).
- `LabelsByName` ambiguity error path (duplicate names → loud failure).
- `CreateLabel` happy path with `teamId` and without (`scope=workspace`).
- `CreateLabel` failure path (logged, snapshot omits).

Follow the existing pattern in `internal/linear/*_test.go`.

### 3. Roundtrip integration (`cmd/bd/linear_roundtrip_test.go`)

Extend the existing roundtrip with:

- Bead created with labels → push to mocked Linear via `Tracker.CreateIssue` → assert `client.CreateIssue` is called with non-nil `labelIDs` matching the resolved set (regression for the today-passes-nil bug at `tracker.go:166`).
- Linear-side label add → pull → assert beads label appears + snapshot updated.
- Linear-side label remove → pull → assert beads label removed.
- Linear-side rename (same ID, new name) → pull → assert beads label renamed via remove + add events, snapshot ID unchanged.
- Beads-side label add then push → assert `CreateLabel` called when missing, `IssueUpdate` carries new ID.
- **Label-only push fires** (regression for the gate change): bead has unchanged title/desc/priority/state but a new label → assert `IssueUpdate` is still called with `labelIds`.
- **Linear `labelIds` replace semantics** (verifies the assumption flagged in Push Step 3): seed Linear with `[X]`, push `labelIds: [Y_id]`, assert Linear ends up with `[Y]` only.
- **Pull-side removals stop being silent** (regression for `syncIssueLabels` replacement): bead has labels `[A, B]`, snapshot has `[A, B]`, Linear has `[A]` → after pull, bead has `[A]` and snapshot has `[A]`.
- **First-sync preserves both sides** (regression for the "no destructive replace on first encounter" rule): bead has labels `[A, B]`, snapshot is empty, Linear has `[A]` → after sync, bead has `[A, B]` (B not removed), Linear has `[A, B]` (B pushed via auto-create), snapshot has `[A, B]`.
- Concurrent both-sides changes → reconciler converges to expected state.

### 4. Dry-run output regression

`bd sync --dry-run --verbose-diff` should print label diffs alongside existing field diffs, in the same format. Add an assertion in the existing dry-run test.

## Open questions / explicitly deferred

- **Beads `Storage.RenameLabel` event type.** Currently a Linear-side rename produces `label_removed` + `label_added` in the bead's audit log. A future enhancement could add a dedicated rename event in the storage layer, which the reconciler would emit instead. Out of scope for v1.
- **Webhook-driven sync.** When the rest of the sync pipeline gains webhook support, label sync joins it for free — the reconciler is independent of trigger.
- **Cross-team label dedup for `scope=team`.** If users want the same label to appear once across all teams, they switch to `scope=workspace`. A future "global label set with per-team mirroring" mode is possible but adds significant complexity for marginal benefit.
