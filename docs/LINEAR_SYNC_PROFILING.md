# Linear Sync Request Profiling (bd-kqt)

This document records the request-amplification profile of `bd linear sync`
on a ~1k-issue dataset, the fixes that removed the amplification, and the
remaining known costs. The measurements come from the reproducible harness in
`cmd/bd/linear_sync_profile_test.go` (`TestLinearSyncRequestProfile`), which
replays the incident dataset shape against a counting mock Linear server:

- N=1000 Linear-linked beads (mirroring the hw dataset's 1010 beads / 648
  linked — every mock issue is linked after the harness's initial import);
- every block of 10 issues forms a parent chain, so the post-sync parent
  reconciler has realistic work;
- **all local beads phantom-dirty**: their `updated_at` was bumped past
  `linear.last_sync` by a content-neutral write, reproducing the write-back
  storm observed on hw (sync write-backs bump `issues.updated_at` past the
  recorded `linear.last_sync`, so the next run sees every linked bead as
  locally changed);
- a 5-issue genuine remote delta (3 with real content edits, 2 with only
  `updatedAt` bumps, i.e. comment-style activity).

Run it with:

```bash
# hermetic Dolt test server on a scratch dir
(cd "$(mktemp -d)" && dolt sql-server -H 127.0.0.1 -P 43217 --no-auto-commit &)

BEADS_TEST_MODE=1 BEADS_DOLT_SERVER_PORT=43217 \
  go test -tags 'gms_pure_go integration' -run TestLinearSyncRequestProfile \
  -timeout 40m -count=1 ./cmd/bd/ -v
```

`BEADS_PROFILE_N` scales the dataset; `BEADS_PROFILE_NO_ASSERT=1` prints the
profile without enforcing the request budgets (used to capture the baseline
numbers below on unfixed code).

## Where the requests came from (baseline, commit 193a87707)

A bidirectional sync touches the Linear API from six code paths. Five of them
scaled **linearly with linked-issue count**, not with the size of the change:

| # | Path | Requests (baseline) | Cardinality driver |
|---|------|--------------------|--------------------|
| 1 | `Engine.DetectConflicts` — per-candidate `FetchIssue` (engine.go) | ~1 per linked bead | With Linear's snapshot infra present, the whole-issue fast-skip is disabled, so **every** linked bead is a candidate — dirty or not |
| 2 | `Engine.fetchPrelinkedIssues` — per-candidate `FetchIssue` | ~1 per locally-changed linked bead | Phantom-dirty makes that "all of them"; each also paid a per-issue `dolt_history_issues` query first |
| 3 | Pull sub-resource hooks — `FetchIssueComments` + `FetchIssueAttachments` per pulled issue (wet runs only) | 2 per issue returned by the pull | Fired even when the issue content was unchanged |
| 4 | `Tracker.BatchPush` skip-check — `FetchIssueByIdentifier` per update candidate (wet runs only) | ~1 per linked bead | `classifyPushIssue` has no dirtiness filter: every linked bead is an update candidate every sync |
| 5 | `ReconcileParents` / `ReconcileProjectMembership` — `FetchIssueByIdentifier` per unique identifier (memoized within the pass) | ~1 per bead participating in parent/project links | Runs on every unscoped push/bidirectional sync, dry-run included |
| 6 | Paged list/metadata queries (`FetchIssuesSince`, `FetchProjects`, `GetTeamStates`) | O(pages) | The only paths that were already proportional to the delta |

On the hw dataset (648 linked, ~558 phantom-dirty, deep parent trees), paths
1+2+5 alone explain the observed ~500 requests per sweep; Linear's API-key
budget is 1,500/hr, so three sweeps tripped the circuit breaker
(`rate limit circuit breaker: 0 requests remaining (floor: 100)`).

### Measured baseline (commit 193a87707, N=300, all-phantom-dirty + 5 delta)

| Run | API requests | Breakdown | Wall |
|-----|--------------|-----------|------|
| Bidirectional dry-run | **604** | issue-detail 600, issues-list 1, projects 2, team-states 1 | 4m07s |
| Bidirectional wet run | **908** | issue-detail 897, comments 3, attachments 3, issues-list 1, projects 2, team-states 2 | 3m52s |
| Bidirectional dry-run, steady state (nothing dirty) | **604** | issue-detail 600, issues-list 1, projects 2, team-states 1 | 3m06s |

The 600 per-issue detail fetches in the dry-run decompose as ~300 from
DetectConflicts (every linked bead, because the snapshot-infra path disables
the fast-skip) + ~295 from pre-linked hydration (every phantom-dirty bead not
already in the incremental window); the wet run adds ~300 more from the
BatchPush skip-check. **Even the steady state — nothing changed anywhere —
cost 604 requests**, because DetectConflicts fetches every linked bead
regardless of dirtiness. Requests scale linearly with linked-bead count:
at the hw dataset's 648 linked issues this is the observed ~500+ requests
per sweep; at 1,000 issues a single bidirectional wet run (~3,000 requests)
would exhaust Linear's 1,500/hr API-key budget twice over.

(The parent/project reconcile passes did not fire in this dataset — the
harness's remote parent links don't materialize as local deps — so their
per-identifier fetch cost is on top of the numbers above on real repos.
Their batching is locked by unit tests in
internal/linear/parent_reconcile_test.go / project_reconcile_test.go.)

## The fixes

One mechanism kills paths 1, 2, 4 and 5: **resolve many identifiers per
request instead of one**. Linear's `IssueFilter` accepts
`number: { in: [...] }` scoped to a team, so up to `MaxPageSize` (100)
identifiers resolve in a single query with the same field selection as
`FetchIssueByIdentifier`.

- `Client.FetchIssuesByIdentifiers` (internal/linear/client.go) — the batched
  query, chunked at `MaxPageSize`, identifier-validated like the single
  fetch.
- `Tracker.batchFetchIssuesAcrossTeams` (internal/linear/batch_fetch.go) —
  team-routing wrapper: primary team first, unresolved identifiers retried
  against later teams; also records which team's client resolved each
  identifier, replacing `clientForExternalID`'s per-issue trial fetches.
- `tracker.BatchIssueFetcher` (internal/tracker/tracker.go) — optional
  capability the engine prefers; trackers without it keep the per-issue path.
- `Engine.DetectConflicts` and `Engine.fetchPrelinkedIssues` collect their
  candidate sets first, then resolve them through one batched call
  (`Engine.batchFetchRemoteIssues`).
- `Tracker.BatchPush` prefetches all update candidates in one batched call
  (skip-check, team routing, and identifier→UUID resolution all come from the
  prefetch). Rate-limit exhaustion during the prefetch aborts the batch
  instead of grinding through doomed per-issue requests.
- `ReconcileParents` / `ReconcileProjectMembership` seed their memo caches
  with one batched prefetch of every identifier their links reference.

Path 3 is gated instead of batched: `Engine.shouldSyncSubresources`
(internal/tracker/engine.go) skips the two per-issue sub-resource requests
unless there is evidence they could have changed — first/full pull, an
explicitly requested issue (`--issues`), a remote `updatedAt` newer than
`last_sync` (comments/attachments bump the remote issue's `updatedAt`), or a
just-(re)linked bead that needs backfill.

The per-issue local cost in path 2 also got a pre-filter:
`externalRefChangedAfter` returns early when the bead hasn't been touched
since `last_sync` (an `external_ref` change always bumps `updated_at`), so
clean beads no longer pay a `dolt_history_issues` scan on every sync.

### Measured after the fixes (same harness, same dataset)

N=300, all local beads phantom-dirty, 5-issue remote delta (3 content edits,
2 timestamp-only), single team, scratch Dolt test server (M-series laptop):

| Run | API requests | Breakdown | Wall |
|-----|--------------|-----------|------|
| Bidirectional **dry-run** (the bd-kqt acceptance scenario) | **10** (was 604) | issues-batch 6, issues-list 1, projects 2, team-states 1 | 3m45s |
| Bidirectional **wet run** | **20** (was 908) | issues-batch 9, comments 3, attachments 3, issues-list 1, projects 2, team-states 2 | 2m45s |
| Bidirectional dry-run, **steady state** (nothing dirty) | **10** (was 604) | issues-batch 6, issues-list 1, projects 2, team-states 1 | **18.9s** (was 3m06s) |
| Initial full import (unavoidable first-sync backfill) | 604 | comments 300, attachments 300, issues-list 3, projects 1 | ~1-2m |

The batched fetches scale as `ceil(N/100)` per consumer, so a 1,000-issue
repo lands around 30–40 requests per bidirectional sync — comfortably inside
the ≤100 budget (the harness asserts it). Sub-resource requests now track
the genuine remote delta (3 comment + 3 attachment fetches for the changed
issues) instead of 2×N. At N=1000 the initial import measured 2,011 requests
(2 per issue + pages) — that first-sync backfill is inherent and documented,
not amplification.

### Wall time: the ≤30s target and what remains

Requests were the incident's cause, and they are fixed. Wall time is now
dominated by **local Dolt work**, not the Linear API:

- With every linked bead phantom-dirty (N=300), the bidirectional dry-run
  measured ~3m24s: field-scoped conflict detection performs a
  `dolt_history_issues` scan + snapshot read per locally-dirty candidate
  (`loadLocalStateAtSync` in internal/tracker/conflict_field_scope.go).
  Under a phantom storm that is every linked bead, ~0.5s each on the test
  server. The ≤30s wall target is therefore **not reachable while the
  phantom-dirty bug holds** — the per-candidate local scans are proportional
  to the (spurious) dirty count. Fixing phantom-dirty (tracked separately)
  restores the dirty set to its true size, which is normally a handful.
- In the **steady state** the both-sides-clean fast-skip added with this
  change (engine.go DetectConflicts) reduces the per-candidate cost to a
  single snapshot-presence read (the dolt_history scan and field diffs are
  skipped once a baseline exists; issues with no baseline still flow
  through detection so the first-sync soft rollout records one). Measured
  no-change bidirectional dry-runs: **18.9s at N=300** with the skip alone
  (vs 3m06s baseline, 2m37s with batching alone) and **24.9s at N=100** on
  a slower scratch server with the final snapshot-gated variant. Wall
  numbers on the scratch Dolt servers vary ~2-4× run-to-run; the structural
  point is that steady-state cost dropped from
  (history scan + snapshot read + field diff) per linked bead to one
  primary-key snapshot read per linked bead.
- A follow-up option if large genuinely-dirty sets need to be fast too:
  batch `loadLocalStateAtSync` into one `dolt_history_issues` query for all
  candidates (same shape as the remote-side batching done here).

## Remaining known costs and follow-ups

- **Phantom-dirty is a separate correctness bug** (sync write-backs bumping
  `issues.updated_at` past `linear.last_sync`; tracked separately per
  bd-kqt's description). The fixes here make request cost roughly
  insensitive to it (batched fetches are ceil(N/100) whether N is 5 or 648),
  but it still inflates local work: `DetectConflicts` runs its per-candidate
  `dolt_history_issues` / snapshot comparisons for every phantom-dirty bead,
  which dominates wall time on large dirty sets, and the push dry-run
  preview prints a "Would update in Linear" line per phantom-dirty bead.
- **Pull skips phantom-dirty beads entirely** (pre-existing `existing.UpdatedAt
  .After(lastSync)` conflict guard in doPull): remote comment-only activity
  on a phantom-dirty bead doesn't import until the bead is clean again. Not
  changed by this work — it is the same guard that existed before.
- **`GetTeamStates` is fetched twice per push** (engine `BuildStateCache`
  hook + `BatchPush`'s own per-team caches) — 2×teams requests total,
  negligible; left alone because `e.stateCache` is consumed by non-batch
  tracker paths.
- **The `native_store_unavailable gate=version_compat` warning is not
  emitted by this repo.** It comes from the Gas Town host binary (`gt`/`gc`)
  that links beads as a library: the host compares the installed `bd`
  binary's version constant (`1.1.0-rc.1`, frozen at the release tag) with
  its own beads module version, which on `local/integrated` (121 commits past
  the tag) is a Go pseudo-version — they can never be equal. Fixing it means
  rebuilding/pinning the host against the same beads commit as the installed
  `bd`; it does not affect `bd linear sync` itself (bd runs in-process
  against its own store), only host-side tooling that shells through the
  compat gate.
