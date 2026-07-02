package linear

import (
	"context"
	"fmt"
	"time"

	"github.com/steveyegge/beads/internal/storage"
)

// bd-ajn: writers that persist the per-issue Linear-side snapshot used
// for field-scoped conflict detection. Every successful Linear mutation
// updates the snapshot so the next DetectConflicts run can correctly
// distinguish "Linear changed this field" from "we changed it ourselves
// during the last sync, the snapshot is stale."
//
// Two flavors:
//   - writeFullSnapshot: full-issue mutations (CreateIssue, UpdateIssue,
//     pull-side import) — server returned the complete Issue, we record
//     all fields.
//   - patchSnapshotField: single-field mutations (AssignIssueToProject,
//     SetIssueParent) — only one field changed on Linear; preserve the
//     other snapshot fields. Skips silently when no baseline snapshot
//     exists (the bead's first full push/pull will populate it).
//
// First-sync handling lives elsewhere: DetectConflicts (P4) treats a
// missing snapshot as the first-sync soft-rollout signal and snapshots
// a baseline. These writers don't try to re-implement that — they
// either know the full state (writeFullSnapshot) or they patch what
// they have and leave the rest to first-sync.

// linearIssueSnapshotStore narrows the storage interface to the methods
// needed by these writers, keeping the dependency surface explicit.
type linearIssueSnapshotStore interface {
	storage.LinearIssueSnapshotStore
}

// buildIssueSnapshot converts a fully-populated Linear *Issue plus the
// owning bead's local ID into a snapshot row ready to upsert. Caller is
// responsible for syncedAt (typically time.Now() right after the
// successful API call).
//
// Field selection mirrors what FetchIssueByIdentifier and the bulk
// issuesQuery now return (after bd-6tv added parent and project
// selections). When `li` was obtained from a code path that does NOT
// fetch parent/project, the snapshot's relation fields will be empty —
// the next full pull will reconcile.
func buildIssueSnapshot(issueID string, li *Issue, syncedAt time.Time) *storage.LinearIssueSnapshot {
	snap := &storage.LinearIssueSnapshot{
		IssueID:     issueID,
		Title:       li.Title,
		Description: li.Description,
		Priority:    li.Priority,
		SyncedAt:    syncedAt,
	}
	if li.State != nil {
		snap.StateID = li.State.ID
		snap.Status = li.State.Name // raw Linear state name; status mapping is the caller's concern when diffing
	}
	if li.Assignee != nil {
		snap.AssigneeID = li.Assignee.ID
	}
	if li.Project != nil {
		snap.ProjectID = li.Project.ID
	}
	if li.Parent != nil {
		snap.ParentID = li.Parent.ID
	}
	return snap
}

// writeIssueSnapshot upserts the snapshot for a fully-known Linear
// issue. Used after CreateIssue / UpdateIssue (which return the complete
// post-mutation Issue) and after pull-side import (which has the fetched
// Issue in hand). Failures are surfaced to the caller — the snapshot is
// part of correctness, not best-effort.
func (t *Tracker) writeIssueSnapshot(ctx context.Context, issueID string, li *Issue) error {
	store, ok := t.store.(linearIssueSnapshotStore)
	if !ok {
		return nil // snapshot capability not provided by this backend; degrade gracefully
	}
	if issueID == "" || li == nil {
		return nil
	}
	return store.UpsertLinearIssueSnapshot(ctx, buildIssueSnapshot(issueID, li, time.Now().UTC()))
}

// patchIssueSnapshotProjectID updates only the project_id field of an
// existing snapshot row. Used after AssignIssueToProject — only the
// projectId changed on Linear; the other snapshot fields stay valid.
//
// When no baseline snapshot exists, this is a silent no-op. Rationale:
// the bead has never been fully snapshotted yet, so writing a partial
// row with only project_id set would lie about the other fields'
// last-sync values. The next full pull / push will populate the
// baseline, and DetectConflicts's first-sync path handles the
// in-between window safely.
func (t *Tracker) patchIssueSnapshotProjectID(ctx context.Context, issueID, projectID string) error {
	return t.patchIssueSnapshot(ctx, issueID, func(snap *storage.LinearIssueSnapshot) {
		snap.ProjectID = projectID
	})
}

// patchIssueSnapshotParentID updates only the parent_id field of an
// existing snapshot row. Used after SetIssueParent. Same first-sync
// no-op semantics as patchIssueSnapshotProjectID.
func (t *Tracker) patchIssueSnapshotParentID(ctx context.Context, issueID, parentUUID string) error {
	return t.patchIssueSnapshot(ctx, issueID, func(snap *storage.LinearIssueSnapshot) {
		snap.ParentID = parentUUID
	})
}

// patchIssueSnapshot is the shared read-modify-write helper for single-
// field updates. Skips silently when:
//   - The backend doesn't implement LinearIssueSnapshotStore (no
//     snapshot capability — degrade gracefully).
//   - No baseline snapshot exists for the issue (see
//     patchIssueSnapshotProjectID's rationale).
//
// Always bumps SyncedAt on a real patch.
func (t *Tracker) patchIssueSnapshot(ctx context.Context, issueID string, mutate func(*storage.LinearIssueSnapshot)) error {
	store, ok := t.store.(linearIssueSnapshotStore)
	if !ok {
		return nil
	}
	if issueID == "" {
		return nil
	}
	existing, err := store.GetLinearIssueSnapshot(ctx, issueID)
	if err != nil {
		return fmt.Errorf("read snapshot for patch: %w", err)
	}
	if existing == nil {
		return nil // first-sync window; full pull/push will baseline
	}
	mutate(existing)
	existing.SyncedAt = time.Now().UTC()
	return store.UpsertLinearIssueSnapshot(ctx, existing)
}
