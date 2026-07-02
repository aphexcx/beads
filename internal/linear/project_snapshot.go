package linear

import (
	"context"
	"time"

	"github.com/steveyegge/beads/internal/storage"
)

// bd-6cl: writers for the per-epic Linear Project snapshot used by
// pull-side field-scoped conflict detection. Symmetric extension of
// bd-ajn's issue_snapshot.go for the Project domain. Every
// successful Project mutation refreshes the snapshot so the next
// DetectProjectConflicts run correctly distinguishes "Linear UI
// changed this Project field" from "we just pushed it."
//
// Two writer flavors:
//   - writeProjectSnapshot: full-Project mutations (CreateProject,
//     UpdateProject, pull-side baseline) — caller has the full
//     *Project payload, we record all fields.
//   - patchProjectSnapshotState: future hook for state-only
//     transitions if needed. (Not used in v1 — UpdateProject covers
//     all current mutations; included as a symmetric placeholder
//     mirroring patchIssueSnapshot* helpers from bd-ajn.)
//
// Like the Issue-side writers, these silently degrade when the
// backend doesn't implement LinearProjectSnapshotStore (mocks,
// non-snapshot backends). First-sync handling lives in the pull-
// side detector (P4) — these writers don't replicate it.

// linearProjectSnapshotStore narrows the storage interface to the
// methods the writers need.
type linearProjectSnapshotStore interface {
	storage.LinearProjectSnapshotStore
}

// buildProjectSnapshot converts a fully-populated Linear *Project
// plus the owning bead's local ID into a snapshot row ready to
// upsert. Caller sets syncedAt to the moment the snapshot was
// captured (typically time.Now() right after the API call).
func buildProjectSnapshot(issueID string, p *Project, syncedAt time.Time) *storage.LinearProjectSnapshot {
	if p == nil {
		return nil
	}
	return &storage.LinearProjectSnapshot{
		IssueID:     issueID,
		ProjectID:   p.ID,
		Name:        p.Name,
		Description: p.Description,
		Content:     p.Content,
		State:       p.State,
		SyncedAt:    syncedAt,
	}
}

// writeProjectSnapshot upserts the snapshot for a fully-known
// Linear Project. Used after CreateProject / UpdateProject (which
// return the post-mutation *Project) and after pull-side baseline
// in P4. Failures surface to the caller — the snapshot is part of
// correctness, not best-effort.
func (t *Tracker) writeProjectSnapshot(ctx context.Context, issueID string, p *Project) error {
	store, ok := t.store.(linearProjectSnapshotStore)
	if !ok {
		return nil // backend doesn't support Project snapshots — degrade gracefully
	}
	if issueID == "" || p == nil {
		return nil
	}
	return store.UpsertLinearProjectSnapshot(ctx, buildProjectSnapshot(issueID, p, time.Now().UTC()))
}
