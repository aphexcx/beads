package storage

import (
	"context"
	"time"
)

// LinearProjectSnapshot is bd-6cl's per-epic record of a Linear
// Project's last-known state, used for field-scoped conflict
// resolution on the pull path. Symmetric extension of bd-ajn's
// LinearIssueSnapshot, scoped to top-level epics that live as Linear
// Projects rather than Issues.
//
// One row per beads epic whose external_ref is a Linear Project URL.
// Fields with no remote value are stored as the zero value of their
// type. The pull-side diff in DetectProjectConflicts compares the
// snapshot fields against the current Linear Project payload.
//
// ProjectID is Linear's stable Project UUID, pinned per-row so a
// future external_ref rewrite (epic re-pointed at a different
// Project) can detect the change and force a re-baseline rather than
// silently misdiffing two unrelated Projects.
type LinearProjectSnapshot struct {
	IssueID     string
	ProjectID   string
	Name        string
	Description string
	Content     string
	State       string // raw Linear Project state: planned|started|paused|completed|canceled
	SyncedAt    time.Time
}

// LinearProjectSnapshotStore is the optional capability backends
// implement to support bd-6cl's pull-side Project conflict
// detection. Parallel to LinearIssueSnapshotStore (bd-ajn).
// Trackers that lack the capability fall back to whole-issue
// timestamp behavior on Project pulls.
type LinearProjectSnapshotStore interface {
	// GetLinearProjectSnapshot returns the snapshot row for issueID
	// (the local epic's bead ID), or (nil, nil) when no snapshot
	// exists — the "first-sync" signal that the caller handles via
	// soft rollout (snapshot baseline, no conflict this run).
	GetLinearProjectSnapshot(ctx context.Context, issueID string) (*LinearProjectSnapshot, error)

	// UpsertLinearProjectSnapshot writes or replaces the snapshot
	// row. Caller sets SyncedAt to the moment of the sync operation
	// that produced the snapshot values (typically time.Now() right
	// after a successful Project mutation or pull).
	UpsertLinearProjectSnapshot(ctx context.Context, snap *LinearProjectSnapshot) error

	// DeleteLinearProjectSnapshot removes the snapshot row. Used
	// when an epic's external_ref is cleared or rewritten so the
	// stale snapshot doesn't shadow the new sync state.
	DeleteLinearProjectSnapshot(ctx context.Context, issueID string) error
}
