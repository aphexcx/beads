package storage

import (
	"context"
	"time"
)

// LinearIssueSnapshot is the last-known state of a Linear issue as of the
// last successful sync (push or pull). bd-ajn uses these snapshots to
// detect which Linear-side fields actually changed since the last sync,
// so the conflict resolver can merge non-conflicting changes instead of
// the old whole-issue "newer wins" behavior.
//
// One row per beads issue with a Linear external_ref. Fields with no
// remote value are stored as the zero value of their type (empty string,
// 0). The diff logic in DetectConflicts compares snapshot fields against
// the current Linear payload — equal = unchanged, unequal = changed.
//
// StateID is Linear's stable workflow-state UUID, stored alongside the
// mapped beads status. Names like "Todo"/"In Progress" can drift on
// Linear; the UUID survives renames and is the authoritative comparator.
type LinearIssueSnapshot struct {
	IssueID     string
	Title       string
	Description string
	Status      string
	StateID     string
	Priority    int
	AssigneeID  string
	ProjectID   string
	ParentID    string
	SyncedAt    time.Time
}

// LinearIssueSnapshotStore is an optional capability backends can
// implement. The Linear tracker adapter type-asserts for this interface
// and falls back to the old whole-issue timestamp behavior when the
// backend doesn't support it (degrades gracefully on mock stores in
// unit tests that don't need conflict-resolution coverage).
type LinearIssueSnapshotStore interface {
	// GetLinearIssueSnapshot returns the snapshot for issueID. Returns
	// (nil, nil) when no snapshot exists yet — caller treats absence as
	// "first sync for this issue" and snapshots a baseline.
	GetLinearIssueSnapshot(ctx context.Context, issueID string) (*LinearIssueSnapshot, error)

	// UpsertLinearIssueSnapshot writes or replaces the snapshot row for
	// snap.IssueID. Caller is responsible for setting SyncedAt to the
	// moment of the sync operation that produced the snapshot values.
	UpsertLinearIssueSnapshot(ctx context.Context, snap *LinearIssueSnapshot) error

	// DeleteLinearIssueSnapshot removes the snapshot row for issueID.
	// Used when a bead's external_ref is cleared or migrated to a
	// different Linear identity so the stale snapshot doesn't shadow
	// the new sync state.
	DeleteLinearIssueSnapshot(ctx context.Context, issueID string) error
}
