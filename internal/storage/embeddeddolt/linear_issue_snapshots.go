//go:build cgo

package embeddeddolt

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/steveyegge/beads/internal/storage"
)

// linearIssueSnapshotColumns is the column projection used for both
// reads and writes. Kept as a single source so the read scan and the
// upsert binding stay in lock-step.
const linearIssueSnapshotColumns = `issue_id, title, description, status, state_id,
	priority, assignee_id, project_id, parent_id, synced_at`

// GetLinearIssueSnapshot returns the snapshot row for issueID, or
// (nil, nil) when no row exists. Absence is the normal first-sync case
// for a freshly-linked bead and must not be treated as an error.
func (s *EmbeddedDoltStore) GetLinearIssueSnapshot(ctx context.Context, issueID string) (*storage.LinearIssueSnapshot, error) {
	var snap *storage.LinearIssueSnapshot
	err := s.withConn(ctx, false, func(tx *sql.Tx) error {
		row := tx.QueryRowContext(ctx,
			`SELECT `+linearIssueSnapshotColumns+`
			   FROM linear_issue_snapshots
			  WHERE issue_id = ?`, issueID)
		var (
			out         storage.LinearIssueSnapshot
			title       sql.NullString
			description sql.NullString
			status      sql.NullString
			stateID     sql.NullString
			priority    sql.NullInt64
			assigneeID  sql.NullString
			projectID   sql.NullString
			parentID    sql.NullString
			syncedAt    time.Time
		)
		if err := row.Scan(
			&out.IssueID, &title, &description, &status, &stateID,
			&priority, &assigneeID, &projectID, &parentID, &syncedAt,
		); err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				return nil
			}
			return fmt.Errorf("scan linear_issue_snapshots: %w", err)
		}
		out.Title = title.String
		out.Description = description.String
		out.Status = status.String
		out.StateID = stateID.String
		out.Priority = int(priority.Int64)
		out.AssigneeID = assigneeID.String
		out.ProjectID = projectID.String
		out.ParentID = parentID.String
		out.SyncedAt = syncedAt
		snap = &out
		return nil
	})
	if err != nil {
		return nil, err
	}
	return snap, nil
}

// UpsertLinearIssueSnapshot writes or replaces the snapshot row for
// snap.IssueID. Caller sets SyncedAt to the moment the snapshot was
// captured (typically time.Now() right after a successful API call).
func (s *EmbeddedDoltStore) UpsertLinearIssueSnapshot(ctx context.Context, snap *storage.LinearIssueSnapshot) error {
	if snap == nil || snap.IssueID == "" {
		return fmt.Errorf("UpsertLinearIssueSnapshot: nil or empty IssueID")
	}
	return s.withConn(ctx, true, func(tx *sql.Tx) error {
		_, err := tx.ExecContext(ctx,
			`INSERT INTO linear_issue_snapshots (`+linearIssueSnapshotColumns+`)
			 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
			 ON DUPLICATE KEY UPDATE
			   title = VALUES(title),
			   description = VALUES(description),
			   status = VALUES(status),
			   state_id = VALUES(state_id),
			   priority = VALUES(priority),
			   assignee_id = VALUES(assignee_id),
			   project_id = VALUES(project_id),
			   parent_id = VALUES(parent_id),
			   synced_at = VALUES(synced_at)`,
			snap.IssueID, snap.Title, snap.Description, snap.Status, snap.StateID,
			snap.Priority, snap.AssigneeID, snap.ProjectID, snap.ParentID, snap.SyncedAt,
		)
		if err != nil {
			return fmt.Errorf("upsert linear_issue_snapshots: %w", err)
		}
		return nil
	})
}

// DeleteLinearIssueSnapshot removes the snapshot row for issueID. Used
// when a bead's external_ref changes (e.g., epic migrated from Issue to
// Project) so the stale Issue-side snapshot doesn't shadow the new
// Project-backed sync state.
func (s *EmbeddedDoltStore) DeleteLinearIssueSnapshot(ctx context.Context, issueID string) error {
	return s.withConn(ctx, true, func(tx *sql.Tx) error {
		_, err := tx.ExecContext(ctx,
			`DELETE FROM linear_issue_snapshots WHERE issue_id = ?`, issueID)
		if err != nil {
			return fmt.Errorf("delete linear_issue_snapshots: %w", err)
		}
		return nil
	})
}

// Compile-time check.
var _ storage.LinearIssueSnapshotStore = (*EmbeddedDoltStore)(nil)
