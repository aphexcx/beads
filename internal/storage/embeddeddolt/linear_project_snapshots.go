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

// linearProjectSnapshotColumns is the column projection used for
// both reads and writes so the scan and the upsert binding stay in
// lock-step.
const linearProjectSnapshotColumns = `issue_id, project_id, name, description,
	content, state, synced_at`

// GetLinearProjectSnapshot returns the snapshot row for issueID, or
// (nil, nil) when no row exists. Absence is the first-sync signal
// (mayor's bd-6cl Q3 soft-rollout) and must not surface as an
// error.
func (s *EmbeddedDoltStore) GetLinearProjectSnapshot(ctx context.Context, issueID string) (*storage.LinearProjectSnapshot, error) {
	var snap *storage.LinearProjectSnapshot
	err := s.withConn(ctx, false, func(tx *sql.Tx) error {
		row := tx.QueryRowContext(ctx,
			`SELECT `+linearProjectSnapshotColumns+`
			   FROM linear_project_snapshots
			  WHERE issue_id = ?`, issueID)
		var (
			out         storage.LinearProjectSnapshot
			projectID   sql.NullString
			name        sql.NullString
			description sql.NullString
			content     sql.NullString
			state       sql.NullString
			syncedAt    time.Time
		)
		if err := row.Scan(
			&out.IssueID, &projectID, &name, &description,
			&content, &state, &syncedAt,
		); err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				return nil
			}
			return fmt.Errorf("scan linear_project_snapshots: %w", err)
		}
		out.ProjectID = projectID.String
		out.Name = name.String
		out.Description = description.String
		out.Content = content.String
		out.State = state.String
		out.SyncedAt = syncedAt
		snap = &out
		return nil
	})
	if err != nil {
		return nil, err
	}
	return snap, nil
}

// UpsertLinearProjectSnapshot writes or replaces the snapshot row
// for snap.IssueID. Caller sets SyncedAt to the moment the snapshot
// was captured.
func (s *EmbeddedDoltStore) UpsertLinearProjectSnapshot(ctx context.Context, snap *storage.LinearProjectSnapshot) error {
	if snap == nil || snap.IssueID == "" {
		return fmt.Errorf("UpsertLinearProjectSnapshot: nil or empty IssueID")
	}
	return s.withConn(ctx, true, func(tx *sql.Tx) error {
		_, err := tx.ExecContext(ctx,
			`INSERT INTO linear_project_snapshots (`+linearProjectSnapshotColumns+`)
			 VALUES (?, ?, ?, ?, ?, ?, ?)
			 ON DUPLICATE KEY UPDATE
			   project_id = VALUES(project_id),
			   name = VALUES(name),
			   description = VALUES(description),
			   content = VALUES(content),
			   state = VALUES(state),
			   synced_at = VALUES(synced_at)`,
			snap.IssueID, snap.ProjectID, snap.Name, snap.Description,
			snap.Content, snap.State, snap.SyncedAt,
		)
		if err != nil {
			return fmt.Errorf("upsert linear_project_snapshots: %w", err)
		}
		return nil
	})
}

// DeleteLinearProjectSnapshot removes the snapshot row for issueID.
// Used when an epic's external_ref is rewritten so a stale snapshot
// doesn't shadow the new sync state.
func (s *EmbeddedDoltStore) DeleteLinearProjectSnapshot(ctx context.Context, issueID string) error {
	return s.withConn(ctx, true, func(tx *sql.Tx) error {
		_, err := tx.ExecContext(ctx,
			`DELETE FROM linear_project_snapshots WHERE issue_id = ?`, issueID)
		if err != nil {
			return fmt.Errorf("delete linear_project_snapshots: %w", err)
		}
		return nil
	})
}

// Compile-time check.
var _ storage.LinearProjectSnapshotStore = (*EmbeddedDoltStore)(nil)
