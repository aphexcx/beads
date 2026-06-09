package dolt

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/steveyegge/beads/internal/storage"
)

// bd-3p8: port of the bd-6cl Project snapshot store to the Dolt-
// server backend. Mirrors internal/storage/embeddeddolt/
// linear_project_snapshots.go.
//
// Without this port the bd-6cl pull-side Project materialization
// was effectively non-functional on Dolt-server rigs: the engine's
// PullProjects probe found LinearProjectSnapshotStore missing and
// returned an error that the engine logged as a warning — easy to
// miss, with the user-visible symptom being "Linear Projects don't
// appear as local epics" with no clear cause.

const linearProjectSnapshotColumns = `issue_id, project_id, name, description,
	content, state, synced_at`

// GetLinearProjectSnapshot returns the snapshot row for issueID,
// or (nil, nil) when no row exists. Absence is the first-sync
// signal (mayor's bd-6cl Q3 soft-rollout).
func (s *DoltStore) GetLinearProjectSnapshot(ctx context.Context, issueID string) (*storage.LinearProjectSnapshot, error) {
	var snap *storage.LinearProjectSnapshot
	err := s.withReadTx(ctx, func(tx *sql.Tx) error {
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
// for snap.IssueID. Caller sets SyncedAt to the moment the
// snapshot was captured.
func (s *DoltStore) UpsertLinearProjectSnapshot(ctx context.Context, snap *storage.LinearProjectSnapshot) error {
	if snap == nil || snap.IssueID == "" {
		return fmt.Errorf("UpsertLinearProjectSnapshot: nil or empty IssueID")
	}
	return s.withWriteTx(ctx, func(tx *sql.Tx) error {
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

// DeleteLinearProjectSnapshot removes the snapshot row for
// issueID. Used when an epic's external_ref is rewritten so a
// stale snapshot doesn't shadow the new sync state.
func (s *DoltStore) DeleteLinearProjectSnapshot(ctx context.Context, issueID string) error {
	return s.withWriteTx(ctx, func(tx *sql.Tx) error {
		_, err := tx.ExecContext(ctx,
			`DELETE FROM linear_project_snapshots WHERE issue_id = ?`, issueID)
		if err != nil {
			return fmt.Errorf("delete linear_project_snapshots: %w", err)
		}
		return nil
	})
}

// Compile-time check.
var _ storage.LinearProjectSnapshotStore = (*DoltStore)(nil)
