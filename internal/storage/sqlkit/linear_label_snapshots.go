package sqlkit

import (
	"context"
	"fmt"
	"time"

	"github.com/steveyegge/beads/internal/storage"
)

// GetLinearLabelSnapshot reads all label-sync snapshot rows for an issue.
// Order is unspecified — callers must not depend on it (matches the
// storage.Transaction contract). Mirrors the Dolt transaction
// implementations; the linear_label_snapshots table is part of every SQL
// backend's schema (no dolt_ignore concept here — the whole database is
// already clone-local for these backends).
func (t *sqlkitTx) GetLinearLabelSnapshot(ctx context.Context, issueID string) ([]storage.LinearLabelSnapshotEntry, error) {
	rows, err := t.tx.QueryContext(ctx,
		`SELECT label_id, label_name FROM linear_label_snapshots WHERE issue_id = ?`, issueID)
	if err != nil {
		return nil, fmt.Errorf("query linear_label_snapshots: %w", err)
	}
	defer rows.Close()

	out := make([]storage.LinearLabelSnapshotEntry, 0)
	for rows.Next() {
		var e storage.LinearLabelSnapshotEntry
		if err := rows.Scan(&e.LabelID, &e.LabelName); err != nil {
			return nil, fmt.Errorf("scan linear_label_snapshots row: %w", err)
		}
		out = append(out, e)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("query linear_label_snapshots: %w", err)
	}
	return out, nil
}

// PutLinearLabelSnapshot atomically replaces the snapshot rows for an issue
// (delete-then-insert inside the surrounding transaction). Passing an empty
// entries slice clears the snapshot.
func (t *sqlkitTx) PutLinearLabelSnapshot(ctx context.Context, issueID string, entries []storage.LinearLabelSnapshotEntry) error {
	if _, err := t.tx.ExecContext(ctx,
		`DELETE FROM linear_label_snapshots WHERE issue_id = ?`, issueID); err != nil {
		return fmt.Errorf("delete linear_label_snapshots: %w", err)
	}
	if len(entries) == 0 {
		return nil
	}
	now := time.Now().UTC()
	for _, e := range entries {
		if _, err := t.tx.ExecContext(ctx,
			`INSERT INTO linear_label_snapshots (issue_id, label_id, label_name, synced_at) VALUES (?, ?, ?, ?)`,
			issueID, e.LabelID, e.LabelName, now); err != nil {
			return fmt.Errorf("insert linear_label_snapshots row: %w", err)
		}
	}
	return nil
}
