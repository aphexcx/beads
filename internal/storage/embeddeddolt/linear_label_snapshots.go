//go:build cgo

package embeddeddolt

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/steveyegge/beads/internal/storage"
)

// selectLinearLabelSnapshot reads all snapshot rows for an issue.
// Order is unspecified — caller must not depend on it (matches the interface contract).
func selectLinearLabelSnapshot(ctx context.Context, tx *sql.Tx, issueID string) ([]storage.LinearLabelSnapshotEntry, error) {
	rows, err := tx.QueryContext(ctx,
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

// replaceLinearLabelSnapshot atomically replaces the snapshot rows for an issue.
// Caller must run this inside a transaction; the DELETE+INSERT pair is not safe outside one.
//
// IMPORTANT: callers must also call dirty.MarkDirty("linear_label_snapshots") so the
// table is staged for the Dolt commit. The transaction methods in transaction.go do this.
func replaceLinearLabelSnapshot(ctx context.Context, tx *sql.Tx, issueID string, entries []storage.LinearLabelSnapshotEntry) error {
	if _, err := tx.ExecContext(ctx,
		`DELETE FROM linear_label_snapshots WHERE issue_id = ?`, issueID); err != nil {
		return fmt.Errorf("delete linear_label_snapshots: %w", err)
	}
	if len(entries) == 0 {
		return nil
	}
	now := time.Now().UTC()
	for _, e := range entries {
		if _, err := tx.ExecContext(ctx,
			`INSERT INTO linear_label_snapshots (issue_id, label_id, label_name, synced_at) VALUES (?, ?, ?, ?)`,
			issueID, e.LabelID, e.LabelName, now); err != nil {
			return fmt.Errorf("insert linear_label_snapshots row: %w", err)
		}
	}
	return nil
}
