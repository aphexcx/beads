package migrations

import (
	"database/sql"
	"fmt"
)

// MigrateCreateLinearLabelSnapshots ensures the linear_label_snapshots table
// exists. This is the compat counterpart to canonical schema migration 0033;
// it's idempotent and safe to run on databases that already have the table
// (e.g. those that were upgraded through the canonical schema path).
func MigrateCreateLinearLabelSnapshots(db *sql.DB) error {
	exists, err := TableExists(db, "linear_label_snapshots")
	if err != nil {
		return fmt.Errorf("failed to check linear_label_snapshots table existence: %w", err)
	}
	if exists {
		return nil
	}

	stmt := `
		CREATE TABLE linear_label_snapshots (
			issue_id     VARCHAR(255) NOT NULL,
			label_id     VARCHAR(64)  NOT NULL,
			label_name   VARCHAR(255) NOT NULL,
			synced_at    DATETIME     NOT NULL,
			PRIMARY KEY (issue_id, label_id),
			INDEX idx_linear_label_snapshots_issue (issue_id),
			CONSTRAINT fk_linear_label_snapshots_issue
				FOREIGN KEY (issue_id) REFERENCES issues(id) ON DELETE CASCADE
		)`
	if _, err := db.Exec(stmt); err != nil {
		return fmt.Errorf("failed to create linear_label_snapshots table: %w", err)
	}
	return nil
}
