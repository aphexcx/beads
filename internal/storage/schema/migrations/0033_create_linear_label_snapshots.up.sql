CREATE TABLE IF NOT EXISTS linear_label_snapshots (
    issue_id     VARCHAR(255) NOT NULL,
    label_id     VARCHAR(64)  NOT NULL,
    label_name   VARCHAR(255) NOT NULL,
    synced_at    TIMESTAMP    NOT NULL,
    PRIMARY KEY (issue_id, label_id),
    INDEX idx_linear_label_snapshots_issue (issue_id),
    CONSTRAINT fk_linear_label_snapshots_issue
        FOREIGN KEY (issue_id) REFERENCES issues(id) ON DELETE CASCADE
);
