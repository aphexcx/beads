-- bd-ajn: per-issue Linear-side snapshot table.
--
-- One row per beads issue with a Linear external_ref, recording the
-- last-known Linear state at the moment of the last successful sync.
-- DetectConflicts diffs Linear's current state vs this snapshot to
-- compute which Linear-side FIELDS changed since lastSync, combined
-- with dolt_history_issues for the local side, to give field-scoped
-- conflict resolution.
--
-- Dolt-ignored via migration 0052 (clone-local sync state). No FK to
-- issues because dolt-ignored tables can't reference committed tables
-- safely. Application layer (DeleteLinearIssueSnapshot) cleans up.
CREATE TABLE IF NOT EXISTS linear_issue_snapshots (
    issue_id     VARCHAR(255) NOT NULL PRIMARY KEY,
    title        TEXT,
    description  LONGTEXT,
    status       VARCHAR(64),
    -- state_id is Linear's stable workflow-state UUID. Stored alongside
    -- the mapped beads status because the UUID survives state renames
    -- on Linear's side while the mapped name (Todo, In Progress, etc.)
    -- may drift. Reverse-mapped through the existing state_map on read.
    state_id     VARCHAR(64),
    priority     INT,
    assignee_id  VARCHAR(64),
    project_id   VARCHAR(64),
    parent_id    VARCHAR(64),
    synced_at    DATETIME NOT NULL,
    INDEX idx_linear_issue_snapshots_synced_at (synced_at)
);
