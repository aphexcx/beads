-- bd-6cl: per-epic Linear-side Project snapshot for field-scoped
-- conflict resolution on the pull path. Symmetric extension of
-- bd-ajn linear_issue_snapshots, scoped to top-level epics that
-- live as Linear Projects rather than Issues.
--
-- One row per beads epic whose external_ref is a Linear Project URL.
-- The pull-side detector diffs the current Linear Project payload
-- vs this snapshot to compute per-field changes, combined with
-- dolt_history_issues for the local epic side.
--
-- Parallel to linear_issue_snapshots (separate table, not a kind-
-- column extension) because Projects lack parent, assignee, and
-- priority. Keeping them distinct also future-proofs against
-- schema drift between Issue and Project shapes on Linear side.
--
-- Dolt-ignored via migration 0052 (clone-local sync state). No FK
-- to issues because dolt-ignored tables cannot reference committed
-- tables safely. Application layer handles cleanup on bead delete.
--
-- project_id is Linear stable Project UUID, pinned per-row so a
-- future external_ref rewrite can detect the change and force a
-- re-baseline.
--
-- state is the Linear Project workflow state value: planned,
-- started, paused, completed, or canceled. Stored raw and the
-- reverse
-- mapper MapProjectStateToBeads handles bead-side translation.
CREATE TABLE IF NOT EXISTS linear_project_snapshots (
    issue_id     VARCHAR(255) NOT NULL PRIMARY KEY,
    project_id   VARCHAR(64),
    name         TEXT,
    description  TEXT,
    content      LONGTEXT,
    state        VARCHAR(32),
    synced_at    DATETIME NOT NULL,
    INDEX idx_linear_project_snapshots_synced_at (synced_at)
);
