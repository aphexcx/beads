-- bd-6cl: register dolt_ignore pattern for linear_project_snapshots
-- BEFORE the table is created (migration 0037), mirroring the
-- 0034→0035 precedent for linear_issue_snapshots (bd-ajn).
--
-- linear_project_snapshots holds per-clone Linear Project sync state
-- and must not replicate across clones — different clones run syncs
-- at different times and replicating the snapshots would cause
-- incorrect "Linear changed this Project" detection on clones that
-- did not run the last sync. Same rationale as bd-ajn's
-- linear_issue_snapshots.
REPLACE INTO dolt_ignore VALUES ('linear_project_snapshots', true);
CALL DOLT_ADD('dolt_ignore');
CALL DOLT_COMMIT('-m', 'chore: dolt-ignore linear_project_snapshots (clone-local sync state)');
