-- bd-ajn: register dolt_ignore pattern for linear_issue_snapshots BEFORE
-- the table is created (migration 0034), mirroring the 0028→0029
-- precedent for local_metadata.
--
-- linear_issue_snapshots holds per-clone Linear sync state and must not
-- replicate across clones — different clones run syncs at different
-- times and replicating the snapshots would cause false "Linear
-- changed" detection on clones that didn't run the last sync.
REPLACE INTO dolt_ignore VALUES ('linear_issue_snapshots', true);
CALL DOLT_ADD('dolt_ignore');
CALL DOLT_COMMIT('-m', 'chore: dolt-ignore linear_issue_snapshots (clone-local sync state)');
