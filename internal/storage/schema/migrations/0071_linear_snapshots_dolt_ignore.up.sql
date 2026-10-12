-- bd-ajn / bd-6cl: register dolt_ignore patterns for the Linear per-clone
-- snapshot tables BEFORE their ignored-source migrations create them
-- (migrations/ignored/0010 and 0011), following the 0028 precedent for
-- local_metadata / repo_mtimes.
--
-- linear_issue_snapshots and linear_project_snapshots hold per-clone Linear
-- sync state and must not replicate across clones — different clones run
-- syncs at different times, and replicating the snapshots would cause false
-- "Linear changed" detection on clones that didn't run the last sync.
--
-- (Originally shipped on the fork as migrations 0034/0036 with inline
-- DOLT_ADD/DOLT_COMMIT calls; the commit is now owned by the migration
-- runner, so this version only registers the patterns.)
REPLACE INTO dolt_ignore VALUES ('linear_issue_snapshots', true);
REPLACE INTO dolt_ignore VALUES ('linear_project_snapshots', true);
