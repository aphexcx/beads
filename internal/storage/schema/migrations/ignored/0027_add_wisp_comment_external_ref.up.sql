-- Ignored migration 0022: wisp_comments.external_ref / updated_at plus the
-- external_ref index, for fresh clones (bd-5rs).
--
-- Companion to main migration 0072, same pattern as ignored/0010's companion
-- to main 0051. Wisp tables are clone-local (dolt-ignored): a fresh clone
-- replicates schema_migrations already past 0072, so 0072's guarded ALTERs
-- never run there, while the ignored chain — which replays in full on every
-- clone — rebuilds wisp_comments from ignored/0001, which predates these
-- columns. Without this migration, wisp comment reads that select
-- external_ref/updated_at (issueops.GetIssueCommentsInTx,
-- doltTransaction.GetIssueComments) fail on every clone with unknown-column
-- errors.
--
-- Each statement is guarded by an INFORMATION_SCHEMA probe, so lineages
-- whose wisp_comments already has the columns (any database that ran fork
-- 0024 / main 0072 against a pre-existing table) re-run this as a no-op.

-- wisp_comments.external_ref
SET @needs_add = (
    SELECT IF(COUNT(*) = 0, 1, 0)
    FROM INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_SCHEMA = DATABASE()
      AND TABLE_NAME = 'wisp_comments'
      AND COLUMN_NAME = 'external_ref'
);
SET @sql = IF(@needs_add = 1,
    'ALTER TABLE wisp_comments ADD COLUMN external_ref VARCHAR(255) DEFAULT ''''',
    'SELECT 1');
PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;

-- wisp_comments.updated_at
SET @needs_add = (
    SELECT IF(COUNT(*) = 0, 1, 0)
    FROM INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_SCHEMA = DATABASE()
      AND TABLE_NAME = 'wisp_comments'
      AND COLUMN_NAME = 'updated_at'
);
SET @sql = IF(@needs_add = 1,
    'ALTER TABLE wisp_comments ADD COLUMN updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP',
    'SELECT 1');
PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;

-- idx_wisp_comments_external_ref
SET @needs_add = (
    SELECT IF(COUNT(*) = 0, 1, 0)
    FROM INFORMATION_SCHEMA.STATISTICS
    WHERE TABLE_SCHEMA = DATABASE()
      AND TABLE_NAME = 'wisp_comments'
      AND INDEX_NAME = 'idx_wisp_comments_external_ref'
);
SET @sql = IF(@needs_add = 1,
    'CREATE INDEX idx_wisp_comments_external_ref ON wisp_comments (external_ref)',
    'SELECT 1');
PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;
