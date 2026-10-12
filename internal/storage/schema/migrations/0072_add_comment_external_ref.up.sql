-- Migration 0053: comment-level external refs + updated_at for Linear comment
-- sync dedup (originally shipped on the fork as duplicate-numbered 0024).
--
-- Databases that ran the fork lineage already have these columns; each ADD
-- COLUMN / CREATE INDEX is guarded by an INFORMATION_SCHEMA check so re-running
-- here is a clean no-op (same pattern as migration 0038).
--
-- The three wisp_comments statements are ALSO guarded on the table's
-- existence (gf-4c9 row 1). wisp_comments is clone-local (dolt-ignored): a
-- clone of an upstream-shaped base opens with this migration pending and no
-- wisp_comments yet, because the ignored chain that materializes the wisp
-- tables runs AFTER the main chain in the same pass. A COLUMNS-only probe read
-- that absent table as a missing column and the prepared ALTER failed the
-- clone's first writable open ("table not found: wisp_comments"). On that
-- door the columns arrive through ignored/0027 instead. This edit of a
-- shipped migration is deliberate and recorded: its prior content hash is in
-- migrationPriorContentHashes (fork_reconcile.go) so cursor rows recorded
-- against the old bytes still verify, and in migrations/frozen-edit-allowlist.txt
-- for scripts/check-migration-hygiene.sh check C.

-- comments.external_ref
SET @needs_add = (
    SELECT IF(COUNT(*) = 0, 1, 0)
    FROM INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_SCHEMA = DATABASE()
      AND TABLE_NAME = 'comments'
      AND COLUMN_NAME = 'external_ref'
);
SET @sql = IF(@needs_add = 1,
    'ALTER TABLE comments ADD COLUMN external_ref VARCHAR(255) DEFAULT ''''',
    'SELECT 1');
PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;

-- comments.updated_at
SET @needs_add = (
    SELECT IF(COUNT(*) = 0, 1, 0)
    FROM INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_SCHEMA = DATABASE()
      AND TABLE_NAME = 'comments'
      AND COLUMN_NAME = 'updated_at'
);
SET @sql = IF(@needs_add = 1,
    'ALTER TABLE comments ADD COLUMN updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP',
    'SELECT 1');
PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;

-- idx_comments_external_ref
SET @needs_add = (
    SELECT IF(COUNT(*) = 0, 1, 0)
    FROM INFORMATION_SCHEMA.STATISTICS
    WHERE TABLE_SCHEMA = DATABASE()
      AND TABLE_NAME = 'comments'
      AND INDEX_NAME = 'idx_comments_external_ref'
);
SET @sql = IF(@needs_add = 1,
    'CREATE INDEX idx_comments_external_ref ON comments (external_ref)',
    'SELECT 1');
PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;

-- wisp_comments.external_ref
SET @needs_add = IF(
    (SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES
        WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'wisp_comments') > 0
    AND
    (SELECT COUNT(*) FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA = DATABASE()
          AND TABLE_NAME = 'wisp_comments'
          AND COLUMN_NAME = 'external_ref') = 0,
    1, 0);
SET @sql = IF(@needs_add = 1,
    'ALTER TABLE wisp_comments ADD COLUMN external_ref VARCHAR(255) DEFAULT ''''',
    'SELECT 1');
PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;

-- wisp_comments.updated_at
SET @needs_add = IF(
    (SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES
        WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'wisp_comments') > 0
    AND
    (SELECT COUNT(*) FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA = DATABASE()
          AND TABLE_NAME = 'wisp_comments'
          AND COLUMN_NAME = 'updated_at') = 0,
    1, 0);
SET @sql = IF(@needs_add = 1,
    'ALTER TABLE wisp_comments ADD COLUMN updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP',
    'SELECT 1');
PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;

-- idx_wisp_comments_external_ref
SET @needs_add = IF(
    (SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES
        WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'wisp_comments') > 0
    AND
    (SELECT COUNT(*) FROM INFORMATION_SCHEMA.STATISTICS
        WHERE TABLE_SCHEMA = DATABASE()
          AND TABLE_NAME = 'wisp_comments'
          AND INDEX_NAME = 'idx_wisp_comments_external_ref') = 0,
    1, 0);
SET @sql = IF(@needs_add = 1,
    'CREATE INDEX idx_wisp_comments_external_ref ON wisp_comments (external_ref)',
    'SELECT 1');
PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;
