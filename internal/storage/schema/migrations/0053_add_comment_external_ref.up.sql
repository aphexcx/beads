-- Migration 0053: comment-level external refs + updated_at for Linear comment
-- sync dedup (originally shipped on the fork as duplicate-numbered 0024).
--
-- Databases that ran the fork lineage already have these columns; each ADD
-- COLUMN / CREATE INDEX is guarded by an INFORMATION_SCHEMA check so re-running
-- here is a clean no-op (same pattern as migration 0038).

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
