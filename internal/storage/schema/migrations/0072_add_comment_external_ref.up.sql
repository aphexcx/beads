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
-- door the columns arrive through ignored/0027 instead. That edit of a
-- shipped migration, and the updated_at backfill below (gf-pu2 finding 2d),
-- are deliberate and recorded: every prior content hash of this file is in
-- migrationPriorContentHashes (fork_reconcile.go) so cursor rows recorded
-- against older bytes still verify as this lineage, and in
-- migrations/frozen-edit-allowlist.txt for scripts/check-migration-hygiene.sh
-- check C. A store that applied any prior text opens without a skew refusal
-- and runs no migration.

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
--
-- Added in three guarded steps rather than one: nullable, backfilled from
-- created_at, then given its default (gf-pu2 finding 2d). A one-step ADD
-- COLUMN with DEFAULT CURRENT_TIMESTAMP stamps every EXISTING comment with
-- the time this migration ran on this clone, so two replicas that upgrade
-- the same pre-0072 data independently disagree on identical rows, and the
-- comment conflict resolver refuses differing columns at the next pull. The
-- backfill is deterministic: the same bytes from two independent applies.
-- Rows created after the migration still take CURRENT_TIMESTAMP by default.
SET @needs_add = (
    SELECT IF(COUNT(*) = 0, 1, 0)
    FROM INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_SCHEMA = DATABASE()
      AND TABLE_NAME = 'comments'
      AND COLUMN_NAME = 'updated_at'
);
SET @sql = IF(@needs_add = 1,
    'ALTER TABLE comments ADD COLUMN updated_at DATETIME NULL',
    'SELECT 1');
PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;
-- The backfill and the default have their own guard, re-read after the ADD:
-- Dolt DDL commits per statement (migrations/README.md), so a migration
-- interrupted after the ADD replays with the column present and nullable,
-- and the two remaining steps must still run then. A column that is already
-- NOT NULL (every store that ran an earlier text of this file) skips both.
SET @needs_default = (
    SELECT IF(COUNT(*) = 1, 1, 0)
    FROM INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_SCHEMA = DATABASE()
      AND TABLE_NAME = 'comments'
      AND COLUMN_NAME = 'updated_at'
      AND IS_NULLABLE = 'YES'
);
SET @sql = IF(@needs_default = 1,
    'UPDATE comments SET updated_at = created_at WHERE updated_at IS NULL',
    'SELECT 1');
PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SET @sql = IF(@needs_default = 1,
    'ALTER TABLE comments MODIFY COLUMN updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP',
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
    'ALTER TABLE wisp_comments ADD COLUMN updated_at DATETIME NULL',
    'SELECT 1');
PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;
-- The same three-step shape as comments.updated_at, with the same resumable
-- guard on the backfill and the default. wisp_comments.created_at is nullable
-- (0021), so a row without one takes the epoch: deterministic, and the table
-- is clone-local (dolt-ignored) in any case. ignored/0027, this file's
-- companion on the ignored chain, keeps its one-step ADD: it runs on a clone
-- whose wisp_comments was just created empty, so there is nothing to stamp.
SET @needs_default = IF(
    (SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES
        WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'wisp_comments') > 0
    AND
    (SELECT COUNT(*) FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA = DATABASE()
          AND TABLE_NAME = 'wisp_comments'
          AND COLUMN_NAME = 'updated_at'
          AND IS_NULLABLE = 'YES') = 1,
    1, 0);
SET @sql = IF(@needs_default = 1,
    'UPDATE wisp_comments SET updated_at = COALESCE(created_at, ''1970-01-01 00:00:00'') WHERE updated_at IS NULL',
    'SELECT 1');
PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SET @sql = IF(@needs_default = 1,
    'ALTER TABLE wisp_comments MODIFY COLUMN updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP',
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
