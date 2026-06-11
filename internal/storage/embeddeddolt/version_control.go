//go:build cgo

package embeddeddolt

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"

	"github.com/steveyegge/beads/internal/storage"
	"github.com/steveyegge/beads/internal/storage/issueops"
	"github.com/steveyegge/beads/internal/storage/versioncontrolops"
)

// withDBConn opens a short-lived database connection configured for the
// store's database and branch and passes it to fn. Unlike withConn, no
// transaction is started — this is required for Dolt stored procedures
// (CALL DOLT_BRANCH, CALL DOLT_MERGE, etc.) that cannot run inside
// explicit SQL transactions.
func (s *EmbeddedDoltStore) withDBConn(ctx context.Context, fn func(db versioncontrolops.DBConn) error) (err error) {
	if s.closed.Load() {
		return errClosed
	}

	var db *sql.DB
	var cleanup func() error
	db, cleanup, err = OpenSQL(ctx, s.dataDir, s.database, s.branch)
	if err != nil {
		return
	}
	defer func() {
		err = errors.Join(err, cleanup())
		// Best-effort cleanup of orphaned tmp_pack_* files left by git
		// fetch in the Dolt git-remote-cache. Rate-limited internally.
		s.cleanGitRemoteCacheGarbage()
	}()

	return fn(db)
}

// withPinnedDBConn is withDBConn pinned to a single *sql.Conn, for operation
// sequences that depend on session state spanning statements — the pull path
// sets @@dolt_allow_commit_conflicts/@@dolt_force_transaction_commit and needs
// the subsequent DOLT_MERGE and settle statements to see them (bd-6dnrw.40).
// A *sql.DB may rotate connections between statements; a pinned conn cannot.
//
// The pinned conn inherits the database/branch session setup OpenSQL applied:
// the pool holds exactly the one connection OpenSQL configured (sequential
// Ping/USE/SET on a fresh pool), and db.Conn returns it — the same invariant
// ApplySchemaMigrations relies on.
func (s *EmbeddedDoltStore) withPinnedDBConn(ctx context.Context, fn func(db versioncontrolops.DBConn) error) (err error) {
	if s.closed.Load() {
		return errClosed
	}

	var db *sql.DB
	var cleanup func() error
	db, cleanup, err = OpenSQL(ctx, s.dataDir, s.database, s.branch)
	if err != nil {
		return
	}
	defer func() {
		err = errors.Join(err, cleanup())
		// Best-effort cleanup of orphaned tmp_pack_* files left by git
		// fetch in the Dolt git-remote-cache. Rate-limited internally.
		s.cleanGitRemoteCacheGarbage()
	}()

	conn, connErr := db.Conn(ctx)
	if connErr != nil {
		return fmt.Errorf("embeddeddolt: pin connection: %w", connErr)
	}
	defer conn.Close()

	return fn(conn)
}

func (s *EmbeddedDoltStore) Commit(ctx context.Context, message string) error {
	return s.withConn(ctx, true, func(tx *sql.Tx) error {
		if _, err := tx.ExecContext(ctx, "CALL DOLT_COMMIT('-Am', ?)", message); err != nil {
			return fmt.Errorf("dolt commit: %w", err)
		}
		return nil
	})
}

// CommitWithConfig commits all working set changes including config.
// so this is just an alias to satisfy the VersionControl interface (GH#3216).
func (s *EmbeddedDoltStore) CommitWithConfig(ctx context.Context, message string) error {
	return s.Commit(ctx, message)
}

func (s *EmbeddedDoltStore) AddRemote(ctx context.Context, name, url string) error {
	return s.withDBConn(ctx, func(db versioncontrolops.DBConn) error {
		_, err := db.ExecContext(ctx, "CALL DOLT_REMOTE('add', ?, ?)", name, url)
		return err
	})
}

func (s *EmbeddedDoltStore) HasRemote(ctx context.Context, name string) (bool, error) {
	var count int
	err := s.withConn(ctx, false, func(tx *sql.Tx) error {
		return tx.QueryRowContext(ctx, "SELECT count(*) FROM dolt_remotes WHERE name = ?", name).Scan(&count)
	})
	if err != nil {
		return false, err
	}
	return count > 0, nil
}

// ---------------------------------------------------------------------------
// Branch operations
// ---------------------------------------------------------------------------

func (s *EmbeddedDoltStore) Branch(ctx context.Context, name string) error {
	return s.withDBConn(ctx, func(db versioncontrolops.DBConn) error {
		return versioncontrolops.CreateBranch(ctx, db, name)
	})
}

func (s *EmbeddedDoltStore) Checkout(ctx context.Context, branch string) error {
	return s.withDBConn(ctx, func(db versioncontrolops.DBConn) error {
		return versioncontrolops.CheckoutBranch(ctx, db, branch)
	})
}

func (s *EmbeddedDoltStore) CurrentBranch(ctx context.Context) (string, error) {
	var branch string
	err := s.withDBConn(ctx, func(db versioncontrolops.DBConn) error {
		var err error
		branch, err = versioncontrolops.CurrentBranch(ctx, db)
		return err
	})
	return branch, err
}

func (s *EmbeddedDoltStore) DeleteBranch(ctx context.Context, branch string) error {
	return s.withDBConn(ctx, func(db versioncontrolops.DBConn) error {
		return versioncontrolops.DeleteBranch(ctx, db, branch)
	})
}

func (s *EmbeddedDoltStore) ListBranches(ctx context.Context) ([]string, error) {
	var branches []string
	err := s.withDBConn(ctx, func(db versioncontrolops.DBConn) error {
		var err error
		branches, err = versioncontrolops.ListBranches(ctx, db)
		return err
	})
	return branches, err
}

// ---------------------------------------------------------------------------
// Version control operations
// ---------------------------------------------------------------------------

// commitAuthor returns the author string for merge commits.
const commitAuthor = commitName + " <" + commitEmail + ">"

func (s *EmbeddedDoltStore) CommitExists(ctx context.Context, commitHash string) (bool, error) {
	var exists bool
	err := s.withDBConn(ctx, func(db versioncontrolops.DBConn) error {
		var err error
		exists, err = versioncontrolops.CommitExists(ctx, db, commitHash)
		return err
	})
	return exists, err
}

func (s *EmbeddedDoltStore) Status(ctx context.Context) (*storage.Status, error) {
	var status *storage.Status
	err := s.withDBConn(ctx, func(db versioncontrolops.DBConn) error {
		var err error
		status, err = versioncontrolops.Status(ctx, db)
		return err
	})
	return status, err
}

func (s *EmbeddedDoltStore) Log(ctx context.Context, limit int) ([]storage.CommitInfo, error) {
	var commits []storage.CommitInfo
	err := s.withDBConn(ctx, func(db versioncontrolops.DBConn) error {
		var err error
		commits, err = versioncontrolops.Log(ctx, db, limit)
		return err
	})
	return commits, err
}

func (s *EmbeddedDoltStore) Merge(ctx context.Context, branch string) ([]storage.Conflict, error) {
	var conflicts []storage.Conflict
	err := s.withDBConn(ctx, func(db versioncontrolops.DBConn) error {
		var err error
		conflicts, err = versioncontrolops.Merge(ctx, db, branch, commitAuthor)
		return err
	})
	return conflicts, err
}

func (s *EmbeddedDoltStore) GetConflicts(ctx context.Context) ([]storage.Conflict, error) {
	var conflicts []storage.Conflict
	err := s.withDBConn(ctx, func(db versioncontrolops.DBConn) error {
		var err error
		conflicts, err = versioncontrolops.GetConflicts(ctx, db)
		return err
	})
	return conflicts, err
}

func (s *EmbeddedDoltStore) ResolveConflicts(ctx context.Context, table string, strategy string) error {
	return s.withDBConn(ctx, func(db versioncontrolops.DBConn) error {
		return versioncontrolops.ResolveConflicts(ctx, db, table, strategy)
	})
}

// ---------------------------------------------------------------------------
// Remote operations
// ---------------------------------------------------------------------------

const defaultRemote = "origin"

// remoteAuthUser returns the username to authenticate with the remote, read
// from DOLT_REMOTE_USER. When set, push/pull/fetch invocations pass --user so
// the in-process Dolt server authenticates against the remotesapi (which
// otherwise rejects with CLONE_ADMIN). DOLT_REMOTE_PASSWORD is read by Dolt
// itself from the same process environment. Returns "" when no auth is
// configured (typical for git+ssh, file://, or unauthenticated remotes).
func remoteAuthUser() string {
	return os.Getenv("DOLT_REMOTE_USER")
}

func (s *EmbeddedDoltStore) RemoveRemote(ctx context.Context, name string) error {
	return s.withDBConn(ctx, func(db versioncontrolops.DBConn) error {
		return versioncontrolops.RemoveRemote(ctx, db, name)
	})
}

func (s *EmbeddedDoltStore) ListRemotes(ctx context.Context) ([]storage.RemoteInfo, error) {
	var remotes []storage.RemoteInfo
	err := s.withDBConn(ctx, func(db versioncontrolops.DBConn) error {
		var err error
		remotes, err = versioncontrolops.ListRemotes(ctx, db)
		return err
	})
	return remotes, err
}

func (s *EmbeddedDoltStore) Push(ctx context.Context) error {
	return s.withDBConn(ctx, func(db versioncontrolops.DBConn) error {
		return versioncontrolops.Push(ctx, db, defaultRemote, s.branch, remoteAuthUser())
	})
}

func (s *EmbeddedDoltStore) Pull(ctx context.Context) error {
	preHead := s.preMergeHead(ctx)
	err := s.withPinnedDBConn(ctx, func(db versioncontrolops.DBConn) error {
		return versioncontrolops.Pull(ctx, db, defaultRemote, s.branch, remoteAuthUser())
	})
	if err != nil {
		return err
	}
	return s.recomputeBlockedAfterPull(ctx, preHead)
}

func (s *EmbeddedDoltStore) ForcePush(ctx context.Context) error {
	return s.withDBConn(ctx, func(db versioncontrolops.DBConn) error {
		return versioncontrolops.ForcePush(ctx, db, defaultRemote, s.branch, remoteAuthUser())
	})
}

func (s *EmbeddedDoltStore) PushRemote(ctx context.Context, remote string, force bool) error {
	return s.withDBConn(ctx, func(db versioncontrolops.DBConn) error {
		if force {
			return versioncontrolops.ForcePush(ctx, db, remote, s.branch, remoteAuthUser())
		}
		return versioncontrolops.Push(ctx, db, remote, s.branch, remoteAuthUser())
	})
}

func (s *EmbeddedDoltStore) PullRemote(ctx context.Context, remote string) error {
	preHead := s.preMergeHead(ctx)
	err := s.withPinnedDBConn(ctx, func(db versioncontrolops.DBConn) error {
		return versioncontrolops.Pull(ctx, db, remote, s.branch, remoteAuthUser())
	})
	if err != nil {
		return err
	}
	return s.recomputeBlockedAfterPull(ctx, preHead)
}

func (s *EmbeddedDoltStore) Fetch(ctx context.Context, peer string) error {
	return s.withDBConn(ctx, func(db versioncontrolops.DBConn) error {
		return versioncontrolops.Fetch(ctx, db, peer, remoteAuthUser())
	})
}

func (s *EmbeddedDoltStore) PushTo(ctx context.Context, peer string) error {
	return s.withDBConn(ctx, func(db versioncontrolops.DBConn) error {
		return versioncontrolops.Push(ctx, db, peer, s.branch, remoteAuthUser())
	})
}

func (s *EmbeddedDoltStore) PullFrom(ctx context.Context, peer string) ([]storage.Conflict, error) {
	// Auto-commit pending changes before pull to prevent
	// "cannot merge with uncommitted changes" errors.
	if _, err := s.CommitPending(ctx, "beads"); err != nil {
		return nil, fmt.Errorf("commit pending before pull: %w", err)
	}

	preHead := s.preMergeHead(ctx)
	var conflicts []storage.Conflict
	err := s.withPinnedDBConn(ctx, func(db versioncontrolops.DBConn) error {
		if pullErr := versioncontrolops.Pull(ctx, db, peer, s.branch, remoteAuthUser()); pullErr != nil {
			// Check if the error is due to merge conflicts.
			c, conflictErr := versioncontrolops.GetConflicts(ctx, db)
			if conflictErr == nil && len(c) > 0 {
				conflicts = c
				return nil
			}
			return fmt.Errorf("pull from %s: %w", peer, pullErr)
		}
		return nil
	})
	if err != nil || len(conflicts) > 0 {
		// Conflicted pulls skip the recompute: the operator resolves first,
		// and the next sync picks the rows up.
		return conflicts, err
	}
	if err := s.recomputeBlockedAfterPull(ctx, preHead); err != nil {
		return conflicts, fmt.Errorf("pull succeeded but is_blocked recompute failed: %w", err)
	}
	return conflicts, nil
}

// preMergeHead reads the pre-pull HEAD for the post-merge is_blocked
// recompute (bd-6dnrw.3). Empty on failure, which degrades the recompute to a
// full pass instead of skipping the hook.
func (s *EmbeddedDoltStore) preMergeHead(ctx context.Context) string {
	head, err := s.GetCurrentCommit(ctx)
	if err != nil {
		return ""
	}
	return head
}

// recomputeBlockedAfterPull recomputes the denormalized is_blocked column for
// the rows a pull's merge changed (bd-6dnrw.3) and creates a Dolt commit for
// the result. is_blocked is otherwise maintained only by local write paths, so
// a merge that brings in another clone's status or dependency changes leaves
// it stale and `bd ready` trusts it. A pull that merged nothing (HEAD
// unchanged) is a no-op; derived state converges, so committing it on every
// clone is merge-safe.
func (s *EmbeddedDoltStore) recomputeBlockedAfterPull(ctx context.Context, preHead string) error {
	if err := s.withConn(ctx, true, func(tx *sql.Tx) error {
		return issueops.RecomputeIsBlockedAfterMergeInTx(ctx, tx, preHead)
	}); err != nil {
		return err
	}
	return s.withDBConn(ctx, func(db versioncontrolops.DBConn) error {
		return versioncontrolops.StageAndCommit(ctx, db,
			map[string]bool{"issues": true}, "bd: recompute is_blocked after pull", commitAuthor)
	})
}

// ---------------------------------------------------------------------------
// Backup operations
// ---------------------------------------------------------------------------

func (s *EmbeddedDoltStore) BackupAdd(ctx context.Context, name, url string) error {
	return s.withDBConn(ctx, func(db versioncontrolops.DBConn) error {
		return versioncontrolops.BackupAdd(ctx, db, name, url)
	})
}

func (s *EmbeddedDoltStore) BackupSync(ctx context.Context, name string) error {
	return s.withDBConn(ctx, func(db versioncontrolops.DBConn) error {
		return versioncontrolops.BackupSync(ctx, db, name)
	})
}

func (s *EmbeddedDoltStore) BackupRemove(ctx context.Context, name string) error {
	return s.withDBConn(ctx, func(db versioncontrolops.DBConn) error {
		return versioncontrolops.BackupRemove(ctx, db, name)
	})
}

// BackupDatabase registers dir as a file:// Dolt backup remote and syncs
// the database to it. The dir must exist locally. This preserves full Dolt
// commit history.
func (s *EmbeddedDoltStore) BackupDatabase(ctx context.Context, dir string) error {
	info, err := os.Stat(dir)
	if err != nil {
		return fmt.Errorf("backup destination does not exist: %w", err)
	}
	if !info.IsDir() {
		return fmt.Errorf("backup destination is not a directory: %s", dir)
	}

	backupURL, err := versioncontrolops.DirToFileURL(dir)
	if err != nil {
		return err
	}
	backupName := "backup_export"

	return s.withDBConn(ctx, func(db versioncontrolops.DBConn) error {
		// Register as a backup remote (idempotent — remove first if exists).
		_ = versioncontrolops.BackupRemove(ctx, db, backupName)
		if err := versioncontrolops.BackupAdd(ctx, db, backupName, backupURL); err != nil {
			// Another backup (e.g. "default" registered by `bd backup init`) may
			// already point to this URL. In that case, sync using the existing
			// remote name rather than failing.
			if conflict := versioncontrolops.ExtractAddressConflictName(err); conflict != "" {
				if syncErr := versioncontrolops.BackupSync(ctx, db, conflict); syncErr != nil {
					return fmt.Errorf("sync to backup: %w", syncErr)
				}
				return nil
			}
			return fmt.Errorf("register backup remote: %w", err)
		}
		if err := versioncontrolops.BackupSync(ctx, db, backupName); err != nil {
			return fmt.Errorf("sync to backup: %w", err)
		}
		return nil
	})
}

// RestoreDatabase restores the database from a Dolt backup at dir.
// The dir must exist locally and contain a valid Dolt backup.
// When force is true, an existing database is overwritten.
func (s *EmbeddedDoltStore) RestoreDatabase(ctx context.Context, dir string, force bool) error {
	info, err := os.Stat(dir)
	if err != nil {
		return fmt.Errorf("backup source does not exist: %w", err)
	}
	if !info.IsDir() {
		return fmt.Errorf("backup source is not a directory: %s", dir)
	}

	backupURL, err := versioncontrolops.DirToFileURL(dir)
	if err != nil {
		return err
	}

	return s.withDBConn(ctx, func(db versioncontrolops.DBConn) error {
		return versioncontrolops.BackupRestore(ctx, db, backupURL, s.database, force)
	})
}
