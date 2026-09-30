package uow

import (
	"fmt"
	"os"
	"sync"

	"context"

	"github.com/steveyegge/beads/internal/storage/dberrors"
	storageissueops "github.com/steveyegge/beads/internal/storage/issueops"
)

// WakeExpiredDefers runs the lazy defer-wake sweep in its own unit of work,
// committing with a wake message iff any permanent issue woke — the same
// commit-iff-changed contract every RunTxResult writer keeps, so the steady
// state (nothing expired) costs one UPDATE on a transaction that never calls
// DOLT_COMMIT.
//
// It runs in a transaction of its own, not the caller's, because every
// ready-work READ in this stack runs under RunTxRead or a caller-owned UOW
// that rolls back — a sweep inside those spans would be silently discarded.
// Sequencing is what matters: sweep first, then read, and the read sees the
// woken rows.
//
// A WISP-only wake takes a third path: wisp tables are dolt_ignored, so the
// wake-message form (DOLT_COMMIT) would find nothing to commit and the
// transaction would roll back, silently discarding the wisp wakes — every
// subsequent ready read would then redo and re-discard the same writes
// forever. Those wakes persist via uw.Commit(ctx, "") — the ephemeral
// plain-COMMIT form RunTxEphemeral keeps for dolt_ignored state (bd-lrgn1) —
// issued inside the work func; a serialization failure from it surfaces as
// the closure's error and retries against a fresh unit of work, exactly like
// a commit issued by RunTxResult itself.
func WakeExpiredDefers(ctx context.Context, p UnitOfWorkProvider) (int, error) {
	// The owner scope of the sweep is a property of the store, read from its
	// own .beads config files (issueops.WakeExpiredDefersInTx), so the
	// provider names the workspace it was opened for. A provider that cannot
	// say where its store lives does not sweep at all: the store's files may
	// declare a scope, and waking its rows unread is the conflicting write.
	dir := WorkspaceDirOf(p)
	if dir == "" {
		unnamedWorkspaceOnce.Do(func() {
			fmt.Fprintln(os.Stderr, "warning: defer-wake sweep skipped: the provider does not name its store's .beads directory (uow.WithWorkspaceDir), so the store's owner scope cannot be read")
		})
		return 0, nil
	}
	ctx = storageissueops.WithDeferWakeWorkspace(ctx, dir)
	return RunTxResult(ctx, p, func(ctx context.Context, uw UnitOfWork) (int, string, error) {
		issues, wisps, err := uw.IssueUseCase().WakeExpiredDefers(ctx)
		if err != nil {
			return 0, "", err
		}
		if issues > 0 {
			return issues, storageissueops.WakeDefersCommitMessage(issues), nil
		}
		if wisps > 0 {
			if err := uw.Commit(ctx, ""); err != nil {
				return 0, "", err
			}
		}
		return 0, "", nil
	})
}

// WorkspaceProvider is implemented by a provider that was opened for one
// workspace and can name its .beads directory (WithWorkspaceDir). The
// defer-wake sweep reads that store's config files for its owner scope, so
// every provider wrapper forwards it (notifyingProvider here, the HTTP
// server's timedProvider): a wrapper that hid it would stop the sweep.
type WorkspaceProvider interface {
	WorkspaceDir() string
}

// WorkspaceDirOf returns the .beads directory a provider names, or "" when
// it names none.
func WorkspaceDirOf(p UnitOfWorkProvider) string {
	if wp, ok := p.(WorkspaceProvider); ok {
		return wp.WorkspaceDir()
	}
	return ""
}

// unnamedWorkspaceOnce keeps the unnamed-provider advisory to one line per
// process.
var unnamedWorkspaceOnce sync.Once

// advisoryAccessDeniedOnce rate-limits the access-denied advisory to one
// warning per process: a read-only-privileged SQL user hits it on every
// ready-front read, and repeating a configuration fact on each `bd ready`
// is noise, not signal.
var advisoryAccessDeniedOnce sync.Once

// WakeExpiredDefersAdvisory is WakeExpiredDefers under the read paths'
// contract: a ready listing must never fail because the sweep could not run,
// so errors are reduced to a stderr warning (warn-once for access-denied).
func WakeExpiredDefersAdvisory(ctx context.Context, p UnitOfWorkProvider) {
	_, err := WakeExpiredDefers(ctx, p)
	if err == nil {
		return
	}
	if dberrors.IsAccessDenied(err) {
		advisoryAccessDeniedOnce.Do(func() {
			fmt.Fprintf(os.Stderr, "warning: defer-wake sweep skipped (SQL user lacks write privileges; expired defers will not auto-wake from this client): %v\n", err)
		})
		return
	}
	fmt.Fprintf(os.Stderr, "warning: defer-wake sweep skipped: %v\n", err)
}
