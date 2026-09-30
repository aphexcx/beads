package uow

import (
	"context"
	"testing"

	"github.com/steveyegge/beads/internal/storage/domain"
	"github.com/steveyegge/beads/internal/storage/issueops"
)

// wakeCaptureUseCase records the workspace the sweep's context carried.
type wakeCaptureUseCase struct {
	domain.IssueUseCase
	swept bool
	dir   string
}

func (u *wakeCaptureUseCase) WakeExpiredDefers(ctx context.Context) (int, int, error) {
	u.swept = true
	u.dir, _ = issueops.DeferWakeWorkspace(ctx)
	return 0, 0, nil
}

// workspaceNamingProvider is the mock provider plus a workspace name.
type workspaceNamingProvider struct {
	*mockUnitOfWorkProvider
	dir string
}

func (p *workspaceNamingProvider) WorkspaceDir() string { return p.dir }

// TestWakeExpiredDefersCarriesTheProviderWorkspace pins the seam the
// defer-wake owner scope depends on: a provider that names its workspace
// (WithWorkspaceDir / WorkspaceProvider) has that workspace on the sweep's
// context, also through the notifying wrapper; a provider that names none
// does not sweep at all, because its store's scope cannot be read.
func TestWakeExpiredDefersCarriesTheProviderWorkspace(t *testing.T) {
	ctx := context.Background()
	named := func(capture *wakeCaptureUseCase) *workspaceNamingProvider {
		return &workspaceNamingProvider{
			mockUnitOfWorkProvider: &mockUnitOfWorkProvider{uows: []*mockUnitOfWork{{issueUseCase: capture}}},
			dir:                    "/tmp/store/.beads",
		}
	}

	t.Run("named workspace reaches the sweep", func(t *testing.T) {
		capture := &wakeCaptureUseCase{}
		if _, err := WakeExpiredDefers(ctx, named(capture)); err != nil {
			t.Fatalf("WakeExpiredDefers: %v", err)
		}
		if !capture.swept || capture.dir != "/tmp/store/.beads" {
			t.Fatalf("swept=%v workspace=%q; want the provider's workspace on the sweep", capture.swept, capture.dir)
		}
	})

	t.Run("the notifying wrapper forwards it", func(t *testing.T) {
		capture := &wakeCaptureUseCase{}
		wrapped := &notifyingProvider{inner: named(capture)}
		if _, err := WakeExpiredDefers(ctx, wrapped); err != nil {
			t.Fatalf("WakeExpiredDefers: %v", err)
		}
		if !capture.swept || capture.dir != "/tmp/store/.beads" {
			t.Fatalf("swept=%v workspace=%q through the wrapper; want the inner provider's", capture.swept, capture.dir)
		}
	})

	t.Run("an unnamed provider does not sweep", func(t *testing.T) {
		capture := &wakeCaptureUseCase{}
		p := &mockUnitOfWorkProvider{uows: []*mockUnitOfWork{{issueUseCase: capture}}}
		if _, err := WakeExpiredDefers(ctx, p); err != nil {
			t.Fatalf("WakeExpiredDefers: %v", err)
		}
		if capture.swept || p.newUOWCalls != 0 {
			t.Fatalf("swept=%v, units of work opened=%d; want no sweep for a provider that names no store", capture.swept, p.newUOWCalls)
		}
	})

	t.Run("WithWorkspaceDir lands on the concrete provider", func(t *testing.T) {
		opts := applyProviderOptions([]ProviderOption{WithWorkspaceDir("/tmp/x/.beads")})
		p := &doltSQLProvider{workspaceDir: opts.workspaceDir}
		if got := WorkspaceDirOf(p); got != "/tmp/x/.beads" {
			t.Fatalf("WorkspaceDirOf(doltSQLProvider) = %q", got)
		}
	})
}
