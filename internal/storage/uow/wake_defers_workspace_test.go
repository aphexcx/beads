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
	dir string
	ok  bool
}

func (u *wakeCaptureUseCase) WakeExpiredDefers(ctx context.Context) (int, int, error) {
	u.dir, u.ok = issueops.DeferWakeWorkspace(ctx)
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
// context, even under provider decorators; a provider that names none runs
// the sweep without one.
func TestWakeExpiredDefersCarriesTheProviderWorkspace(t *testing.T) {
	ctx := context.Background()

	t.Run("named workspace reaches the sweep", func(t *testing.T) {
		capture := &wakeCaptureUseCase{}
		p := &workspaceNamingProvider{
			mockUnitOfWorkProvider: &mockUnitOfWorkProvider{uows: []*mockUnitOfWork{{issueUseCase: capture}}},
			dir:                    "/tmp/store/.beads",
		}
		if _, err := WakeExpiredDefers(ctx, p); err != nil {
			t.Fatalf("WakeExpiredDefers: %v", err)
		}
		if !capture.ok || capture.dir != "/tmp/store/.beads" {
			t.Fatalf("sweep context workspace = %q, %v; want the provider's", capture.dir, capture.ok)
		}
	})

	t.Run("named workspace survives a decorator", func(t *testing.T) {
		capture := &wakeCaptureUseCase{}
		inner := &workspaceNamingProvider{
			mockUnitOfWorkProvider: &mockUnitOfWorkProvider{uows: []*mockUnitOfWork{{issueUseCase: capture}}},
			dir:                    "/tmp/store/.beads",
		}
		if _, err := WakeExpiredDefers(ctx, NewNotifyingProvider(inner, Sinks{})); err != nil {
			t.Fatalf("WakeExpiredDefers: %v", err)
		}
		if !capture.ok || capture.dir != "/tmp/store/.beads" {
			t.Fatalf("sweep context workspace through the decorator = %q, %v; want the inner provider's", capture.dir, capture.ok)
		}
	})

	t.Run("unnamed provider runs the legacy sweep", func(t *testing.T) {
		capture := &wakeCaptureUseCase{}
		p := &mockUnitOfWorkProvider{uows: []*mockUnitOfWork{{issueUseCase: capture}}}
		if _, err := WakeExpiredDefers(ctx, p); err != nil {
			t.Fatalf("WakeExpiredDefers: %v", err)
		}
		if capture.ok {
			t.Fatalf("sweep context carried a workspace %q from a provider that named none", capture.dir)
		}
	})

	t.Run("WithWorkspaceDir lands on the concrete provider", func(t *testing.T) {
		opts := applyProviderOptions([]ProviderOption{WithWorkspaceDir("/tmp/x/.beads")})
		if opts.workspaceDir != "/tmp/x/.beads" {
			t.Fatalf("applyProviderOptions(WithWorkspaceDir) = %q", opts.workspaceDir)
		}
		p := &doltSQLProvider{workspaceDir: opts.workspaceDir}
		if got := workspaceDirOf(p); got != "/tmp/x/.beads" {
			t.Fatalf("workspaceDirOf(doltSQLProvider) = %q", got)
		}
	})
}
