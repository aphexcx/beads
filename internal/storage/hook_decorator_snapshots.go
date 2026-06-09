package storage

import (
	"context"
	"fmt"
)

// bd-0iv: HookFiringStore (defined in hook_decorator.go) wraps a
// DoltStorage and adds hook-firing on mutations. The snapshot
// interfaces (LinearIssueSnapshotStore, LinearProjectSnapshotStore)
// were added AFTER HookFiringStore — DoltStorage doesn't mention
// them, so the embedded-passthrough mechanism doesn't satisfy them.
//
// Result: cmd/bd/main.go's `store = NewHookFiringStore(store, ...)`
// wrap silently drops both capabilities. The engine's hard-fail
// capability check (bd-3p8) then fires for the WRONG reason — the
// inner *DoltStore implements the interfaces; only the wrapper
// dropped them.
//
// Fix is the transparent-wrapper Go idiom: HookFiringStore declares
// each snapshot method as a pure pass-through to the inner store.
// Compile-time assertions prevent future interface evolution from
// silently breaking this again.
//
// Snapshot writes are NOT hook-firing surfaces today. Snapshots
// are clone-local sync state (dolt-ignored tables); they don't
// trigger on_create / on_update / on_close. So the pass-through
// is correct — no hook-firing wrapper logic needed.

// ── LinearIssueSnapshotStore pass-through (bd-ajn) ───────────────────

// GetLinearIssueSnapshot delegates to the inner store. The
// snapshot read is not a hook surface (clone-local sync state).
func (h *HookFiringStore) GetLinearIssueSnapshot(ctx context.Context, issueID string) (*LinearIssueSnapshot, error) {
	inner, ok := h.inner.(LinearIssueSnapshotStore)
	if !ok {
		return nil, fmt.Errorf("HookFiringStore: inner store %T does not implement LinearIssueSnapshotStore", h.inner)
	}
	return inner.GetLinearIssueSnapshot(ctx, issueID)
}

// UpsertLinearIssueSnapshot delegates to the inner store.
func (h *HookFiringStore) UpsertLinearIssueSnapshot(ctx context.Context, snap *LinearIssueSnapshot) error {
	inner, ok := h.inner.(LinearIssueSnapshotStore)
	if !ok {
		return fmt.Errorf("HookFiringStore: inner store %T does not implement LinearIssueSnapshotStore", h.inner)
	}
	return inner.UpsertLinearIssueSnapshot(ctx, snap)
}

// DeleteLinearIssueSnapshot delegates to the inner store.
func (h *HookFiringStore) DeleteLinearIssueSnapshot(ctx context.Context, issueID string) error {
	inner, ok := h.inner.(LinearIssueSnapshotStore)
	if !ok {
		return fmt.Errorf("HookFiringStore: inner store %T does not implement LinearIssueSnapshotStore", h.inner)
	}
	return inner.DeleteLinearIssueSnapshot(ctx, issueID)
}

// ── LinearProjectSnapshotStore pass-through (bd-6cl) ─────────────────

// GetLinearProjectSnapshot delegates to the inner store.
func (h *HookFiringStore) GetLinearProjectSnapshot(ctx context.Context, issueID string) (*LinearProjectSnapshot, error) {
	inner, ok := h.inner.(LinearProjectSnapshotStore)
	if !ok {
		return nil, fmt.Errorf("HookFiringStore: inner store %T does not implement LinearProjectSnapshotStore", h.inner)
	}
	return inner.GetLinearProjectSnapshot(ctx, issueID)
}

// UpsertLinearProjectSnapshot delegates to the inner store.
func (h *HookFiringStore) UpsertLinearProjectSnapshot(ctx context.Context, snap *LinearProjectSnapshot) error {
	inner, ok := h.inner.(LinearProjectSnapshotStore)
	if !ok {
		return fmt.Errorf("HookFiringStore: inner store %T does not implement LinearProjectSnapshotStore", h.inner)
	}
	return inner.UpsertLinearProjectSnapshot(ctx, snap)
}

// DeleteLinearProjectSnapshot delegates to the inner store.
func (h *HookFiringStore) DeleteLinearProjectSnapshot(ctx context.Context, issueID string) error {
	inner, ok := h.inner.(LinearProjectSnapshotStore)
	if !ok {
		return fmt.Errorf("HookFiringStore: inner store %T does not implement LinearProjectSnapshotStore", h.inner)
	}
	return inner.DeleteLinearProjectSnapshot(ctx, issueID)
}

// Compile-time guards (bd-0iv). If a snapshot interface evolves
// (new method, signature change) and HookFiringStore isn't
// updated to match, these assertions fail at build time INSTEAD
// of the engine silently dropping the capability at runtime —
// the regression mode bd-0iv was filed to prevent.
var (
	_ LinearIssueSnapshotStore   = (*HookFiringStore)(nil)
	_ LinearProjectSnapshotStore = (*HookFiringStore)(nil)
)
