package storage

import (
	"context"
	"strings"
	"testing"
	"time"
)

// bd-0iv regression test: HookFiringStore must transparently
// expose both LinearIssueSnapshotStore and LinearProjectSnapshotStore
// capabilities of its inner store. Without this, the engine's
// bd-3p8 capability check fires for the WRONG reason — the inner
// *DoltStore satisfies the interface, but the wrapper drops it.
//
// Test composition: a minimal in-memory DoltStorage stub that ALSO
// implements both snapshot interfaces. After wrapping with
// HookFiringStore, the resulting type assertion must succeed and
// the calls must reach the inner stub.

// snapshotCapableStub is a test double that implements the DoltStorage
// interface minimally PLUS both snapshot stores. We only exercise the
// snapshot paths through HookFiringStore; other DoltStorage methods
// panic to make accidental hook-side test reliance obvious.
type snapshotCapableStub struct {
	DoltStorage // embed nil — calls into non-snapshot methods would panic; not what we test
	issueRows   map[string]*LinearIssueSnapshot
	projectRows map[string]*LinearProjectSnapshot
	calls       []string // ordered call log for "was the inner method actually invoked" assertions
}

func newSnapshotCapableStub() *snapshotCapableStub {
	return &snapshotCapableStub{
		issueRows:   map[string]*LinearIssueSnapshot{},
		projectRows: map[string]*LinearProjectSnapshot{},
	}
}

func (s *snapshotCapableStub) GetLinearIssueSnapshot(_ context.Context, issueID string) (*LinearIssueSnapshot, error) {
	s.calls = append(s.calls, "GetLinearIssueSnapshot:"+issueID)
	row, ok := s.issueRows[issueID]
	if !ok {
		return nil, nil
	}
	return row, nil
}

func (s *snapshotCapableStub) UpsertLinearIssueSnapshot(_ context.Context, snap *LinearIssueSnapshot) error {
	s.calls = append(s.calls, "UpsertLinearIssueSnapshot:"+snap.IssueID)
	s.issueRows[snap.IssueID] = snap
	return nil
}

func (s *snapshotCapableStub) DeleteLinearIssueSnapshot(_ context.Context, issueID string) error {
	s.calls = append(s.calls, "DeleteLinearIssueSnapshot:"+issueID)
	delete(s.issueRows, issueID)
	return nil
}

func (s *snapshotCapableStub) GetLinearProjectSnapshot(_ context.Context, issueID string) (*LinearProjectSnapshot, error) {
	s.calls = append(s.calls, "GetLinearProjectSnapshot:"+issueID)
	row, ok := s.projectRows[issueID]
	if !ok {
		return nil, nil
	}
	return row, nil
}

func (s *snapshotCapableStub) UpsertLinearProjectSnapshot(_ context.Context, snap *LinearProjectSnapshot) error {
	s.calls = append(s.calls, "UpsertLinearProjectSnapshot:"+snap.IssueID)
	s.projectRows[snap.IssueID] = snap
	return nil
}

func (s *snapshotCapableStub) DeleteLinearProjectSnapshot(_ context.Context, issueID string) error {
	s.calls = append(s.calls, "DeleteLinearProjectSnapshot:"+issueID)
	delete(s.projectRows, issueID)
	return nil
}

// TestHookFiringStore_PreservesSnapshotCapabilities is the core
// bd-0iv lock. Type-asserts the wrapped store against both
// snapshot interfaces — must succeed after the bd-0iv fix.
func TestHookFiringStore_PreservesSnapshotCapabilities(t *testing.T) {
	inner := newSnapshotCapableStub()
	// Use interface-typed local before asserting — matches the
	// existing pattern in internal/storage/dolt/linear_snapshots_test.go's
	// TestDoltStoreSatisfiesSnapshotInterfaces (bd-3p8).
	var s interface{} = NewHookFiringStore(inner, nil)

	if _, ok := s.(LinearIssueSnapshotStore); !ok {
		t.Error("HookFiringStore must expose LinearIssueSnapshotStore from inner *DoltStore (bd-0iv)")
	}
	if _, ok := s.(LinearProjectSnapshotStore); !ok {
		t.Error("HookFiringStore must expose LinearProjectSnapshotStore from inner *DoltStore (bd-0iv)")
	}
}

// TestHookFiringStore_SnapshotCallsReachInnerStore — the pass-
// through actually delegates. Verifies the wrapper doesn't break
// the call chain (e.g., a typo in the impl that loops on itself).
func TestHookFiringStore_SnapshotCallsReachInnerStore(t *testing.T) {
	inner := newSnapshotCapableStub()
	wrapped := NewHookFiringStore(inner, nil)
	ctx := context.Background()

	// Upsert via wrapper, verify inner saw it.
	wantIssue := &LinearIssueSnapshot{IssueID: "iss-1", Title: "t", SyncedAt: time.Now()}
	if err := wrapped.UpsertLinearIssueSnapshot(ctx, wantIssue); err != nil {
		t.Fatalf("UpsertLinearIssueSnapshot via wrapper: %v", err)
	}
	if got, ok := inner.issueRows["iss-1"]; !ok || got != wantIssue {
		t.Errorf("inner store didn't receive UpsertLinearIssueSnapshot; rows=%v", inner.issueRows)
	}

	wantProject := &LinearProjectSnapshot{IssueID: "ep-1", Name: "p", SyncedAt: time.Now()}
	if err := wrapped.UpsertLinearProjectSnapshot(ctx, wantProject); err != nil {
		t.Fatalf("UpsertLinearProjectSnapshot via wrapper: %v", err)
	}
	if got, ok := inner.projectRows["ep-1"]; !ok || got != wantProject {
		t.Errorf("inner store didn't receive UpsertLinearProjectSnapshot; rows=%v", inner.projectRows)
	}

	// Read paths.
	got, err := wrapped.GetLinearIssueSnapshot(ctx, "iss-1")
	if err != nil || got == nil || got.IssueID != "iss-1" {
		t.Errorf("GetLinearIssueSnapshot round-trip via wrapper failed: got=%+v err=%v", got, err)
	}
	gotP, err := wrapped.GetLinearProjectSnapshot(ctx, "ep-1")
	if err != nil || gotP == nil || gotP.IssueID != "ep-1" {
		t.Errorf("GetLinearProjectSnapshot round-trip via wrapper failed: got=%+v err=%v", gotP, err)
	}

	// Delete paths.
	if err := wrapped.DeleteLinearIssueSnapshot(ctx, "iss-1"); err != nil {
		t.Errorf("DeleteLinearIssueSnapshot via wrapper: %v", err)
	}
	if _, ok := inner.issueRows["iss-1"]; ok {
		t.Errorf("inner issue snapshot not deleted: rows=%v", inner.issueRows)
	}
	if err := wrapped.DeleteLinearProjectSnapshot(ctx, "ep-1"); err != nil {
		t.Errorf("DeleteLinearProjectSnapshot via wrapper: %v", err)
	}
	if _, ok := inner.projectRows["ep-1"]; ok {
		t.Errorf("inner project snapshot not deleted: rows=%v", inner.projectRows)
	}

	// Call ordering sanity — verifies every method exercised hits
	// the inner store's recorder.
	want := []string{
		"UpsertLinearIssueSnapshot:iss-1",
		"UpsertLinearProjectSnapshot:ep-1",
		"GetLinearIssueSnapshot:iss-1",
		"GetLinearProjectSnapshot:ep-1",
		"DeleteLinearIssueSnapshot:iss-1",
		"DeleteLinearProjectSnapshot:ep-1",
	}
	if len(inner.calls) != len(want) {
		t.Fatalf("call count: got %d, want %d (calls=%v)", len(inner.calls), len(want), inner.calls)
	}
	for i, w := range want {
		if inner.calls[i] != w {
			t.Errorf("call[%d]: got %q, want %q", i, inner.calls[i], w)
		}
	}
}

// nonCapableStub implements DoltStorage but NOT the snapshot
// interfaces. Used to verify the wrapper's error path when the
// inner store doesn't expose the capability (defensive — shouldn't
// happen in production after bd-3p8, but the pass-through code
// has a fallback that should surface a clear error).
type nonCapableStub struct {
	DoltStorage
}

func TestHookFiringStore_NonCapableInnerReturnsError(t *testing.T) {
	wrapped := NewHookFiringStore(&nonCapableStub{}, nil)
	ctx := context.Background()

	// Call a snapshot method — wrapper should return a descriptive
	// error rather than panic or silently no-op.
	err := wrapped.UpsertLinearIssueSnapshot(ctx, &LinearIssueSnapshot{IssueID: "x"})
	if err == nil {
		t.Fatal("expected error when inner store lacks LinearIssueSnapshotStore")
	}
	if !strings.Contains(err.Error(), "LinearIssueSnapshotStore") {
		t.Errorf("error should mention the missing interface: %v", err)
	}

	err = wrapped.UpsertLinearProjectSnapshot(ctx, &LinearProjectSnapshot{IssueID: "x"})
	if err == nil {
		t.Fatal("expected error when inner store lacks LinearProjectSnapshotStore")
	}
	if !strings.Contains(err.Error(), "LinearProjectSnapshotStore") {
		t.Errorf("error should mention the missing interface: %v", err)
	}
}
