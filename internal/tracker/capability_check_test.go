package tracker

import (
	"context"
	"strings"
	"testing"
)

// bd-3p8 unit tests for checkRequiredStoreCapabilities. Covers the
// failure surface using minimal mocks; the positive case (real
// Linear tracker against an embedded/dolt backend that implements
// both snapshot interfaces) is covered implicitly by every
// engine_test.go test that constructs a full Engine — those tests
// pass through checkRequiredStoreCapabilities silently.

// mockSnapshotterTracker implements IssueTracker + PostPullSnapshotter
// without implementing ProjectPuller. Used to verify the
// LinearIssueSnapshotStore requirement.
type mockSnapshotterTracker struct {
	*mockTracker
}

func (m *mockSnapshotterTracker) RecordPullSnapshot(_ context.Context, _ string, _ TrackerIssue) error {
	return nil
}

// mockPullerTracker implements IssueTracker + ProjectPuller without
// implementing PostPullSnapshotter. Used to verify the
// LinearProjectSnapshotStore requirement.
type mockPullerTracker struct {
	*mockTracker
}

func (m *mockPullerTracker) PullProjects(_ context.Context, _ ProjectPullOptions) (*ProjectPullStats, error) {
	return &ProjectPullStats{}, nil
}

// mockNeitherCapabilityTracker is a plain tracker — neither
// PostPullSnapshotter nor ProjectPuller. The check must accept it
// regardless of what the store implements (GitHub/Jira shape).
type mockNeitherCapabilityTracker struct {
	*mockTracker
}

func TestCheckRequiredStoreCapabilities_PassesWithoutTrackerCapabilities(t *testing.T) {
	e := &Engine{
		Tracker: &mockNeitherCapabilityTracker{mockTracker: newMockTracker("plain")},
		// Store deliberately nil — the check doesn't reach the
		// assertion because the tracker doesn't advertise any
		// requirement.
	}
	if err := e.checkRequiredStoreCapabilities(); err != nil {
		t.Errorf("plain tracker should pass regardless of store: %v", err)
	}
}

// TestCheckRequiredStoreCapabilities_FailsOnMissingIssueSnapshotStore
// is the bd-3p8 regression: a tracker advertising
// PostPullSnapshotter must reject a backend that doesn't implement
// LinearIssueSnapshotStore.
func TestCheckRequiredStoreCapabilities_FailsOnMissingIssueSnapshotStore(t *testing.T) {
	e := &Engine{
		Tracker: &mockSnapshotterTracker{mockTracker: newMockTracker("snap-tracker")},
		Store:   nil, // any store that doesn't implement LinearIssueSnapshotStore
	}
	err := e.checkRequiredStoreCapabilities()
	if err == nil {
		t.Fatal("expected hard-fail when tracker is PostPullSnapshotter but store lacks LinearIssueSnapshotStore")
	}
	if !strings.Contains(err.Error(), "LinearIssueSnapshotStore") {
		t.Errorf("error should mention LinearIssueSnapshotStore: %v", err)
	}
	if !strings.Contains(err.Error(), "bd-3p8") {
		t.Errorf("error should reference bd-3p8 for traceability: %v", err)
	}
}

// TestCheckRequiredStoreCapabilities_FailsOnMissingProjectSnapshotStore
// is the bd-3p8 regression for the Project side.
func TestCheckRequiredStoreCapabilities_FailsOnMissingProjectSnapshotStore(t *testing.T) {
	e := &Engine{
		Tracker: &mockPullerTracker{mockTracker: newMockTracker("puller-tracker")},
		Store:   nil,
	}
	err := e.checkRequiredStoreCapabilities()
	if err == nil {
		t.Fatal("expected hard-fail when tracker is ProjectPuller but store lacks LinearProjectSnapshotStore")
	}
	if !strings.Contains(err.Error(), "LinearProjectSnapshotStore") {
		t.Errorf("error should mention LinearProjectSnapshotStore: %v", err)
	}
}

// Positive-case coverage: the existing engine_test.go suite
// constructs full Engines (real Linear tracker + embedded/dolt
// store) and would fail loudly if checkRequiredStoreCapabilities
// rejected them — so the success path is locked indirectly. No
// dedicated test needed here.
