package linear

import (
	"testing"
	"time"

	"github.com/steveyegge/beads/internal/storage"
	"github.com/steveyegge/beads/internal/tracker"
	"github.com/steveyegge/beads/internal/types"
)

// bd-6cl P4 unit tests for the per-Project resolve primitive +
// reverse status map. Pure functions, no Dolt/HTTP needed.

func TestMapProjectStateToBeads(t *testing.T) {
	cases := []struct {
		state           string
		wantStatus      types.Status
		wantCloseReason string
	}{
		{"completed", types.StatusClosed, ""},
		{"canceled", types.StatusClosed, "canceled"},
		{"started", types.StatusInProgress, ""},
		{"planned", types.StatusOpen, ""},
		{"paused", types.StatusOpen, ""},
		{"unknown_value", types.StatusOpen, ""}, // defensive default
		{"", types.StatusOpen, ""},
	}
	for _, tc := range cases {
		t.Run(tc.state, func(t *testing.T) {
			got := MapProjectStateToBeads(tc.state)
			if got.Status != tc.wantStatus {
				t.Errorf("Status: got %s, want %s", got.Status, tc.wantStatus)
			}
			if got.CloseReason != tc.wantCloseReason {
				t.Errorf("CloseReason: got %q, want %q", got.CloseReason, tc.wantCloseReason)
			}
		})
	}
}

// TestMapEpicToProjectStateRoundTrip — push followed by pull
// preserves the bead status for the cases that actually round-trip.
// Note: "canceled" is a pull-side ONLY mapping (push has no path
// to it because beads StatusClosed always maps to "completed").
// That asymmetry is intentional and locked by this test.
func TestMapEpicToProjectStateRoundTrip(t *testing.T) {
	for _, status := range []types.Status{types.StatusOpen, types.StatusInProgress, types.StatusClosed} {
		t.Run(string(status), func(t *testing.T) {
			projState := MapEpicToProjectState(status)
			back := MapProjectStateToBeads(projState)
			if back.Status != status {
				t.Errorf("round-trip %s: pushed as %s, pulled back as %s",
					status, projState, back.Status)
			}
		})
	}
}

func TestResolveProjectPull_FirstSyncBaseline(t *testing.T) {
	now := time.Now()
	epic := &types.Issue{ID: "ep-1", Title: "T", Description: "D", Status: types.StatusOpen}
	remote := tracker.TrackerProject{ID: "pid-1", Name: "T", Description: "D", State: "planned"}

	got := resolveProjectPull(epic, remote, epic, nil /* no snapshot */, tracker.ConflictTimestamp, now)

	if got.SkipReason == "" {
		t.Errorf("first sync should produce a SkipReason, got empty")
	}
	if len(got.Updates) != 0 {
		t.Errorf("first sync should produce no updates, got %v", got.Updates)
	}
	if got.NewSnapshot == nil {
		t.Fatal("first sync MUST stage a baseline snapshot")
	}
	if got.NewSnapshot.ProjectID != "pid-1" {
		t.Errorf("snapshot ProjectID: got %q want pid-1", got.NewSnapshot.ProjectID)
	}
}

// TestResolveProjectPull_NoChangeRefreshesSnapshotOnly — when
// neither side moved since lastSync, the only output is a fresh
// snapshot (synced_at bumped). Updates stays empty.
func TestResolveProjectPull_NoChange(t *testing.T) {
	now := time.Now()
	epic := &types.Issue{ID: "ep-1", Title: "Same", Description: "Same desc", Status: types.StatusOpen}
	remote := tracker.TrackerProject{ID: "pid-1", Name: "Same", Description: "Same desc", State: "planned"}
	snap := &storage.LinearProjectSnapshot{
		IssueID: "ep-1", ProjectID: "pid-1",
		Name: "Same", Description: "Same desc", State: "planned",
		SyncedAt: now.Add(-time.Hour),
	}

	got := resolveProjectPull(epic, remote, epic, snap, tracker.ConflictTimestamp, now)

	if len(got.Updates) != 0 {
		t.Errorf("no-change should produce no updates, got %v", got.Updates)
	}
	if got.NewSnapshot == nil {
		t.Fatal("no-change should still refresh snapshot")
	}
	if !got.NewSnapshot.SyncedAt.Equal(now) {
		t.Errorf("snapshot SyncedAt not bumped: got %v want %v", got.NewSnapshot.SyncedAt, now)
	}
}

// TestResolveProjectPull_RemoteOnlyChange — Linear UI edited the
// Project name; local epic untouched. The pull should apply the
// name change to bead.Title.
func TestResolveProjectPull_RemoteOnlyChange(t *testing.T) {
	now := time.Now()
	epic := &types.Issue{ID: "ep-1", Title: "Original", Description: "D", Status: types.StatusOpen}
	remote := tracker.TrackerProject{ID: "pid-1", Name: "Edited on Linear", Description: "D", State: "planned"}
	snap := &storage.LinearProjectSnapshot{
		IssueID: "ep-1", ProjectID: "pid-1",
		Name: "Original", Description: "D", State: "planned",
		SyncedAt: now.Add(-time.Hour),
	}

	got := resolveProjectPull(epic, remote, epic, snap, tracker.ConflictTimestamp, now)

	if got.Updates["title"] != "Edited on Linear" {
		t.Errorf("expected title update, got %v", got.Updates)
	}
}

// TestResolveProjectPull_Q1ClosedStatePreservation — Linear state
// is "started" but local epic is StatusClosed. Per Q1, the pull
// must NOT reopen the bead. status field omitted from Updates,
// SkipReason explains why.
func TestResolveProjectPull_Q1ClosedStatePreservation(t *testing.T) {
	now := time.Now()
	epic := &types.Issue{ID: "ep-1", Title: "Done epic", Status: types.StatusClosed}
	remote := tracker.TrackerProject{ID: "pid-1", Name: "Done epic", State: "started"}
	snap := &storage.LinearProjectSnapshot{
		IssueID: "ep-1", ProjectID: "pid-1",
		Name: "Done epic", State: "completed",
		SyncedAt: now.Add(-time.Hour),
	}

	got := resolveProjectPull(epic, remote, epic, snap, tracker.ConflictTimestamp, now)

	if _, hasStatus := got.Updates["status"]; hasStatus {
		t.Errorf("Q1 violation: status would reopen closed bead: %v", got.Updates)
	}
	if got.SkipReason == "" {
		t.Errorf("Q1 path should annotate SkipReason for observability")
	}
}

// TestResolveProjectPull_Q1AllowsClosedTransitions — when Linear
// is "canceled" and local is closed (open path), the mapping is
// still closed (with close_reason="canceled"). Status field IS
// applied because we're not reopening — we might be sharpening
// the close_reason from empty to "canceled".
func TestResolveProjectPull_Q1AllowsClosedToClosedTransitions(t *testing.T) {
	now := time.Now()
	epic := &types.Issue{ID: "ep-1", Title: "X", Status: types.StatusClosed}
	remote := tracker.TrackerProject{ID: "pid-1", Name: "X", State: "canceled"}
	snap := &storage.LinearProjectSnapshot{
		IssueID: "ep-1", ProjectID: "pid-1",
		Name: "X", State: "completed",
		SyncedAt: now.Add(-time.Hour),
	}

	got := resolveProjectPull(epic, remote, epic, snap, tracker.ConflictTimestamp, now)

	if got.Updates["status"] != string(types.StatusClosed) {
		t.Errorf("closed→closed transition should update status: %v", got.Updates)
	}
	if got.Updates["close_reason"] != "canceled" {
		t.Errorf("canceled mapping should set close_reason: %v", got.Updates)
	}
}

// TestResolveProjectPull_DescriptionRecombine — push split bd-cs1
// puts short summary in description and long body in content.
// Pull-side recombine prefers content when non-empty.
func TestResolveProjectPull_DescriptionRecombinePrefersContent(t *testing.T) {
	now := time.Now()
	epic := &types.Issue{ID: "ep-1", Title: "T", Description: "old", Status: types.StatusOpen}
	remote := tracker.TrackerProject{
		ID: "pid-1", Name: "T",
		Description: "Short summary",
		Content:     "Long-form authoritative body",
		State:       "planned",
	}
	snap := &storage.LinearProjectSnapshot{
		IssueID: "ep-1", ProjectID: "pid-1",
		Name: "T", Description: "old summary", Content: "old long-form", State: "planned",
		SyncedAt: now.Add(-time.Hour),
	}

	got := resolveProjectPull(epic, remote, epic, snap, tracker.ConflictTimestamp, now)

	if got.Updates["description"] != "Long-form authoritative body" {
		t.Errorf("recombine should prefer content: got %v", got.Updates["description"])
	}
}

// TestResolveProjectPull_TrueConflictTimestamp — both sides moved
// the same field; default policy is timestamp (whole-Project
// updatedAt comparison; remote newer means apply).
func TestResolveProjectPull_TrueConflictTimestampRemoteWins(t *testing.T) {
	syncedAt := time.Date(2026, 6, 8, 12, 0, 0, 0, time.UTC)
	remoteUpdated := syncedAt.Add(time.Hour) // newer than snapshot
	now := time.Now()
	epic := &types.Issue{ID: "ep-1", Title: "Local edited", Status: types.StatusOpen}
	remote := tracker.TrackerProject{
		ID: "pid-1", Name: "Remote edited", State: "planned",
		UpdatedAt: remoteUpdated,
	}
	snap := &storage.LinearProjectSnapshot{
		IssueID: "ep-1", ProjectID: "pid-1",
		Name: "Original", State: "planned",
		SyncedAt: syncedAt,
	}
	atSync := &types.Issue{ID: "ep-1", Title: "Original"}

	got := resolveProjectPull(epic, remote, atSync, snap, tracker.ConflictTimestamp, now)

	if got.Updates["title"] != "Remote edited" {
		t.Errorf("conflict + remote newer should pull remote, got %v", got.Updates)
	}
	if len(got.Conflicting) != 1 || got.Conflicting[0] != ProjectFieldName {
		t.Errorf("expected Conflicting=[name], got %v", got.Conflicting)
	}
}

// TestResolveProjectPull_TrueConflictLocalPolicy — same conflict,
// policy=local; remote update should NOT be applied.
func TestResolveProjectPull_TrueConflictLocalPolicy(t *testing.T) {
	syncedAt := time.Date(2026, 6, 8, 12, 0, 0, 0, time.UTC)
	now := time.Now()
	epic := &types.Issue{ID: "ep-1", Title: "Local edited", Status: types.StatusOpen}
	remote := tracker.TrackerProject{
		ID: "pid-1", Name: "Remote edited", State: "planned",
		UpdatedAt: syncedAt.Add(time.Hour),
	}
	snap := &storage.LinearProjectSnapshot{
		IssueID: "ep-1", ProjectID: "pid-1",
		Name: "Original", State: "planned",
		SyncedAt: syncedAt,
	}
	atSync := &types.Issue{ID: "ep-1", Title: "Original"}

	got := resolveProjectPull(epic, remote, atSync, snap, tracker.ConflictLocal, now)

	if _, has := got.Updates["title"]; has {
		t.Errorf("policy=local should NOT apply remote name change, got %v", got.Updates)
	}
}

// TestDiffLocalEpicFields_NoAtSync — newly-created since lastSync,
// every populated field marked as changed.
func TestDiffLocalEpicFields_NoAtSync(t *testing.T) {
	cur := &types.Issue{ID: "ep-1", Title: "T", Description: "D", Status: types.StatusOpen}
	got := diffLocalEpicFields(cur, nil)
	for _, want := range []ProjectFieldKind{ProjectFieldName, ProjectFieldDescription, ProjectFieldContent, ProjectFieldState} {
		if !got[want] {
			t.Errorf("expected %s flagged, got %v", want, got)
		}
	}
}

func TestRecombineProjectDescription(t *testing.T) {
	cases := []struct {
		name                       string
		description, content, want string
	}{
		{"content-only", "", "long body", "long body"},
		{"both", "short", "long body", "long body"}, // content wins
		{"description-only", "short summary", "", "short summary"},
		{"both-empty", "", "", ""},
		{"content-whitespace-falls-back", "summary", "   \n  ", "summary"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := recombineProjectDescription(tracker.TrackerProject{
				Description: tc.description, Content: tc.content,
			})
			if got != tc.want {
				t.Errorf("got %q, want %q", got, tc.want)
			}
		})
	}
}
