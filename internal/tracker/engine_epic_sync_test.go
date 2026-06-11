//go:build cgo

package tracker

import (
	"context"
	"testing"

	"github.com/steveyegge/beads/internal/types"
)

// mockProjectSyncerTracker extends mockTracker with the ProjectSyncer
// capability. Records every Create/Update/Assign call so tests can
// assert what doEpicSync did (or didn't) trigger.
type mockProjectSyncerTracker struct {
	*mockTracker
	createCalls         []*types.Issue
	updateCalls         map[string]*types.Issue // projectID → epic
	assignCalls         map[string]string       // issueExtID → projectID
	setParentCalls      map[string]string       // issueExtID → parentExtID
	fetchProjectsResult []TrackerProject
}

func newMockProjectSyncerTracker(name string) *mockProjectSyncerTracker {
	return &mockProjectSyncerTracker{
		mockTracker:    newMockTracker(name),
		updateCalls:    make(map[string]*types.Issue),
		assignCalls:    make(map[string]string),
		setParentCalls: make(map[string]string),
	}
}

func (m *mockProjectSyncerTracker) CreateProject(_ context.Context, epic *types.Issue) (string, string, error) {
	m.createCalls = append(m.createCalls, epic)
	return "https://linear.app/test/project/" + epic.ID, "uuid-" + epic.ID, nil
}

func (m *mockProjectSyncerTracker) UpdateProject(_ context.Context, projectID string, epic *types.Issue) error {
	m.updateCalls[projectID] = epic
	return nil
}

func (m *mockProjectSyncerTracker) FetchProjects(_ context.Context, _ string) ([]TrackerProject, error) {
	return m.fetchProjectsResult, nil
}

func (m *mockProjectSyncerTracker) AssignIssueToProject(_ context.Context, issueExternalID, projectID string) error {
	m.assignCalls[issueExternalID] = projectID
	return nil
}

func (m *mockProjectSyncerTracker) SetIssueParent(_ context.Context, issueExternalID, parentExternalID string) error {
	m.setParentCalls[issueExternalID] = parentExternalID
	return nil
}

func (m *mockProjectSyncerTracker) IsProjectRef(ref string) bool {
	return len(ref) > 0 && (ref == "PROJECT_URL_SENTINEL" || (len(ref) > 30 && ref[:30] == "https://linear.app/test/project"))
}

func (m *mockProjectSyncerTracker) ExtractProjectID(ref string) string {
	return ref
}

// TestDoEpicSync_RespectsCreateClosed is the bd-pcb regression: closed
// top-level epics with no Linear identity must not have Projects
// auto-created unless --create-closed is explicitly set. Mirrors the
// existing CreateClosed gate doPush applies to non-epic beads.
func TestDoEpicSync_RespectsCreateClosed(t *testing.T) {
	ctx := context.Background()
	store := newTestStore(t)
	defer store.Close()

	// Local closed top-level epic, no external_ref.
	closedEpic := &types.Issue{
		ID: "bd-closed-epic", Title: "Historical work", Status: types.StatusClosed,
		IssueType: types.TypeEpic, Priority: 2,
	}
	if err := store.CreateIssue(ctx, closedEpic, "test-actor"); err != nil {
		t.Fatalf("CreateIssue: %v", err)
	}

	tracker := newMockProjectSyncerTracker("test")
	engine := NewEngine(tracker, store, "test-actor")

	// CreateClosed=false (default) → epic should be skipped.
	stats, err := engine.doEpicSync(ctx, SyncOptions{Push: true, CreateClosed: false})
	if err != nil {
		t.Fatalf("doEpicSync: %v", err)
	}
	if len(stats) != 0 {
		t.Errorf("epicProjectMap = %v, want empty (closed epic with no ref + CreateClosed=false → skip)", stats)
	}
	if len(tracker.createCalls) != 0 {
		t.Errorf("CreateProject called for closed epic: got %v", tracker.createCalls)
	}
}

// TestDoEpicSync_CreateClosedTrueAllowsClosedCreate verifies that the
// --create-closed escape hatch still works for historical-backfill
// users (matching the existing semantic for non-epic beads in doPush).
func TestDoEpicSync_CreateClosedTrueAllowsClosedCreate(t *testing.T) {
	ctx := context.Background()
	store := newTestStore(t)
	defer store.Close()

	closedEpic := &types.Issue{
		ID: "bd-closed-epic-2", Title: "Historical work 2", Status: types.StatusClosed,
		IssueType: types.TypeEpic, Priority: 2,
	}
	if err := store.CreateIssue(ctx, closedEpic, "test-actor"); err != nil {
		t.Fatalf("CreateIssue: %v", err)
	}

	tracker := newMockProjectSyncerTracker("test")
	engine := NewEngine(tracker, store, "test-actor")

	stats, err := engine.doEpicSync(ctx, SyncOptions{Push: true, CreateClosed: true})
	if err != nil {
		t.Fatalf("doEpicSync: %v", err)
	}
	if len(stats) != 1 {
		t.Errorf("epicProjectMap = %v, want 1 entry (--create-closed → create allowed)", stats)
	}
	if len(tracker.createCalls) != 1 {
		t.Fatalf("expected 1 CreateProject call, got %d", len(tracker.createCalls))
	}
	if tracker.createCalls[0].ID != closedEpic.ID {
		t.Errorf("CreateProject called for wrong epic: got %s, want %s",
			tracker.createCalls[0].ID, closedEpic.ID)
	}
}

// TestDoEpicSync_OpenEpicIgnoresCreateClosed verifies the gate ONLY
// fires for closed epics — open epics always go through CreateProject
// regardless of the flag.
func TestDoEpicSync_OpenEpicIgnoresCreateClosed(t *testing.T) {
	ctx := context.Background()
	store := newTestStore(t)
	defer store.Close()

	openEpic := &types.Issue{
		ID: "bd-open-epic", Title: "Active work", Status: types.StatusOpen,
		IssueType: types.TypeEpic, Priority: 2,
	}
	if err := store.CreateIssue(ctx, openEpic, "test-actor"); err != nil {
		t.Fatalf("CreateIssue: %v", err)
	}

	tracker := newMockProjectSyncerTracker("test")
	engine := NewEngine(tracker, store, "test-actor")

	stats, err := engine.doEpicSync(ctx, SyncOptions{Push: true, CreateClosed: false})
	if err != nil {
		t.Fatalf("doEpicSync: %v", err)
	}
	if len(stats) != 1 {
		t.Errorf("epicProjectMap = %v, want 1 entry (open epic always creates)", stats)
	}
	if len(tracker.createCalls) != 1 {
		t.Errorf("CreateProject not called for open epic: got %d", len(tracker.createCalls))
	}
}

// TestDoEpicSync_ClosedEpicAdoptionSkippedUnderCreateClosedFalse is
// codex bd-pcb round-1 D: the CreateClosed gate fires BEFORE
// ensureLinearProjectForEpic's adoption safety net. So a closed epic
// whose empty external_ref happens to match an existing Linear
// Project by title is NOT adopted under the default CreateClosed=false.
//
// This is the INTENDED semantic — the user said "don't touch closed
// historical work" and that includes adoption. To adopt, they pass
// --create-closed (same flag that allows creating). Documents the
// behavior so future readers don't add an adoption-vs-create carve-out
// by accident.
func TestDoEpicSync_ClosedEpicAdoptionSkippedUnderCreateClosedFalse(t *testing.T) {
	ctx := context.Background()
	store := newTestStore(t)
	defer store.Close()

	closedEpic := &types.Issue{
		ID: "bd-adopt-closed", Title: "Old work that has a Project somehow",
		Status: types.StatusClosed, IssueType: types.TypeEpic, Priority: 2,
	}
	if err := store.CreateIssue(ctx, closedEpic, "test-actor"); err != nil {
		t.Fatalf("CreateIssue: %v", err)
	}

	tracker := newMockProjectSyncerTracker("test")
	// Seed FetchProjects so the adoption-by-title path WOULD match
	// if it ran. Gate must prevent it under CreateClosed=false.
	tracker.fetchProjectsResult = []TrackerProject{
		{ID: "uuid-adopt", URL: "https://linear.app/test/project/adopt", Name: closedEpic.Title},
	}
	engine := NewEngine(tracker, store, "test-actor")

	stats, err := engine.doEpicSync(ctx, SyncOptions{Push: true, CreateClosed: false})
	if err != nil {
		t.Fatalf("doEpicSync: %v", err)
	}
	if len(stats) != 0 {
		t.Errorf("epicProjectMap = %v, want empty (adoption suppressed under CreateClosed=false)", stats)
	}
	if len(tracker.createCalls) != 0 {
		t.Errorf("CreateProject called: %v", tracker.createCalls)
	}
	if len(tracker.updateCalls) != 0 {
		t.Errorf("UpdateProject called via adoption path: %v", tracker.updateCalls)
	}
}

// TestDoEpicSync_ClosedEpicWithProjectRefStillUpdates verifies the
// codex-level distinction: the CreateClosed gate ONLY suppresses the
// CREATE path. A closed epic that already has a Project URL
// external_ref still flows to UpdateProject so MapEpicToProjectState
// propagates "completed" status to Linear. Otherwise closing an epic
// locally would silently fail to mark the corresponding Project done.
func TestDoEpicSync_ClosedEpicWithProjectRefStillUpdates(t *testing.T) {
	ctx := context.Background()
	store := newTestStore(t)
	defer store.Close()

	projectURL := "https://linear.app/test/project/abc123"
	closedEpic := &types.Issue{
		ID: "bd-closed-with-project", Title: "Done epic", Status: types.StatusClosed,
		IssueType: types.TypeEpic, Priority: 2,
		ExternalRef: strPtr(projectURL),
	}
	if err := store.CreateIssue(ctx, closedEpic, "test-actor"); err != nil {
		t.Fatalf("CreateIssue: %v", err)
	}

	tracker := newMockProjectSyncerTracker("test")
	// Seed FetchProjects so resolveProjectIDFromRef can find the UUID.
	tracker.fetchProjectsResult = []TrackerProject{{ID: "uuid-abc", URL: projectURL}}
	engine := NewEngine(tracker, store, "test-actor")

	stats, err := engine.doEpicSync(ctx, SyncOptions{Push: true, CreateClosed: false})
	if err != nil {
		t.Fatalf("doEpicSync: %v", err)
	}
	if len(tracker.createCalls) != 0 {
		t.Errorf("CreateProject called on update path: %v", tracker.createCalls)
	}
	if len(tracker.updateCalls) != 1 {
		t.Errorf("expected 1 UpdateProject call (state propagation), got %d", len(tracker.updateCalls))
	}
	if _, ok := stats[closedEpic.ID]; !ok {
		t.Errorf("epicProjectMap missing closed-with-project epic: %v", stats)
	}
}
