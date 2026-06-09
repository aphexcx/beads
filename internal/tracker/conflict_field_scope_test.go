package tracker

import (
	"testing"
	"time"

	"github.com/steveyegge/beads/internal/storage"
	"github.com/steveyegge/beads/internal/types"
)

// bd-ajn unit tests for the field-scoped conflict primitives. These
// exercise the pure-function diff + intersect logic without spinning
// up Dolt or HTTP mocks — that integration coverage lives in P8's
// engine-level tests (which require the embedded backend).

func TestDiffLocalFields_NoChange(t *testing.T) {
	atSync := &types.Issue{ID: "bd-1", Title: "T", Description: "D", Status: types.StatusOpen, Priority: 2}
	current := &types.Issue{ID: "bd-1", Title: "T", Description: "D", Status: types.StatusOpen, Priority: 2}
	got := diffLocalFields(current, atSync)
	if len(got) != 0 {
		t.Errorf("expected no changes, got %v", got)
	}
}

func TestDiffLocalFields_StatusChanged(t *testing.T) {
	atSync := &types.Issue{ID: "bd-1", Title: "T", Description: "D", Status: types.StatusOpen, Priority: 2}
	current := &types.Issue{ID: "bd-1", Title: "T", Description: "D", Status: types.StatusClosed, Priority: 2}
	got := diffLocalFields(current, atSync)
	if !got[FieldStatus] || len(got) != 1 {
		t.Errorf("expected only status changed, got %v", got)
	}
}

// TestDiffLocalFields_NoSyncSnapshot — when atSync is nil (no
// dolt_history entry before lastSync, e.g., issue created since
// lastSync), every populated field reports as changed so the push
// path treats the issue as new work.
func TestDiffLocalFields_NoSyncSnapshot(t *testing.T) {
	current := &types.Issue{ID: "bd-new", Title: "T", Description: "D", Status: types.StatusOpen, Priority: 2}
	got := diffLocalFields(current, nil)
	for _, f := range []ConflictField{FieldTitle, FieldDescription, FieldStatus, FieldPriority} {
		if !got[f] {
			t.Errorf("expected %s to be marked changed, got %v", f, got)
		}
	}
}

func TestDiffExternalFields_FirstSyncReturnsNil(t *testing.T) {
	ti := &TrackerIssue{Title: "T", Description: "D", Priority: 2}
	got := diffExternalFields(ti, nil)
	if got != nil {
		t.Errorf("expected nil (first-sync signal), got %v", got)
	}
}

func TestDiffExternalFields_NoChangeAfterSnapshotPatch(t *testing.T) {
	// bd-ajn motivating scenario: snapshot was patched after
	// AssignIssueToProject succeeded, so Linear's current state
	// matches the snapshot. No external changes should register.
	syncedAt := time.Date(2026, 6, 8, 12, 0, 0, 0, time.UTC)
	snap := &storage.LinearIssueSnapshot{
		IssueID:     "bd-1",
		Title:       "Task A",
		Description: "Desc",
		Status:      "Todo",
		StateID:     "state-uuid-todo",
		Priority:    2,
		AssigneeID:  "user-1",
		ProjectID:   "project-uuid-after-migration",
		ParentID:    "",
		SyncedAt:    syncedAt,
	}
	ti := &TrackerIssue{
		Title:       "Task A",
		Description: "Desc",
		Priority:    2,
		AssigneeID:  "user-1",
		Metadata: map[string]interface{}{
			"project_id": "project-uuid-after-migration",
		},
	}
	got := diffExternalFields(ti, snap)
	// State comparison without a State value falls back to name
	// comparison (extractStatusName returns "" when State is nil),
	// which matches snap.Status only if snap.Status == "". Since
	// snap.Status here is "Todo", the name fallback would say
	// "changed". Validate by giving the TrackerIssue a State.
	if got[FieldStatus] {
		t.Errorf("status flagged as changed despite matching state: %v", got)
	}
	if got[FieldProject] {
		t.Errorf("project flagged as changed despite snapshot patch: %v", got)
	}
	if len(got) != 0 {
		t.Errorf("expected no external changes, got %v", got)
	}
}

// stateValue implements the GetID/GetName interfaces extractStateID /
// extractStatusName probe for.
type stateValue struct {
	id, name string
}

func (s stateValue) GetID() string   { return s.id }
func (s stateValue) GetName() string { return s.name }

func TestDiffExternalFields_StateIDChange(t *testing.T) {
	// state_id is the authoritative comparator (mayor Q2). Even when
	// the name matches (Linear renamed the state), the UUID change
	// flags as a status field change.
	snap := &storage.LinearIssueSnapshot{
		IssueID: "bd-1", StateID: "state-old", Status: "Todo",
		SyncedAt: time.Now(),
	}
	ti := &TrackerIssue{State: stateValue{id: "state-new", name: "Todo"}}
	got := diffExternalFields(ti, snap)
	if !got[FieldStatus] {
		t.Errorf("expected status changed when state_id differs, got %v", got)
	}
}

// TestDiffExternalFields_StateIDStable — Linear renamed the state
// "Todo" → "Backlog" but the underlying UUID is unchanged. The
// state_id-first comparator should NOT flag status as changed (we
// care about the SEMANTIC state, not the display label).
func TestDiffExternalFields_StateIDStable(t *testing.T) {
	snap := &storage.LinearIssueSnapshot{
		IssueID: "bd-1", StateID: "state-uuid-1", Status: "Todo",
		SyncedAt: time.Now(),
	}
	ti := &TrackerIssue{State: stateValue{id: "state-uuid-1", name: "Backlog"}}
	got := diffExternalFields(ti, snap)
	if got[FieldStatus] {
		t.Errorf("status should NOT change on rename when state_id stable, got %v", got)
	}
}

func TestComputeConflictingFields_EmptyWhenDisjoint(t *testing.T) {
	local := map[ConflictField]bool{FieldStatus: true}
	external := map[ConflictField]bool{FieldProject: true}
	got := computeConflictingFields(local, external)
	if len(got) != 0 {
		t.Errorf("expected empty intersect, got %v", got)
	}
}

func TestComputeConflictingFields_OverlapDetected(t *testing.T) {
	local := map[ConflictField]bool{FieldTitle: true, FieldStatus: true}
	external := map[ConflictField]bool{FieldStatus: true, FieldPriority: true}
	got := computeConflictingFields(local, external)
	if len(got) != 1 || got[0] != FieldStatus {
		t.Errorf("expected [status], got %v", got)
	}
}

// TestResolveFieldScopedConflict_BdAjnScenario — the motivating case
// from mayor's spec. Migration AssignIssueToProject patched the
// snapshot's project_id, so on the next sync the external diff sees
// NO change (snapshot matches Linear). Local user closed the issue.
// Resolver should:
//   - push status (local-only change)
//   - NOT mark for pull-overwrite (would revert the close)
//   - allow the push path to fire (forceIDs set)
func TestResolveFieldScopedConflict_BdAjnScenario(t *testing.T) {
	e := &Engine{} // resolveFieldScopedConflict doesn't touch e.Store or e.Tracker
	conflict := Conflict{
		IssueID:         "bd-1",
		LocalChanged:    map[ConflictField]bool{FieldStatus: true},
		ExternalChanged: map[ConflictField]bool{}, // snapshot patched → no external change
		Conflicting:     nil,
	}
	skipIDs := map[string]bool{}
	forceIDs := map[string]bool{}
	allowPullOverwriteIDs := map[string]bool{}
	pushFieldScopes := map[string]map[ConflictField]bool{}
	pullFieldScopes := map[string]map[ConflictField]bool{}

	e.resolveFieldScopedConflict(SyncOptions{}, conflict,
		skipIDs, forceIDs, allowPullOverwriteIDs,
		pushFieldScopes, pullFieldScopes)

	if !forceIDs["bd-1"] {
		t.Errorf("expected forceIDs[bd-1]=true (push the close), got %v", forceIDs)
	}
	if allowPullOverwriteIDs["bd-1"] {
		t.Errorf("expected NO pull-overwrite (would revert close), got %v", allowPullOverwriteIDs)
	}
	if !pushFieldScopes["bd-1"][FieldStatus] {
		t.Errorf("expected pushFieldScopes[bd-1][status]=true, got %v", pushFieldScopes)
	}
	if len(pullFieldScopes) != 0 {
		t.Errorf("expected no pull fields, got %v", pullFieldScopes)
	}
}

// TestResolveFieldScopedConflict_TrueConflictTimestamp — both sides
// changed the same field. Default policy is timestamp (local newer
// wins). Verify push fires, pull doesn't.
func TestResolveFieldScopedConflict_TrueConflictTimestamp(t *testing.T) {
	e := &Engine{}
	now := time.Now()
	conflict := Conflict{
		IssueID:         "bd-1",
		LocalUpdated:    now,                 // newer
		ExternalUpdated: now.Add(-time.Hour), // older
		LocalChanged:    map[ConflictField]bool{FieldStatus: true},
		ExternalChanged: map[ConflictField]bool{FieldStatus: true},
		Conflicting:     []ConflictField{FieldStatus},
	}
	skipIDs := map[string]bool{}
	forceIDs := map[string]bool{}
	allowPullOverwriteIDs := map[string]bool{}
	pushFieldScopes := map[string]map[ConflictField]bool{}
	pullFieldScopes := map[string]map[ConflictField]bool{}

	e.resolveFieldScopedConflict(SyncOptions{ConflictResolution: ConflictTimestamp}, conflict,
		skipIDs, forceIDs, allowPullOverwriteIDs,
		pushFieldScopes, pullFieldScopes)

	if !forceIDs["bd-1"] {
		t.Errorf("expected forceIDs[bd-1]=true (local newer), got %v", forceIDs)
	}
	if allowPullOverwriteIDs["bd-1"] {
		t.Errorf("expected NO pull-overwrite, got %v", allowPullOverwriteIDs)
	}
	if !pushFieldScopes["bd-1"][FieldStatus] {
		t.Errorf("expected pushFieldScopes[bd-1][status], got %v", pushFieldScopes)
	}
}

// TestResolveFieldScopedConflict_TrueConflictExternalPolicy — same as
// above but external-wins policy.
func TestResolveFieldScopedConflict_TrueConflictExternalPolicy(t *testing.T) {
	e := &Engine{}
	conflict := Conflict{
		IssueID:         "bd-1",
		LocalChanged:    map[ConflictField]bool{FieldStatus: true},
		ExternalChanged: map[ConflictField]bool{FieldStatus: true},
		Conflicting:     []ConflictField{FieldStatus},
	}
	skipIDs := map[string]bool{}
	forceIDs := map[string]bool{}
	allowPullOverwriteIDs := map[string]bool{}
	pushFieldScopes := map[string]map[ConflictField]bool{}
	pullFieldScopes := map[string]map[ConflictField]bool{}

	e.resolveFieldScopedConflict(SyncOptions{ConflictResolution: ConflictExternal}, conflict,
		skipIDs, forceIDs, allowPullOverwriteIDs,
		pushFieldScopes, pullFieldScopes)

	if forceIDs["bd-1"] {
		t.Errorf("expected NO force-push under external policy, got %v", forceIDs)
	}
	if !skipIDs["bd-1"] {
		t.Errorf("expected skipIDs[bd-1]=true under external policy, got %v", skipIDs)
	}
	if !allowPullOverwriteIDs["bd-1"] {
		t.Errorf("expected allowPullOverwriteIDs[bd-1]=true under external policy, got %v", allowPullOverwriteIDs)
	}
	if !pullFieldScopes["bd-1"][FieldStatus] {
		t.Errorf("expected pullFieldScopes[bd-1][status], got %v", pullFieldScopes)
	}
}

// TestResolveFieldScopedConflict_MixedAutoMerge — different fields
// changed on each side, no overlap. The auto-merge path: push
// LocalChanged, pull ExternalChanged. Both forceIDs AND
// allowPullOverwriteIDs end up set; without P6/P7 strict scoping,
// the whole-issue paths fire for both, which has the known mixed-
// field caveat documented in the commit message. The per-field maps
// (consumed by P6/P7 when they land) record the correct intent.
func TestResolveFieldScopedConflict_MixedAutoMerge(t *testing.T) {
	e := &Engine{}
	conflict := Conflict{
		IssueID:         "bd-1",
		LocalChanged:    map[ConflictField]bool{FieldTitle: true},
		ExternalChanged: map[ConflictField]bool{FieldDescription: true},
		Conflicting:     nil,
	}
	skipIDs := map[string]bool{}
	forceIDs := map[string]bool{}
	allowPullOverwriteIDs := map[string]bool{}
	pushFieldScopes := map[string]map[ConflictField]bool{}
	pullFieldScopes := map[string]map[ConflictField]bool{}

	e.resolveFieldScopedConflict(SyncOptions{}, conflict,
		skipIDs, forceIDs, allowPullOverwriteIDs,
		pushFieldScopes, pullFieldScopes)

	if !pushFieldScopes["bd-1"][FieldTitle] {
		t.Errorf("expected push scope title, got %v", pushFieldScopes)
	}
	if !pullFieldScopes["bd-1"][FieldDescription] {
		t.Errorf("expected pull scope description, got %v", pullFieldScopes)
	}
	// Both forceIDs and allowPullOverwriteIDs set — v1 fallback.
	if !forceIDs["bd-1"] || !allowPullOverwriteIDs["bd-1"] {
		t.Errorf("expected both force and overwrite set (whole-issue fallback): force=%v overwrite=%v",
			forceIDs, allowPullOverwriteIDs)
	}
}

// TestHasFieldScopedDiff — sanity check on the discriminator.
func TestHasFieldScopedDiff(t *testing.T) {
	cases := []struct {
		name string
		c    Conflict
		want bool
	}{
		{"empty", Conflict{}, false},
		{"local-only", Conflict{LocalChanged: map[ConflictField]bool{}}, true},
		{"external-only", Conflict{ExternalChanged: map[ConflictField]bool{}}, true},
		{"both", Conflict{LocalChanged: map[ConflictField]bool{}, ExternalChanged: map[ConflictField]bool{}}, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := tc.c.HasFieldScopedDiff(); got != tc.want {
				t.Errorf("got %v, want %v", got, tc.want)
			}
		})
	}
}
