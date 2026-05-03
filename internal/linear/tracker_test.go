package linear

import (
	"context"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"

	"github.com/steveyegge/beads/internal/tracker"
	"github.com/steveyegge/beads/internal/types"
)

// fakeLabelClient stubs LabelsByName and CreateLabel for tracker tests.
type fakeLabelClient struct {
	existing map[string]string // name → ID (case-insensitive — keys are lowercase)
	created  []string          // names passed to CreateLabel, in order
	scope    LabelScope
}

func (f *fakeLabelClient) LabelsByName(ctx context.Context, names []string) (map[string]LinearLabel, error) {
	out := map[string]LinearLabel{}
	for _, n := range names {
		// Mirror real LabelsByName: lowercase key, preserve display case in Name.
		if id, ok := f.existing[n]; ok { // exact match (already lowercase here for simplicity)
			out[n] = LinearLabel{Name: n, ID: id}
		}
	}
	return out, nil
}

func (f *fakeLabelClient) CreateLabel(ctx context.Context, name string, scope LabelScope) (LinearLabel, error) {
	f.created = append(f.created, name)
	f.scope = scope
	id := "auto-" + name
	if f.existing == nil {
		f.existing = map[string]string{}
	}
	f.existing[name] = id
	return LinearLabel{Name: name, ID: id}, nil
}

func TestResolveLabelIDs_AutoCreatesMissing(t *testing.T) {
	fc := &fakeLabelClient{existing: map[string]string{"bug": "L-bug"}}
	got, err := resolveLabelIDs(context.Background(), fc, []string{"bug", "flaky-test"}, LabelScopeTeam, nil)
	if err != nil {
		t.Fatalf("resolveLabelIDs: %v", err)
	}
	if got["bug"].ID != "L-bug" {
		t.Errorf("bug: got %+v, want ID=L-bug", got["bug"])
	}
	if got["flaky-test"].ID != "auto-flaky-test" {
		t.Errorf("flaky-test: got %+v, want ID=auto-flaky-test", got["flaky-test"])
	}
	if !reflect.DeepEqual(fc.created, []string{"flaky-test"}) {
		t.Errorf("created: got %v, want [flaky-test]", fc.created)
	}
	if fc.scope != LabelScopeTeam {
		t.Errorf("scope: got %v, want team", fc.scope)
	}
}

func TestRegistered(t *testing.T) {
	factory := tracker.Get("linear")
	if factory == nil {
		t.Fatal("linear tracker not registered")
	}
	tr := factory()
	if tr.Name() != "linear" {
		t.Errorf("Name() = %q, want %q", tr.Name(), "linear")
	}
	if tr.DisplayName() != "Linear" {
		t.Errorf("DisplayName() = %q, want %q", tr.DisplayName(), "Linear")
	}
	if tr.ConfigPrefix() != "linear" {
		t.Errorf("ConfigPrefix() = %q, want %q", tr.ConfigPrefix(), "linear")
	}
}

func TestIsExternalRef(t *testing.T) {
	tr := &Tracker{}
	tests := []struct {
		ref  string
		want bool
	}{
		{"https://linear.app/team/issue/PROJ-123", true},
		{"https://linear.app/team/issue/PROJ-123/some-title", true},
		{"https://github.com/org/repo/issues/1", false},
		{"", false},
	}
	for _, tt := range tests {
		if got := tr.IsExternalRef(tt.ref); got != tt.want {
			t.Errorf("IsExternalRef(%q) = %v, want %v", tt.ref, got, tt.want)
		}
	}
}

func TestExtractIdentifier(t *testing.T) {
	tr := &Tracker{}
	tests := []struct {
		ref  string
		want string
	}{
		{"https://linear.app/team/issue/PROJ-123/some-title", "PROJ-123"},
		{"https://linear.app/team/issue/PROJ-123", "PROJ-123"},
	}
	for _, tt := range tests {
		if got := tr.ExtractIdentifier(tt.ref); got != tt.want {
			t.Errorf("ExtractIdentifier(%q) = %q, want %q", tt.ref, got, tt.want)
		}
	}
}

func TestBuildExternalRef(t *testing.T) {
	tr := &Tracker{}
	ti := &tracker.TrackerIssue{
		URL:        "https://linear.app/team/issue/PROJ-123/some-title-slug",
		Identifier: "PROJ-123",
	}
	ref := tr.BuildExternalRef(ti)
	want := "https://linear.app/team/issue/PROJ-123"
	if ref != want {
		t.Errorf("BuildExternalRef() = %q, want %q", ref, want)
	}
}

func TestFieldMapperPriority(t *testing.T) {
	m := &linearFieldMapper{config: DefaultMappingConfig()}

	// Linear 1 (urgent) -> Beads 0 (critical)
	if got := m.PriorityToBeads(1); got != 0 {
		t.Errorf("PriorityToBeads(1) = %d, want 0", got)
	}
	// Beads 0 (critical) -> Linear 1 (urgent)
	if got := m.PriorityToTracker(0); got != 1 {
		t.Errorf("PriorityToTracker(0) = %v, want 1", got)
	}
}

func TestFieldMapperStatus(t *testing.T) {
	m := &linearFieldMapper{config: DefaultMappingConfig()}

	// Started -> in_progress
	state := &State{Type: "started", Name: "In Progress"}
	if got := m.StatusToBeads(state); got != types.StatusInProgress {
		t.Errorf("StatusToBeads(started) = %q, want %q", got, types.StatusInProgress)
	}

	// Completed -> closed
	state = &State{Type: "completed", Name: "Done"}
	if got := m.StatusToBeads(state); got != types.StatusClosed {
		t.Errorf("StatusToBeads(completed) = %q, want %q", got, types.StatusClosed)
	}
}

func TestTrackerMultiTeamValidate(t *testing.T) {
	// Empty tracker should fail validation.
	tr := &Tracker{}
	if err := tr.Validate(); err == nil {
		t.Error("expected Validate() to fail on uninitialized tracker")
	}

	// Tracker with clients should pass.
	tr.clients = map[string]*Client{
		"team-1": NewClient("key", "team-1"),
	}
	if err := tr.Validate(); err != nil {
		t.Errorf("Validate() = %v, want nil", err)
	}
}

func TestTrackerSetTeamIDs(t *testing.T) {
	tr := &Tracker{}
	ids := []string{"id-1", "id-2", "id-3"}
	tr.SetTeamIDs(ids)

	if len(tr.teamIDs) != 3 {
		t.Fatalf("expected 3 team IDs, got %d", len(tr.teamIDs))
	}
	for i, want := range ids {
		if tr.teamIDs[i] != want {
			t.Errorf("teamIDs[%d] = %q, want %q", i, tr.teamIDs[i], want)
		}
	}
}

func TestTrackerTeamIDsAccessor(t *testing.T) {
	tr := &Tracker{teamIDs: []string{"a", "b"}}
	got := tr.TeamIDs()
	if len(got) != 2 || got[0] != "a" || got[1] != "b" {
		t.Errorf("TeamIDs() = %v, want [a b]", got)
	}
}

func TestTrackerPrimaryClient(t *testing.T) {
	tr := &Tracker{
		teamIDs: []string{"team-1", "team-2"},
		clients: map[string]*Client{
			"team-1": NewClient("key", "team-1"),
			"team-2": NewClient("key", "team-2"),
		},
	}

	client := tr.PrimaryClient()
	if client == nil {
		t.Fatal("PrimaryClient() returned nil")
	}
	if client.TeamID != "team-1" {
		t.Errorf("PrimaryClient().TeamID = %q, want %q", client.TeamID, "team-1")
	}

	// Empty tracker should return nil.
	empty := &Tracker{}
	if empty.PrimaryClient() != nil {
		t.Error("PrimaryClient() should return nil for empty tracker")
	}
}

func TestLinearToTrackerIssue(t *testing.T) {
	li := &Issue{
		ID:          "uuid-123",
		Identifier:  "TEAM-42",
		Title:       "Fix the bug",
		Description: "It's broken",
		URL:         "https://linear.app/team/issue/TEAM-42/fix-the-bug",
		Priority:    2,
		CreatedAt:   "2026-01-15T10:00:00Z",
		UpdatedAt:   "2026-01-16T14:30:00Z",
		Assignee:    &User{ID: "user-1", Name: "Alice", Email: "alice@example.com"},
		State:       &State{Type: "started", Name: "In Progress"},
	}

	ti := linearToTrackerIssue(li)

	if ti.ID != "uuid-123" {
		t.Errorf("ID = %q, want %q", ti.ID, "uuid-123")
	}
	if ti.Identifier != "TEAM-42" {
		t.Errorf("Identifier = %q, want %q", ti.Identifier, "TEAM-42")
	}
	if ti.Assignee != "Alice" {
		t.Errorf("Assignee = %q, want %q", ti.Assignee, "Alice")
	}
	if ti.AssigneeEmail != "alice@example.com" {
		t.Errorf("AssigneeEmail = %q, want %q", ti.AssigneeEmail, "alice@example.com")
	}
	if ti.Raw != li {
		t.Error("Raw should reference original linear.Issue")
	}
}

// TestRemoteStatusMatchesLocal covers the helper that lets UpdateIssueWithRemote
// decide whether to skip stateId in the GraphQL update. Pure logic — no network.
func TestRemoteStatusMatchesLocal(t *testing.T) {
	defaultConfig := DefaultMappingConfig()
	withExplicitDeferred := DefaultMappingConfig()
	withExplicitDeferred.StateMap["deferred"] = "deferred"
	withExplicitDeferred.ExplicitStateMap["deferred"] = "deferred"

	cases := []struct {
		name   string
		config *MappingConfig
		remote *tracker.TrackerIssue
		issue  *types.Issue
		want   bool
	}{
		{
			name:   "nil_remote",
			config: defaultConfig,
			remote: nil,
			issue:  &types.Issue{Status: types.StatusInProgress},
			want:   false,
		},
		{
			name:   "nil_issue",
			config: defaultConfig,
			remote: &tracker.TrackerIssue{State: &State{Type: "started", Name: "In Progress"}},
			issue:  nil,
			want:   false,
		},
		{
			name:   "nil_state_field",
			config: defaultConfig,
			remote: &tracker.TrackerIssue{State: nil},
			issue:  &types.Issue{Status: types.StatusInProgress},
			want:   false,
		},
		{
			name:   "wrong_state_type",
			config: defaultConfig,
			remote: &tracker.TrackerIssue{State: "not a *State"},
			issue:  &types.Issue{Status: types.StatusInProgress},
			want:   false,
		},
		{
			// Mayor's hw-gxrq scenario: GitHub PR moves issue to "In Review",
			// bead is still in_progress. Both map to in_progress through default
			// config (started type → in_progress). Helper returns true → engine
			// skips stateId → "In Review" preserved.
			name:   "in_review_preserves_in_progress",
			config: defaultConfig,
			remote: &tracker.TrackerIssue{State: &State{Type: "started", Name: "In Review"}},
			issue:  &types.Issue{Status: types.StatusInProgress},
			want:   true,
		},
		{
			// Real transition: bead just got closed locally, Linear is still
			// "In Progress". Helper returns false → engine pushes stateId.
			name:   "closed_does_not_match_in_progress",
			config: defaultConfig,
			remote: &tracker.TrackerIssue{State: &State{Type: "started", Name: "In Progress"}},
			issue:  &types.Issue{Status: types.StatusClosed},
			want:   false,
		},
		{
			// Reverse: Linear marked done by GitHub merge automation, bead is
			// still in_progress locally. Helper returns false → engine will
			// push stateId for "In Progress" (the user's actual transition,
			// e.g., a manual reopen) — preserves user intent.
			name:   "done_does_not_match_in_progress",
			config: defaultConfig,
			remote: &tracker.TrackerIssue{State: &State{Type: "completed", Name: "Done"}},
			issue:  &types.Issue{Status: types.StatusInProgress},
			want:   false,
		},
		{
			// With default config (no explicit name map), Linear "Deferred"
			// state-typed-backlog falls back to type → open. Local status
			// is deferred. They don't match → don't skip stateId.
			name:   "default_config_no_explicit_deferred_map",
			config: defaultConfig,
			remote: &tracker.TrackerIssue{State: &State{Type: "backlog", Name: "Deferred"}},
			issue:  &types.Issue{Status: types.StatusDeferred},
			want:   false,
		},
		{
			// With explicit name map, Linear "Deferred" pulls as deferred.
			// Local status deferred. Match → skip stateId.
			name:   "explicit_deferred_map_matches",
			config: withExplicitDeferred,
			remote: &tracker.TrackerIssue{State: &State{Type: "backlog", Name: "Deferred"}},
			issue:  &types.Issue{Status: types.StatusDeferred},
			want:   true,
		},
		{
			// Closed-bead exception (regression for codex MAJOR #1):
			// Both Linear "Done" (completed) and "Canceled" (canceled) map
			// to the coarse Beads `closed`. A bead intending Done (no cancel
			// close_reason) being pushed when remote is Canceled MUST push
			// stateId — otherwise the cancel state silently sticks.
			name:   "closed_done_intent_does_not_match_canceled_remote",
			config: defaultConfig,
			remote: &tracker.TrackerIssue{State: &State{Type: "canceled", Name: "Canceled"}},
			issue:  &types.Issue{Status: types.StatusClosed, CloseReason: ""},
			want:   false,
		},
		{
			// Reverse: bead intending Canceled (close_reason=stale:) being
			// pushed when remote is Done MUST also push stateId.
			name:   "closed_cancel_intent_does_not_match_done_remote",
			config: defaultConfig,
			remote: &tracker.TrackerIssue{State: &State{Type: "completed", Name: "Done"}},
			issue:  &types.Issue{Status: types.StatusClosed, CloseReason: "stale: no activity"},
			want:   false,
		},
		{
			// Agreement: bead intending Done + remote Done → preserve.
			name:   "closed_done_intent_matches_done_remote",
			config: defaultConfig,
			remote: &tracker.TrackerIssue{State: &State{Type: "completed", Name: "Done"}},
			issue:  &types.Issue{Status: types.StatusClosed, CloseReason: ""},
			want:   true,
		},
		{
			// Agreement: bead intending Canceled + remote Canceled → preserve.
			name:   "closed_cancel_intent_matches_canceled_remote",
			config: defaultConfig,
			remote: &tracker.TrackerIssue{State: &State{Type: "canceled", Name: "Canceled"}},
			issue:  &types.Issue{Status: types.StatusClosed, CloseReason: "duplicate"},
			want:   true,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			tr := &Tracker{config: tc.config}
			got := tr.remoteStatusMatchesLocal(tc.remote, tc.issue)
			if got != tc.want {
				t.Errorf("remoteStatusMatchesLocal: got %v, want %v", got, tc.want)
			}
		})
	}
}

// TestUpdateIssueWithRemote_PreservesInReviewOnTitleEdit verifies that when
// the remote's mapped status matches the local bead's status, the GraphQL
// update payload omits stateId — so Linear's "In Review" survives a metadata
// edit even though bead's status is still in_progress.
func TestUpdateIssueWithRemote_PreservesInReviewOnTitleEdit(t *testing.T) {
	var captured []string
	server := mockGraphQLServer(t, func(req string) string {
		captured = append(captured, req)
		if strings.Contains(req, "issueUpdate") {
			return `{"issueUpdate":{"success":true,"issue":{"id":"L-1","identifier":"TEST-1","title":"new title"}}}`
		}
		return `{}`
	})
	defer server.Close()

	tr := &Tracker{
		teamIDs: []string{"team-1"},
		clients: map[string]*Client{"team-1": NewClient("key", "team-1").WithEndpoint(server.URL)},
		config:  DefaultMappingConfig(),
	}

	// Remote is "In Review" (started type → in_progress); bead is in_progress.
	// They match → skip stateId.
	remote := &tracker.TrackerIssue{
		State: &State{Type: "started", Name: "In Review"},
		Raw:   &Issue{ID: "L-1", Identifier: "TEST-1"},
	}
	issue := &types.Issue{
		ID:     "bd-1",
		Title:  "new title",
		Status: types.StatusInProgress,
	}

	if _, err := tr.UpdateIssueWithRemote(context.Background(), "TEST-1", issue, remote); err != nil {
		t.Fatalf("UpdateIssueWithRemote: %v", err)
	}

	var updateReq string
	for _, r := range captured {
		if strings.Contains(r, "issueUpdate") {
			updateReq = r
			break
		}
	}
	if updateReq == "" {
		t.Fatal("no issueUpdate request captured")
	}
	if strings.Contains(updateReq, "stateId") {
		t.Errorf("expected NO stateId in payload (state preserved), got: %s", updateReq)
	}
	// Sanity: title should be in the payload (the actual change being pushed).
	if !strings.Contains(updateReq, "new title") {
		t.Errorf("expected title in payload, got: %s", updateReq)
	}
}

// TestUpdateIssueWithRemote_PushesStateOnRealTransition verifies that when
// the remote's mapped status does NOT match the local bead's status, the
// GraphQL update includes stateId — so a real transition (e.g., user closed
// the bead locally) propagates to Linear.
func TestUpdateIssueWithRemote_PushesStateOnRealTransition(t *testing.T) {
	var captured []string
	server := mockGraphQLServer(t, func(req string) string {
		captured = append(captured, req)
		// findStateIDForIssue calls BuildStateCache which queries TeamStates.
		if strings.Contains(req, "team(id:") || strings.Contains(req, "states") {
			return `{"team":{"id":"team-1","states":{"nodes":[
				{"id":"state-done","name":"Done","type":"completed"},
				{"id":"state-canceled","name":"Canceled","type":"canceled"}
			]}}}`
		}
		if strings.Contains(req, "issueUpdate") {
			return `{"issueUpdate":{"success":true,"issue":{"id":"L-1","identifier":"TEST-1"}}}`
		}
		return `{}`
	})
	defer server.Close()

	cfg := DefaultMappingConfig()
	cfg.ExplicitStateMap["done"] = "closed" // resolve closed → "Done" by name
	cfg.StateMap["done"] = "closed"

	tr := &Tracker{
		teamIDs: []string{"team-1"},
		clients: map[string]*Client{"team-1": NewClient("key", "team-1").WithEndpoint(server.URL)},
		config:  cfg,
	}

	// Remote is "In Progress"; bead just got closed locally. They differ →
	// engine should push stateId.
	remote := &tracker.TrackerIssue{
		State: &State{Type: "started", Name: "In Progress"},
		Raw:   &Issue{ID: "L-1", Identifier: "TEST-1"},
	}
	issue := &types.Issue{
		ID:     "bd-1",
		Title:  "shipped",
		Status: types.StatusClosed,
	}

	if _, err := tr.UpdateIssueWithRemote(context.Background(), "TEST-1", issue, remote); err != nil {
		t.Fatalf("UpdateIssueWithRemote: %v", err)
	}

	var updateReq string
	for _, r := range captured {
		if strings.Contains(r, "issueUpdate") {
			updateReq = r
			break
		}
	}
	if updateReq == "" {
		t.Fatal("no issueUpdate request captured")
	}
	if !strings.Contains(updateReq, "stateId") {
		t.Errorf("expected stateId in payload (real transition), got: %s", updateReq)
	}
}

// TestUpdateIssueWithRemote_NilRemote_FallsBack verifies that when no remote
// is provided (callers without a fresh fetch), the behavior matches the
// legacy UpdateIssue — stateId is always resolved and included.
func TestUpdateIssueWithRemote_NilRemote_FallsBack(t *testing.T) {
	var captured []string
	server := mockGraphQLServer(t, func(req string) string {
		captured = append(captured, req)
		if strings.Contains(req, "team(id:") || strings.Contains(req, "states") {
			return `{"team":{"id":"team-1","states":{"nodes":[
				{"id":"state-progress","name":"In Progress","type":"started"}
			]}}}`
		}
		if strings.Contains(req, "issueUpdate") {
			return `{"issueUpdate":{"success":true,"issue":{"id":"L-1","identifier":"TEST-1"}}}`
		}
		return `{}`
	})
	defer server.Close()

	cfg := DefaultMappingConfig()
	cfg.ExplicitStateMap["in progress"] = "in_progress"
	cfg.StateMap["in progress"] = "in_progress"

	tr := &Tracker{
		teamIDs: []string{"team-1"},
		clients: map[string]*Client{"team-1": NewClient("key", "team-1").WithEndpoint(server.URL)},
		config:  cfg,
	}

	issue := &types.Issue{
		ID:     "bd-1",
		Title:  "x",
		Status: types.StatusInProgress,
	}

	// remote == nil → must NOT skip stateId.
	if _, err := tr.UpdateIssueWithRemote(context.Background(), "TEST-1", issue, nil); err != nil {
		t.Fatalf("UpdateIssueWithRemote: %v", err)
	}

	var updateReq string
	for _, r := range captured {
		if strings.Contains(r, "issueUpdate") {
			updateReq = r
			break
		}
	}
	if updateReq == "" {
		t.Fatal("no issueUpdate request captured")
	}
	if !strings.Contains(updateReq, "stateId") {
		t.Errorf("nil remote: expected stateId in payload (legacy behavior), got: %s", updateReq)
	}
}

// TestValidatePushStateMappings_FailsOnMissingDeferredState verifies that
// when the rig config explicitly maps `linear.state_map.deferred = deferred`
// but the Linear team has no state named "Deferred" (and no backlog-type
// state to match by type), validation fails with a clear error before any
// push runs. This is the "Phase 2 deployed before Phase 1" guard.
func TestValidatePushStateMappings_FailsOnMissingDeferredState(t *testing.T) {
	server := mockGraphQLServer(t, func(req string) string {
		// Team workflow query — no Deferred state, no backlog type.
		if strings.Contains(req, "team(id:") || strings.Contains(req, "states") {
			return `{"team":{"id":"team-1","states":{"nodes":[
				{"id":"state-todo","name":"Todo","type":"unstarted"},
				{"id":"state-progress","name":"In Progress","type":"started"},
				{"id":"state-done","name":"Done","type":"completed"}
			]}}}`
		}
		return `{}`
	})
	defer server.Close()

	cfg := DefaultMappingConfig()
	// Explicitly map the standard 4 + deferred. Validator must check deferred too.
	for k, v := range map[string]string{
		"todo":        "open",
		"in progress": "in_progress",
		"done":        "closed",
		"deferred":    "deferred",
	} {
		cfg.StateMap[k] = v
		cfg.ExplicitStateMap[k] = v
	}

	tr := &Tracker{
		teamIDs: []string{"team-1"},
		clients: map[string]*Client{"team-1": NewClient("key", "team-1").WithEndpoint(server.URL)},
		config:  cfg,
	}

	err := tr.ValidatePushStateMappings(context.Background())
	if err == nil {
		t.Fatal("expected validation to fail (no Deferred state on team), got nil")
	}
	if !strings.Contains(err.Error(), "deferred") {
		t.Errorf("expected error to mention 'deferred', got: %v", err)
	}
}

// TestValidatePushStateMappings_PassesWhenDeferredStateExists is the success
// counterpart — verifies the new validator branch doesn't false-fail when
// the Linear team DOES have a matching state.
func TestValidatePushStateMappings_PassesWhenDeferredStateExists(t *testing.T) {
	server := mockGraphQLServer(t, func(req string) string {
		if strings.Contains(req, "team(id:") || strings.Contains(req, "states") {
			return `{"team":{"id":"team-1","states":{"nodes":[
				{"id":"state-deferred","name":"Deferred","type":"backlog"},
				{"id":"state-todo","name":"Todo","type":"unstarted"},
				{"id":"state-progress","name":"In Progress","type":"started"},
				{"id":"state-done","name":"Done","type":"completed"}
			]}}}`
		}
		return `{}`
	})
	defer server.Close()

	cfg := DefaultMappingConfig()
	for k, v := range map[string]string{
		"todo":        "open",
		"in progress": "in_progress",
		"done":        "closed",
		"deferred":    "deferred",
	} {
		cfg.StateMap[k] = v
		cfg.ExplicitStateMap[k] = v
	}

	tr := &Tracker{
		teamIDs: []string{"team-1"},
		clients: map[string]*Client{"team-1": NewClient("key", "team-1").WithEndpoint(server.URL)},
		config:  cfg,
	}

	if err := tr.ValidatePushStateMappings(context.Background()); err != nil {
		t.Errorf("expected validation to pass when Deferred state exists, got: %v", err)
	}
}

// TestPreviewUpdate exercises the DryRunPreviewer implementation —
// dry-run categorization that mirrors UpdateIssueWithRemote's state-skip
// decision. Pure local computation; no network.
func TestPreviewUpdate(t *testing.T) {
	tr := &Tracker{config: DefaultMappingConfig()}

	cases := []struct {
		name        string
		remote      *tracker.TrackerIssue
		localStatus types.Status
		want        tracker.DryRunDecision
	}{
		{
			name:        "nil_remote_treated_as_state_change",
			remote:      nil,
			localStatus: types.StatusInProgress,
			want:        tracker.DryRunStateChange,
		},
		{
			name: "in_review_remote_in_progress_local_state_preserved",
			remote: &tracker.TrackerIssue{
				State: &State{Type: "started", Name: "In Review"},
			},
			localStatus: types.StatusInProgress,
			want:        tracker.DryRunStatePreserved,
		},
		{
			name: "real_transition_state_change",
			remote: &tracker.TrackerIssue{
				State: &State{Type: "started", Name: "In Progress"},
			},
			localStatus: types.StatusClosed,
			want:        tracker.DryRunStateChange,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			issue := &types.Issue{Status: tc.localStatus}
			got := tr.PreviewUpdate(context.Background(), "TEST-1", issue, tc.remote)
			if got != tc.want {
				t.Errorf("PreviewUpdate: got %v, want %v", got, tc.want)
			}
		})
	}
}

// keep linker happy if httptest is otherwise unused
var _ = httptest.NewServer
