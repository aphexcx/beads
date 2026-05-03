package linear

import (
	"strings"
	"testing"
	"time"

	"github.com/steveyegge/beads/internal/types"
)

func TestGenerateIssueIDs(t *testing.T) {
	// Create test issues without IDs
	issues := []*types.Issue{
		{
			Title:       "First issue",
			Description: "Description 1",
			CreatedAt:   time.Now(),
		},
		{
			Title:       "Second issue",
			Description: "Description 2",
			CreatedAt:   time.Now().Add(-time.Hour),
		},
		{
			Title:       "Third issue",
			Description: "Description 3",
			CreatedAt:   time.Now().Add(-2 * time.Hour),
		},
	}

	// Generate IDs
	err := GenerateIssueIDs(issues, "test", "linear-import", IDGenerationOptions{})
	if err != nil {
		t.Fatalf("GenerateIssueIDs failed: %v", err)
	}

	// Verify all issues have IDs
	for i, issue := range issues {
		if issue.ID == "" {
			t.Errorf("Issue %d has empty ID", i)
		}
		// Verify prefix
		if !hasPrefix(issue.ID, "test-") {
			t.Errorf("Issue %d ID '%s' doesn't have prefix 'test-'", i, issue.ID)
		}
	}

	// Verify all IDs are unique
	seen := make(map[string]bool)
	for i, issue := range issues {
		if seen[issue.ID] {
			t.Errorf("Duplicate ID found: %s (issue %d)", issue.ID, i)
		}
		seen[issue.ID] = true
	}
}

func TestGenerateIssueIDsPreservesExisting(t *testing.T) {
	existingID := "test-existing"
	issues := []*types.Issue{
		{
			ID:          existingID,
			Title:       "Existing issue",
			Description: "Has an ID already",
			CreatedAt:   time.Now(),
		},
		{
			Title:       "New issue",
			Description: "Needs an ID",
			CreatedAt:   time.Now(),
		},
	}

	err := GenerateIssueIDs(issues, "test", "linear-import", IDGenerationOptions{})
	if err != nil {
		t.Fatalf("GenerateIssueIDs failed: %v", err)
	}

	// First issue should keep its original ID
	if issues[0].ID != existingID {
		t.Errorf("Existing ID was changed: got %s, want %s", issues[0].ID, existingID)
	}

	// Second issue should have a new ID
	if issues[1].ID == "" {
		t.Error("Second issue has empty ID")
	}
	if issues[1].ID == existingID {
		t.Error("Second issue has same ID as first (collision)")
	}
}

func TestGenerateIssueIDsNoDuplicates(t *testing.T) {
	// Create issues with identical content - should still get unique IDs
	now := time.Now()
	issues := []*types.Issue{
		{
			Title:       "Same title",
			Description: "Same description",
			CreatedAt:   now,
		},
		{
			Title:       "Same title",
			Description: "Same description",
			CreatedAt:   now,
		},
	}

	err := GenerateIssueIDs(issues, "bd", "linear-import", IDGenerationOptions{})
	if err != nil {
		t.Fatalf("GenerateIssueIDs failed: %v", err)
	}

	// Both should have IDs
	if issues[0].ID == "" || issues[1].ID == "" {
		t.Error("One or both issues have empty IDs")
	}

	// IDs should be different (nonce handles collision)
	if issues[0].ID == issues[1].ID {
		t.Errorf("Both issues have same ID: %s", issues[0].ID)
	}
}

func TestNormalizeIssueForLinearHashCanonicalizesExternalRef(t *testing.T) {
	slugged := "https://linear.app/crown-dev/issue/BEA-93/updated-title-for-beads"
	canonical := "https://linear.app/crown-dev/issue/BEA-93"
	issue := &types.Issue{
		Title:       "Title",
		Description: "Description",
		ExternalRef: &slugged,
	}

	normalized := NormalizeIssueForLinearHash(issue)
	if normalized.ExternalRef == nil {
		t.Fatal("expected external_ref to be present")
	}
	if *normalized.ExternalRef != canonical {
		t.Fatalf("expected canonical external_ref %q, got %q", canonical, *normalized.ExternalRef)
	}
}

func hasPrefix(s, prefix string) bool {
	return len(s) >= len(prefix) && s[:len(prefix)] == prefix
}

func TestDefaultMappingConfig(t *testing.T) {
	config := DefaultMappingConfig()

	// Check priority mappings
	if config.PriorityMap["0"] != 4 {
		t.Errorf("PriorityMap[0] = %d, want 4", config.PriorityMap["0"])
	}
	if config.PriorityMap["1"] != 0 {
		t.Errorf("PriorityMap[1] = %d, want 0", config.PriorityMap["1"])
	}

	// Check state mappings
	if config.StateMap["backlog"] != "open" {
		t.Errorf("StateMap[backlog] = %s, want open", config.StateMap["backlog"])
	}
	if config.StateMap["started"] != "in_progress" {
		t.Errorf("StateMap[started] = %s, want in_progress", config.StateMap["started"])
	}
	if config.StateMap["completed"] != "closed" {
		t.Errorf("StateMap[completed] = %s, want closed", config.StateMap["completed"])
	}

	// Check label type mappings
	if config.LabelTypeMap["bug"] != "bug" {
		t.Errorf("LabelTypeMap[bug] = %s, want bug", config.LabelTypeMap["bug"])
	}
	if config.LabelTypeMap["feature"] != "feature" {
		t.Errorf("LabelTypeMap[feature] = %s, want feature", config.LabelTypeMap["feature"])
	}

	// Check relation mappings
	if config.RelationMap["blocks"] != "blocks" {
		t.Errorf("RelationMap[blocks] = %s, want blocks", config.RelationMap["blocks"])
	}
}

func TestPriorityToBeads(t *testing.T) {
	config := DefaultMappingConfig()

	tests := []struct {
		linearPriority int
		want           int
	}{
		{0, 4}, // No priority -> Backlog
		{1, 0}, // Urgent -> Critical
		{2, 1}, // High -> High
		{3, 2}, // Medium -> Medium
		{4, 3}, // Low -> Low
		{5, 2}, // Unknown -> Medium (default)
	}

	for _, tt := range tests {
		got := PriorityToBeads(tt.linearPriority, config)
		if got != tt.want {
			t.Errorf("PriorityToBeads(%d) = %d, want %d", tt.linearPriority, got, tt.want)
		}
	}
}

func TestPriorityToLinear(t *testing.T) {
	config := DefaultMappingConfig()

	tests := []struct {
		beadsPriority int
		want          int
	}{
		{0, 1}, // Critical -> Urgent
		{1, 2}, // High -> High
		{2, 3}, // Medium -> Medium
		{3, 4}, // Low -> Low
		{4, 0}, // Backlog -> No priority
		{5, 3}, // Unknown -> Medium (default)
	}

	for _, tt := range tests {
		got := PriorityToLinear(tt.beadsPriority, config)
		if got != tt.want {
			t.Errorf("PriorityToLinear(%d) = %d, want %d", tt.beadsPriority, got, tt.want)
		}
	}
}

func TestStateToBeadsStatus(t *testing.T) {
	config := DefaultMappingConfig()

	tests := []struct {
		state *State
		want  types.Status
	}{
		{nil, types.StatusOpen},
		{&State{Type: "backlog", Name: "Backlog"}, types.StatusOpen},
		{&State{Type: "unstarted", Name: "Todo"}, types.StatusOpen},
		{&State{Type: "started", Name: "In Progress"}, types.StatusInProgress},
		{&State{Type: "completed", Name: "Done"}, types.StatusClosed},
		{&State{Type: "canceled", Name: "Cancelled"}, types.StatusClosed},
		{&State{Type: "unknown", Name: "Unknown"}, types.StatusOpen}, // Default
	}

	for _, tt := range tests {
		got := StateToBeadsStatus(tt.state, config)
		if got != tt.want {
			stateName := "nil"
			if tt.state != nil {
				stateName = tt.state.Type
			}
			t.Errorf("StateToBeadsStatus(%s) = %v, want %v", stateName, got, tt.want)
		}
	}
}

func TestParseBeadsStatus(t *testing.T) {
	tests := []struct {
		input string
		want  types.Status
	}{
		{"open", types.StatusOpen},
		{"OPEN", types.StatusOpen},
		{"in_progress", types.StatusInProgress},
		{"in-progress", types.StatusInProgress},
		{"inprogress", types.StatusInProgress},
		{"blocked", types.StatusBlocked},
		{"deferred", types.StatusDeferred},
		{"DEFERRED", types.StatusDeferred},
		{"closed", types.StatusClosed},
		{"CLOSED", types.StatusClosed},
		{"unknown", types.StatusOpen}, // Default
	}

	for _, tt := range tests {
		got := ParseBeadsStatus(tt.input)
		if got != tt.want {
			t.Errorf("ParseBeadsStatus(%q) = %v, want %v", tt.input, got, tt.want)
		}
	}
}

// TestParseBeadsStatus_Deferred is a focused regression test for the round
// of fixes that landed `deferred` round-trippable through ParseBeadsStatus
// (A1 of the bd-47l plan). Adjacent code already accepted the StatusDeferred
// constant; this just confirms the string-form parser accepts it too.
func TestParseBeadsStatus_Deferred(t *testing.T) {
	if got := ParseBeadsStatus("deferred"); got != types.StatusDeferred {
		t.Errorf("ParseBeadsStatus(\"deferred\") = %v, want StatusDeferred", got)
	}
}

func TestStatusToLinearStateType(t *testing.T) {
	tests := []struct {
		status types.Status
		want   string
	}{
		{types.StatusOpen, "unstarted"},
		{types.StatusInProgress, "started"},
		{types.StatusBlocked, "started"},
		{types.StatusDeferred, "backlog"}, // A2: deferred maps to Linear's backlog category
		{types.StatusClosed, "completed"},
		{types.Status("unknown"), "unstarted"}, // Unknown -> default
	}

	for _, tt := range tests {
		got := StatusToLinearStateType(tt.status)
		if got != tt.want {
			t.Errorf("StatusToLinearStateType(%v) = %q, want %q", tt.status, got, tt.want)
		}
	}
}

// TestStateToBeadsStatus_ExplicitNameWinsOverTypeFallback verifies the A3
// precedence inversion: when the user has explicitly mapped a Linear state
// name (e.g., `linear.state_map.deferred = deferred`), that mapping wins
// over the default type-based fallback (`backlog → open`).
//
// The test also exercises the regression case (a) from the plan: a team
// with NO explicit name mapping still gets the old type-only behavior.
func TestStateToBeadsStatus_ExplicitNameWinsOverTypeFallback(t *testing.T) {
	deferredState := &State{Type: "backlog", Name: "Deferred"}

	// Case (a) — no explicit name mapping. Should fall back to type → open.
	configWithoutExplicit := DefaultMappingConfig()
	if got := StateToBeadsStatus(deferredState, configWithoutExplicit); got != types.StatusOpen {
		t.Errorf("no explicit map: got %v, want StatusOpen (type-fallback for backlog)", got)
	}

	// Case (b) — explicit name mapping wins.
	configWithExplicit := DefaultMappingConfig()
	configWithExplicit.StateMap["deferred"] = "deferred"
	configWithExplicit.ExplicitStateMap["deferred"] = "deferred"
	if got := StateToBeadsStatus(deferredState, configWithExplicit); got != types.StatusDeferred {
		t.Errorf("explicit map: got %v, want StatusDeferred (name-match wins over type-fallback)", got)
	}

	// Bonus: no regression on the other 5 default state types when the
	// explicit map only covers `deferred`. Spot-check started → in_progress.
	startedState := &State{Type: "started", Name: "In Progress"}
	if got := StateToBeadsStatus(startedState, configWithExplicit); got != types.StatusInProgress {
		t.Errorf("started type-fallback: got %v, want StatusInProgress", got)
	}
}

func TestLabelToIssueType(t *testing.T) {
	config := DefaultMappingConfig()

	tests := []struct {
		labels *Labels
		want   types.IssueType
	}{
		{nil, types.TypeTask},
		{&Labels{Nodes: []Label{}}, types.TypeTask},
		{&Labels{Nodes: []Label{{Name: "bug"}}}, types.TypeBug},
		{&Labels{Nodes: []Label{{Name: "Bug"}}}, types.TypeBug},
		{&Labels{Nodes: []Label{{Name: "feature"}}}, types.TypeFeature},
		{&Labels{Nodes: []Label{{Name: "epic"}}}, types.TypeEpic},
		{&Labels{Nodes: []Label{{Name: "chore"}}}, types.TypeChore},
		{&Labels{Nodes: []Label{{Name: "random"}, {Name: "bug"}}}, types.TypeBug},
		{&Labels{Nodes: []Label{{Name: "contains-bug-keyword"}}}, types.TypeBug},
	}

	for _, tt := range tests {
		got := LabelToIssueType(tt.labels, config)
		if got != tt.want {
			t.Errorf("LabelToIssueType(%v) = %v, want %v", tt.labels, got, tt.want)
		}
	}
}

func TestParseIssueType(t *testing.T) {
	tests := []struct {
		input string
		want  types.IssueType
	}{
		{"bug", types.TypeBug},
		{"BUG", types.TypeBug},
		{"feature", types.TypeFeature},
		{"task", types.TypeTask},
		{"epic", types.TypeEpic},
		{"chore", types.TypeChore},
		{"unknown", types.TypeTask}, // Default
	}

	for _, tt := range tests {
		got := ParseIssueType(tt.input)
		if got != tt.want {
			t.Errorf("ParseIssueType(%q) = %v, want %v", tt.input, got, tt.want)
		}
	}
}

func TestRelationToBeadsDep(t *testing.T) {
	config := DefaultMappingConfig()

	tests := []struct {
		relationType string
		want         string
	}{
		{"blocks", "blocks"},
		{"blockedBy", "blocks"},
		{"duplicate", "duplicates"},
		{"related", "related"},
		{"unknown", "related"}, // Default
	}

	for _, tt := range tests {
		got := RelationToBeadsDep(tt.relationType, config)
		if got != tt.want {
			t.Errorf("RelationToBeadsDep(%q) = %q, want %q", tt.relationType, got, tt.want)
		}
	}
}

func TestIssueToBeads(t *testing.T) {
	config := DefaultMappingConfig()

	linearIssue := &Issue{
		ID:          "uuid-123",
		Identifier:  "PROJ-123",
		Title:       "Test Issue",
		Description: "Test description",
		URL:         "https://linear.app/team/issue/PROJ-123/test-issue",
		Priority:    2, // High
		State:       &State{Type: "started", Name: "In Progress"},
		Assignee:    &User{Name: "John Doe", Email: "john@example.com"},
		Labels:      &Labels{Nodes: []Label{{Name: "bug"}}},
		CreatedAt:   "2024-01-15T10:00:00Z",
		UpdatedAt:   "2024-01-16T12:00:00Z",
	}

	result := IssueToBeads(linearIssue, config)
	issue := result.Issue.(*types.Issue)

	if issue.Title != "Test Issue" {
		t.Errorf("Title = %q, want %q", issue.Title, "Test Issue")
	}
	if issue.Description != "Test description" {
		t.Errorf("Description = %q, want %q", issue.Description, "Test description")
	}
	if issue.Priority != 1 { // High in beads
		t.Errorf("Priority = %d, want 1", issue.Priority)
	}
	if issue.Status != types.StatusInProgress {
		t.Errorf("Status = %v, want %v", issue.Status, types.StatusInProgress)
	}
	if issue.Assignee != "john@example.com" {
		t.Errorf("Assignee = %q, want %q", issue.Assignee, "john@example.com")
	}
	if issue.IssueType != types.TypeBug {
		t.Errorf("IssueType = %v, want %v", issue.IssueType, types.TypeBug)
	}
	if issue.ExternalRef == nil {
		t.Error("ExternalRef should not be nil")
	}
}

// TestIssueToBeadsSetsCancelCloseReason verifies that pulling a Linear
// issue in a canceled-type state stamps a close_reason on the beads copy
// so the subsequent push doesn't route it as Done. Issues pulled in
// Done/other states get no auto close_reason so user-written reasons
// survive on unrelated beads (buildPullIssueUpdates enforces that).
func TestIssueToBeadsSetsCancelCloseReason(t *testing.T) {
	config := DefaultMappingConfig()

	base := func(state *State) *Issue {
		return &Issue{
			ID: "u-1", Identifier: "T-1", Title: "t", URL: "https://linear.app/t/issue/T-1",
			State:     state,
			CreatedAt: "2026-04-01T00:00:00Z", UpdatedAt: "2026-04-01T00:00:00Z",
		}
	}

	closeReasonOf := func(conv *IssueConversion) string {
		issue, ok := conv.Issue.(*types.Issue)
		if !ok {
			t.Fatalf("conv.Issue is not *types.Issue: %T", conv.Issue)
		}
		return issue.CloseReason
	}

	got := closeReasonOf(IssueToBeads(base(&State{Type: "canceled", Name: "Canceled"}), config))
	if got == "" || !strings.Contains(got, "canceled") {
		t.Errorf("canceled state should signal cancel intent, got %q", got)
	}

	got = closeReasonOf(IssueToBeads(base(&State{Type: "canceled", Name: "Duplicate"}), config))
	if got == "" {
		t.Errorf("duplicate (canceled-type) should set CloseReason, got empty")
	}

	got = closeReasonOf(IssueToBeads(base(&State{Type: "completed", Name: "Done"}), config))
	if got != "" {
		t.Errorf("completed state should leave CloseReason empty, got %q", got)
	}

	got = closeReasonOf(IssueToBeads(base(&State{Type: "started", Name: "In Progress"}), config))
	if got != "" {
		t.Errorf("non-closed state should leave CloseReason empty, got %q", got)
	}
}

func TestIssueToBeadsWithParent(t *testing.T) {
	config := DefaultMappingConfig()

	linearIssue := &Issue{
		ID:          "uuid-456",
		Identifier:  "PROJ-456",
		Title:       "Child Issue",
		Description: "Child description",
		URL:         "https://linear.app/team/issue/PROJ-456",
		Priority:    3,
		State:       &State{Type: "unstarted", Name: "Todo"},
		Parent:      &Parent{ID: "uuid-123", Identifier: "PROJ-123"},
		CreatedAt:   "2024-01-15T10:00:00Z",
		UpdatedAt:   "2024-01-16T12:00:00Z",
	}

	result := IssueToBeads(linearIssue, config)

	if len(result.Dependencies) != 1 {
		t.Fatalf("Expected 1 dependency, got %d", len(result.Dependencies))
	}
	if result.Dependencies[0].Type != "parent-child" {
		t.Errorf("Dependency type = %q, want %q", result.Dependencies[0].Type, "parent-child")
	}
	if result.Dependencies[0].FromLinearID != "PROJ-456" {
		t.Errorf("FromLinearID = %q, want %q", result.Dependencies[0].FromLinearID, "PROJ-456")
	}
	if result.Dependencies[0].ToLinearID != "PROJ-123" {
		t.Errorf("ToLinearID = %q, want %q", result.Dependencies[0].ToLinearID, "PROJ-123")
	}
}

func TestBuildLinearToLocalUpdates(t *testing.T) {
	config := DefaultMappingConfig()

	linearIssue := &Issue{
		ID:          "uuid-123",
		Identifier:  "PROJ-123",
		Title:       "Updated Title",
		Description: "Updated description",
		Priority:    1, // Urgent
		State:       &State{Type: "completed", Name: "Done"},
		Assignee:    &User{Name: "Jane Doe", Email: "jane@example.com"},
		Labels:      &Labels{Nodes: []Label{{Name: "feature"}, {Name: "priority"}}},
		UpdatedAt:   "2024-01-20T15:00:00Z",
		CompletedAt: "2024-01-20T14:00:00Z",
	}

	updates := BuildLinearToLocalUpdates(linearIssue, config)

	if updates["title"] != "Updated Title" {
		t.Errorf("title = %v, want %q", updates["title"], "Updated Title")
	}
	if updates["description"] != "Updated description" {
		t.Errorf("description = %v, want %q", updates["description"], "Updated description")
	}
	if updates["priority"] != 0 { // Critical in beads
		t.Errorf("priority = %v, want 0", updates["priority"])
	}
	if updates["status"] != "closed" {
		t.Errorf("status = %v, want %q", updates["status"], "closed")
	}
	if updates["assignee"] != "jane@example.com" {
		t.Errorf("assignee = %v, want %q", updates["assignee"], "jane@example.com")
	}

	labels, ok := updates["labels"].([]string)
	if !ok || len(labels) != 2 {
		t.Errorf("labels = %v, want 2 labels", updates["labels"])
	}
}

func TestBuildLinearToLocalUpdatesNoAssignee(t *testing.T) {
	config := DefaultMappingConfig()

	linearIssue := &Issue{
		ID:          "uuid-123",
		Identifier:  "PROJ-123",
		Title:       "No Assignee",
		Description: "Test",
		Priority:    3,
		State:       &State{Type: "unstarted", Name: "Todo"},
		Assignee:    nil,
		UpdatedAt:   "2024-01-20T15:00:00Z",
	}

	updates := BuildLinearToLocalUpdates(linearIssue, config)

	if updates["assignee"] != "" {
		t.Errorf("assignee = %v, want empty string", updates["assignee"])
	}
}

// mockConfigLoader implements ConfigLoader for testing
type mockConfigLoader struct {
	config map[string]string
}

func (m *mockConfigLoader) GetAllConfig() (map[string]string, error) {
	return m.config, nil
}

func TestLoadMappingConfig(t *testing.T) {
	loader := &mockConfigLoader{
		config: map[string]string{
			"linear.priority_map.0":       "3",
			"linear.state_map.custom":     "in_progress",
			"linear.label_type_map.story": "feature",
			"linear.relation_map.parent":  "parent-child",
		},
	}

	config := LoadMappingConfig(loader)

	// Check custom priority mapping
	if config.PriorityMap["0"] != 3 {
		t.Errorf("PriorityMap[0] = %d, want 3", config.PriorityMap["0"])
	}

	// Check custom state mapping
	if config.StateMap["custom"] != "in_progress" {
		t.Errorf("StateMap[custom] = %s, want in_progress", config.StateMap["custom"])
	}
	if config.ExplicitStateMap["custom"] != "in_progress" {
		t.Errorf("ExplicitStateMap[custom] = %s, want in_progress", config.ExplicitStateMap["custom"])
	}

	// Check custom label type mapping
	if config.LabelTypeMap["story"] != "feature" {
		t.Errorf("LabelTypeMap[story] = %s, want feature", config.LabelTypeMap["story"])
	}

	// Check custom relation mapping
	if config.RelationMap["parent"] != "parent-child" {
		t.Errorf("RelationMap[parent] = %s, want parent-child", config.RelationMap["parent"])
	}

	// Check that defaults are preserved
	if config.StateMap["started"] != "in_progress" {
		t.Errorf("StateMap[started] = %s, want in_progress (default preserved)", config.StateMap["started"])
	}
}

func TestLoadMappingConfigNilLoader(t *testing.T) {
	config := LoadMappingConfig(nil)

	// Should return defaults
	if config.PriorityMap["1"] != 0 {
		t.Errorf("Expected default priority map with nil loader")
	}
}

func TestBuildLinearDescription(t *testing.T) {
	tests := []struct {
		name  string
		issue *types.Issue
		want  string
	}{
		{
			name:  "description only",
			issue: &types.Issue{Description: "Basic description"},
			want:  "Basic description",
		},
		{
			name: "with acceptance criteria",
			issue: &types.Issue{
				Description:        "Main description",
				AcceptanceCriteria: "- Must do X\n- Must do Y",
			},
			want: "Main description\n\n## Acceptance Criteria\n- Must do X\n- Must do Y",
		},
		{
			name: "with all fields",
			issue: &types.Issue{
				Description:        "Main description",
				AcceptanceCriteria: "AC here",
				Design:             "Design notes",
				Notes:              "Additional notes",
			},
			want: "Main description\n\n## Acceptance Criteria\nAC here\n\n## Design\nDesign notes\n\n## Notes\nAdditional notes",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := BuildLinearDescription(tt.issue)
			if got != tt.want {
				t.Errorf("BuildLinearDescription() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestResolveStateIDForBeadsStatusRequiresExplicitMappings(t *testing.T) {
	cache := &StateCache{
		States: []State{
			{ID: "state-1", Name: "Todo", Type: "unstarted"},
		},
	}

	_, err := ResolveStateIDForBeadsStatus(cache, types.StatusOpen, DefaultMappingConfig())
	if err == nil {
		t.Fatal("expected missing explicit state map to fail")
	}
	if !strings.Contains(err.Error(), "linear.state_map is not configured") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestResolveStateIDForBeadsStatusRejectsAmbiguousTypeFallback(t *testing.T) {
	cache := &StateCache{
		States: []State{
			{ID: "state-1", Name: "Done", Type: "completed"},
			{ID: "state-2", Name: "Monitoring", Type: "completed"},
		},
	}
	config := DefaultMappingConfig()
	config.ExplicitStateMap["completed"] = "closed"

	_, err := ResolveStateIDForBeadsStatus(cache, types.StatusClosed, config)
	if err == nil {
		t.Fatal("expected ambiguous completed mapping to fail")
	}
	if got := err.Error(); !strings.Contains(got, "type fallback is ambiguous") || !strings.Contains(got, "Done, Monitoring") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestResolveStateIDForBeadsStatusPrefersExplicitStateName(t *testing.T) {
	cache := &StateCache{
		States: []State{
			{ID: "state-1", Name: "Done", Type: "completed"},
			{ID: "state-2", Name: "Monitoring", Type: "completed"},
		},
	}
	config := DefaultMappingConfig()
	config.ExplicitStateMap["done"] = "closed"

	got, err := ResolveStateIDForBeadsStatus(cache, types.StatusClosed, config)
	if err != nil {
		t.Fatalf("ResolveStateIDForBeadsStatus() error = %v", err)
	}
	if got != "state-1" {
		t.Fatalf("ResolveStateIDForBeadsStatus() = %q, want state-1", got)
	}
}

func TestPushFieldsEqualIgnoresLocalOnlyDifferences(t *testing.T) {
	config := DefaultMappingConfig()
	local := &types.Issue{
		Title:       "Ship the fix",
		Description: "Main body",
		Notes:       "Local-only notes",
		Status:      types.StatusInProgress,
		Priority:    1,
		IssueType:   types.TypeFeature,
		Labels:      []string{"customer-visible"},
	}
	remote := &Issue{
		Title:       "Ship the fix",
		Description: "Main body\n\n## Notes\nLocal-only notes",
		Priority:    2,
		State:       &State{ID: "state-3", Name: "In Progress", Type: "started"},
	}

	if !PushFieldsEqual(local, remote, config) {
		t.Fatal("expected push fields to compare equal despite local-only issue type and labels")
	}
}

func TestPushFieldsEqualToBeads(t *testing.T) {
	local := &types.Issue{
		Title:       "Ship the fix",
		Description: "Main body",
		Notes:       "Local-only notes",
		Status:      types.StatusInProgress,
		Priority:    1,
		IssueType:   types.TypeFeature,
		Labels:      []string{"customer-visible"},
	}
	remote := &types.Issue{
		Title:       "Ship the fix",
		Description: "Main body\n\n## Notes\nLocal-only notes",
		Status:      types.StatusInProgress,
		Priority:    1,
		IssueType:   types.TypeTask,
		Labels:      []string{"ignored"},
	}

	if !PushFieldsEqualToBeads(local, remote) {
		t.Fatal("expected beads-form fallback comparison to ignore local-only fields")
	}
}

func TestClassifyCloseReason(t *testing.T) {
	tests := []struct {
		reason string
		want   CloseIntent
	}{
		{"", CloseIntentCompleted},
		{"Closed", CloseIntentCompleted},
		{"Work completed and merged via PRs #115 and #124", CloseIntentCompleted},
		{"Deleted /app/admin/ directory", CloseIntentCompleted},
		{"stale:auto-closed by reaper", CloseIntentCanceled},
		{"STALE: no activity for 30 days", CloseIntentCanceled},
		{"canceled - not shipping this quarter", CloseIntentCanceled},
		{"Cancelled (British spelling)", CloseIntentCanceled},
		{"duplicate of hw-abc", CloseIntentCanceled},
		{"superseded by hw-def", CloseIntentCanceled},
		{"obsolete", CloseIntentCanceled},
		{"wontfix: by design", CloseIntentCanceled},
		{"won't fix — external dep", CloseIntentCanceled},
		{"abandoned after spike", CloseIntentCanceled},
	}
	for _, tt := range tests {
		t.Run(tt.reason, func(t *testing.T) {
			got := ClassifyCloseReason(tt.reason)
			if got != tt.want {
				t.Errorf("ClassifyCloseReason(%q) = %v, want %v", tt.reason, got, tt.want)
			}
		})
	}
}

// TestResolveStateIDForIssueUsesCloseReason verifies that closed beads with
// done-ish close_reason land on a completed-type Linear state, while
// cancellation-ish reasons land on a canceled-type state. Critically,
// configuring state_map with both canceled and done → closed (the natural
// config for a team with both terminal states) no longer triggers the
// ambiguity error on push — close_reason is the disambiguator now.
func TestResolveStateIDForIssueUsesCloseReason(t *testing.T) {
	cache := &StateCache{
		States: []State{
			{ID: "done-id", Name: "Done", Type: "completed"},
			{ID: "canceled-id", Name: "Canceled", Type: "canceled"},
			{ID: "duplicate-id", Name: "Duplicate", Type: "canceled"},
		},
	}
	config := DefaultMappingConfig()
	// Matches the user-facing config we recommend: both terminal types map
	// to the single beads closed status.
	config.ExplicitStateMap["done"] = "closed"
	config.ExplicitStateMap["canceled"] = "closed"

	tests := []struct {
		name    string
		issue   *types.Issue
		wantID  string
		wantErr bool
	}{
		{
			name:   "completed via empty close_reason",
			issue:  &types.Issue{Status: types.StatusClosed, CloseReason: ""},
			wantID: "done-id",
		},
		{
			name:   "completed via done-ish close_reason",
			issue:  &types.Issue{Status: types.StatusClosed, CloseReason: "merged in PR #42"},
			wantID: "done-id",
		},
		{
			name:   "canceled via reaper stale prefix",
			issue:  &types.Issue{Status: types.StatusClosed, CloseReason: "stale:auto-closed by reaper"},
			wantID: "canceled-id",
		},
		{
			name:   "canceled via duplicate (prefers first canceled-type state by name)",
			issue:  &types.Issue{Status: types.StatusClosed, CloseReason: "duplicate of hw-abc"},
			wantID: "canceled-id", // "canceled" name match beats "duplicate"
		},
		{
			name:    "open status delegates to state_map",
			issue:   &types.Issue{Status: types.StatusOpen, CloseReason: ""},
			wantErr: true, // open isn't mapped in this cache, state_map path errors
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := ResolveStateIDForIssue(cache, tt.issue, config)
			if tt.wantErr {
				if err == nil {
					t.Fatalf("expected error, got state %q", got)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got != tt.wantID {
				t.Errorf("ResolveStateIDForIssue() = %q, want %q", got, tt.wantID)
			}
		})
	}
}

func TestNormalizeLinearMarkdown(t *testing.T) {
	tests := []struct {
		name string
		in   string
		want string
	}{
		{
			name: "bullet markers normalized to *",
			in:   "First line\n- item one\n- item two\n+ item three",
			want: "First line\n* item one\n* item two\n* item three",
		},
		{
			name: "punctuation escapes stripped",
			in:   `Step 1\. WiFi config\nUses \~880 lines\.`,
			want: `Step 1. WiFi config\nUses ~880 lines.`,
		},
		{
			name: "auto-linked URL collapsed",
			in:   "PR opened: [https://github.com/x/y/pull/1](<https://github.com/x/y/pull/1>) — done",
			want: "PR opened: https://github.com/x/y/pull/1 — done",
		},
		{
			name: "table separator widths normalized",
			in:   "| File | Lines |\n|------|-------|\n| `a` | 100 |",
			want: "| File | Lines |\n| --- | --- |\n| `a` | 100 |",
		},
		{
			name: "multiple newlines collapsed to single",
			in:   "## Heading\n\n\nFirst para\n\n\n\nSecond para",
			want: "## Heading\nFirst para\nSecond para",
		},
		{
			name: "trailing whitespace stripped per line",
			in:   "Line one  \nLine two\t\nLine three",
			want: "Line one\nLine two\nLine three",
		},
		{
			name: "round-trip: local form vs Linear form normalize equally",
			in:   "Section:\n- item a\n- item b\nuses \\~5 chars",
			want: "Section:\n* item a\n* item b\nuses ~5 chars",
		},
		{
			name: "leading and trailing whitespace trimmed",
			in:   "\n\n  body\n\n",
			want: "body",
		},
		{
			name: "empty input",
			in:   "",
			want: "",
		},
		{
			name: "escaped quotes stripped",
			in:   `Set the \"YOU CONTROL\" label to keep card height stable`,
			want: `Set the "YOU CONTROL" label to keep card height stable`,
		},
		{
			name: "numbered-list digit-width padding stripped",
			in:   "Steps:\n 1. First\n 2. Second\n10. Tenth",
			want: "Steps:\n1. First\n2. Second\n10. Tenth",
		},
		{
			name: "continuation paragraph indent stripped",
			in:   "* item one (does X)\n  Known limitation (documented in spec)",
			want: "* item one (does X)\nKnown limitation (documented in spec)",
		},
		{
			name: "nested bullet indent preserved",
			in:   "* outer\n  * inner one\n  * inner two",
			want: "* outer\n  * inner one\n  * inner two",
		},
		{
			name: "4+ space indent preserved (potential code block)",
			in:   "Example:\n    code line one\n    code line two",
			want: "Example:\n    code line one\n    code line two",
		},
		{
			name: "nested dash bullet preserved through bullet+indent",
			in:   "* outer\n  - inner",
			want: "* outer\n  * inner",
		},
		{
			name: "nested ordered list (3-space indent) preserved",
			in:   "1. outer\n   1. inner one\n   2. inner two",
			want: "1. outer\n   1. inner one\n   2. inner two",
		},
		{
			name: "nested ordered list (2-space indent) preserved",
			in:   "1. outer\n  1. inner",
			want: "1. outer\n  1. inner",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := NormalizeLinearMarkdown(tt.in)
			if got != tt.want {
				t.Errorf("NormalizeLinearMarkdown() got:\n%q\nwant:\n%q", got, tt.want)
			}
		})
	}
}

func TestNormalizeLinearMarkdownIdempotent(t *testing.T) {
	// Normalizing twice should equal normalizing once (steady state).
	inputs := []string{
		"- a\n- b",
		"foo\\.bar",
		"[https://x.com](<https://x.com>)",
		"|---|---|\n|x|y|",
		"a\n\n\nb",
		`\"quoted\"`,
		" 1. first\n 2. second",
		"* outer\n  * inner",
		"* item\n  continuation paragraph",
		"1. outer\n   1. inner",
	}
	for _, in := range inputs {
		once := NormalizeLinearMarkdown(in)
		twice := NormalizeLinearMarkdown(once)
		if once != twice {
			t.Errorf("NormalizeLinearMarkdown not idempotent for %q: %q vs %q", in, once, twice)
		}
	}
}

func TestPushFieldsEqualUsesNormalization(t *testing.T) {
	config := DefaultMappingConfig()
	local := &types.Issue{
		Title:       "title",
		Description: "Body:\n- item one\n- item two",
		Status:      types.StatusOpen,
		Priority:    2,
	}
	remote := &Issue{
		Title:       "title",
		Description: "Body:\n* item one\n* item two", // bullet normalized
		Priority:    1,                               // 2 (medium) → linear 1 via priority map default? validate
		State:       &State{Type: "backlog", Name: "Backlog"},
	}
	// Use config's default priority map for medium → 3 (linear medium)
	// We don't care about exact priority equivalence here — set both to 0
	local.Priority = 4
	remote.Priority = PriorityToLinear(local.Priority, config)
	if !PushFieldsEqual(local, remote, config) {
		t.Errorf("PushFieldsEqual should treat bullet markdown variants as equal")
	}
}

func TestPushFieldsDiffReportsFields(t *testing.T) {
	config := DefaultMappingConfig()
	local := &types.Issue{
		Title:       "new title",
		Description: "new body",
		Status:      types.StatusInProgress,
		Priority:    1,
	}
	remote := &Issue{
		Title:       "old title",
		Description: "old body",
		Priority:    PriorityToLinear(2, config),
		State:       &State{Type: "backlog", Name: "Backlog"},
	}
	diffs := PushFieldsDiff(local, remote, config)
	if len(diffs) == 0 {
		t.Fatal("expected non-empty diff, got empty")
	}
	joined := strings.Join(diffs, "\n")
	for _, want := range []string{"title:", "description:", "priority:", "status:"} {
		if !strings.Contains(joined, want) {
			t.Errorf("diff should mention %q, got: %s", want, joined)
		}
	}
}

func TestPushFieldsDiffEmptyForEqualIssues(t *testing.T) {
	config := DefaultMappingConfig()
	local := &types.Issue{
		Title:       "same",
		Description: "same body\n- a\n- b",
		Status:      types.StatusOpen,
		Priority:    2,
	}
	remote := &Issue{
		Title:       "same",
		Description: "same body\n* a\n* b", // bullet diff is normalized away
		Priority:    PriorityToLinear(local.Priority, config),
		State:       &State{Type: "backlog", Name: "Backlog"},
	}
	diffs := PushFieldsDiff(local, remote, config)
	if len(diffs) != 0 {
		t.Errorf("expected empty diff for equal issues (modulo normalization), got: %v", diffs)
	}
}

func TestCanonicalPushStatus(t *testing.T) {
	tests := []struct {
		in   types.Status
		want types.Status
	}{
		// CategoryActive
		{types.StatusOpen, types.StatusOpen},
		// CategoryWIP — secondary statuses canonicalize to in_progress
		{types.StatusInProgress, types.StatusInProgress},
		{types.StatusBlocked, types.StatusInProgress},
		{types.StatusHooked, types.StatusInProgress},
		// CategoryDone
		{types.StatusClosed, types.StatusClosed},
		// CategoryFrozen — pinned/deferred canonicalize to open
		{types.StatusDeferred, types.StatusOpen},
		{types.StatusPinned, types.StatusOpen},
	}
	for _, tt := range tests {
		t.Run(string(tt.in), func(t *testing.T) {
			got := canonicalPushStatus(tt.in)
			if got != tt.want {
				t.Errorf("canonicalPushStatus(%q) = %q, want %q", tt.in, got, tt.want)
			}
		})
	}
}

func TestResolveStateIDForBeadsStatusFallsBackToCanonical(t *testing.T) {
	// state_map only has explicit entry for in_progress; pushing a hooked bead
	// must succeed via canonical fallback (hooked → in_progress).
	config := DefaultMappingConfig()
	config.ExplicitStateMap = map[string]string{
		"in progress": "in_progress",
		"backlog":     "open",
		"done":        "closed",
	}
	cache := &StateCache{
		States: []State{
			{ID: "state-todo", Name: "Backlog", Type: "backlog"},
			{ID: "state-wip", Name: "In Progress", Type: "started"},
			{ID: "state-done", Name: "Done", Type: "completed"},
		},
	}

	id, err := ResolveStateIDForBeadsStatus(cache, types.StatusHooked, config)
	if err != nil {
		t.Fatalf("expected hooked to resolve via canonical fallback, got error: %v", err)
	}
	if id != "state-wip" {
		t.Errorf("hooked should map to state-wip (in progress), got %q", id)
	}
}

func TestResolveStateIDForBeadsStatusOriginalAmbiguityNotMasked(t *testing.T) {
	// User configured an ambiguous explicit map for the requested status —
	// must surface the ambiguity error, NOT silently fall back to canonical.
	config := DefaultMappingConfig()
	config.ExplicitStateMap = map[string]string{
		"in progress": "hooked", // explicit (and ambiguous when paired with below)
		"working":     "hooked", // also maps to hooked
		"backlog":     "open",
		"done":        "closed",
	}
	cache := &StateCache{
		States: []State{
			{ID: "state-todo", Name: "Backlog", Type: "backlog"},
			{ID: "state-wip1", Name: "In Progress", Type: "started"},
			{ID: "state-wip2", Name: "Working", Type: "started"},
			{ID: "state-done", Name: "Done", Type: "completed"},
		},
	}
	_, err := ResolveStateIDForBeadsStatus(cache, types.StatusHooked, config)
	if err == nil {
		t.Fatal("expected ambiguity error, got nil")
	}
	if !strings.Contains(err.Error(), "multiple Linear states") {
		t.Errorf("expected 'multiple Linear states' ambiguity error, got: %v", err)
	}
}

func TestResolveStateIDForBeadsStatusCanonicalAmbiguitySurfaces(t *testing.T) {
	// Original status has no match; canonical fallback hits an ambiguity.
	// Must surface the canonical ambiguity error directly.
	config := DefaultMappingConfig()
	config.ExplicitStateMap = map[string]string{
		"in progress": "in_progress",
		"working":     "in_progress", // ambiguous in_progress
		"backlog":     "open",
		"done":        "closed",
	}
	cache := &StateCache{
		States: []State{
			{ID: "state-todo", Name: "Backlog", Type: "backlog"},
			{ID: "state-wip1", Name: "In Progress", Type: "started"},
			{ID: "state-wip2", Name: "Working", Type: "started"},
			{ID: "state-done", Name: "Done", Type: "completed"},
		},
	}
	// hooked has no explicit entry → canonical fallback to in_progress → ambiguous
	_, err := ResolveStateIDForBeadsStatus(cache, types.StatusHooked, config)
	if err == nil {
		t.Fatal("expected ambiguity error from canonical fallback, got nil")
	}
	if !strings.Contains(err.Error(), "multiple Linear states") {
		t.Errorf("expected canonical ambiguity to surface, got: %v", err)
	}
}

func TestResolveStateIDForBeadsStatusNoMatchAnywhere(t *testing.T) {
	// No explicit entry for the original status, no canonical-equivalent
	// state in the workflow either — error message should mention both.
	config := DefaultMappingConfig()
	config.ExplicitStateMap = map[string]string{
		"backlog": "open",
		"done":    "closed",
	}
	cache := &StateCache{
		States: []State{
			{ID: "state-todo", Name: "Backlog", Type: "backlog"},
			{ID: "state-done", Name: "Done", Type: "completed"},
		},
	}
	_, err := ResolveStateIDForBeadsStatus(cache, types.StatusHooked, config)
	if err == nil {
		t.Fatal("expected no-state error, got nil")
	}
	if !strings.Contains(err.Error(), "category-canonical") {
		t.Errorf("expected error to mention canonical fallback was tried, got: %v", err)
	}
}

func TestCanonicalPushStatusUnknownStatusPassesThrough(t *testing.T) {
	// A custom status that BuiltInStatusCategory doesn't recognize stays
	// unchanged — the function shouldn't claim a guess.
	custom := types.Status("triage-pending")
	got := canonicalPushStatus(custom)
	if got != custom {
		t.Errorf("canonicalPushStatus(%q) = %q, want unchanged %q", custom, got, custom)
	}
}

func TestNormalizeLinearMarkdownEscapeBrackets(t *testing.T) {
	// Linear escapes square brackets in plain text to avoid markdown-link
	// parsing. Local sends bare brackets. Both must normalize to the bare form.
	in := `default seed \[5,5,5\] confirmed`
	want := `default seed [5,5,5] confirmed`
	got := NormalizeLinearMarkdown(in)
	if got != want {
		t.Errorf("escape-brackets: got %q, want %q", got, want)
	}
}

func TestNormalizeLinearMarkdownBoldUnderscore(t *testing.T) {
	tests := []struct {
		name string
		in   string
		want string
	}{
		{"simple", `Run __tests__ first`, `Run **tests** first`},
		{"file path", `lib/simulation/__tests__/foo.test.ts`, `lib/simulation/**tests**/foo.test.ts`},
		{"already bold", `Run **tests** first`, `Run **tests** first`}, // no-op
		{"single underscore not matched", `_emphasis_ stays`, `_emphasis_ stays`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := NormalizeLinearMarkdown(tt.in)
			if got != tt.want {
				t.Errorf("got %q, want %q", got, tt.want)
			}
		})
	}
}
