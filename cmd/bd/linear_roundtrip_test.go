//go:build cgo

package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/steveyegge/beads/internal/linear"
	"github.com/steveyegge/beads/internal/storage"
	"github.com/steveyegge/beads/internal/storage/dolt"
	"github.com/steveyegge/beads/internal/tracker"
	"github.com/steveyegge/beads/internal/types"
)

// mockLinearServer is a stateful mock that only stores what the Linear client
// actually sends — no fabricated fields. This keeps the round-trip test honest.
type mockLinearServer struct {
	mu       sync.Mutex
	issues   map[string]*linear.Issue // keyed by Linear UUID
	nextSeq  int
	teamID   string
	teamKey  string // e.g. "MOCK"
	states   []linear.State
	stateMap map[string]linear.State // state type → State

	// Label state for Phase G tests.
	teamLabels      map[string]linear.Label // ID → Label, scoped to teamID
	workspaceLabels map[string]linear.Label // ID → Label, organization-wide
	labelCreates    []string                // names passed to issueLabelCreate, in order
	updateCalls     map[string]int          // issueID → count of issueUpdate calls
}

func newMockLinearServer(teamID, teamKey string) *mockLinearServer {
	states := []linear.State{
		{ID: "state-backlog", Name: "Backlog", Type: "backlog"},
		{ID: "state-unstarted", Name: "Todo", Type: "unstarted"},
		{ID: "state-started", Name: "In Progress", Type: "started"},
		{ID: "state-completed", Name: "Done", Type: "completed"},
		{ID: "state-canceled", Name: "Canceled", Type: "canceled"},
	}
	stateMap := make(map[string]linear.State, len(states))
	for _, s := range states {
		stateMap[s.Type] = s
	}
	return &mockLinearServer{
		issues:          make(map[string]*linear.Issue),
		teamID:          teamID,
		teamKey:         teamKey,
		states:          states,
		stateMap:        stateMap,
		teamLabels:      make(map[string]linear.Label),
		workspaceLabels: make(map[string]linear.Label),
		updateCalls:     make(map[string]int),
	}
}

func (m *mockLinearServer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	var req linear.GraphQLRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}

	var data interface{}
	var err error

	switch {
	case strings.Contains(req.Query, "issueLabelCreate"):
		data, err = m.handleLabelCreate(req)
	case strings.Contains(req.Query, "LabelsByName"):
		data, err = m.handleLabelsByName(req)
	case strings.Contains(req.Query, "issueCreate"):
		data, err = m.handleCreate(req)
	case strings.Contains(req.Query, "issueUpdate"):
		data, err = m.handleUpdate(req)
	case strings.Contains(req.Query, "TeamStates") || strings.Contains(req.Query, "team(id:") || (strings.Contains(req.Query, "team(") && strings.Contains(req.Query, "states")):
		data = m.handleTeamStates()
	case strings.Contains(req.Query, "issues"):
		data, err = m.handleFetchIssues(req)
	default:
		http.Error(w, fmt.Sprintf("unhandled query: %s", req.Query[:min(80, len(req.Query))]), http.StatusBadRequest)
		return
	}

	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	respBytes, _ := json.Marshal(data)
	resp := map[string]json.RawMessage{"data": respBytes}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(resp)
}

func (m *mockLinearServer) handleCreate(req linear.GraphQLRequest) (interface{}, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	vars := req.Variables
	inputRaw, ok := vars["input"]
	if !ok {
		return nil, fmt.Errorf("missing input")
	}
	input, ok := inputRaw.(map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("input is not a map")
	}

	m.nextSeq++
	id := fmt.Sprintf("uuid-%d", m.nextSeq)
	identifier := fmt.Sprintf("%s-%d", m.teamKey, m.nextSeq)
	now := time.Now().UTC().Format(time.RFC3339)

	issue := &linear.Issue{
		ID:          id,
		Identifier:  identifier,
		Title:       strVal(input, "title"),
		Description: strVal(input, "description"),
		URL:         fmt.Sprintf("https://linear.app/mock/issue/%s", identifier),
		CreatedAt:   now,
		UpdatedAt:   now,
	}

	if p, ok := input["priority"]; ok {
		if pf, ok := p.(float64); ok {
			issue.Priority = int(pf)
		}
	}

	if stateID := strVal(input, "stateId"); stateID != "" {
		for _, s := range m.states {
			if s.ID == stateID {
				issue.State = &linear.State{ID: s.ID, Name: s.Name, Type: s.Type}
				break
			}
		}
	}

	// Apply labelIds if provided (mirrors handleUpdate logic for replace semantics).
	if labelIDsRaw, ok := input["labelIds"]; ok {
		labelIDs, _ := labelIDsRaw.([]interface{})
		nodes := make([]linear.Label, 0, len(labelIDs))
		for _, raw := range labelIDs {
			idStr, _ := raw.(string)
			if l, ok := m.teamLabels[idStr]; ok {
				nodes = append(nodes, l)
			} else if l, ok := m.workspaceLabels[idStr]; ok {
				nodes = append(nodes, l)
			} else {
				nodes = append(nodes, linear.Label{ID: idStr, Name: "<unknown:" + idStr + ">"})
			}
		}
		issue.Labels = &linear.Labels{Nodes: nodes}
	}

	m.issues[id] = issue

	return map[string]interface{}{
		"issueCreate": map[string]interface{}{
			"success": true,
			"issue":   issue,
		},
	}, nil
}

func (m *mockLinearServer) handleUpdate(req linear.GraphQLRequest) (interface{}, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	vars := req.Variables
	id := ""
	if v, ok := vars["id"]; ok {
		id, _ = v.(string)
	}

	issue, exists := m.issues[id]
	if !exists {
		return nil, fmt.Errorf("issue %s not found", id)
	}

	inputRaw, ok := vars["input"]
	if !ok {
		return nil, fmt.Errorf("missing input")
	}
	input, ok := inputRaw.(map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("input is not a map")
	}

	if v := strVal(input, "title"); v != "" {
		issue.Title = v
	}
	if v := strVal(input, "description"); v != "" {
		issue.Description = v
	}
	if p, ok := input["priority"]; ok {
		if pf, ok := p.(float64); ok {
			issue.Priority = int(pf)
		}
	}
	if stateID := strVal(input, "stateId"); stateID != "" {
		for _, s := range m.states {
			if s.ID == stateID {
				issue.State = &linear.State{ID: s.ID, Name: s.Name, Type: s.Type}
				break
			}
		}
	}

	// Track call count.
	m.updateCalls[id]++

	// Apply labelIds REPLACE semantics — Linear's actual behavior per the spec.
	if labelIDsRaw, ok := input["labelIds"]; ok {
		labelIDs, _ := labelIDsRaw.([]interface{})
		nodes := make([]linear.Label, 0, len(labelIDs))
		for _, raw := range labelIDs {
			idStr, _ := raw.(string)
			if l, ok := m.teamLabels[idStr]; ok {
				nodes = append(nodes, l)
			} else if l, ok := m.workspaceLabels[idStr]; ok {
				nodes = append(nodes, l)
			} else {
				// Unknown ID — keep it but with a synthetic name (mirrors Linear's behavior of error-on-unknown,
				// but we're permissive so tests can verify the wire format separately).
				nodes = append(nodes, linear.Label{ID: idStr, Name: "<unknown:" + idStr + ">"})
			}
		}
		issue.Labels = &linear.Labels{Nodes: nodes}
	}

	issue.UpdatedAt = time.Now().UTC().Format(time.RFC3339)

	return map[string]interface{}{
		"issueUpdate": map[string]interface{}{
			"success": true,
			"issue":   issue,
		},
	}, nil
}

func (m *mockLinearServer) handleTeamStates() interface{} {
	return map[string]interface{}{
		"team": map[string]interface{}{
			"id": m.teamID,
			"states": map[string]interface{}{
				"nodes": m.states,
			},
		},
	}
}

func (m *mockLinearServer) handleFetchIssues(req linear.GraphQLRequest) (interface{}, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	// Check for identifier filter (FetchIssueByIdentifier)
	vars := req.Variables
	if filterRaw, ok := vars["filter"]; ok {
		if filter, ok := filterRaw.(map[string]interface{}); ok {
			if idFilter, ok := filter["identifier"]; ok {
				if idMap, ok := idFilter.(map[string]interface{}); ok {
					if eqVal, ok := idMap["eq"]; ok {
						identifier, _ := eqVal.(string)
						for _, issue := range m.issues {
							if issue.Identifier == identifier {
								return map[string]interface{}{
									"issues": map[string]interface{}{
										"nodes":    []interface{}{issue},
										"pageInfo": map[string]interface{}{"hasNextPage": false, "endCursor": ""},
									},
								}, nil
							}
						}
						return map[string]interface{}{
							"issues": map[string]interface{}{
								"nodes":    []interface{}{},
								"pageInfo": map[string]interface{}{"hasNextPage": false, "endCursor": ""},
							},
						}, nil
					}
				}
			}
		}
	}

	// Return all issues
	nodes := make([]*linear.Issue, 0, len(m.issues))
	for _, issue := range m.issues {
		nodes = append(nodes, issue)
	}

	return map[string]interface{}{
		"issues": map[string]interface{}{
			"nodes":    nodes,
			"pageInfo": map[string]interface{}{"hasNextPage": false, "endCursor": ""},
		},
	}, nil
}

func (m *mockLinearServer) issueCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.issues)
}

func strVal(m map[string]interface{}, key string) string {
	if v, ok := m[key]; ok {
		if s, ok := v.(string); ok {
			return s
		}
	}
	return ""
}

// TestLinearRoundTripCoreFields tests push→pull fidelity for fields that the
// Linear integration currently supports: title, description, priority, status,
// and external_ref. See upstream #3187.
func TestLinearRoundTripCoreFields(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	ctx := context.Background()
	teamID := "test-team-uuid"

	// --- 1. Setup source DB ---
	sourceStore, cleanup := setupTestDB(t)
	defer cleanup()

	// Configure Linear settings in source store
	for k, v := range map[string]string{
		"linear.api_key": "test-api-key",
		"linear.team_id": teamID,
		"issue_prefix":   "bd",
	} {
		if err := sourceStore.SetConfig(ctx, k, v); err != nil {
			t.Fatalf("SetConfig(%s): %v", k, err)
		}
	}

	// Start mock server
	mock := newMockLinearServer(teamID, "MOCK")
	server := httptest.NewServer(mock)
	defer server.Close()

	if err := sourceStore.SetConfig(ctx, "linear.api_endpoint", server.URL); err != nil {
		t.Fatalf("SetConfig(endpoint): %v", err)
	}

	// --- 2. Seed source DB with varied issues ---
	type seedIssue struct {
		title       string
		description string
		priority    int
		status      types.Status
	}
	seeds := []seedIssue{
		{"Critical security fix", "Fix the auth bypass vulnerability", 0, types.StatusOpen},
		{"Add search feature", "Implement full-text search for issues", 1, types.StatusInProgress},
		{"Update dependencies", "Routine dep update for Q2", 3, types.StatusClosed},
	}

	sourceIssueIDs := make([]string, 0, len(seeds))
	for i, s := range seeds {
		issue := &types.Issue{
			ID:          fmt.Sprintf("bd-rt-%d", i),
			Title:       s.title,
			Description: s.description,
			Priority:    s.priority,
			Status:      s.status,
			IssueType:   types.TypeTask,
		}
		if s.status == types.StatusClosed {
			now := time.Now()
			issue.ClosedAt = &now
		}
		if err := sourceStore.CreateIssue(ctx, issue, "test-actor"); err != nil {
			t.Fatalf("CreateIssue(%s): %v", issue.ID, err)
		}
		sourceIssueIDs = append(sourceIssueIDs, issue.ID)
	}

	// --- 3. Push to mock Linear ---
	lt := &linear.Tracker{}
	lt.SetTeamIDs([]string{teamID})
	if err := lt.Init(ctx, sourceStore); err != nil {
		t.Fatalf("Tracker.Init: %v", err)
	}

	pushEngine := tracker.NewEngine(lt, sourceStore, "test-actor")
	pushEngine.PushHooks = buildLinearPushHooksForTest(ctx, lt)

	pushResult, err := pushEngine.Sync(ctx, tracker.SyncOptions{Push: true})
	if err != nil {
		t.Fatalf("Push sync failed: %v", err)
	}
	if pushResult.Stats.Created != len(seeds) {
		t.Fatalf("expected %d pushed, got created=%d", len(seeds), pushResult.Stats.Created)
	}

	// Verify external refs were written
	for _, id := range sourceIssueIDs {
		issue, err := sourceStore.GetIssue(ctx, id)
		if err != nil {
			t.Fatalf("GetIssue(%s) after push: %v", id, err)
		}
		if issue.ExternalRef == nil || *issue.ExternalRef == "" {
			t.Errorf("issue %s: expected external_ref after push, got nil", id)
		}
	}

	// Verify mock server received all issues
	if got := mock.issueCount(); got != len(seeds) {
		t.Fatalf("mock server has %d issues, want %d", got, len(seeds))
	}

	// --- 4. Setup target DB (fresh) ---
	targetStore, cleanup2 := setupTestDB(t)
	defer cleanup2()

	for k, v := range map[string]string{
		"linear.api_key":      "test-api-key",
		"linear.team_id":      teamID,
		"linear.api_endpoint": server.URL,
		"issue_prefix":        "bd",
	} {
		if err := targetStore.SetConfig(ctx, k, v); err != nil {
			t.Fatalf("SetConfig(%s) target: %v", k, err)
		}
	}

	// --- 5. Pull from mock Linear into fresh DB ---
	lt2 := &linear.Tracker{}
	lt2.SetTeamIDs([]string{teamID})
	if err := lt2.Init(ctx, targetStore); err != nil {
		t.Fatalf("Tracker.Init (target): %v", err)
	}

	pullEngine := tracker.NewEngine(lt2, targetStore, "test-actor")
	pullEngine.PullHooks = buildLinearPullHooksForTest(ctx, targetStore)

	pullResult, err := pullEngine.Sync(ctx, tracker.SyncOptions{Pull: true})
	if err != nil {
		t.Fatalf("Pull sync failed: %v", err)
	}
	if pullResult.Stats.Created != len(seeds) {
		t.Fatalf("expected %d pulled/created, got created=%d", len(seeds), pullResult.Stats.Created)
	}

	// --- 6. Assert fidelity ---
	// Build a map of pulled issues keyed by external_ref
	pulledIssues, err := targetStore.SearchIssues(ctx, "", types.IssueFilter{})
	if err != nil {
		t.Fatalf("SearchIssues on target: %v", err)
	}
	if len(pulledIssues) != len(seeds) {
		t.Fatalf("target has %d issues, want %d", len(pulledIssues), len(seeds))
	}

	pulledByRef := make(map[string]*types.Issue)
	for _, issue := range pulledIssues {
		if issue.ExternalRef != nil && *issue.ExternalRef != "" {
			pulledByRef[*issue.ExternalRef] = issue
		}
	}

	// For each source issue, find the corresponding pulled issue and compare
	for i, id := range sourceIssueIDs {
		source, err := sourceStore.GetIssue(ctx, id)
		if err != nil {
			t.Fatalf("GetIssue(%s) from source: %v", id, err)
		}
		if source.ExternalRef == nil {
			t.Fatalf("source issue %s has no external_ref", id)
		}

		pulled, ok := pulledByRef[*source.ExternalRef]
		if !ok {
			t.Fatalf("seed[%d] %s: no pulled issue with external_ref %s", i, id, *source.ExternalRef)
		}

		t.Run(fmt.Sprintf("seed_%d_%s", i, source.Title), func(t *testing.T) {
			// Title
			if pulled.Title != source.Title {
				t.Errorf("title: got %q, want %q", pulled.Title, source.Title)
			}

			// Priority round-trip (beads→linear→beads)
			if pulled.Priority != source.Priority {
				t.Errorf("priority: got %d, want %d", pulled.Priority, source.Priority)
			}

			// Status round-trip
			// Note: StatusOpen→unstarted→open, StatusInProgress→started→in_progress,
			// StatusClosed→completed→closed
			if pulled.Status != source.Status {
				t.Errorf("status: got %q, want %q", pulled.Status, source.Status)
			}

			// External ref preserved
			if pulled.ExternalRef == nil || *pulled.ExternalRef != *source.ExternalRef {
				pulledRef := "<nil>"
				if pulled.ExternalRef != nil {
					pulledRef = *pulled.ExternalRef
				}
				t.Errorf("external_ref: got %q, want %q", pulledRef, *source.ExternalRef)
			}
		})
	}
}

// TestLinearRoundTripRelationships is a spec test documenting that parent-child
// hierarchy, blocking dependencies, and issue type do not survive a push→pull
// round-trip because the Linear push path does not yet send these fields.
// When those features are implemented, remove the Skip and this test becomes
// a regression gate. See upstream #3187.
func TestLinearRoundTripRelationships(t *testing.T) {
	t.Skip("push does not yet support parent/relations/type — see upstream #3187")

	// When enabled, this test should:
	// 1. Create an epic + child tasks + blocking dep
	// 2. Push to mock Linear
	// 3. Verify mock received parent and relation fields
	// 4. Pull into fresh DB
	// 5. Assert:
	//    - Epic exists with IssueType=epic
	//    - Child tasks have parent-child dep to epic
	//    - Blocking dep preserved
	//    - Issue types preserved via label round-trip
}

// buildLinearPushHooksForTest mirrors buildLinearPushHooks from linear.go
// but works with an explicit store instead of the global.
func buildLinearPushHooksForTest(ctx context.Context, lt *linear.Tracker) *tracker.PushHooks {
	return &tracker.PushHooks{
		FormatDescription: func(issue *types.Issue) string {
			return linear.BuildLinearDescription(issue)
		},
		ContentEqual: func(local *types.Issue, remote *tracker.TrackerIssue) bool {
			localComparable := linear.NormalizeIssueForLinearHash(local)
			remoteConv := lt.FieldMapper().IssueToBeads(remote)
			if remoteConv == nil || remoteConv.Issue == nil {
				return false
			}
			return localComparable.ComputeContentHash() == remoteConv.Issue.ComputeContentHash()
		},
		BuildStateCache: func(ctx context.Context) (interface{}, error) {
			return linear.BuildStateCacheFromTracker(ctx, lt)
		},
		ResolveState: func(cache interface{}, status types.Status) (string, bool) {
			sc, ok := cache.(*linear.StateCache)
			if !ok || sc == nil {
				return "", false
			}
			id := sc.FindStateForBeadsStatus(status)
			return id, id != ""
		},
	}
}

// buildLinearPullHooksForTest mirrors buildLinearPullHooks from linear.go
// but works with an explicit store.
func buildLinearPullHooksForTest(ctx context.Context, store interface {
	SearchIssues(ctx context.Context, query string, filter types.IssueFilter) ([]*types.Issue, error)
}) *tracker.PullHooks {
	hooks := &tracker.PullHooks{}

	existingIssues, err := store.SearchIssues(ctx, "", types.IssueFilter{})
	usedIDs := make(map[string]bool)
	if err == nil {
		for _, issue := range existingIssues {
			if issue.ID != "" {
				usedIDs[issue.ID] = true
			}
		}
	}

	hooks.GenerateID = func(_ context.Context, issue *types.Issue) error {
		ids := []*types.Issue{issue}
		idOpts := linear.IDGenerationOptions{
			BaseLength: 6,
			MaxLength:  8,
			UsedIDs:    usedIDs,
		}
		if err := linear.GenerateIssueIDs(ids, "bd", "linear-import", idOpts); err != nil {
			return err
		}
		usedIDs[issue.ID] = true
		return nil
	}

	return hooks
}

func (m *mockLinearServer) handleLabelsByName(req linear.GraphQLRequest) (interface{}, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	teamNodes := make([]linear.Label, 0, len(m.teamLabels))
	for _, l := range m.teamLabels {
		teamNodes = append(teamNodes, l)
	}
	orgNodes := make([]linear.Label, 0, len(m.workspaceLabels))
	for _, l := range m.workspaceLabels {
		orgNodes = append(orgNodes, l)
	}
	return map[string]interface{}{
		"team": map[string]interface{}{
			"labels": map[string]interface{}{"nodes": teamNodes},
		},
		"organization": map[string]interface{}{
			"labels": map[string]interface{}{"nodes": orgNodes},
		},
	}, nil
}

func (m *mockLinearServer) handleLabelCreate(req linear.GraphQLRequest) (interface{}, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	inputRaw, ok := req.Variables["input"]
	if !ok {
		return nil, fmt.Errorf("missing input")
	}
	input, ok := inputRaw.(map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("input is not a map")
	}
	name := strVal(input, "name")
	teamID := strVal(input, "teamId")

	m.nextSeq++
	id := fmt.Sprintf("auto-label-%d", m.nextSeq)
	label := linear.Label{ID: id, Name: name}

	if teamID != "" {
		m.teamLabels[id] = label
	} else {
		m.workspaceLabels[id] = label
	}
	m.labelCreates = append(m.labelCreates, name)

	return map[string]interface{}{
		"issueLabelCreate": map[string]interface{}{
			"success":    true,
			"issueLabel": label,
		},
	}, nil
}

// SeedLinearLabels lets tests pre-populate labels on the team or workspace.
// Use scope="team" or "workspace".
func (m *mockLinearServer) SeedLinearLabels(scope string, labels ...linear.Label) {
	m.mu.Lock()
	defer m.mu.Unlock()
	target := m.teamLabels
	if scope == "workspace" {
		target = m.workspaceLabels
	}
	for _, l := range labels {
		target[l.ID] = l
	}
}

// SetIssueLabels assigns labels to an existing issue (test helper).
func (m *mockLinearServer) SetIssueLabels(issueID string, labels []linear.Label) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if issue, ok := m.issues[issueID]; ok {
		issue.Labels = &linear.Labels{Nodes: labels}
		issue.UpdatedAt = time.Now().UTC().Format(time.RFC3339)
	}
}

// IssueLabels returns the current labels on a Linear issue (test helper).
func (m *mockLinearServer) IssueLabels(issueID string) []linear.Label {
	m.mu.Lock()
	defer m.mu.Unlock()
	issue, ok := m.issues[issueID]
	if !ok || issue.Labels == nil {
		return nil
	}
	out := make([]linear.Label, len(issue.Labels.Nodes))
	copy(out, issue.Labels.Nodes)
	return out
}

// LabelCreates returns names that were created via issueLabelCreate, in order.
func (m *mockLinearServer) LabelCreates() []string {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]string, len(m.labelCreates))
	copy(out, m.labelCreates)
	return out
}

// UpdateCallCount returns how many issueUpdate calls were made for an issue.
func (m *mockLinearServer) UpdateCallCount(issueID string) int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.updateCalls[issueID]
}

// enableLabelSyncForTest sets the three label sync config keys in a test store.
// Call this BEFORE Tracker.Init so the config is available when SetLabelSyncConfig
// is called from runLinearSync (or call SetLabelSyncConfig directly after Init
// for finer-grained tests).
func enableLabelSyncForTest(t *testing.T, store interface {
	SetConfig(ctx context.Context, key, value string) error
}) {
	t.Helper()
	ctx := context.Background()
	for k, v := range map[string]string{
		"linear.label_sync_enabled": "true",
		"linear.label_create_scope": "team",
	} {
		if err := store.SetConfig(ctx, k, v); err != nil {
			t.Fatalf("SetConfig(%s): %v", k, err)
		}
	}
}

// setupLabelSyncTest creates a test store + mock server + tracker + engine all
// wired together for label-sync roundtrip tests. Returns the parts the tests
// need to seed state and observe results.
//
// Skips in short mode and when no Dolt server is available (standard pattern).
func setupLabelSyncTest(t *testing.T) (sourceStore *dolt.DoltStore, mock *mockLinearServer, lt *linear.Tracker, engine *tracker.Engine) {
	t.Helper()
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	ctx := context.Background()
	teamID := "test-team-uuid"
	store, cleanup := setupTestDB(t)
	t.Cleanup(cleanup)

	mock = newMockLinearServer(teamID, "MOCK")
	server := httptest.NewServer(mock)
	t.Cleanup(server.Close)

	for k, v := range map[string]string{
		"linear.api_key":      "test-api-key",
		"linear.team_id":      teamID,
		"linear.api_endpoint": server.URL,
		"issue_prefix":        "bd",
	} {
		if err := store.SetConfig(ctx, k, v); err != nil {
			t.Fatalf("SetConfig(%s): %v", k, err)
		}
	}
	enableLabelSyncForTest(t, store)

	lt = &linear.Tracker{}
	lt.SetTeamIDs([]string{teamID})
	if err := lt.Init(ctx, store); err != nil {
		t.Fatalf("Tracker.Init: %v", err)
	}
	allCfg, _ := store.GetAllConfig(ctx)
	lsCfg := loadLinearLabelSyncConfig(allCfg)
	lt.SetLabelSyncConfig(lsCfg.Enabled, lsCfg.Exclude, lsCfg.CreateScope, func(format string, args ...interface{}) {
		t.Logf("label-sync warn: "+format, args...)
	})

	engine = tracker.NewEngine(lt, store, "test-actor")
	engine.PushHooks = buildLinearPushHooksForTest(ctx, lt)
	engine.PullHooks = buildLinearPullHooksForTest(ctx, store)
	patchTestHooksForLabelSync(ctx, lt, engine.PushHooks, engine.PullHooks)
	engine.PullHooks.ReconcileLabels = installReconcileLabelsHook(ctx, lt)

	return store, mock, lt, engine
}

// installReconcileLabelsHook returns a PullHooks.ReconcileLabels callback
// equivalent to what buildLinearPullHooks installs in production. Lifted to a
// helper so tests don't have to import cmd/bd's private builder.
func installReconcileLabelsHook(ctx context.Context, lt *linear.Tracker) func(context.Context, storage.Transaction, string, []string, *tracker.TrackerIssue, string) error {
	_ = ctx
	return func(ctx context.Context, tx storage.Transaction, issueID string, desired []string, extIssue *tracker.TrackerIssue, actor string) error {
		_ = desired
		remoteIssue, ok := extIssue.Raw.(*linear.Issue)
		if !ok || remoteIssue == nil {
			return fmt.Errorf("ReconcileLabels: unexpected raw type %T", extIssue.Raw)
		}
		linearLabels := make([]linear.LinearLabel, 0)
		if remoteIssue.Labels != nil {
			for _, l := range remoteIssue.Labels.Nodes {
				linearLabels = append(linearLabels, linear.LinearLabel{Name: l.Name, ID: l.ID})
			}
		}
		snap, err := tx.GetLinearLabelSnapshot(ctx, issueID)
		if err != nil {
			return err
		}
		snapEntries := make([]linear.SnapshotEntry, len(snap))
		for i, s := range snap {
			snapEntries[i] = linear.SnapshotEntry{Name: s.LabelName, ID: s.LabelID}
		}
		currentLabels, err := tx.GetLabels(ctx, issueID)
		if err != nil {
			return err
		}
		res := linear.ReconcileLabels(linear.LabelReconcileInput{
			Beads: currentLabels, Linear: linearLabels, Snapshot: snapEntries, Exclude: lt.LabelExclude(),
		})
		for _, n := range res.RemoveFromBeads {
			if err := tx.RemoveLabel(ctx, issueID, n, actor); err != nil {
				return err
			}
		}
		for _, n := range res.AddToBeads {
			if err := tx.AddLabel(ctx, issueID, n, actor); err != nil {
				return err
			}
		}
		snapshotEntries := make([]storage.LinearLabelSnapshotEntry, len(res.NewSnapshot))
		for i, e := range res.NewSnapshot {
			snapshotEntries[i] = storage.LinearLabelSnapshotEntry{LabelID: e.ID, LabelName: e.Name}
		}
		return tx.PutLinearLabelSnapshot(ctx, issueID, snapshotEntries)
	}
}

// patchTestHooksForLabelSync wraps the test hooks' ContentEqual callbacks with
// label-aware versions, mirroring what buildLinearPushHooks/buildLinearPullHooks
// install in production. Without this patch the engine skips both pull and push
// for label-only deltas (the gate problem from spec decision #12).
func patchTestHooksForLabelSync(ctx context.Context, lt *linear.Tracker, push *tracker.PushHooks, pull *tracker.PullHooks) {
	if !lt.LabelSyncEnabled() {
		return
	}
	pushOriginal := push.ContentEqual
	push.ContentEqual = func(local *types.Issue, remote *tracker.TrackerIssue) bool {
		if pushOriginal != nil && !pushOriginal(local, remote) {
			return false
		}
		remoteIssue, ok := remote.Raw.(*linear.Issue)
		if !ok || remoteIssue == nil {
			return true
		}
		return !hasLabelDeltaTest(ctx, lt, local, remoteIssue)
	}
	pullOriginal := pull.ContentEqual
	pull.ContentEqual = func(local, remote *types.Issue) bool {
		if pullOriginal != nil && !pullOriginal(local, remote) {
			return false
		}
		return !pullHasLabelDeltaTest(lt, local, remote)
	}
}

// hasLabelDeltaTest mirrors hasLabelDelta from cmd/bd/linear.go — push direction only.
// On LoadSnapshot error, falls through with nil snap (matches production behavior:
// the reconciler's first-sync synthesis treats nil as "use intersection" which
// produces no false delta when label sets agree).
func hasLabelDeltaTest(ctx context.Context, lt *linear.Tracker, local *types.Issue, remoteIssue *linear.Issue) bool {
	linearLabels := make([]linear.LinearLabel, 0)
	if remoteIssue.Labels != nil {
		for _, l := range remoteIssue.Labels.Nodes {
			linearLabels = append(linearLabels, linear.LinearLabel{Name: l.Name, ID: l.ID})
		}
	}
	snap, _ := lt.LoadSnapshot(ctx, local.ID)
	res := linear.ReconcileLabels(linear.LabelReconcileInput{
		Beads: local.Labels, Linear: linearLabels, Snapshot: snap, Exclude: lt.LabelExclude(),
	})
	return len(res.AddToLinear) > 0 || len(res.RemoveFromLinear) > 0
}

// pullHasLabelDeltaTest mirrors pullHasLabelDelta from cmd/bd/linear.go.
// Case-insensitive comparison so bead/Linear casing differences don't trigger false deltas.
func pullHasLabelDeltaTest(lt *linear.Tracker, local, remote *types.Issue) bool {
	localLower := make(map[string]bool, len(local.Labels))
	for _, n := range local.Labels {
		localLower[strings.ToLower(n)] = true
	}
	remoteLower := make(map[string]bool, len(remote.Labels))
	for _, n := range remote.Labels {
		remoteLower[strings.ToLower(n)] = true
	}
	excluded := lt.LabelExclude()
	for k := range remoteLower {
		if excluded != nil && excluded[k] {
			continue
		}
		if !localLower[k] {
			return true
		}
	}
	for k := range localLower {
		if excluded != nil && excluded[k] {
			continue
		}
		if !remoteLower[k] {
			return true
		}
	}
	return false
}

// setEq compares two string slices ignoring order.
func setEq(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	m := map[string]bool{}
	for _, x := range a {
		m[x] = true
	}
	for _, x := range b {
		if !m[x] {
			return false
		}
	}
	return true
}

// labelNames extracts the names from a slice of linear.Labels.
func labelNames(ls []linear.Label) []string {
	out := make([]string, len(ls))
	for i, l := range ls {
		out[i] = l.Name
	}
	return out
}

// snapshotNames extracts the names from a snapshot.
func snapshotNames(snap []storage.LinearLabelSnapshotEntry) []string {
	out := make([]string, len(snap))
	for i, e := range snap {
		out[i] = e.LabelName
	}
	return out
}

// readSnapshot is a test helper that reads the snapshot for an issue.
func readSnapshot(t *testing.T, store *dolt.DoltStore, issueID string) []storage.LinearLabelSnapshotEntry {
	t.Helper()
	var out []storage.LinearLabelSnapshotEntry
	err := store.RunInTransaction(context.Background(), "test read snapshot", func(tx storage.Transaction) error {
		var err error
		out, err = tx.GetLinearLabelSnapshot(context.Background(), issueID)
		return err
	})
	if err != nil {
		t.Fatalf("readSnapshot: %v", err)
	}
	return out
}

// writeSnapshotForTest pre-seeds the snapshot for an issue.
func writeSnapshotForTest(t *testing.T, store *dolt.DoltStore, issueID string, entries []storage.LinearLabelSnapshotEntry) {
	t.Helper()
	err := store.RunInTransaction(context.Background(), "test seed snapshot", func(tx storage.Transaction) error {
		return tx.PutLinearLabelSnapshot(context.Background(), issueID, entries)
	})
	if err != nil {
		t.Fatalf("writeSnapshot: %v", err)
	}
}

// pushAndPull runs a sync that does both directions.
func pushAndPull(t *testing.T, engine *tracker.Engine) {
	t.Helper()
	_, err := engine.Sync(context.Background(), tracker.SyncOptions{Push: true, Pull: true})
	if err != nil {
		t.Fatalf("Sync failed: %v", err)
	}
}

func TestRoundtrip_FirstSyncPreservesBothSides(t *testing.T) {
	store, mock, _, engine := setupLabelSyncTest(t)
	ctx := context.Background()

	// Seed Linear with an issue that has label A.
	mock.SeedLinearLabels("team", linear.Label{ID: "id-A", Name: "A"}, linear.Label{ID: "id-B", Name: "B"})
	mock.issues["lin-1"] = &linear.Issue{
		ID: "lin-1", Identifier: "MOCK-1", Title: "test",
		URL:       "https://linear.app/mock/issue/MOCK-1",
		Labels:    &linear.Labels{Nodes: []linear.Label{{ID: "id-A", Name: "A"}}},
		CreatedAt: time.Now().UTC().Format(time.RFC3339),
		UpdatedAt: time.Now().UTC().Format(time.RFC3339),
	}

	// Seed beads with corresponding bead linked to that Linear issue, with
	// labels [A, B] (B is local-only).
	extRef := "https://linear.app/mock/issue/MOCK-1"
	beadID := "bd-1"
	if err := store.CreateIssue(ctx, &types.Issue{
		ID: beadID, Title: "test", Status: types.StatusOpen,
		ExternalRef: &extRef,
		Labels:      []string{"A", "B"},
	}, "test-actor"); err != nil {
		t.Fatalf("CreateIssue: %v", err)
	}

	// No snapshot yet → first-sync rule applies.
	pushAndPull(t, engine)

	// Bead labels: still [A, B] (no removal on first sync).
	pulled, err := store.GetIssue(ctx, beadID)
	if err != nil {
		t.Fatalf("GetIssue: %v", err)
	}
	if !setEq(pulled.Labels, []string{"A", "B"}) {
		t.Errorf("bead labels: got %v, want [A B]", pulled.Labels)
	}

	// Linear labels: now [A, B] (B was pushed).
	got := labelNames(mock.IssueLabels("lin-1"))
	if !setEq(got, []string{"A", "B"}) {
		t.Errorf("linear labels: got %v, want [A B]", got)
	}

	// Snapshot reflects both labels.
	snap := readSnapshot(t, store, beadID)
	if !setEq(snapshotNames(snap), []string{"A", "B"}) {
		t.Errorf("snapshot: got %+v, want entries for [A B]", snap)
	}
}

// G2: pull-side removal works
func TestRoundtrip_PullSideRemovalApplies(t *testing.T) {
	store, mock, _, engine := setupLabelSyncTest(t)
	ctx := context.Background()

	mock.SeedLinearLabels("team", linear.Label{ID: "id-A", Name: "A"}, linear.Label{ID: "id-B", Name: "B"})
	mock.issues["lin-1"] = &linear.Issue{
		ID: "lin-1", Identifier: "MOCK-1", Title: "test",
		URL:       "https://linear.app/mock/issue/MOCK-1",
		Labels:    &linear.Labels{Nodes: []linear.Label{{ID: "id-A", Name: "A"}}}, // B already gone from Linear
		CreatedAt: time.Now().UTC().Format(time.RFC3339),
		UpdatedAt: time.Now().UTC().Format(time.RFC3339),
	}

	extRef := "https://linear.app/mock/issue/MOCK-1"
	beadID := "bd-1"
	if err := store.CreateIssue(ctx, &types.Issue{
		ID: beadID, Title: "test", Status: types.StatusOpen,
		ExternalRef: &extRef,
		Labels:      []string{"A", "B"}, // bead still has B locally
	}, "test-actor"); err != nil {
		t.Fatalf("CreateIssue: %v", err)
	}

	// Seed snapshot showing both A and B were last agreed.
	writeSnapshotForTest(t, store, beadID, []storage.LinearLabelSnapshotEntry{
		{LabelID: "id-A", LabelName: "A"},
		{LabelID: "id-B", LabelName: "B"},
	})

	pushAndPull(t, engine)

	pulled, err := store.GetIssue(ctx, beadID)
	if err != nil {
		t.Fatalf("GetIssue: %v", err)
	}
	if !setEq(pulled.Labels, []string{"A"}) {
		t.Errorf("bead labels: got %v, want [A] (B removed in Linear)", pulled.Labels)
	}
}

// G3: new bead pushes labels (CreateIssue regression — was passing nil for labelIDs)
func TestRoundtrip_NewBeadPushesLabels(t *testing.T) {
	store, mock, _, engine := setupLabelSyncTest(t)
	ctx := context.Background()

	mock.SeedLinearLabels("team", linear.Label{ID: "id-bug", Name: "bug"})

	beadID := "bd-1"
	if err := store.CreateIssue(ctx, &types.Issue{
		ID: beadID, Title: "test", Status: types.StatusOpen,
		Labels: []string{"bug", "p1"}, // bug exists on Linear, p1 will auto-create
	}, "test-actor"); err != nil {
		t.Fatalf("CreateIssue: %v", err)
	}

	pushAndPull(t, engine)

	// Find the Linear issue created from this push (only one in mock).
	var created *linear.Issue
	for _, issue := range mock.issues {
		created = issue
		break
	}
	if created == nil {
		t.Fatal("no issue was created in Linear")
	}
	got := labelNames(created.Labels.Nodes)
	if !setEq(got, []string{"bug", "p1"}) {
		t.Errorf("created issue labels: got %v, want [bug p1]", got)
	}
	if !sliceContains(mock.LabelCreates(), "p1") {
		t.Errorf("expected p1 to be auto-created, LabelCreates=%v", mock.LabelCreates())
	}
}

func sliceContains(xs []string, x string) bool {
	for _, s := range xs {
		if s == x {
			return true
		}
	}
	return false
}

// G4: label-only push fires (gate regression for decision #12 — ContentEqual must be label-aware)
func TestRoundtrip_LabelOnlyPushFires(t *testing.T) {
	store, mock, _, engine := setupLabelSyncTest(t)
	ctx := context.Background()

	mock.SeedLinearLabels("team",
		linear.Label{ID: "id-A", Name: "A"},
		linear.Label{ID: "id-B", Name: "B"},
	)
	mock.issues["lin-1"] = &linear.Issue{
		ID: "lin-1", Identifier: "MOCK-1", Title: "test", Description: "body", Priority: 0,
		URL:       "https://linear.app/mock/issue/MOCK-1",
		Labels:    &linear.Labels{Nodes: []linear.Label{{ID: "id-A", Name: "A"}}},
		CreatedAt: time.Now().UTC().Format(time.RFC3339),
		UpdatedAt: time.Now().UTC().Format(time.RFC3339),
	}

	extRef := "https://linear.app/mock/issue/MOCK-1"
	beadID := "bd-1"
	if err := store.CreateIssue(ctx, &types.Issue{
		ID: beadID, Title: "test", Description: "body", Priority: 0, Status: types.StatusOpen,
		ExternalRef: &extRef,
		Labels:      []string{"A", "B"}, // only labels differ from Linear
	}, "test-actor"); err != nil {
		t.Fatalf("CreateIssue: %v", err)
	}
	writeSnapshotForTest(t, store, beadID, []storage.LinearLabelSnapshotEntry{
		{LabelID: "id-A", LabelName: "A"},
	})

	pushAndPull(t, engine)

	got := labelNames(mock.IssueLabels("lin-1"))
	if !setEq(got, []string{"A", "B"}) {
		t.Errorf("expected B pushed to Linear, got %v", got)
	}
	if mock.UpdateCallCount("lin-1") == 0 {
		t.Errorf("expected at least one issueUpdate call (gate must allow label-only deltas)")
	}
}

// G5: Linear labelIds replace semantics
func TestRoundtrip_LabelIdsReplaceSemantics(t *testing.T) {
	store, mock, _, engine := setupLabelSyncTest(t)
	ctx := context.Background()

	mock.SeedLinearLabels("team",
		linear.Label{ID: "id-X", Name: "X"},
		linear.Label{ID: "id-Y", Name: "Y"},
	)
	mock.issues["lin-1"] = &linear.Issue{
		ID: "lin-1", Identifier: "MOCK-1", Title: "test",
		URL:       "https://linear.app/mock/issue/MOCK-1",
		Labels:    &linear.Labels{Nodes: []linear.Label{{ID: "id-X", Name: "X"}}},
		CreatedAt: time.Now().UTC().Format(time.RFC3339),
		UpdatedAt: time.Now().UTC().Format(time.RFC3339),
	}

	extRef := "https://linear.app/mock/issue/MOCK-1"
	beadID := "bd-1"
	if err := store.CreateIssue(ctx, &types.Issue{
		ID: beadID, Title: "test", Status: types.StatusOpen,
		ExternalRef: &extRef,
		Labels:      []string{"Y"}, // user removed X locally and added Y
	}, "test-actor"); err != nil {
		t.Fatalf("CreateIssue: %v", err)
	}
	writeSnapshotForTest(t, store, beadID, []storage.LinearLabelSnapshotEntry{
		{LabelID: "id-X", LabelName: "X"},
	})

	pushAndPull(t, engine)

	got := labelNames(mock.IssueLabels("lin-1"))
	if !setEq(got, []string{"Y"}) {
		t.Errorf("expected Linear labels [Y] after replace, got %v", got)
	}
}

// G6: auto-create missing label on push
func TestRoundtrip_AutoCreatesMissingLabel(t *testing.T) {
	store, mock, _, engine := setupLabelSyncTest(t)
	ctx := context.Background()

	mock.issues["lin-1"] = &linear.Issue{
		ID: "lin-1", Identifier: "MOCK-1", Title: "test",
		URL:       "https://linear.app/mock/issue/MOCK-1",
		Labels:    &linear.Labels{Nodes: []linear.Label{}},
		CreatedAt: time.Now().UTC().Format(time.RFC3339),
		UpdatedAt: time.Now().UTC().Format(time.RFC3339),
	}

	extRef := "https://linear.app/mock/issue/MOCK-1"
	beadID := "bd-1"
	if err := store.CreateIssue(ctx, &types.Issue{
		ID: beadID, Title: "test", Status: types.StatusOpen,
		ExternalRef: &extRef,
		Labels:      []string{"never-seen"},
	}, "test-actor"); err != nil {
		t.Fatalf("CreateIssue: %v", err)
	}

	pushAndPull(t, engine)

	created := mock.LabelCreates()
	if len(created) != 1 || created[0] != "never-seen" {
		t.Errorf("expected one CreateLabel call for never-seen, got %v", created)
	}
	got := labelNames(mock.IssueLabels("lin-1"))
	if !setEq(got, []string{"never-seen"}) {
		t.Errorf("expected Linear to have never-seen label, got %v", got)
	}
}

// G7: concurrent both-sides changes converge
func TestRoundtrip_ConcurrentBothSidesChangesConverge(t *testing.T) {
	store, mock, _, engine := setupLabelSyncTest(t)
	ctx := context.Background()

	mock.SeedLinearLabels("team",
		linear.Label{ID: "id-A", Name: "A"},
		linear.Label{ID: "id-C", Name: "C"},
		linear.Label{ID: "id-D", Name: "D"},
	)
	mock.issues["lin-1"] = &linear.Issue{
		ID: "lin-1", Identifier: "MOCK-1", Title: "test",
		URL:       "https://linear.app/mock/issue/MOCK-1",
		Labels:    &linear.Labels{Nodes: []linear.Label{{ID: "id-A", Name: "A"}, {ID: "id-D", Name: "D"}}},
		CreatedAt: time.Now().UTC().Format(time.RFC3339),
		UpdatedAt: time.Now().UTC().Format(time.RFC3339),
	}

	extRef := "https://linear.app/mock/issue/MOCK-1"
	beadID := "bd-1"
	if err := store.CreateIssue(ctx, &types.Issue{
		ID: beadID, Title: "test", Status: types.StatusOpen,
		ExternalRef: &extRef,
		Labels:      []string{"B", "C"}, // beads removed A, added C; B is local-only
	}, "test-actor"); err != nil {
		t.Fatalf("CreateIssue: %v", err)
	}
	writeSnapshotForTest(t, store, beadID, []storage.LinearLabelSnapshotEntry{
		{LabelID: "id-A", LabelName: "A"},
		{LabelID: "id-B", LabelName: "B"},
	})

	pushAndPull(t, engine)

	pulled, _ := store.GetIssue(ctx, beadID)
	if !setEq(pulled.Labels, []string{"C", "D"}) {
		t.Errorf("bead labels: got %v, want [C D]", pulled.Labels)
	}
	got := labelNames(mock.IssueLabels("lin-1"))
	if !setEq(got, []string{"C", "D"}) {
		t.Errorf("linear labels: got %v, want [C D]", got)
	}
}

// G8: rename + concurrent local delete (decision #10)
func TestRoundtrip_RenamePlusLocalDeleteWins(t *testing.T) {
	store, mock, _, engine := setupLabelSyncTest(t)
	ctx := context.Background()

	mock.SeedLinearLabels("team", linear.Label{ID: "X", Name: "new"})
	mock.issues["lin-1"] = &linear.Issue{
		ID: "lin-1", Identifier: "MOCK-1", Title: "test",
		URL:       "https://linear.app/mock/issue/MOCK-1",
		Labels:    &linear.Labels{Nodes: []linear.Label{{ID: "X", Name: "new"}}}, // Linear renamed
		CreatedAt: time.Now().UTC().Format(time.RFC3339),
		UpdatedAt: time.Now().UTC().Format(time.RFC3339),
	}

	extRef := "https://linear.app/mock/issue/MOCK-1"
	beadID := "bd-1"
	if err := store.CreateIssue(ctx, &types.Issue{
		ID: beadID, Title: "test", Status: types.StatusOpen,
		ExternalRef: &extRef,
		Labels:      []string{}, // user deleted "old" locally
	}, "test-actor"); err != nil {
		t.Fatalf("CreateIssue: %v", err)
	}
	writeSnapshotForTest(t, store, beadID, []storage.LinearLabelSnapshotEntry{
		{LabelID: "X", LabelName: "old"},
	})

	pushAndPull(t, engine)

	// Delete wins: bead stays empty, Linear loses the label entirely.
	pulled, _ := store.GetIssue(ctx, beadID)
	if len(pulled.Labels) != 0 {
		t.Errorf("bead: got %v, want empty", pulled.Labels)
	}
	got := mock.IssueLabels("lin-1")
	if len(got) != 0 {
		t.Errorf("linear: got %v, want empty (delete wins)", labelNames(got))
	}
	snap := readSnapshot(t, store, beadID)
	if len(snap) != 0 {
		t.Errorf("snapshot: got %+v, want empty", snap)
	}
}
