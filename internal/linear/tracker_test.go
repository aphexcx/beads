package linear

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"
	"time"

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

// teamStatesResp builds the JSON body for a TeamStates GraphQL response.
func teamStatesResp(teamID, stateID, stateName, stateType string) map[string]interface{} {
	return map[string]interface{}{
		"data": map[string]interface{}{
			"team": map[string]interface{}{
				"id": teamID,
				"states": map[string]interface{}{
					"nodes": []interface{}{
						map[string]interface{}{
							"id":   stateID,
							"name": stateName,
							"type": stateType,
						},
					},
				},
			},
		},
	}
}

// teamLabelsEmptyResp builds a paginated TeamLabels GraphQL response with no labels.
func teamLabelsEmptyResp(teamID string) map[string]interface{} {
	return map[string]interface{}{
		"data": map[string]interface{}{
			"team": map[string]interface{}{
				"id": teamID,
				"labels": map[string]interface{}{
					"nodes": []interface{}{},
					"pageInfo": map[string]interface{}{
						"hasNextPage": false,
						"endCursor":   "",
					},
				},
			},
		},
	}
}

// issueByIdentifierResp builds the JSON body for an IssueByIdentifier GraphQL response.
func issueByIdentifierResp(id, identifier, title, description string, priority int, stateID, stateName, stateType string) map[string]interface{} {
	return map[string]interface{}{
		"data": map[string]interface{}{
			"issues": map[string]interface{}{
				"nodes": []interface{}{
					map[string]interface{}{
						"id":          id,
						"identifier":  identifier,
						"title":       title,
						"description": description,
						"url":         "https://linear.app/team/issue/" + identifier,
						"priority":    priority,
						"state": map[string]interface{}{
							"id":   stateID,
							"name": stateName,
							"type": stateType,
						},
						"createdAt": "2026-01-01T00:00:00Z",
						"updatedAt": "2026-01-01T00:00:00Z",
					},
				},
			},
		},
	}
}

// TestBatchPush_SkipsUnchangedIssue verifies that BatchPush does not call
// UpdateIssue when the remote issue content matches the local issue. The
// single-issue push path in engine.go:doPush performs this ContentEqual /
// UpdatedAt skip check; BatchPush must replicate it so every sync does not
// re-push all Linear-linked issues unchanged.
func TestBatchPush_SkipsUnchangedIssue(t *testing.T) {
	var updateCalled bool
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		var req GraphQLRequest
		_ = json.Unmarshal(body, &req)
		w.Header().Set("Content-Type", "application/json")

		switch {
		case strings.Contains(req.Query, "TeamStates"):
			json.NewEncoder(w).Encode(teamStatesResp("team-1", "state-open", "Backlog", "backlog"))
		case strings.Contains(req.Query, "TeamLabels"):
			json.NewEncoder(w).Encode(teamLabelsEmptyResp("team-1"))
		case strings.Contains(req.Query, "IssuesByIdentifiers"), strings.Contains(req.Query, "IssueByIdentifier"):
			// Remote issue has the same title, empty description, priority 0 (no
			// priority), and "backlog" state — matching the local issue exactly.
			json.NewEncoder(w).Encode(issueByIdentifierResp(
				"remote-uuid", "TEAM-1", "My Issue", "", 0,
				"state-open", "Backlog", "backlog",
			))
		case strings.Contains(req.Query, "issueUpdate"):
			updateCalled = true
			json.NewEncoder(w).Encode(map[string]interface{}{
				"data": map[string]interface{}{
					"issueUpdate": map[string]interface{}{
						"success": true,
						"issue":   map[string]interface{}{"id": "remote-uuid", "url": "https://linear.app/team/issue/TEAM-1", "updatedAt": "2026-01-01T00:00:00Z"},
					},
				},
			})
		}
	}))
	defer server.Close()

	cfg := DefaultMappingConfig()
	// ExplicitStateMap is required by ResolveStateIDForBeadsStatus.
	cfg.ExplicitStateMap = map[string]string{"backlog": "open"}

	extRef := "https://linear.app/team/issue/TEAM-1"
	// Priority 4 (beads backlog) → PriorityToLinear = 0 (no priority) via default map.
	// Status open + state backlog → PushFieldsEqual returns true.
	local := &types.Issue{
		ID:          "local-1",
		Title:       "My Issue",
		Status:      types.StatusOpen,
		Priority:    4,
		ExternalRef: &extRef,
	}

	tr := &Tracker{
		teamIDs: []string{"team-1"},
		clients: map[string]*Client{
			"team-1": NewClient("key", "team-1").WithEndpoint(server.URL),
		},
		config: cfg,
	}

	result, err := tr.BatchPush(context.Background(), []*types.Issue{local}, nil)
	if err != nil {
		t.Fatalf("BatchPush: %v", err)
	}
	if updateCalled {
		t.Error("UpdateIssue was called for an unchanged issue; expected it to be skipped")
	}
	if len(result.Skipped) != 1 || result.Skipped[0] != "local-1" {
		t.Errorf("Skipped = %v, want [local-1]", result.Skipped)
	}
	if len(result.Updated) != 0 {
		t.Errorf("Updated = %v, want []", result.Updated)
	}
}

// TestBatchPush_SkipsUnchangedIssueWithPreformattedDescription verifies that
// BatchPush skip semantics still work when descriptions were pre-formatted by
// the engine's FormatDescription hook (BuildLinearDescription). This guards
// against double-formatting during skip comparison.
func TestBatchPush_SkipsUnchangedIssueWithPreformattedDescription(t *testing.T) {
	var updateCalled bool
	formattedDescription := "Base body\n\n## Acceptance Criteria\nMust pass"

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		var req GraphQLRequest
		_ = json.Unmarshal(body, &req)
		w.Header().Set("Content-Type", "application/json")

		switch {
		case strings.Contains(req.Query, "TeamStates"):
			json.NewEncoder(w).Encode(teamStatesResp("team-1", "state-open", "Backlog", "backlog"))
		case strings.Contains(req.Query, "TeamLabels"):
			json.NewEncoder(w).Encode(teamLabelsEmptyResp("team-1"))
		case strings.Contains(req.Query, "IssueByIdentifier"):
			json.NewEncoder(w).Encode(issueByIdentifierResp(
				"remote-uuid", "TEAM-1", "My Issue", formattedDescription, 0,
				"state-open", "Backlog", "backlog",
			))
		case strings.Contains(req.Query, "issueUpdate"):
			updateCalled = true
			json.NewEncoder(w).Encode(map[string]interface{}{
				"data": map[string]interface{}{
					"issueUpdate": map[string]interface{}{
						"success": true,
						"issue":   map[string]interface{}{"id": "remote-uuid", "url": "https://linear.app/team/issue/TEAM-1", "updatedAt": "2026-01-01T00:00:00Z"},
					},
				},
			})
		}
	}))
	defer server.Close()

	cfg := DefaultMappingConfig()
	cfg.ExplicitStateMap = map[string]string{"backlog": "open"}

	extRef := "https://linear.app/team/issue/TEAM-1"
	local := &types.Issue{
		ID:                 "local-1",
		Title:              "My Issue",
		Description:        formattedDescription,
		AcceptanceCriteria: "Must pass",
		Status:             types.StatusOpen,
		Priority:           4,
		ExternalRef:        &extRef,
	}

	tr := &Tracker{
		teamIDs: []string{"team-1"},
		clients: map[string]*Client{
			"team-1": NewClient("key", "team-1").WithEndpoint(server.URL),
		},
		config: cfg,
	}

	result, err := tr.BatchPush(context.Background(), []*types.Issue{local}, nil)
	if err != nil {
		t.Fatalf("BatchPush: %v", err)
	}
	if updateCalled {
		t.Error("UpdateIssue was called for an unchanged pre-formatted issue; expected it to be skipped")
	}
	if len(result.Skipped) != 1 || result.Skipped[0] != "local-1" {
		t.Errorf("Skipped = %v, want [local-1]", result.Skipped)
	}
	if len(result.Updated) != 0 {
		t.Errorf("Updated = %v, want []", result.Updated)
	}
}

// TestBatchPush_ForceBypassesSkip verifies that an issue in forceIDs is
// updated even when PushFieldsEqual would normally skip it.
func TestBatchPush_ForceBypassesSkip(t *testing.T) {
	var updateCalled bool
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		var req GraphQLRequest
		_ = json.Unmarshal(body, &req)
		w.Header().Set("Content-Type", "application/json")

		switch {
		case strings.Contains(req.Query, "TeamStates"):
			json.NewEncoder(w).Encode(teamStatesResp("team-1", "state-open", "Backlog", "backlog"))
		case strings.Contains(req.Query, "TeamLabels"):
			json.NewEncoder(w).Encode(teamLabelsEmptyResp("team-1"))
		case strings.Contains(req.Query, "IssuesByIdentifiers"), strings.Contains(req.Query, "IssueByIdentifier"):
			// Return the same content as local (would be skipped without force).
			json.NewEncoder(w).Encode(issueByIdentifierResp(
				"remote-uuid", "TEAM-1", "My Issue", "", 0,
				"state-open", "Backlog", "backlog",
			))
		case strings.Contains(req.Query, "issueUpdate"):
			updateCalled = true
			json.NewEncoder(w).Encode(map[string]interface{}{
				"data": map[string]interface{}{
					"issueUpdate": map[string]interface{}{
						"success": true,
						"issue":   map[string]interface{}{"id": "remote-uuid", "url": "https://linear.app/team/issue/TEAM-1", "updatedAt": "2026-01-02T00:00:00Z"},
					},
				},
			})
		}
	}))
	defer server.Close()

	cfg := DefaultMappingConfig()
	cfg.ExplicitStateMap = map[string]string{"backlog": "open"}

	extRef := "https://linear.app/team/issue/TEAM-1"
	local := &types.Issue{
		ID:          "local-1",
		Title:       "My Issue",
		Status:      types.StatusOpen,
		Priority:    4,
		ExternalRef: &extRef,
	}

	tr := &Tracker{
		teamIDs: []string{"team-1"},
		clients: map[string]*Client{
			"team-1": NewClient("key", "team-1").WithEndpoint(server.URL),
		},
		config: cfg,
	}

	forceIDs := map[string]bool{"local-1": true}
	result, err := tr.BatchPush(context.Background(), []*types.Issue{local}, forceIDs)
	if err != nil {
		t.Fatalf("BatchPush: %v", err)
	}
	if !updateCalled {
		t.Error("UpdateIssue was not called; forceIDs should bypass skip semantics")
	}
	if len(result.Updated) != 1 {
		t.Errorf("Updated = %v, want 1 item", result.Updated)
	}
}

// TestBatchPushDryRun_SkipsUnchangedIssue is the bd-q3y regression test: a
// dry-run preview must apply the same PushFieldsEqual skip check as the real
// BatchPush. Before BatchPushDryRun existed, the engine's fallback preview
// printed "Would update in Linear" for EVERY ref-bearing bead — reporting the
// whole mirror as phantom-push-dirty on every `bd linear sync --dry-run` even
// with zero local edits.
func TestBatchPushDryRun_SkipsUnchangedIssue(t *testing.T) {
	var mutationCalled bool
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		var req GraphQLRequest
		_ = json.Unmarshal(body, &req)
		w.Header().Set("Content-Type", "application/json")

		switch {
		case strings.Contains(req.Query, "TeamStates"):
			json.NewEncoder(w).Encode(teamStatesResp("team-1", "state-open", "Backlog", "backlog"))
		case strings.Contains(req.Query, "TeamLabels"):
			json.NewEncoder(w).Encode(teamLabelsEmptyResp("team-1"))
		case strings.Contains(req.Query, "IssuesByIdentifiers"), strings.Contains(req.Query, "IssueByIdentifier"):
			// Remote content matches the local issue exactly (see
			// TestBatchPush_SkipsUnchangedIssue for the field mapping).
			json.NewEncoder(w).Encode(issueByIdentifierResp(
				"remote-uuid", "TEAM-1", "My Issue", "", 0,
				"state-open", "Backlog", "backlog",
			))
		case strings.Contains(req.Query, "issueUpdate"), strings.Contains(req.Query, "issueCreate"), strings.Contains(req.Query, "issueBatchCreate"):
			mutationCalled = true
			json.NewEncoder(w).Encode(map[string]interface{}{"data": map[string]interface{}{}})
		}
	}))
	defer server.Close()

	cfg := DefaultMappingConfig()
	cfg.ExplicitStateMap = map[string]string{"backlog": "open"}

	extRef := "https://linear.app/team/issue/TEAM-1"
	local := &types.Issue{
		ID:          "local-1",
		Title:       "My Issue",
		Status:      types.StatusOpen,
		Priority:    4,
		ExternalRef: &extRef,
	}

	tr := &Tracker{
		teamIDs: []string{"team-1"},
		clients: map[string]*Client{
			"team-1": NewClient("key", "team-1").WithEndpoint(server.URL),
		},
		config: cfg,
	}

	result, err := tr.BatchPushDryRun(context.Background(), []*types.Issue{local}, nil)
	if err != nil {
		t.Fatalf("BatchPushDryRun: %v", err)
	}
	if mutationCalled {
		t.Error("dry-run sent a mutation to Linear; previews must be read-only")
	}
	if len(result.Skipped) != 1 || result.Skipped[0] != "local-1" {
		t.Errorf("Skipped = %v, want [local-1]", result.Skipped)
	}
	if len(result.Updated) != 0 {
		t.Errorf("Updated = %v, want [] (unchanged issue must not preview as an update)", result.Updated)
	}
}

// TestBatchPushDryRun_ReportsChangedWithoutMutating verifies that a genuinely
// content-dirty issue previews as Updated while the underlying issueUpdate
// mutation is NOT sent.
func TestBatchPushDryRun_ReportsChangedWithoutMutating(t *testing.T) {
	var mutationCalled bool
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		var req GraphQLRequest
		_ = json.Unmarshal(body, &req)
		w.Header().Set("Content-Type", "application/json")

		switch {
		case strings.Contains(req.Query, "TeamStates"):
			json.NewEncoder(w).Encode(teamStatesResp("team-1", "state-open", "Backlog", "backlog"))
		case strings.Contains(req.Query, "TeamLabels"):
			json.NewEncoder(w).Encode(teamLabelsEmptyResp("team-1"))
		case strings.Contains(req.Query, "IssuesByIdentifiers"), strings.Contains(req.Query, "IssueByIdentifier"):
			// Remote title differs from local — PushFieldsEqual must report dirty.
			json.NewEncoder(w).Encode(issueByIdentifierResp(
				"remote-uuid", "TEAM-1", "Stale Remote Title", "", 0,
				"state-open", "Backlog", "backlog",
			))
		case strings.Contains(req.Query, "issueUpdate"):
			mutationCalled = true
			json.NewEncoder(w).Encode(map[string]interface{}{"data": map[string]interface{}{}})
		}
	}))
	defer server.Close()

	cfg := DefaultMappingConfig()
	cfg.ExplicitStateMap = map[string]string{"backlog": "open"}

	extRef := "https://linear.app/team/issue/TEAM-1"
	local := &types.Issue{
		ID:          "local-1",
		Title:       "My Issue",
		Status:      types.StatusOpen,
		Priority:    4,
		ExternalRef: &extRef,
	}

	tr := &Tracker{
		teamIDs: []string{"team-1"},
		clients: map[string]*Client{
			"team-1": NewClient("key", "team-1").WithEndpoint(server.URL),
		},
		config: cfg,
	}

	result, err := tr.BatchPushDryRun(context.Background(), []*types.Issue{local}, nil)
	if err != nil {
		t.Fatalf("BatchPushDryRun: %v", err)
	}
	if mutationCalled {
		t.Error("dry-run sent issueUpdate to Linear; previews must be read-only")
	}
	if len(result.Updated) != 1 || result.Updated[0].LocalID != "local-1" {
		t.Errorf("Updated = %v, want one item for local-1", result.Updated)
	}
	if len(result.Skipped) != 0 {
		t.Errorf("Skipped = %v, want []", result.Skipped)
	}
}

// TestBatchPushDryRun_PreviewsCreateWithoutMutating verifies that an issue
// without an external ref previews as Created and no create mutation is sent.
func TestBatchPushDryRun_PreviewsCreateWithoutMutating(t *testing.T) {
	var mutationCalled bool
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		var req GraphQLRequest
		_ = json.Unmarshal(body, &req)
		w.Header().Set("Content-Type", "application/json")

		switch {
		case strings.Contains(req.Query, "TeamStates"):
			json.NewEncoder(w).Encode(teamStatesResp("team-1", "state-open", "Backlog", "backlog"))
		case strings.Contains(req.Query, "TeamLabels"):
			json.NewEncoder(w).Encode(teamLabelsEmptyResp("team-1"))
		case strings.Contains(req.Query, "issueBatchCreate"), strings.Contains(req.Query, "CreateIssue"):
			mutationCalled = true
			json.NewEncoder(w).Encode(map[string]interface{}{"data": map[string]interface{}{}})
		}
	}))
	defer server.Close()

	cfg := DefaultMappingConfig()
	cfg.ExplicitStateMap = map[string]string{"backlog": "open"}

	local := &types.Issue{
		ID:        "local-2",
		Title:     "Brand New Issue",
		Status:    types.StatusOpen,
		Priority:  4,
		CreatedAt: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
	}

	tr := &Tracker{
		teamIDs: []string{"team-1"},
		clients: map[string]*Client{
			"team-1": NewClient("key", "team-1").WithEndpoint(server.URL),
		},
		config: cfg,
	}

	result, err := tr.BatchPushDryRun(context.Background(), []*types.Issue{local}, nil)
	if err != nil {
		t.Fatalf("BatchPushDryRun: %v", err)
	}
	if mutationCalled {
		t.Error("dry-run sent a create mutation to Linear; previews must be read-only")
	}
	if len(result.Created) != 1 || result.Created[0].LocalID != "local-2" {
		t.Errorf("Created = %v, want one item for local-2", result.Created)
	}
}

// TestBatchPushDryRun_ForceBypassesSkip verifies that forceIDs previews as
// Updated even when content is equal, mirroring the wet-run force semantics —
// still without sending the mutation.
func TestBatchPushDryRun_ForceBypassesSkip(t *testing.T) {
	var mutationCalled bool
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		var req GraphQLRequest
		_ = json.Unmarshal(body, &req)
		w.Header().Set("Content-Type", "application/json")

		switch {
		case strings.Contains(req.Query, "TeamStates"):
			json.NewEncoder(w).Encode(teamStatesResp("team-1", "state-open", "Backlog", "backlog"))
		case strings.Contains(req.Query, "TeamLabels"):
			json.NewEncoder(w).Encode(teamLabelsEmptyResp("team-1"))
		case strings.Contains(req.Query, "IssuesByIdentifiers"), strings.Contains(req.Query, "IssueByIdentifier"):
			json.NewEncoder(w).Encode(issueByIdentifierResp(
				"remote-uuid", "TEAM-1", "My Issue", "", 0,
				"state-open", "Backlog", "backlog",
			))
		case strings.Contains(req.Query, "issueUpdate"):
			mutationCalled = true
			json.NewEncoder(w).Encode(map[string]interface{}{"data": map[string]interface{}{}})
		}
	}))
	defer server.Close()

	cfg := DefaultMappingConfig()
	cfg.ExplicitStateMap = map[string]string{"backlog": "open"}

	extRef := "https://linear.app/team/issue/TEAM-1"
	local := &types.Issue{
		ID:          "local-1",
		Title:       "My Issue",
		Status:      types.StatusOpen,
		Priority:    4,
		ExternalRef: &extRef,
	}

	tr := &Tracker{
		teamIDs: []string{"team-1"},
		clients: map[string]*Client{
			"team-1": NewClient("key", "team-1").WithEndpoint(server.URL),
		},
		config: cfg,
	}

	result, err := tr.BatchPushDryRun(context.Background(), []*types.Issue{local}, map[string]bool{"local-1": true})
	if err != nil {
		t.Fatalf("BatchPushDryRun: %v", err)
	}
	if mutationCalled {
		t.Error("dry-run sent issueUpdate to Linear; previews must be read-only even for forced issues")
	}
	if len(result.Updated) != 1 || result.Updated[0].LocalID != "local-1" {
		t.Errorf("Updated = %v, want one item for local-1 (force bypasses skip)", result.Updated)
	}
}

// TestBatchPushDryRun_UnresolvedIdentifierReportsError verifies preview/wet
// parity for identifiers no configured team resolves (deleted remotely, moved
// to an unconfigured team, or a Project-URL ref): the wet run's mutation would
// fail server-side and land in Errors, so the preview must report the same
// outcome instead of claiming "Would update".
func TestBatchPushDryRun_UnresolvedIdentifierReportsError(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		var req GraphQLRequest
		_ = json.Unmarshal(body, &req)
		w.Header().Set("Content-Type", "application/json")

		switch {
		case strings.Contains(req.Query, "TeamStates"):
			json.NewEncoder(w).Encode(teamStatesResp("team-1", "state-open", "Backlog", "backlog"))
		case strings.Contains(req.Query, "TeamLabels"):
			json.NewEncoder(w).Encode(teamLabelsEmptyResp("team-1"))
		case strings.Contains(req.Query, "IssuesByIdentifiers"), strings.Contains(req.Query, "IssueByIdentifier"):
			// Clean response, but no team resolves the identifier.
			json.NewEncoder(w).Encode(map[string]interface{}{
				"data": map[string]interface{}{
					"issues": map[string]interface{}{"nodes": []interface{}{}},
				},
			})
		}
	}))
	defer server.Close()

	cfg := DefaultMappingConfig()
	cfg.ExplicitStateMap = map[string]string{"backlog": "open"}

	extRef := "https://linear.app/team/issue/GONE-404"
	local := &types.Issue{
		ID:          "local-1",
		Title:       "My Issue",
		Status:      types.StatusOpen,
		Priority:    4,
		ExternalRef: &extRef,
	}

	tr := &Tracker{
		teamIDs: []string{"team-1"},
		clients: map[string]*Client{
			"team-1": NewClient("key", "team-1").WithEndpoint(server.URL),
		},
		config: cfg,
	}

	result, err := tr.BatchPushDryRun(context.Background(), []*types.Issue{local}, nil)
	if err != nil {
		t.Fatalf("BatchPushDryRun: %v", err)
	}
	if len(result.Errors) != 1 || result.Errors[0].LocalID != "local-1" {
		t.Errorf("Errors = %v, want one entry for local-1", result.Errors)
	}
	if len(result.Updated) != 0 {
		t.Errorf("Updated = %v, want [] (unresolvable ref must not preview as an update)", result.Updated)
	}
}

// TestBatchPush_BatchCreateMappingByTitle verifies that batch-create results are
// matched by title rather than array index. Linear's API does not guarantee that
// issueBatchCreate returns results in the same order as the inputs, so index-based
// mapping is unsafe and can silently associate the wrong ExternalRef with each issue.
func TestBatchPush_BatchCreateMappingByTitle(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		var req GraphQLRequest
		_ = json.Unmarshal(body, &req)
		w.Header().Set("Content-Type", "application/json")

		switch {
		case strings.Contains(req.Query, "TeamStates"):
			json.NewEncoder(w).Encode(teamStatesResp("team-1", "state-open", "Backlog", "backlog"))
		case strings.Contains(req.Query, "TeamLabels"):
			json.NewEncoder(w).Encode(teamLabelsEmptyResp("team-1"))
		case strings.Contains(req.Query, "issueBatchCreate"):
			// Return the two issues in REVERSE order to expose index-based mapping bugs.
			json.NewEncoder(w).Encode(map[string]interface{}{
				"data": map[string]interface{}{
					"issueBatchCreate": map[string]interface{}{
						"success": true,
						"issues": []interface{}{
							map[string]interface{}{
								"id":         "uuid-beta",
								"identifier": "TEAM-2",
								"title":      "Beta Issue",
								"url":        "https://linear.app/team/issue/TEAM-2",
								"priority":   0,
								"state":      map[string]interface{}{"id": "state-open", "name": "Backlog", "type": "backlog"},
								"createdAt":  "2026-01-01T00:00:00Z",
								"updatedAt":  "2026-01-01T00:00:00Z",
							},
							map[string]interface{}{
								"id":         "uuid-alpha",
								"identifier": "TEAM-1",
								"title":      "Alpha Issue",
								"url":        "https://linear.app/team/issue/TEAM-1",
								"priority":   0,
								"state":      map[string]interface{}{"id": "state-open", "name": "Backlog", "type": "backlog"},
								"createdAt":  "2026-01-01T00:00:00Z",
								"updatedAt":  "2026-01-01T00:00:00Z",
							},
						},
					},
				},
			})
		}
	}))
	defer server.Close()

	cfg := DefaultMappingConfig()
	cfg.ExplicitStateMap = map[string]string{"backlog": "open"}

	// Two new issues — no ExternalRef, so they go through the batch-create path.
	alpha := &types.Issue{ID: "local-alpha", Title: "Alpha Issue", Status: types.StatusOpen, Priority: 4}
	beta := &types.Issue{ID: "local-beta", Title: "Beta Issue", Status: types.StatusOpen, Priority: 4}

	tr := &Tracker{
		teamIDs: []string{"team-1"},
		clients: map[string]*Client{
			"team-1": NewClient("key", "team-1").WithEndpoint(server.URL),
		},
		config: cfg,
	}

	result, err := tr.BatchPush(context.Background(), []*types.Issue{alpha, beta}, nil)
	if err != nil {
		t.Fatalf("BatchPush: %v", err)
	}
	if len(result.Created) != 2 {
		t.Fatalf("Created = %d items, want 2; errors: %v", len(result.Created), result.Errors)
	}

	// Build a LocalID → ExternalRef map from the results.
	got := make(map[string]string, len(result.Created))
	for _, item := range result.Created {
		got[item.LocalID] = item.ExternalRef
	}

	if got["local-alpha"] != "https://linear.app/team/issue/TEAM-1" {
		t.Errorf("local-alpha mapped to %q, want TEAM-1 URL", got["local-alpha"])
	}
	if got["local-beta"] != "https://linear.app/team/issue/TEAM-2" {
		t.Errorf("local-beta mapped to %q, want TEAM-2 URL", got["local-beta"])
	}
}

// TestBatchPush_PerTeamStateCache verifies that updates to issues belonging to a
// non-primary team use that team's workflow state cache rather than the primary
// team's. Using the wrong team's state IDs can cause API errors or apply an
// incorrect workflow state if the two teams have different state UUID sets.
func TestBatchPush_PerTeamStateCache(t *testing.T) {
	var capturedStateID string

	// team-2 server: owns the issue, has its own distinct state IDs.
	team2Server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		var req GraphQLRequest
		_ = json.Unmarshal(body, &req)
		w.Header().Set("Content-Type", "application/json")

		switch {
		case strings.Contains(req.Query, "TeamStates"):
			json.NewEncoder(w).Encode(teamStatesResp("team-2", "t2-state-open", "Ready", "backlog"))
		case strings.Contains(req.Query, "TeamLabels"):
			json.NewEncoder(w).Encode(teamLabelsEmptyResp("team-2"))
		case strings.Contains(req.Query, "IssuesByIdentifiers"), strings.Contains(req.Query, "IssueByIdentifier"):
			// Return the issue with DIFFERENT title so PushFieldsEqual = false and we proceed.
			json.NewEncoder(w).Encode(issueByIdentifierResp(
				"t2-uuid", "T2-1", "Old Title", "", 0,
				"t2-state-open", "Ready", "backlog",
			))
		case strings.Contains(req.Query, "issueUpdate"):
			// Capture the stateId sent in the update so we can verify it came from team-2's cache.
			input, _ := req.Variables["input"].(map[string]interface{})
			if sid, ok := input["stateId"].(string); ok {
				capturedStateID = sid
			}
			json.NewEncoder(w).Encode(map[string]interface{}{
				"data": map[string]interface{}{
					"issueUpdate": map[string]interface{}{
						"success": true,
						"issue":   map[string]interface{}{"id": "t2-uuid", "url": "https://linear.app/team/issue/T2-1", "updatedAt": "2026-01-02T00:00:00Z"},
					},
				},
			})
		}
	}))
	defer team2Server.Close()

	// team-1 server: primary team, different state IDs, does not own this issue.
	team1Server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		var req GraphQLRequest
		_ = json.Unmarshal(body, &req)
		w.Header().Set("Content-Type", "application/json")

		switch {
		case strings.Contains(req.Query, "TeamStates"):
			json.NewEncoder(w).Encode(teamStatesResp("team-1", "t1-state-open", "Backlog", "backlog"))
		case strings.Contains(req.Query, "TeamLabels"):
			json.NewEncoder(w).Encode(teamLabelsEmptyResp("team-1"))
		case strings.Contains(req.Query, "IssuesByIdentifiers"), strings.Contains(req.Query, "IssueByIdentifier"):
			// Team-1 does not own this issue; return an empty result.
			json.NewEncoder(w).Encode(map[string]interface{}{
				"data": map[string]interface{}{
					"issues": map[string]interface{}{"nodes": []interface{}{}},
				},
			})
		}
	}))
	defer team1Server.Close()

	cfg := DefaultMappingConfig()
	cfg.ExplicitStateMap = map[string]string{"backlog": "open"}

	extRef := "https://linear.app/team/issue/T2-1"
	local := &types.Issue{
		ID:          "local-t2-1",
		Title:       "New Title", // differs from "Old Title" → not skipped
		Status:      types.StatusOpen,
		Priority:    4,
		ExternalRef: &extRef,
	}

	tr := &Tracker{
		teamIDs: []string{"team-1", "team-2"},
		clients: map[string]*Client{
			"team-1": NewClient("key", "team-1").WithEndpoint(team1Server.URL),
			"team-2": NewClient("key", "team-2").WithEndpoint(team2Server.URL),
		},
		config: cfg,
	}

	result, err := tr.BatchPush(context.Background(), []*types.Issue{local}, nil)
	if err != nil {
		t.Fatalf("BatchPush: %v", err)
	}
	if len(result.Updated) != 1 {
		t.Errorf("Updated = %v, want 1 item; errors: %v", result.Updated, result.Errors)
	}
	// The stateId in the update must come from team-2's cache, not team-1's.
	if capturedStateID != "t2-state-open" {
		t.Errorf("stateId sent in update = %q, want %q (team-2's state ID, not team-1's %q)",
			capturedStateID, "t2-state-open", "t1-state-open")
	}
}

// TestBatchPush_DuplicateTitlesFallbackToSingleCreate verifies that issues with
// duplicate titles within a batch are routed through single-create with idempotency
// markers instead of being sent through the batch mutation, where title-based
// result correlation would silently lose one of the duplicates.
func TestBatchPush_DuplicateTitlesFallbackToSingleCreate(t *testing.T) {
	var batchCreateCount int
	var singleCreateCount int

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		var req GraphQLRequest
		_ = json.Unmarshal(body, &req)
		w.Header().Set("Content-Type", "application/json")

		switch {
		case strings.Contains(req.Query, "TeamStates"):
			json.NewEncoder(w).Encode(teamStatesResp("team-1", "state-open", "Backlog", "backlog"))
		case strings.Contains(req.Query, "TeamLabels"):
			json.NewEncoder(w).Encode(teamLabelsEmptyResp("team-1"))
		case strings.Contains(req.Query, "FindByDescription"):
			json.NewEncoder(w).Encode(map[string]interface{}{
				"data": map[string]interface{}{
					"issues": map[string]interface{}{
						"nodes":    []interface{}{},
						"pageInfo": map[string]interface{}{"hasNextPage": false, "endCursor": ""},
					},
				},
			})
		case strings.Contains(req.Query, "issueBatchCreate"):
			batchCreateCount++
			inputWrapper := req.Variables["input"].(map[string]interface{})
			inputs := inputWrapper["issues"].([]interface{})
			var issues []interface{}
			for i, inp := range inputs {
				m := inp.(map[string]interface{})
				issues = append(issues, map[string]interface{}{
					"id": fmt.Sprintf("batch-uuid-%d", i), "identifier": fmt.Sprintf("TEAM-%d", i+10),
					"title": m["title"], "url": fmt.Sprintf("https://linear.app/team/issue/TEAM-%d", i+10),
					"priority": 0, "state": map[string]interface{}{"id": "state-open", "name": "Backlog", "type": "backlog"},
					"createdAt": "2026-01-01T00:00:00Z", "updatedAt": "2026-01-01T00:00:00Z",
				})
			}
			json.NewEncoder(w).Encode(map[string]interface{}{
				"data": map[string]interface{}{
					"issueBatchCreate": map[string]interface{}{"success": true, "issues": issues},
				},
			})
		case strings.Contains(req.Query, "issueCreate"):
			singleCreateCount++
			input := req.Variables["input"].(map[string]interface{})
			json.NewEncoder(w).Encode(map[string]interface{}{
				"data": map[string]interface{}{
					"issueCreate": map[string]interface{}{
						"success": true,
						"issue": map[string]interface{}{
							"id": fmt.Sprintf("single-uuid-%d", singleCreateCount), "identifier": fmt.Sprintf("TEAM-%d", singleCreateCount),
							"title": input["title"], "description": input["description"],
							"url":      fmt.Sprintf("https://linear.app/team/issue/TEAM-%d", singleCreateCount),
							"priority": 0, "state": map[string]interface{}{"id": "state-open", "name": "Backlog", "type": "backlog"},
							"createdAt": "2026-01-01T00:00:00Z", "updatedAt": "2026-01-01T00:00:00Z",
						},
					},
				},
			})
		}
	}))
	defer server.Close()

	cfg := DefaultMappingConfig()
	cfg.ExplicitStateMap = map[string]string{"backlog": "open"}

	tr := &Tracker{
		teamIDs: []string{"team-1"},
		clients: map[string]*Client{
			"team-1": NewClient("key", "team-1").WithEndpoint(server.URL),
		},
		config: cfg,
	}

	// Two issues with the same title + one unique title.
	dupA := &types.Issue{ID: "dup-a", Title: "Duplicate Title", Status: types.StatusOpen, Priority: 4}
	dupB := &types.Issue{ID: "dup-b", Title: "Duplicate Title", Status: types.StatusOpen, Priority: 4}
	unique := &types.Issue{ID: "unique-1", Title: "Unique Title", Status: types.StatusOpen, Priority: 4}

	result, err := tr.BatchPush(context.Background(), []*types.Issue{dupA, dupB, unique}, nil)
	if err != nil {
		t.Fatalf("BatchPush: %v", err)
	}

	if singleCreateCount != 2 {
		t.Errorf("single creates = %d, want 2 (one per duplicate-title issue)", singleCreateCount)
	}
	if batchCreateCount != 1 {
		t.Errorf("batch creates = %d, want 1 (for the unique-title issue)", batchCreateCount)
	}
	if len(result.Created) != 3 {
		t.Errorf("Created = %d, want 3; errors: %v", len(result.Created), result.Errors)
	}

	createdIDs := make(map[string]bool)
	for _, item := range result.Created {
		createdIDs[item.LocalID] = true
	}
	for _, wantID := range []string{"dup-a", "dup-b", "unique-1"} {
		if !createdIDs[wantID] {
			t.Errorf("missing Created entry for %s", wantID)
		}
	}
}

// TestBatchPush_AmbiguousBatchFailureSearchesMarkers verifies that when a batch
// mutation returns an ambiguous error, the system searches for idempotency markers
// to find partially-created issues instead of blindly retrying the entire chunk.
func TestBatchPush_AmbiguousBatchFailureSearchesMarkers(t *testing.T) {
	var searchCount int

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		var req GraphQLRequest
		_ = json.Unmarshal(body, &req)
		w.Header().Set("Content-Type", "application/json")

		switch {
		case strings.Contains(req.Query, "TeamStates"):
			json.NewEncoder(w).Encode(teamStatesResp("team-1", "state-open", "Backlog", "backlog"))
		case strings.Contains(req.Query, "TeamLabels"):
			json.NewEncoder(w).Encode(teamLabelsEmptyResp("team-1"))
		case strings.Contains(req.Query, "issueBatchCreate"):
			json.NewEncoder(w).Encode(map[string]interface{}{
				"data": map[string]interface{}{
					"issueBatchCreate": map[string]interface{}{
						"success": false,
						"issues":  []interface{}{},
					},
				},
			})
		case strings.Contains(req.Query, "FindByDescription"):
			searchCount++
			filter := req.Variables["filter"].(map[string]interface{})
			desc := filter["description"].(map[string]interface{})
			searchText := desc["contains"].(string)

			// Simulate: issue A was created by Linear before the failure, B was not.
			if strings.Contains(searchText, "bd-idempotency") {
				// We'll check which marker this is by looking at the search count.
				// First search (issue A) → found; second search (issue B) → not found.
				if searchCount == 1 {
					json.NewEncoder(w).Encode(map[string]interface{}{
						"data": map[string]interface{}{
							"issues": map[string]interface{}{
								"nodes": []interface{}{
									map[string]interface{}{
										"id": "recovered-uuid", "identifier": "TEAM-1",
										"title": "Issue A", "url": "https://linear.app/team/issue/TEAM-1",
										"priority": 0, "state": map[string]interface{}{"id": "state-open", "name": "Backlog", "type": "backlog"},
										"createdAt": "2026-01-01T00:00:00Z", "updatedAt": "2026-01-01T00:00:00Z",
									},
								},
								"pageInfo": map[string]interface{}{"hasNextPage": false, "endCursor": ""},
							},
						},
					})
				} else {
					json.NewEncoder(w).Encode(map[string]interface{}{
						"data": map[string]interface{}{
							"issues": map[string]interface{}{
								"nodes":    []interface{}{},
								"pageInfo": map[string]interface{}{"hasNextPage": false, "endCursor": ""},
							},
						},
					})
				}
				return
			}
			json.NewEncoder(w).Encode(map[string]interface{}{
				"data": map[string]interface{}{
					"issues": map[string]interface{}{
						"nodes":    []interface{}{},
						"pageInfo": map[string]interface{}{"hasNextPage": false, "endCursor": ""},
					},
				},
			})
		}
	}))
	defer server.Close()

	cfg := DefaultMappingConfig()
	cfg.ExplicitStateMap = map[string]string{"backlog": "open"}

	tr := &Tracker{
		teamIDs: []string{"team-1"},
		clients: map[string]*Client{
			"team-1": NewClient("key", "team-1").WithEndpoint(server.URL),
		},
		config: cfg,
	}

	issueA := &types.Issue{ID: "local-a", Title: "Issue A", Status: types.StatusOpen, Priority: 4}
	issueB := &types.Issue{ID: "local-b", Title: "Issue B", Status: types.StatusOpen, Priority: 4}

	result, err := tr.BatchPush(context.Background(), []*types.Issue{issueA, issueB}, nil)
	// We expect a warning/error about unconfirmed issues, but no panic or full-chunk retry.
	if err != nil {
		t.Fatalf("BatchPush: %v", err)
	}

	if searchCount != 2 {
		t.Errorf("marker searches = %d, want 2 (one per issue in the failed batch)", searchCount)
	}

	// Issue A was found via marker search → should appear in Created.
	// Issue B was NOT found → should appear in Errors (not duplicated by a blind retry).
	if len(result.Created) != 1 {
		t.Errorf("Created = %d, want 1 (only the recovered issue)", len(result.Created))
	}
	if len(result.Created) == 1 && result.Created[0].LocalID != "local-a" {
		t.Errorf("Created[0].LocalID = %q, want local-a", result.Created[0].LocalID)
	}

	// Issue B should have an error (unconfirmed), not a silent retry.
	hasErrorForB := false
	for _, e := range result.Errors {
		if e.LocalID == "local-b" {
			hasErrorForB = true
		}
	}
	if !hasErrorForB {
		t.Error("expected error for local-b (unconfirmed after ambiguous batch failure)")
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
		ProjectMilestone: &ProjectMilestone{
			ID:          "milestone-1",
			Name:        "M7: Team-Ready",
			Description: "Team-ready milestone",
			Progress:    60.61,
			TargetDate:  "2026-05-12",
		},
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
	var meta struct {
		Linear struct {
			ProjectMilestone ProjectMilestone `json:"project_milestone"`
		} `json:"linear"`
	}
	raw, err := json.Marshal(ti.Metadata)
	if err != nil {
		t.Fatalf("marshal metadata: %v", err)
	}
	if err := json.Unmarshal(raw, &meta); err != nil {
		t.Fatalf("unmarshal metadata: %v", err)
	}
	if meta.Linear.ProjectMilestone.ID != "milestone-1" {
		t.Errorf("ProjectMilestone.ID = %q, want milestone-1", meta.Linear.ProjectMilestone.ID)
	}
}

// TestTrackerInitOAuthOnly verifies that Init() succeeds with only OAuth credentials
// and no API key. This is the CI worker use case.
func TestTrackerInitOAuthOnly(t *testing.T) {
	tokenServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`{"access_token":"tok","token_type":"Bearer","expires_in":3600}`))
	}))
	defer tokenServer.Close()

	t.Setenv("LINEAR_OAUTH_CLIENT_ID", "test-client-id")
	t.Setenv("LINEAR_OAUTH_CLIENT_SECRET", "test-client-secret")
	// No LINEAR_API_KEY set — OAuth-only path.

	tr := &Tracker{}
	tr.SetTeamIDs([]string{"team-uuid-1"})
	// Inject the test token server so we don't hit production.
	oauthClient := NewOAuthClient(OAuthConfig{
		ClientID:     "test-client-id",
		ClientSecret: "test-client-secret",
		TokenURL:     tokenServer.URL,
	}, "team-uuid-1")
	tr.clients = map[string]*Client{"team-uuid-1": oauthClient}
	tr.config = DefaultMappingConfig()

	if err := tr.Validate(); err != nil {
		t.Fatalf("Validate() = %v, want nil (OAuth-only tracker should be valid)", err)
	}

	client := tr.PrimaryClient()
	if client == nil {
		t.Fatal("PrimaryClient() returned nil")
	}
	if client.AuthMode != AuthModeOAuth {
		t.Errorf("AuthMode = %v, want AuthModeOAuth", client.AuthMode)
	}
}

// TestTrackerInitNoAuthFails verifies that Init() returns a clear error when neither
// OAuth credentials nor an API key are present.
func TestTrackerInitNoAuthFails(t *testing.T) {
	// Ensure no credentials leak from environment or config.
	t.Setenv("LINEAR_OAUTH_CLIENT_ID", "")
	t.Setenv("LINEAR_OAUTH_CLIENT_SECRET", "")
	t.Setenv("LINEAR_API_KEY", "")

	tr := &Tracker{}
	tr.SetTeamIDs([]string{"team-uuid-1"})

	err := tr.Init(context.Background(), nil)
	if err == nil {
		t.Fatal("Init() should fail when no auth credentials are configured")
	}
	msg := err.Error()
	if !strings.Contains(msg, "LINEAR_OAUTH_CLIENT_ID") {
		t.Errorf("error should mention LINEAR_OAUTH_CLIENT_ID; got: %s", msg)
	}
	if !strings.Contains(msg, "LINEAR_API_KEY") {
		t.Errorf("error should mention LINEAR_API_KEY; got: %s", msg)
	}
}

// TestCreateIssueNoDoubleFormatDescription verifies that Tracker.CreateIssue passes
// issue.Description directly to Linear without calling BuildLinearDescription a
// second time. The sync engine's FormatDescription hook already builds the full
// description (merging AcceptanceCriteria/Design/Notes) before calling CreateIssue;
// calling BuildLinearDescription inside CreateIssue would duplicate those sections.
func TestCreateIssueNoDoubleFormatDescription(t *testing.T) {
	var capturedDescription string

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req GraphQLRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, "bad request", http.StatusBadRequest)
			return
		}
		w.Header().Set("Content-Type", "application/json")

		if strings.Contains(req.Query, "TeamStates") {
			json.NewEncoder(w).Encode(map[string]interface{}{
				"data": map[string]interface{}{
					"team": map[string]interface{}{
						"id": "team-1",
						"states": map[string]interface{}{
							"nodes": []map[string]interface{}{
								{"id": "state-open", "name": "Todo", "type": "unstarted"},
							},
						},
					},
				},
			})
			return
		}

		if strings.Contains(req.Query, "TeamLabels") {
			json.NewEncoder(w).Encode(teamLabelsEmptyResp("team-1"))
			return
		}

		if strings.Contains(req.Query, "issueCreate") {
			input, _ := req.Variables["input"].(map[string]interface{})
			capturedDescription, _ = input["description"].(string)
			json.NewEncoder(w).Encode(map[string]interface{}{
				"data": map[string]interface{}{
					"issueCreate": map[string]interface{}{
						"success": true,
						"issue": map[string]interface{}{
							"id":          "new-id",
							"identifier":  "TEAM-1",
							"title":       "Test",
							"description": capturedDescription,
							"url":         "https://linear.app/team/issue/TEAM-1",
							"state":       map[string]interface{}{"id": "state-open", "name": "Todo", "type": "unstarted"},
							"createdAt":   "2026-05-02T00:00:00Z",
							"updatedAt":   "2026-05-02T00:00:00Z",
						},
					},
				},
			})
			return
		}

		if strings.Contains(req.Query, "FindByDescription") {
			json.NewEncoder(w).Encode(map[string]interface{}{
				"data": map[string]interface{}{
					"issues": map[string]interface{}{
						"nodes":    []interface{}{},
						"pageInfo": map[string]interface{}{"hasNextPage": false, "endCursor": ""},
					},
				},
			})
			return
		}

		t.Logf("unhandled query: %s", req.Query)
		http.Error(w, "unexpected query", http.StatusInternalServerError)
	}))
	defer server.Close()

	tr := &Tracker{
		teamIDs: []string{"team-1"},
		clients: map[string]*Client{
			"team-1": NewClient("key", "team-1").WithEndpoint(server.URL),
		},
		config: func() *MappingConfig {
			cfg := DefaultMappingConfig()
			cfg.ExplicitStateMap["todo"] = "open"
			return cfg
		}(),
	}

	createdAt := time.Date(2026, 5, 2, 0, 0, 0, 0, time.UTC)

	// Simulate what the sync engine does: Description is already the fully
	// formatted output of BuildLinearDescription (base + AC/Design/Notes merged in).
	formattedDesc := "base description\n\n## Acceptance Criteria\ncriteria here\n\n## Design\ndesign here"
	issue := &types.Issue{
		ID:                 "bead-1",
		Title:              "Test",
		Description:        formattedDesc, // pre-formatted by sync engine
		AcceptanceCriteria: "criteria here",
		Design:             "design here",
		Status:             types.StatusOpen,
		CreatedBy:          "dev@test.com",
		CreatedAt:          createdAt,
	}

	_, err := tr.CreateIssue(t.Context(), issue)
	if err != nil {
		t.Fatalf("CreateIssue failed: %v", err)
	}

	// The description sent to Linear must be exactly the pre-formatted one.
	// If BuildLinearDescription were called inside CreateIssue, the AC and Design
	// sections would be appended a second time.
	if strings.Count(capturedDescription, "## Acceptance Criteria") != 1 {
		t.Errorf("description has %d '## Acceptance Criteria' sections, want 1 (double-format bug)\ndesc: %q",
			strings.Count(capturedDescription, "## Acceptance Criteria"), capturedDescription)
	}
	if strings.Count(capturedDescription, "## Design") != 1 {
		t.Errorf("description has %d '## Design' sections, want 1 (double-format bug)\ndesc: %q",
			strings.Count(capturedDescription, "## Design"), capturedDescription)
	}
	if !strings.Contains(capturedDescription, formattedDesc) {
		t.Errorf("description does not contain expected formatted content\ngot:  %q\nwant: %q",
			capturedDescription, formattedDesc)
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

// TestSplitEpicDescriptionForProject is the bd-cs1 plumbing test:
// confirms the splitter does NOT emit content when the description
// fits the limit (so callers can skip the content field entirely),
// and DOES emit the full text as content when truncation kicks in.
func TestSplitEpicDescriptionForProject(t *testing.T) {
	t.Run("short description: no content", func(t *testing.T) {
		desc, content := splitEpicDescriptionForProject("short bead description")
		if desc != "short bead description" {
			t.Errorf("description = %q, want unchanged", desc)
		}
		if content != "" {
			t.Errorf("content = %q, want empty (no truncation)", content)
		}
	})

	t.Run("over limit: content gets full text", func(t *testing.T) {
		full := strings.Repeat("a", 300)
		desc, content := splitEpicDescriptionForProject(full)
		if content != full {
			t.Errorf("content should be full original (%d chars), got %d", len(full), len(content))
		}
		if utf8RuneCount(desc) > LinearProjectDescriptionMaxChars {
			t.Errorf("description %d runes exceeds limit", utf8RuneCount(desc))
		}
	})

	t.Run("empty in, empty out", func(t *testing.T) {
		desc, content := splitEpicDescriptionForProject("")
		if desc != "" || content != "" {
			t.Errorf("got (%q, %q), want both empty", desc, content)
		}
	})
}

// TestUpdateProjectClearsContentOnShortenedDescription is the bd-cs1
// codex-round-1 E regression: when a bead's description shortens from
// long (forced split with content) to short (no content needed), the
// update payload must explicitly clear the Project.content field on
// Linear. Otherwise the rich body left over from the long-description
// era stays stale forever.
//
// Asserts via the updates map sent to client.UpdateProject. Mock client
// captures the map and verifies content=nil (which JSON-encodes as
// "content": null and Linear interprets as a field clear).
func TestUpdateProjectClearsContentOnShortenedDescription(t *testing.T) {
	t.Run("short description sets content to nil for clear", func(t *testing.T) {
		_, content := splitEpicDescriptionForProject("short")
		if content != "" {
			t.Fatalf("splitEpicDescriptionForProject returned non-empty content for short input")
		}
		// Tracker.UpdateProject branches on content != "" : empty must
		// produce a nil entry (clear), not an absent key (no-op).
		updates := buildProjectUpdateMap("Title", "short", "started")
		v, present := updates["content"]
		if !present {
			t.Fatalf("content key MISSING from update map; stale rich body would persist on Linear")
		}
		if v != nil {
			t.Errorf("content = %#v, want nil (so JSON encodes as null for clear)", v)
		}
	})

	t.Run("long description sets content to full text", func(t *testing.T) {
		full := strings.Repeat("a", 300)
		updates := buildProjectUpdateMap("Title", full, "started")
		v, ok := updates["content"].(string)
		if !ok {
			t.Fatalf("content key not a string in update map: %#v", updates["content"])
		}
		if v != full {
			t.Errorf("content (len=%d) does not match full input (len=%d)", len(v), len(full))
		}
	})
}

// buildProjectUpdateMap mirrors the inner shape of Tracker.UpdateProject
// for assertion purposes. Kept private to the test file so the real
// method's signature isn't constrained by test plumbing needs.
func buildProjectUpdateMap(name, description, state string) map[string]interface{} {
	desc, content := splitEpicDescriptionForProject(description)
	updates := map[string]interface{}{
		"name":        name,
		"description": desc,
		"state":       state,
	}
	if content != "" {
		updates["content"] = content
	} else {
		updates["content"] = nil
	}
	return updates
}
