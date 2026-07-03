package linear

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// TestReconcileProjectMembership_HappyPath verifies the bd-1ay core
// property: a non-epic descendant needing projectId set produces one
// IssueUpdate carrying the Project UUID.
func TestReconcileProjectMembership_HappyPath(t *testing.T) {
	mock := newLinearMock(t)
	mock.issues["TEAM-1"] = &Issue{ID: "uuid-child", Identifier: "TEAM-1"}
	server := httptest.NewServer(mock)
	defer server.Close()

	tr := newTestLinearTracker(t, server.URL)
	stats, err := tr.ReconcileProjectMembership(context.Background(), []ProjectMembershipLink{
		{IssueIdentifier: "TEAM-1", ProjectID: "uuid-project-A"},
	}, false)
	if err != nil {
		t.Fatalf("ReconcileProjectMembership err: %v", err)
	}
	if stats.Updated != 1 || stats.Skipped != 0 || len(stats.Errors) != 0 {
		t.Fatalf("stats = %+v, want Updated=1 Skipped=0", stats)
	}
	if len(mock.updates) != 1 {
		t.Fatalf("expected 1 issueUpdate call, got %d", len(mock.updates))
	}
	input, ok := mock.updates["uuid-child"]
	if !ok {
		t.Fatalf("expected update on uuid-child, got: %v", mock.updates)
	}
	if !strings.Contains(string(input), `"projectId":"uuid-project-A"`) {
		t.Errorf("update input missing projectId UUID: %s", input)
	}
}

// TestReconcileProjectMembership_IdempotentSkip verifies that when the
// issue's remote projectId already matches, no IssueUpdate is issued.
func TestReconcileProjectMembership_IdempotentSkip(t *testing.T) {
	mock := newLinearMock(t)
	mock.issues["TEAM-1"] = &Issue{ID: "uuid-child", Identifier: "TEAM-1",
		Project: &Project{ID: "uuid-project-A"}}
	server := httptest.NewServer(mock)
	defer server.Close()

	tr := newTestLinearTracker(t, server.URL)
	stats, err := tr.ReconcileProjectMembership(context.Background(), []ProjectMembershipLink{
		{IssueIdentifier: "TEAM-1", ProjectID: "uuid-project-A"},
	}, false)
	if err != nil {
		t.Fatalf("ReconcileProjectMembership err: %v", err)
	}
	if stats.Updated != 0 || stats.Skipped != 1 {
		t.Fatalf("stats = %+v, want Updated=0 Skipped=1", stats)
	}
	if len(mock.updates) != 0 {
		t.Errorf("expected no updates (idempotent), got %d", len(mock.updates))
	}
}

// TestReconcileProjectMembership_RewiresWrongProject verifies that an
// issue currently pointing at a different Project gets rewired (e.g.
// epic was migrated and descendants need re-homing).
func TestReconcileProjectMembership_RewiresWrongProject(t *testing.T) {
	mock := newLinearMock(t)
	mock.issues["TEAM-1"] = &Issue{ID: "uuid-child", Identifier: "TEAM-1",
		Project: &Project{ID: "uuid-OLD-project"}}
	server := httptest.NewServer(mock)
	defer server.Close()

	tr := newTestLinearTracker(t, server.URL)
	stats, err := tr.ReconcileProjectMembership(context.Background(), []ProjectMembershipLink{
		{IssueIdentifier: "TEAM-1", ProjectID: "uuid-NEW-project"},
	}, false)
	if err != nil {
		t.Fatalf("ReconcileProjectMembership err: %v", err)
	}
	if stats.Updated != 1 {
		t.Errorf("Updated = %d, want 1 (wrong project should be rewired)", stats.Updated)
	}
	input := mock.updates["uuid-child"]
	if !strings.Contains(string(input), `"projectId":"uuid-NEW-project"`) {
		t.Errorf("expected rewire to uuid-NEW-project, got: %s", input)
	}
}

// TestReconcileProjectMembership_DryRunNoMutations is the bd-5zh-style
// preview: dry-run produces accurate WouldUpdate + Mutations but ZERO
// IssueUpdate calls.
func TestReconcileProjectMembership_DryRunNoMutations(t *testing.T) {
	mock := newLinearMock(t)
	mock.issues["TEAM-1"] = &Issue{ID: "uuid-c1", Identifier: "TEAM-1"}
	mock.issues["TEAM-2"] = &Issue{ID: "uuid-c2", Identifier: "TEAM-2",
		Project: &Project{ID: "uuid-project-A"}} // already correct
	mock.issues["TEAM-3"] = &Issue{ID: "uuid-c3", Identifier: "TEAM-3"}
	server := httptest.NewServer(mock)
	defer server.Close()

	tr := newTestLinearTracker(t, server.URL)
	stats, err := tr.ReconcileProjectMembership(context.Background(), []ProjectMembershipLink{
		{IssueIdentifier: "TEAM-1", ProjectID: "uuid-project-A"},
		{IssueIdentifier: "TEAM-2", ProjectID: "uuid-project-A"},
		{IssueIdentifier: "TEAM-3", ProjectID: "uuid-project-A"},
	}, true)
	if err != nil {
		t.Fatalf("ReconcileProjectMembership err: %v", err)
	}
	if stats.Updated != 0 {
		t.Errorf("Updated = %d, want 0 in dry-run", stats.Updated)
	}
	if stats.WouldUpdate != 2 {
		t.Errorf("WouldUpdate = %d, want 2 (TEAM-1 + TEAM-3 need projectId)", stats.WouldUpdate)
	}
	if stats.Skipped != 1 {
		t.Errorf("Skipped = %d, want 1 (TEAM-2 already correct)", stats.Skipped)
	}
	if len(mock.updates) != 0 {
		t.Errorf("dry-run must not issue IssueUpdate; got %d", len(mock.updates))
	}
	if len(stats.Mutations) != 2 {
		t.Fatalf("Mutations = %v, want 2 entries", stats.Mutations)
	}
}

// TestReconcileProjectMembership_DryRunMatchesWetRun verifies the
// symmetry property: same input, same Mutations set, only the
// Updated/WouldUpdate counter differs. The bd-5zh contract that makes
// dry-run a trustworthy pre-flight.
func TestReconcileProjectMembership_DryRunMatchesWetRun(t *testing.T) {
	makeMock := func() *linearMockHandler {
		m := newLinearMock(t)
		m.issues["TEAM-1"] = &Issue{ID: "uuid-c1", Identifier: "TEAM-1"}
		m.issues["TEAM-2"] = &Issue{ID: "uuid-c2", Identifier: "TEAM-2"}
		return m
	}
	links := []ProjectMembershipLink{
		{IssueIdentifier: "TEAM-1", ProjectID: "uuid-project-A"},
		{IssueIdentifier: "TEAM-2", ProjectID: "uuid-project-A"},
	}

	dryMock := makeMock()
	drySrv := httptest.NewServer(dryMock)
	defer drySrv.Close()
	dryTr := newTestLinearTracker(t, drySrv.URL)
	dryStats, err := dryTr.ReconcileProjectMembership(context.Background(), links, true)
	if err != nil {
		t.Fatalf("dry: %v", err)
	}

	wetMock := makeMock()
	wetSrv := httptest.NewServer(wetMock)
	defer wetSrv.Close()
	wetTr := newTestLinearTracker(t, wetSrv.URL)
	wetStats, err := wetTr.ReconcileProjectMembership(context.Background(), links, false)
	if err != nil {
		t.Fatalf("wet: %v", err)
	}

	if dryStats.WouldUpdate != wetStats.Updated {
		t.Errorf("dry WouldUpdate=%d vs wet Updated=%d (must match)",
			dryStats.WouldUpdate, wetStats.Updated)
	}
	if len(dryStats.Mutations) != len(wetStats.Mutations) {
		t.Fatalf("Mutations length differs: dry=%d wet=%d",
			len(dryStats.Mutations), len(wetStats.Mutations))
	}
	if len(dryMock.updates) != 0 {
		t.Errorf("dry mock saw %d updates; expected 0", len(dryMock.updates))
	}
	if len(wetMock.updates) != 2 {
		t.Errorf("wet mock saw %d updates; expected 2", len(wetMock.updates))
	}
}

// TestReconcileProjectMembership_EmptyLinks short-circuits cleanly.
func TestReconcileProjectMembership_EmptyLinks(t *testing.T) {
	tr := &Tracker{}
	stats, err := tr.ReconcileProjectMembership(context.Background(), nil, false)
	if err != nil {
		t.Fatalf("ReconcileProjectMembership err: %v", err)
	}
	if stats.Updated != 0 || stats.Skipped != 0 {
		t.Errorf("stats = %+v, want zero", stats)
	}
}

// TestReconcileProjectMembership_BlankFieldsSkipped — defensive guard
// against malformed link entries.
func TestReconcileProjectMembership_BlankFieldsSkipped(t *testing.T) {
	mock := newLinearMock(t)
	server := httptest.NewServer(mock)
	defer server.Close()

	tr := newTestLinearTracker(t, server.URL)
	stats, err := tr.ReconcileProjectMembership(context.Background(), []ProjectMembershipLink{
		{IssueIdentifier: "", ProjectID: "uuid-project-A"},
		{IssueIdentifier: "TEAM-1", ProjectID: ""},
	}, false)
	if err != nil {
		t.Fatalf("ReconcileProjectMembership err: %v", err)
	}
	if stats.Updated != 0 || stats.WouldUpdate != 0 {
		t.Errorf("expected no work, got %+v", stats)
	}
	if len(mock.updates) != 0 || len(mock.fetches) != 0 {
		t.Errorf("expected no API calls for blank links, fetches=%v updates=%v",
			mock.fetches, mock.updates)
	}
}

// TestReconcileProjectMembership_WetRunFailureDoesNotRecordMutation
// locks the bd-cs1-round-1 contract for this pass: Mutations only
// reflects state actually propagated to Linear, not attempted writes.
func TestReconcileProjectMembership_WetRunFailureDoesNotRecordMutation(t *testing.T) {
	// Custom handler: succeeds on fetch, returns success=false on update.
	server := httptest.NewServer(failingUpdateMockHandler(t))
	defer server.Close()

	tr := newTestLinearTracker(t, server.URL)
	stats, err := tr.ReconcileProjectMembership(context.Background(), []ProjectMembershipLink{
		{IssueIdentifier: "TEAM-1", ProjectID: "uuid-project-A"},
	}, false)
	if err != nil {
		t.Fatalf("ReconcileProjectMembership err: %v", err)
	}
	if stats.Updated != 0 {
		t.Errorf("Updated = %d, want 0 (API call failed)", stats.Updated)
	}
	if len(stats.Mutations) != 0 {
		t.Errorf("Mutations = %v, want empty (no successful mutation)", stats.Mutations)
	}
	if len(stats.Errors) != 1 {
		t.Errorf("Errors = %v, want 1 entry", stats.Errors)
	}
}

// failingUpdateMockHandler returns an http.HandlerFunc that:
//   - Resolves IssueByIdentifier requests using the same suffix-scan
//     pattern as linearMockHandler, but fabricates the Issue with a
//     deterministic UUID derived from the number filter (so tests
//     don't need to seed an issues map).
//   - Returns success=false on issueUpdate mutations.
//
// Used to drive the wet-run-failure-doesn't-record-mutation contract.
func failingUpdateMockHandler(t *testing.T) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		var req GraphQLRequest
		if err := json.Unmarshal(body, &req); err != nil {
			t.Fatalf("bad request: %v", err)
		}
		w.Header().Set("Content-Type", "application/json")

		switch {
		case strings.Contains(req.Query, "IssuesByIdentifiers"):
			filter, _ := req.Variables["filter"].(map[string]interface{})
			number, _ := filter["number"].(map[string]interface{})
			in, _ := number["in"].([]interface{})
			nodes := []interface{}{}
			for _, nRaw := range in {
				n, _ := nRaw.(float64)
				nodes = append(nodes, map[string]interface{}{
					"id": "uuid-" + itoa(int(n)), "identifier": "TEAM-" + itoa(int(n)),
					"createdAt": "2026-06-08T00:00:00Z", "updatedAt": "2026-06-08T00:00:00Z",
				})
			}
			_ = json.NewEncoder(w).Encode(map[string]interface{}{
				"data": map[string]interface{}{
					"issues": map[string]interface{}{
						"nodes":    nodes,
						"pageInfo": map[string]interface{}{"hasNextPage": false, "endCursor": ""},
					},
				},
			})
		case strings.Contains(req.Query, "IssueByIdentifier"):
			filter, _ := req.Variables["filter"].(map[string]interface{})
			number, _ := filter["number"].(map[string]interface{})
			eq, _ := number["eq"].(float64)
			id := "uuid-" + itoa(int(eq))
			ident := "TEAM-" + itoa(int(eq))
			_ = json.NewEncoder(w).Encode(map[string]interface{}{
				"data": map[string]interface{}{
					"issues": map[string]interface{}{
						"nodes": []interface{}{
							map[string]interface{}{"id": id, "identifier": ident,
								"createdAt": "2026-06-08T00:00:00Z", "updatedAt": "2026-06-08T00:00:00Z"},
						},
					},
				},
			})
		case strings.Contains(req.Query, "issueUpdate"):
			_ = json.NewEncoder(w).Encode(map[string]interface{}{
				"data": map[string]interface{}{
					"issueUpdate": map[string]interface{}{"success": false, "issue": nil},
				},
			})
		default:
			_ = json.NewEncoder(w).Encode(map[string]interface{}{"data": map[string]interface{}{}})
		}
	}
}
