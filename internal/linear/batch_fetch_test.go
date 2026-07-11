package linear

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"

	"github.com/steveyegge/beads/internal/types"
)

// batchFetchMock serves IssuesByIdentifiers queries from a fixed issue set,
// honoring the number-in filter and first/after pagination, and records how
// many requests it saw and the size of each number-in list.
type batchFetchMock struct {
	issues       []*Issue // sorted by number
	requests     int
	chunkSizes   []int
	otherQueries int
}

func (m *batchFetchMock) handler(t *testing.T) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		var req GraphQLRequest
		if err := json.Unmarshal(body, &req); err != nil {
			t.Fatalf("mock: bad request: %v", err)
		}
		w.Header().Set("Content-Type", "application/json")

		if !strings.Contains(req.Query, "IssuesByIdentifiers") {
			m.otherQueries++
			_ = json.NewEncoder(w).Encode(map[string]interface{}{"data": map[string]interface{}{}})
			return
		}
		m.requests++

		wanted := map[int]bool{}
		if filter, ok := req.Variables["filter"].(map[string]interface{}); ok {
			if number, ok := filter["number"].(map[string]interface{}); ok {
				if in, ok := number["in"].([]interface{}); ok {
					m.chunkSizes = append(m.chunkSizes, len(in))
					for _, v := range in {
						if f, ok := v.(float64); ok {
							wanted[int(f)] = true
						}
					}
				}
			}
		}

		var matches []*Issue
		for _, issue := range m.issues {
			parts := strings.Split(issue.Identifier, "-")
			n, _ := strconv.Atoi(parts[len(parts)-1])
			if wanted[n] {
				matches = append(matches, issue)
			}
		}

		first := len(matches)
		if f, ok := req.Variables["first"].(float64); ok && int(f) > 0 {
			first = int(f)
		}
		start := 0
		if a, ok := req.Variables["after"].(string); ok && a != "" {
			start, _ = strconv.Atoi(a)
		}
		end := start + first
		if start > len(matches) {
			start = len(matches)
		}
		if end > len(matches) {
			end = len(matches)
		}

		_ = json.NewEncoder(w).Encode(map[string]interface{}{
			"data": map[string]interface{}{
				"issues": map[string]interface{}{
					"nodes":    matches[start:end],
					"pageInfo": map[string]interface{}{"hasNextPage": end < len(matches), "endCursor": strconv.Itoa(end)},
				},
			},
		})
	}
}

func TestFetchIssuesByIdentifiers_ChunksRequests(t *testing.T) {
	mock := &batchFetchMock{}
	count := MaxPageSize + 50 // forces two chunks
	for i := 1; i <= count; i++ {
		mock.issues = append(mock.issues, &Issue{
			ID:         fmt.Sprintf("uuid-%d", i),
			Identifier: fmt.Sprintf("TEAM-%d", i),
			Title:      fmt.Sprintf("Issue %d", i),
		})
	}
	server := httptest.NewServer(mock.handler(t))
	defer server.Close()

	client := NewClient("key", "team-1").WithEndpoint(server.URL)
	identifiers := make([]string, 0, count)
	for i := 1; i <= count; i++ {
		identifiers = append(identifiers, fmt.Sprintf("TEAM-%d", i))
	}

	got, err := client.FetchIssuesByIdentifiers(context.Background(), identifiers)
	if err != nil {
		t.Fatalf("FetchIssuesByIdentifiers: %v", err)
	}
	if len(got) != count {
		t.Errorf("resolved %d issues, want %d", len(got), count)
	}
	if mock.requests != 2 {
		t.Errorf("mock saw %d requests, want 2 (chunked at %d)", mock.requests, MaxPageSize)
	}
	if len(mock.chunkSizes) != 2 || mock.chunkSizes[0] != MaxPageSize || mock.chunkSizes[1] != 50 {
		t.Errorf("chunk sizes = %v, want [%d 50]", mock.chunkSizes, MaxPageSize)
	}
	if got["TEAM-1"] == nil || got["TEAM-1"].ID != "uuid-1" {
		t.Errorf("TEAM-1 = %+v, want uuid-1", got["TEAM-1"])
	}
}

func TestFetchIssuesByIdentifiers_ValidatesIdentifierMatch(t *testing.T) {
	// The number filter can match issues from other teams whose identifier
	// shares the number. The client must drop nodes whose identifier wasn't
	// requested — mirroring FetchIssueByIdentifier's exact-match check.
	mock := &batchFetchMock{issues: []*Issue{
		{ID: "uuid-ours", Identifier: "TEAM-7", Title: "ours"},
		{ID: "uuid-theirs", Identifier: "OTHER-7", Title: "theirs"},
	}}
	server := httptest.NewServer(mock.handler(t))
	defer server.Close()

	client := NewClient("key", "team-1").WithEndpoint(server.URL)
	got, err := client.FetchIssuesByIdentifiers(context.Background(), []string{"TEAM-7"})
	if err != nil {
		t.Fatalf("FetchIssuesByIdentifiers: %v", err)
	}
	if len(got) != 1 || got["TEAM-7"] == nil || got["TEAM-7"].ID != "uuid-ours" {
		t.Errorf("got %+v, want only TEAM-7 → uuid-ours", got)
	}
}

func TestFetchIssuesByIdentifiers_SkipsUnparseableIdentifiers(t *testing.T) {
	mock := &batchFetchMock{issues: []*Issue{
		{ID: "uuid-1", Identifier: "TEAM-1", Title: "one"},
	}}
	server := httptest.NewServer(mock.handler(t))
	defer server.Close()

	client := NewClient("key", "team-1").WithEndpoint(server.URL)
	got, err := client.FetchIssuesByIdentifiers(context.Background(), []string{"", "no-number-x", "TEAM-1"})
	if err != nil {
		t.Fatalf("FetchIssuesByIdentifiers: %v", err)
	}
	if len(got) != 1 || got["TEAM-1"] == nil {
		t.Errorf("got %+v, want only TEAM-1", got)
	}
	if mock.requests != 1 {
		t.Errorf("mock saw %d requests, want 1", mock.requests)
	}
}

func TestFetchIssuesByIdentifiers_Empty(t *testing.T) {
	client := NewClient("key", "team-1").WithEndpoint("http://127.0.0.1:1") // must not be contacted
	got, err := client.FetchIssuesByIdentifiers(context.Background(), nil)
	if err != nil {
		t.Fatalf("FetchIssuesByIdentifiers(nil): %v", err)
	}
	if len(got) != 0 {
		t.Errorf("got %+v, want empty", got)
	}
}

// TestBatchFetchIssues_MultiTeamRouting verifies the tracker-level batch
// fetch tries the primary team first and retries unresolved identifiers
// against later teams, with one batch request per team rather than one per
// identifier.
func TestBatchFetchIssues_MultiTeamRouting(t *testing.T) {
	team1 := &batchFetchMock{issues: []*Issue{
		{ID: "uuid-a", Identifier: "TEAM-1", Title: "a"},
	}}
	team2 := &batchFetchMock{issues: []*Issue{
		{ID: "uuid-b", Identifier: "TEAM-2", Title: "b"},
	}}
	server1 := httptest.NewServer(team1.handler(t))
	defer server1.Close()
	server2 := httptest.NewServer(team2.handler(t))
	defer server2.Close()

	tr := &Tracker{
		teamIDs: []string{"team-1", "team-2"},
		clients: map[string]*Client{
			"team-1": NewClient("key", "team-1").WithEndpoint(server1.URL),
			"team-2": NewClient("key", "team-2").WithEndpoint(server2.URL),
		},
	}

	got, err := tr.BatchFetchIssues(context.Background(), []string{"TEAM-1", "TEAM-2", "TEAM-3"})
	if err != nil {
		t.Fatalf("BatchFetchIssues: %v", err)
	}
	if len(got) != 2 {
		t.Errorf("resolved %d issues, want 2 (TEAM-3 unresolvable)", len(got))
	}
	if got["TEAM-1"] == nil || got["TEAM-1"].ID != "uuid-a" {
		t.Errorf("TEAM-1 = %+v, want uuid-a", got["TEAM-1"])
	}
	if got["TEAM-2"] == nil || got["TEAM-2"].ID != "uuid-b" {
		t.Errorf("TEAM-2 = %+v, want uuid-b", got["TEAM-2"])
	}
	if team1.requests != 1 || team2.requests != 1 {
		t.Errorf("per-team requests = %d/%d, want 1/1", team1.requests, team2.requests)
	}
	// Team 2 should only have been asked for what team 1 couldn't resolve.
	if len(team2.chunkSizes) != 1 || team2.chunkSizes[0] != 2 {
		t.Errorf("team2 chunk sizes = %v, want [2] (TEAM-2 and TEAM-3 only)", team2.chunkSizes)
	}
}

// TestBatchFetchIssues_ExhaustionOutranksTransientError locks the codex
// bd-kqt round-3 MAJOR: when an earlier team fails transiently and a later
// team trips the rate-limit circuit breaker, the returned error must report
// exhaustion — callers use it to abort instead of degrading to per-issue
// fetches against a dead budget.
func TestBatchFetchIssues_ExhaustionOutranksTransientError(t *testing.T) {
	team1 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "transient failure", http.StatusInternalServerError)
	}))
	defer team1.Close()
	team2 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		// Remaining below the default floor (100) trips the circuit breaker.
		w.Header().Set("X-RateLimit-Requests-Remaining", "1")
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]interface{}{"data": map[string]interface{}{}})
	}))
	defer team2.Close()

	tr := &Tracker{
		teamIDs: []string{"team-1", "team-2"},
		clients: map[string]*Client{
			"team-1": NewClient("key", "team-1").WithEndpoint(team1.URL),
			"team-2": NewClient("key", "team-2").WithEndpoint(team2.URL),
		},
	}

	_, err := tr.BatchFetchIssues(context.Background(), []string{"TEAM-1"})
	if err == nil {
		t.Fatal("expected an error")
	}
	if !isRateLimitExhausted(err) {
		t.Errorf("error = %v, want rate-limit exhaustion to outrank the earlier transient failure", err)
	}
}

// TestBatchPush_PrefetchesRemotesInOneBatch locks the bd-kqt fix: pushing N
// linked-but-unchanged beads costs one batched lookup, not one
// FetchIssueByIdentifier per bead.
func TestBatchPush_PrefetchesRemotesInOneBatch(t *testing.T) {
	const n = 30
	mock := &batchFetchMock{}
	for i := 1; i <= n; i++ {
		mock.issues = append(mock.issues, &Issue{
			ID:         fmt.Sprintf("uuid-%d", i),
			Identifier: fmt.Sprintf("TEAM-%d", i),
			Title:      fmt.Sprintf("Issue %d", i),
			State:      &State{ID: "state-open", Name: "Backlog", Type: "backlog"},
		})
	}

	var detailFetches, updates int
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		var req GraphQLRequest
		_ = json.Unmarshal(body, &req)
		w.Header().Set("Content-Type", "application/json")
		switch {
		case strings.Contains(req.Query, "TeamStates"):
			_ = json.NewEncoder(w).Encode(teamStatesResp("team-1", "state-open", "Backlog", "backlog"))
		case strings.Contains(req.Query, "TeamLabels"):
			_ = json.NewEncoder(w).Encode(teamLabelsEmptyResp("team-1"))
		case strings.Contains(req.Query, "IssuesByIdentifiers"):
			r.Body = io.NopCloser(strings.NewReader(string(body)))
			mock.handler(t)(w, r)
		case strings.Contains(req.Query, "IssueByIdentifier"):
			detailFetches++
			_ = json.NewEncoder(w).Encode(map[string]interface{}{
				"data": map[string]interface{}{"issues": map[string]interface{}{"nodes": []interface{}{}}},
			})
		case strings.Contains(req.Query, "issueUpdate"):
			updates++
			_ = json.NewEncoder(w).Encode(map[string]interface{}{
				"data": map[string]interface{}{"issueUpdate": map[string]interface{}{"success": true, "issue": map[string]interface{}{"id": "x", "url": "u", "updatedAt": "2026-01-01T00:00:00Z"}}},
			})
		}
	}))
	defer server.Close()

	cfg := DefaultMappingConfig()
	cfg.ExplicitStateMap = map[string]string{"backlog": "open"}
	tr := &Tracker{
		teamIDs: []string{"team-1"},
		clients: map[string]*Client{"team-1": NewClient("key", "team-1").WithEndpoint(server.URL)},
		config:  cfg,
	}

	// Build local beads matching the remote content exactly (priority 4 →
	// Linear 0, status open → backlog per the explicit map), so every
	// candidate takes the unchanged-skip path.
	push := make([]*types.Issue, 0, n)
	for i := 1; i <= n; i++ {
		ref := fmt.Sprintf("https://linear.app/team/issue/TEAM-%d", i)
		push = append(push, &types.Issue{
			ID:          fmt.Sprintf("local-%d", i),
			Title:       fmt.Sprintf("Issue %d", i),
			Status:      types.StatusOpen,
			Priority:    4,
			ExternalRef: &ref,
		})
	}
	result, err := tr.BatchPush(context.Background(), push, nil)
	if err != nil {
		t.Fatalf("BatchPush: %v", err)
	}
	if len(result.Skipped) != n {
		t.Errorf("Skipped = %d, want %d (all unchanged); errors: %v", len(result.Skipped), n, result.Errors)
	}
	if detailFetches != 0 {
		t.Errorf("saw %d per-issue detail fetches, want 0 (batched prefetch)", detailFetches)
	}
	if updates != 0 {
		t.Errorf("saw %d updates, want 0", updates)
	}
	if mock.requests != 1 {
		t.Errorf("saw %d batch requests, want 1 for %d candidates", mock.requests, n)
	}
}

// TestBatchPush_DegradedPrefetchFallsBackPerIssue locks the codex bd-kqt
// round-1 MAJOR: when the batched prefetch fails transiently, update
// candidates recover through the per-issue lookup path — keeping their
// unchanged-skip check and UUID resolution — instead of mutating with the
// human identifier as the issue ID.
func TestBatchPush_DegradedPrefetchFallsBackPerIssue(t *testing.T) {
	var updateIDs []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		var req GraphQLRequest
		_ = json.Unmarshal(body, &req)
		w.Header().Set("Content-Type", "application/json")
		switch {
		case strings.Contains(req.Query, "TeamStates"):
			_ = json.NewEncoder(w).Encode(teamStatesResp("team-1", "state-open", "Backlog", "backlog"))
		case strings.Contains(req.Query, "TeamLabels"):
			_ = json.NewEncoder(w).Encode(teamLabelsEmptyResp("team-1"))
		case strings.Contains(req.Query, "IssuesByIdentifiers"):
			http.Error(w, "transient batch failure", http.StatusInternalServerError)
		case strings.Contains(req.Query, "IssueByIdentifier"):
			// Per-issue lookup works: remote title differs so the update fires.
			_ = json.NewEncoder(w).Encode(issueByIdentifierResp(
				"remote-uuid", "TEAM-1", "Old Title", "", 0,
				"state-open", "Backlog", "backlog",
			))
		case strings.Contains(req.Query, "issueUpdate"):
			if id, ok := req.Variables["id"].(string); ok {
				updateIDs = append(updateIDs, id)
			}
			_ = json.NewEncoder(w).Encode(map[string]interface{}{
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
	ref := "https://linear.app/team/issue/TEAM-1"
	local := &types.Issue{
		ID:          "local-1",
		Title:       "New Title",
		Status:      types.StatusOpen,
		Priority:    4,
		ExternalRef: &ref,
	}
	tr := &Tracker{
		teamIDs: []string{"team-1"},
		clients: map[string]*Client{"team-1": NewClient("key", "team-1").WithEndpoint(server.URL)},
		config:  cfg,
	}

	result, err := tr.BatchPush(context.Background(), []*types.Issue{local}, nil)
	if err != nil {
		t.Fatalf("BatchPush: %v", err)
	}
	if len(result.Updated) != 1 {
		t.Fatalf("Updated = %v, want 1; errors: %v", result.Updated, result.Errors)
	}
	if len(updateIDs) != 1 || updateIDs[0] != "remote-uuid" {
		t.Errorf("update mutation used id %v, want [remote-uuid] (per-issue fallback must resolve the UUID)", updateIDs)
	}
	if len(result.Warnings) == 0 {
		t.Error("expected a degraded-prefetch warning")
	}
}
