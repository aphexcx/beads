package linear

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"regexp"
	"testing"
)

// parentBlockRE matches a GraphQL `parent { id identifier }` selection
// allowing arbitrary whitespace. Tight enough that removing `identifier`
// from inside the block (or replacing `id` with something else) will
// fail the test — that was bd-6tv codex round-1 NIT.
//
// Pinned to today's exact selections. Future expansion (e.g. adding
// `parent { id identifier title }`) is INTENDED to break these tests so
// the new field choice gets a deliberate review; just update the regex
// to include the additional field(s).
var parentBlockRE = regexp.MustCompile(`parent\s*\{\s*id\s+identifier\s*\}`)

// projectBlockRE matches a GraphQL `project { id }` selection, similarly
// tight: an empty block or a block with the wrong field would not match.
// Pinned to today's selection; update when adding more fields here.
var projectBlockRE = regexp.MustCompile(`project\s*\{\s*id\s*\}`)

// bd-6tv regression suite. The bug: FetchIssueByIdentifier's GraphQL query
// did not select `parent { id identifier }` or `project { id }`, so
// Issue.Parent and Issue.Project always came back nil from that path. The
// parent reconciler (bd-9w3) and project membership reconciler (bd-1ay)
// both use FetchIssueByIdentifier (via fetchIssueAcrossTeams) and check
// `issue.Parent != nil && issue.Parent.ID == ...` / the analogous
// `issue.Project != nil && ...` for idempotency. With the fields missing,
// both checks always evaluated false → every link was treated as needing
// mutation → dry-run showed false positives and wet-run issued no-op API
// calls. The bulk issuesQuery had the same gap for `project { id }`
// (parent was already selected there).
//
// These tests lock the field selections in. They intentionally inspect
// the literal query strings rather than going through the existing
// mock-server tests because those mocks don't model GraphQL
// field-selection semantics — they return whatever's stored in their
// issues map regardless of what the query asks for, so they passed with
// the broken code.

// TestIssuesQuery_SelectsParentAndProject — the bulk query (used by
// FetchIssues/FetchIssuesSince) must select both parent and project so
// downstream consumers see remote relationships. Uses tight regexes
// against the const so future readers can't accidentally drop
// parent.identifier or replace project.id with something else.
func TestIssuesQuery_SelectsParentAndProject(t *testing.T) {
	if !parentBlockRE.MatchString(issuesQuery) {
		t.Errorf("issuesQuery missing `parent { id identifier }` selection")
	}
	if !projectBlockRE.MatchString(issuesQuery) {
		t.Errorf("issuesQuery missing `project { id }` selection (bd-6tv: " +
			"breaks project membership reconciler idempotency when this " +
			"query feeds the reconciler in the future)")
	}
}

// TestFetchIssueByIdentifier_QuerySelectsParentAndProject — capture the
// query string sent by FetchIssueByIdentifier and assert it selects both
// parent and project. Direct lock-in for the bd-6tv fix.
func TestFetchIssueByIdentifier_QuerySelectsParentAndProject(t *testing.T) {
	var capturedQuery string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		var req GraphQLRequest
		_ = json.Unmarshal(body, &req)
		capturedQuery = req.Query
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]interface{}{
			"data": map[string]interface{}{
				"issues": map[string]interface{}{"nodes": []interface{}{}},
			},
		})
	}))
	defer server.Close()

	c := NewClient("test-key", "team-uuid").WithEndpoint(server.URL)
	if _, err := c.FetchIssueByIdentifier(context.Background(), "TEAM-1"); err != nil {
		t.Fatalf("FetchIssueByIdentifier err: %v", err)
	}
	if capturedQuery == "" {
		t.Fatal("server never received a query")
	}
	if !parentBlockRE.MatchString(capturedQuery) {
		t.Errorf("FetchIssueByIdentifier query missing `parent { id identifier }` "+
			"selection — this was the bd-6tv bug. Query was: %s", capturedQuery)
	}
	if !projectBlockRE.MatchString(capturedQuery) {
		t.Errorf("FetchIssueByIdentifier query missing `project { id }` "+
			"selection — this was the bd-6tv bug. Query was: %s", capturedQuery)
	}
}

// TestFetchIssueByIdentifier_PopulatesParentAndProject — happy-path
// parsing test: when the API returns parent and project, the parsed
// Issue struct has them populated. Catches future regressions where
// somebody might remove the Issue struct fields or break JSON tags.
func TestFetchIssueByIdentifier_PopulatesParentAndProject(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]interface{}{
			"data": map[string]interface{}{
				"issues": map[string]interface{}{
					"nodes": []map[string]interface{}{
						{
							"id":         "uuid-child",
							"identifier": "TEAM-1",
							"title":      "child",
							"parent": map[string]interface{}{
								"id":         "uuid-parent",
								"identifier": "TEAM-2",
							},
							"project": map[string]interface{}{
								"id": "uuid-project",
							},
						},
					},
				},
			},
		})
	}))
	defer server.Close()

	c := NewClient("test-key", "team-uuid").WithEndpoint(server.URL)
	issue, err := c.FetchIssueByIdentifier(context.Background(), "TEAM-1")
	if err != nil {
		t.Fatalf("FetchIssueByIdentifier err: %v", err)
	}
	if issue == nil {
		t.Fatal("expected issue, got nil")
	}
	if issue.Parent == nil {
		t.Fatal("issue.Parent is nil (bd-6tv: parent field not parsed)")
	}
	if issue.Parent.ID != "uuid-parent" || issue.Parent.Identifier != "TEAM-2" {
		t.Errorf("issue.Parent = %+v, want {uuid-parent, TEAM-2}", issue.Parent)
	}
	if issue.Project == nil {
		t.Fatal("issue.Project is nil (bd-6tv: project field not parsed)")
	}
	if issue.Project.ID != "uuid-project" {
		t.Errorf("issue.Project.ID = %q, want uuid-project", issue.Project.ID)
	}
}
