package linear

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

// mockGraphQLServer wraps httptest with a JSON-aware request handler.
// Returns a server whose URL the caller passes to Client via WithEndpoint.
func mockGraphQLServer(t *testing.T, respond func(reqBody string) string) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		w.Header().Set("Content-Type", "application/json")
		// The Client wraps the response body into {"data": ...}, so respond() should
		// return just the inner data object (e.g. `{"team": {...}}`).
		_, _ = io.WriteString(w, `{"data":`+respond(string(body))+`}`)
	}))
}

func newTestClient(serverURL string) *Client {
	c := NewClient("test-api-key", "team-1")
	return c.WithEndpoint(serverURL)
}

// keep json import alive
var _ = json.Unmarshal

func TestCanonicalizeLinearExternalRef(t *testing.T) {
	tests := []struct {
		name        string
		externalRef string
		want        string
		ok          bool
	}{
		{
			name:        "slugged url",
			externalRef: "https://linear.app/crown-dev/issue/BEA-93/updated-title-for-beads",
			want:        "https://linear.app/crown-dev/issue/BEA-93",
			ok:          true,
		},
		{
			name:        "canonical url",
			externalRef: "https://linear.app/crown-dev/issue/BEA-93",
			want:        "https://linear.app/crown-dev/issue/BEA-93",
			ok:          true,
		},
		{
			name:        "not linear",
			externalRef: "https://example.com/issues/BEA-93",
			want:        "",
			ok:          false,
		},
	}

	for _, tt := range tests {
		got, ok := CanonicalizeLinearExternalRef(tt.externalRef)
		if ok != tt.ok {
			t.Fatalf("%s: ok=%v, want %v", tt.name, ok, tt.ok)
		}
		if got != tt.want {
			t.Fatalf("%s: got %q, want %q", tt.name, got, tt.want)
		}
	}
}

func TestNewClient(t *testing.T) {
	client := NewClient("test-api-key", "test-team-id")

	if client.APIKey != "test-api-key" {
		t.Errorf("APIKey = %q, want %q", client.APIKey, "test-api-key")
	}
	if client.TeamID != "test-team-id" {
		t.Errorf("TeamID = %q, want %q", client.TeamID, "test-team-id")
	}
	if client.Endpoint != DefaultAPIEndpoint {
		t.Errorf("Endpoint = %q, want %q", client.Endpoint, DefaultAPIEndpoint)
	}
	if client.HTTPClient == nil {
		t.Error("HTTPClient should not be nil")
	}
}

func TestWithEndpoint(t *testing.T) {
	client := NewClient("key", "team")
	customEndpoint := "https://custom.linear.app/graphql"

	newClient := client.WithEndpoint(customEndpoint)

	if newClient.Endpoint != customEndpoint {
		t.Errorf("Endpoint = %q, want %q", newClient.Endpoint, customEndpoint)
	}
	// Original should be unchanged
	if client.Endpoint != DefaultAPIEndpoint {
		t.Errorf("Original endpoint changed: %q", client.Endpoint)
	}
	// Other fields preserved
	if newClient.APIKey != "key" {
		t.Errorf("APIKey not preserved: %q", newClient.APIKey)
	}
}

func TestWithHTTPClient(t *testing.T) {
	client := NewClient("key", "team")
	customHTTPClient := &http.Client{Timeout: 60 * time.Second}

	newClient := client.WithHTTPClient(customHTTPClient)

	if newClient.HTTPClient != customHTTPClient {
		t.Error("HTTPClient not set correctly")
	}
	// Other fields preserved
	if newClient.APIKey != "key" {
		t.Errorf("APIKey not preserved: %q", newClient.APIKey)
	}
	if newClient.Endpoint != DefaultAPIEndpoint {
		t.Errorf("Endpoint not preserved: %q", newClient.Endpoint)
	}
}

func TestExtractLinearIdentifier(t *testing.T) {
	tests := []struct {
		name        string
		externalRef string
		want        string
	}{
		{
			name:        "standard URL",
			externalRef: "https://linear.app/team/issue/PROJ-123",
			want:        "PROJ-123",
		},
		{
			name:        "URL with slug",
			externalRef: "https://linear.app/team/issue/PROJ-456/some-title-here",
			want:        "PROJ-456",
		},
		{
			name:        "URL with trailing slash",
			externalRef: "https://linear.app/team/issue/ABC-789/",
			want:        "ABC-789",
		},
		{
			name:        "non-linear URL",
			externalRef: "https://jira.example.com/browse/PROJ-123",
			want:        "",
		},
		{
			name:        "empty string",
			externalRef: "",
			want:        "",
		},
		{
			name:        "malformed URL",
			externalRef: "not-a-url",
			want:        "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := ExtractLinearIdentifier(tt.externalRef)
			if got != tt.want {
				t.Errorf("ExtractLinearIdentifier(%q) = %q, want %q", tt.externalRef, got, tt.want)
			}
		})
	}
}

func TestIsLinearExternalRef(t *testing.T) {
	tests := []struct {
		ref  string
		want bool
	}{
		{"https://linear.app/team/issue/PROJ-123", true},
		{"https://linear.app/team/issue/PROJ-123/slug", true},
		{"https://jira.example.com/browse/PROJ-123", false},
		{"https://github.com/org/repo/issues/123", false},
		{"", false},
	}

	for _, tt := range tests {
		t.Run(tt.ref, func(t *testing.T) {
			got := IsLinearExternalRef(tt.ref)
			if got != tt.want {
				t.Errorf("IsLinearExternalRef(%q) = %v, want %v", tt.ref, got, tt.want)
			}
		})
	}
}

// Note: BuildStateCache and FindStateForBeadsStatus require API calls
// and would need mocking to test. Skipping unit tests for those.

func TestLabelsByName_TeamScoped(t *testing.T) {
	server := mockGraphQLServer(t, func(req string) string {
		// Assert the query targets both team.labels and organization.labels.
		if !strings.Contains(req, "team(") || !strings.Contains(req, "labels") {
			t.Errorf("expected query to fetch team labels, got: %s", req)
		}
		return `{"team":{"labels":{"nodes":[
			{"id":"L1","name":"bug"},
			{"id":"L2","name":"p1"}
		]}},"organization":{"labels":{"nodes":[]}}}`
	})
	defer server.Close()

	c := newTestClient(server.URL)
	out, err := c.LabelsByName(context.Background(), []string{"Bug", "missing"}) // beads-side spelling differs from Linear
	if err != nil {
		t.Fatalf("LabelsByName: %v", err)
	}
	// Result is keyed by lowercase name (case-insensitive match), but the
	// LinearLabel.Name field preserves Linear's display casing.
	if got, ok := out["bug"]; !ok || got.ID != "L1" || got.Name != "bug" {
		t.Errorf("bug: got %+v, want {ID: L1, Name: bug}", got)
	}
	if _, ok := out["missing"]; ok {
		t.Errorf("missing: should not be in result map")
	}
}

func TestLabelsByName_DuplicateNamesFailLoudly(t *testing.T) {
	server := mockGraphQLServer(t, func(_ string) string {
		return `{"team":{"labels":{"nodes":[
			{"id":"L1","name":"bug"},
			{"id":"L2","name":"bug"}
		]}},"organization":{"labels":{"nodes":[]}}}`
	})
	defer server.Close()

	c := newTestClient(server.URL)
	_, err := c.LabelsByName(context.Background(), []string{"bug"})
	if err == nil {
		t.Fatal("expected duplicate-name error, got nil")
	}
	if !strings.Contains(err.Error(), "ambiguous") && !strings.Contains(err.Error(), "duplicate") {
		t.Errorf("error should mention ambiguity, got: %v", err)
	}
}
