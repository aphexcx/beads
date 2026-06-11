//go:build cgo

package main

import (
	"encoding/json"
	"strings"
	"testing"
)

// TestLinearTeamSlugFromRef verifies the Linear-URL team-slug extractor
// used by planEpicMigration's multi-team refusal check.
func TestLinearTeamSlugFromRef(t *testing.T) {
	cases := []struct {
		name string
		ref  string
		want string
	}{
		{"issue URL", "https://linear.app/houmanoids/issue/HOU-159/teleop-video-livekit", "houmanoids"},
		{"project URL", "https://linear.app/houmanoids/project/teleop-abc123", "houmanoids"},
		{"trailing slash", "https://linear.app/team-x/", "team-x"},
		{"no path after host", "https://linear.app/team-y", ""},
		{"not a Linear URL", "https://github.com/foo/bar/issues/1", ""},
		{"empty", "", ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := linearTeamSlugFromRef(tc.ref)
			if got != tc.want {
				t.Errorf("linearTeamSlugFromRef(%q) = %q, want %q", tc.ref, got, tc.want)
			}
		})
	}
}

// TestExtractStoredProjectURL verifies the resume-metadata reader. Empty,
// malformed, and missing-key inputs must return empty without erroring —
// fresh migrations have no metadata.
func TestExtractStoredProjectURL(t *testing.T) {
	cases := []struct {
		name string
		raw  json.RawMessage
		want string
	}{
		{"empty raw", nil, ""},
		{"empty object", json.RawMessage(`{}`), ""},
		{"key set", json.RawMessage(`{"bd:linear_migration_project_url":"https://linear.app/x/project/abc"}`),
			"https://linear.app/x/project/abc"},
		{"trims whitespace", json.RawMessage(`{"bd:linear_migration_project_url":"  https://linear.app/x  "}`),
			"https://linear.app/x"},
		{"wrong type", json.RawMessage(`{"bd:linear_migration_project_url":42}`), ""},
		{"malformed json", json.RawMessage(`{not json`), ""},
		{"unrelated key present", json.RawMessage(`{"other_key":"v"}`), ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := extractStoredProjectURL(tc.raw)
			if got != tc.want {
				t.Errorf("extractStoredProjectURL(%s) = %q, want %q", string(tc.raw), got, tc.want)
			}
		})
	}
}

// TestSetStoredProjectURL verifies the resume-metadata writer preserves
// arbitrary other fields and round-trips with extractStoredProjectURL.
// Critical for: a bead with existing metadata gets the migration key set
// WITHOUT losing other annotations the user may have added.
func TestSetStoredProjectURL(t *testing.T) {
	t.Run("preserves other keys", func(t *testing.T) {
		in := json.RawMessage(`{"existing_key":"v","another":42}`)
		out, err := setStoredProjectURL(in, "https://linear.app/x/project/abc")
		if err != nil {
			t.Fatalf("setStoredProjectURL: %v", err)
		}
		var parsed map[string]interface{}
		if uErr := json.Unmarshal(out, &parsed); uErr != nil {
			t.Fatalf("output not valid JSON: %v", uErr)
		}
		if parsed["existing_key"] != "v" {
			t.Errorf("lost existing_key: %v", parsed["existing_key"])
		}
		if parsed["another"].(float64) != 42 {
			t.Errorf("lost another: %v", parsed["another"])
		}
		if parsed["bd:linear_migration_project_url"] != "https://linear.app/x/project/abc" {
			t.Errorf("migration URL not set: %v", parsed["bd:linear_migration_project_url"])
		}
		// Round-trip
		got := extractStoredProjectURL(out)
		if got != "https://linear.app/x/project/abc" {
			t.Errorf("round-trip read = %q", got)
		}
	})

	t.Run("clears key on empty URL", func(t *testing.T) {
		in := json.RawMessage(`{"keep":"me","bd:linear_migration_project_url":"old"}`)
		out, err := setStoredProjectURL(in, "")
		if err != nil {
			t.Fatalf("setStoredProjectURL: %v", err)
		}
		got := extractStoredProjectURL(out)
		if got != "" {
			t.Errorf("expected key cleared, got %q", got)
		}
		// Other keys still present.
		if !strings.Contains(string(out), `"keep":"me"`) {
			t.Errorf("lost unrelated key: %s", string(out))
		}
	})

	t.Run("from empty raw", func(t *testing.T) {
		out, err := setStoredProjectURL(nil, "https://linear.app/x/project/abc")
		if err != nil {
			t.Fatalf("setStoredProjectURL on nil: %v", err)
		}
		got := extractStoredProjectURL(out)
		if got != "https://linear.app/x/project/abc" {
			t.Errorf("round-trip from nil = %q", got)
		}
	})

	t.Run("rejects non-object metadata", func(t *testing.T) {
		// If the bead's existing metadata happens to be an array or
		// scalar, we should error rather than silently lose data.
		_, err := setStoredProjectURL(json.RawMessage(`[1,2,3]`), "x")
		if err == nil {
			t.Error("expected error on array metadata, got nil")
		}
	})
}
