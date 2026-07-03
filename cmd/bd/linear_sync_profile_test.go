//go:build cgo && integration

package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/steveyegge/beads/internal/linear"
	"github.com/steveyegge/beads/internal/storage/dolt"
	"github.com/steveyegge/beads/internal/testutil"
	"github.com/steveyegge/beads/internal/tracker"
	"github.com/steveyegge/beads/internal/types"
)

// This file is the bd-kqt profiling harness: it reproduces the hw dataset's
// request-amplification scenario (a ~1k-issue repo where every linked bead
// looks locally dirty because sync write-backs bumped updated_at past
// linear.last_sync) against a counting mock Linear server, and asserts the
// request budget of a bidirectional sync stays bounded.
//
// Run with:
//
//	BEADS_TEST_MODE=1 BEADS_DOLT_SERVER_PORT=<test-server-port> \
//	  go test -tags 'gms_pure_go integration' -run TestLinearSyncRequestProfile \
//	  -timeout 20m ./cmd/bd/ -v
//
// Set BEADS_PROFILE_N to change the dataset size (default 1000) and
// BEADS_PROFILE_NO_ASSERT=1 to only print the profile (used to capture
// baseline numbers on unfixed code).

// profilingLinearServer is a stateful mock Linear API that classifies and
// counts every GraphQL request it serves, so a sync's request profile can be
// asserted and reported per operation type.
type profilingLinearServer struct {
	mu       sync.Mutex
	teamID   string
	teamKey  string
	states   []linear.State
	issues   map[string]*linear.Issue // keyed by UUID
	sorted   []string                 // UUIDs sorted by issue number
	counts   map[string]int
	sequence []profiledOp
}

type profiledOp struct {
	op string
	at time.Time
}

func newProfilingLinearServer(teamID, teamKey string) *profilingLinearServer {
	return &profilingLinearServer{
		teamID:  teamID,
		teamKey: teamKey,
		states: []linear.State{
			{ID: "state-backlog", Name: "Backlog", Type: "backlog"},
			{ID: "state-unstarted", Name: "Todo", Type: "unstarted"},
			{ID: "state-started", Name: "In Progress", Type: "started"},
			{ID: "state-completed", Name: "Done", Type: "completed"},
			{ID: "state-canceled", Name: "Canceled", Type: "canceled"},
		},
		issues: make(map[string]*linear.Issue),
		counts: make(map[string]int),
	}
}

// seedIssue registers a mock Linear issue. Call before serving traffic.
func (m *profilingLinearServer) seedIssue(issue *linear.Issue) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.issues[issue.ID] = issue
	m.resortLocked()
}

func (m *profilingLinearServer) resortLocked() {
	m.sorted = m.sorted[:0]
	for id := range m.issues {
		m.sorted = append(m.sorted, id)
	}
	sort.Slice(m.sorted, func(i, j int) bool {
		return issueNumber(m.issues[m.sorted[i]]) < issueNumber(m.issues[m.sorted[j]])
	})
}

func issueNumber(issue *linear.Issue) int {
	parts := strings.Split(issue.Identifier, "-")
	n, _ := strconv.Atoi(parts[len(parts)-1])
	return n
}

func (m *profilingLinearServer) resetCounts() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.counts = make(map[string]int)
	m.sequence = nil
}

func (m *profilingLinearServer) snapshotCounts() (map[string]int, int) {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make(map[string]int, len(m.counts))
	total := 0
	for k, v := range m.counts {
		out[k] = v
		total += v
	}
	return out, total
}

// reportProfile logs the per-operation request counts for a named run.
func (m *profilingLinearServer) reportProfile(t *testing.T, label string, wall time.Duration) int {
	t.Helper()
	counts, total := m.snapshotCounts()
	keys := make([]string, 0, len(counts))
	for k := range counts {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	t.Logf("=== profile: %s ===", label)
	t.Logf("  wall time: %s", wall.Round(time.Millisecond))
	t.Logf("  API requests: %d total", total)
	for _, k := range keys {
		t.Logf("    %-22s %d", k, counts[k])
	}
	return total
}

func (m *profilingLinearServer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	var req linear.GraphQLRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}

	op, data, err := m.dispatch(req)

	m.mu.Lock()
	m.counts[op]++
	m.sequence = append(m.sequence, profiledOp{op: op, at: time.Now()})
	m.mu.Unlock()

	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	respBytes, _ := json.Marshal(data)
	resp := map[string]json.RawMessage{"data": respBytes}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(resp)
}

func (m *profilingLinearServer) dispatch(req linear.GraphQLRequest) (string, interface{}, error) {
	q := req.Query
	switch {
	case strings.Contains(q, "IssuesByIdentifiers"):
		data, err := m.handleIssuesQuery(req)
		return "issues-batch", data, err
	case strings.Contains(q, "IssueByIdentifier"):
		data, err := m.handleIssuesQuery(req)
		return "issue-detail", data, err
	case strings.Contains(q, "IssueComments"):
		return "comments", emptyIssueConnection("comments"), nil
	case strings.Contains(q, "IssueAttachments"):
		return "attachments", emptyIssueConnection("attachments"), nil
	case strings.Contains(q, "issueBatchCreate"):
		return "issue-create-batch", nil, fmt.Errorf("profile harness does not expect creates")
	case strings.Contains(q, "issueCreate"):
		return "issue-create", nil, fmt.Errorf("profile harness does not expect creates")
	case strings.Contains(q, "issueUpdate"):
		data, err := m.handleUpdate(req)
		return "issue-update", data, err
	case strings.Contains(q, "query Projects"):
		return "projects", map[string]interface{}{
			"projects": map[string]interface{}{
				"nodes":    []interface{}{},
				"pageInfo": map[string]interface{}{"hasNextPage": false, "endCursor": ""},
			},
		}, nil
	case strings.Contains(q, "states") && strings.Contains(q, "team"):
		return "team-states", map[string]interface{}{
			"team": map[string]interface{}{
				"id":     m.teamID,
				"states": map[string]interface{}{"nodes": m.states},
			},
		}, nil
	case strings.Contains(q, "query Issues("):
		data, err := m.handleIssuesQuery(req)
		return "issues-list", data, err
	default:
		snippet := q
		if len(snippet) > 120 {
			snippet = snippet[:120]
		}
		return "other", nil, fmt.Errorf("unhandled query: %s", snippet)
	}
}

func emptyIssueConnection(field string) interface{} {
	return map[string]interface{}{
		"issue": map[string]interface{}{
			field: map[string]interface{}{
				"nodes":    []interface{}{},
				"pageInfo": map[string]interface{}{"hasNextPage": false, "endCursor": ""},
			},
		},
	}
}

// handleIssuesQuery serves every issues(filter:...) query shape the client
// uses: full list, updatedAt-gte incremental list, number-eq detail lookup,
// and number-in batch lookup. Pagination honors first/after with a numeric
// cursor over the number-sorted issue list.
func (m *profilingLinearServer) handleIssuesQuery(req linear.GraphQLRequest) (interface{}, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	var numberEq int
	numberIn := map[int]bool{}
	var updatedGte time.Time
	hasNumberEq, hasNumberIn := false, false

	if filterRaw, ok := req.Variables["filter"].(map[string]interface{}); ok {
		if numRaw, ok := filterRaw["number"].(map[string]interface{}); ok {
			if eqRaw, ok := numRaw["eq"].(float64); ok {
				hasNumberEq = true
				numberEq = int(eqRaw)
			}
			if inRaw, ok := numRaw["in"].([]interface{}); ok {
				hasNumberIn = true
				for _, v := range inRaw {
					if f, ok := v.(float64); ok {
						numberIn[int(f)] = true
					}
				}
			}
		}
		if upRaw, ok := filterRaw["updatedAt"].(map[string]interface{}); ok {
			if gteRaw, ok := upRaw["gte"].(string); ok {
				if ts, err := time.Parse(time.RFC3339, gteRaw); err == nil {
					updatedGte = ts
				}
			}
		}
	}

	var matches []*linear.Issue
	for _, id := range m.sorted {
		issue := m.issues[id]
		n := issueNumber(issue)
		if hasNumberEq && n != numberEq {
			continue
		}
		if hasNumberIn && !numberIn[n] {
			continue
		}
		if !updatedGte.IsZero() {
			ts, err := time.Parse(time.RFC3339, issue.UpdatedAt)
			if err != nil || ts.Before(updatedGte) {
				continue
			}
		}
		matches = append(matches, issue)
	}

	first := len(matches)
	if fRaw, ok := req.Variables["first"].(float64); ok && int(fRaw) > 0 {
		first = int(fRaw)
	}
	start := 0
	if aRaw, ok := req.Variables["after"].(string); ok && aRaw != "" {
		if idx, err := strconv.Atoi(aRaw); err == nil {
			start = idx
		}
	}
	end := start + first
	if start > len(matches) {
		start = len(matches)
	}
	if end > len(matches) {
		end = len(matches)
	}
	page := matches[start:end]
	hasNext := end < len(matches)

	return map[string]interface{}{
		"issues": map[string]interface{}{
			"nodes":    page,
			"pageInfo": map[string]interface{}{"hasNextPage": hasNext, "endCursor": strconv.Itoa(end)},
		},
	}, nil
}

func (m *profilingLinearServer) handleUpdate(req linear.GraphQLRequest) (interface{}, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	id, _ := req.Variables["id"].(string)
	issue, ok := m.issues[id]
	if !ok {
		return nil, fmt.Errorf("issue %s not found", id)
	}
	input, _ := req.Variables["input"].(map[string]interface{})
	if v, ok := input["title"].(string); ok && v != "" {
		issue.Title = v
	}
	if v, ok := input["description"].(string); ok && v != "" {
		issue.Description = v
	}
	if p, ok := input["priority"].(float64); ok {
		issue.Priority = int(p)
	}
	if stateID, ok := input["stateId"].(string); ok && stateID != "" {
		for i := range m.states {
			if m.states[i].ID == stateID {
				issue.State = &linear.State{ID: m.states[i].ID, Name: m.states[i].Name, Type: m.states[i].Type}
				break
			}
		}
	}
	issue.UpdatedAt = time.Now().UTC().Format(time.RFC3339)
	return map[string]interface{}{
		"issueUpdate": map[string]interface{}{"success": true, "issue": issue},
	}, nil
}

func profileEnvInt(key string, def int) int {
	if raw := os.Getenv(key); raw != "" {
		if v, err := strconv.Atoi(raw); err == nil && v > 0 {
			return v
		}
	}
	return def
}

// setupProfileStore creates a dedicated database for the profile run on the
// test Dolt server named by BEADS_DOLT_SERVER_PORT, and drops it on cleanup
// so profile runs don't accumulate orphan test databases.
func setupProfileStore(t *testing.T) *dolt.DoltStore {
	t.Helper()
	portRaw := os.Getenv("BEADS_DOLT_SERVER_PORT")
	if portRaw == "" {
		t.Skip("BEADS_DOLT_SERVER_PORT not set; profile harness needs a test Dolt server")
	}
	port, err := strconv.Atoi(portRaw)
	if err != nil {
		t.Skipf("invalid BEADS_DOLT_SERVER_PORT %q", portRaw)
	}

	dbName := fmt.Sprintf("bdkqt_profile_%d", os.Getpid())
	admin, err := testutil.SetupSharedTestDB(port, dbName)
	if err != nil {
		t.Skipf("test Dolt server not available on port %d: %v", port, err)
	}
	t.Cleanup(func() {
		_, _ = admin.Exec(fmt.Sprintf("DROP DATABASE IF EXISTS `%s`", dbName))
		_ = admin.Close()
	})

	ctx := context.Background()
	st, err := dolt.New(ctx, &dolt.Config{
		Path:       t.TempDir(),
		ServerHost: "127.0.0.1",
		ServerPort: port,
		Database:   dbName,
	})
	if err != nil {
		t.Fatalf("dolt.New: %v", err)
	}
	t.Cleanup(func() { st.Close() })

	if err := st.SetConfig(ctx, "issue_prefix", "hw"); err != nil {
		t.Fatalf("SetConfig(issue_prefix): %v", err)
	}
	return st
}

// TestLinearSyncRequestProfile reproduces the bd-kqt hw scenario: N issues
// pulled from Linear, then every local bead's updated_at bumped past
// linear.last_sync (the phantom-dirty state created by sync write-backs)
// plus a small genuine remote delta, and profiles the request cost of
// bidirectional syncs. The acceptance target: a bidirectional dry-run over
// ~1k linked issues costs at most 100 Linear API requests.
func TestLinearSyncRequestProfile(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration profile in short mode")
	}

	// Default 300: the request-count assertions are scale-independent (the
	// batched fetches cost ceil(N/100) requests either way), while dataset
	// setup is ~1s per issue against a test Dolt server. Use
	// BEADS_PROFILE_N=1000 for the full bd-kqt acceptance-scale run.
	n := profileEnvInt("BEADS_PROFILE_N", 300)
	deltaSize := 5
	assertBudgets := os.Getenv("BEADS_PROFILE_NO_ASSERT") != "1"

	ctx := context.Background()
	teamID := "profile-team-uuid"

	st := setupProfileStore(t)

	// Hook builders (getLinearIDMode, buildCommentPullHook, ...) read the
	// package-global store.
	origStore := store
	store = st
	defer func() { store = origStore }()

	mock := newProfilingLinearServer(teamID, "HW")
	server := httptest.NewServer(mock)
	defer server.Close()

	// linear.api_key is a yaml-only config key; the tracker reads it from
	// config.yaml or the env var — the env var is the test-friendly path.
	t.Setenv("LINEAR_API_KEY", "profile-api-key")
	for k, v := range map[string]string{
		"linear.team_id":             teamID,
		"linear.api_endpoint":        server.URL,
		"linear.state_map.backlog":   "open",
		"linear.state_map.unstarted": "open",
		"linear.state_map.started":   "in_progress",
		"linear.state_map.completed": "closed",
		"linear.state_map.canceled":  "closed",
	} {
		if err := st.SetConfig(ctx, k, v); err != nil {
			t.Fatalf("SetConfig(%s): %v", k, err)
		}
	}

	// Seed N remote issues, all last touched well in the past. Every block
	// of 10 carries a remote parent link (first issue parents the next 9).
	// Note: the pull path only wires local parent-child deps from Project
	// milestones/relations, so the post-sync parent reconciler sees no links
	// in this dataset — its batching is locked by the unit tests in
	// internal/linear/parent_reconcile_test.go instead.
	staleTime := time.Now().UTC().Add(-3 * time.Hour).Format(time.RFC3339)
	for i := 1; i <= n; i++ {
		issue := &linear.Issue{
			ID:          fmt.Sprintf("uuid-%d", i),
			Identifier:  fmt.Sprintf("HW-%d", i),
			Title:       fmt.Sprintf("Profile issue %d", i),
			Description: fmt.Sprintf("Profile issue body %d.", i),
			URL:         fmt.Sprintf("https://linear.app/hw/issue/HW-%d", i),
			Priority:    3,
			State:       &linear.State{ID: "state-unstarted", Name: "Todo", Type: "unstarted"},
			CreatedAt:   staleTime,
			UpdatedAt:   staleTime,
		}
		if i%10 != 1 {
			parentNum := ((i-1)/10)*10 + 1
			issue.Parent = &linear.Parent{
				ID:         fmt.Sprintf("uuid-%d", parentNum),
				Identifier: fmt.Sprintf("HW-%d", parentNum),
			}
		}
		mock.seedIssue(issue)
	}

	lt := &linear.Tracker{}
	lt.SetTeamIDs([]string{teamID})
	if err := lt.Init(ctx, st); err != nil {
		t.Fatalf("Tracker.Init: %v", err)
	}

	engine := tracker.NewEngine(lt, st, "profile-actor")
	engine.PullHooks = buildLinearPullHooksForStore(ctx, lt, st, linearPullHookOptions{Actor: "profile-actor"})
	engine.PushHooks = buildLinearPushHooks(ctx, lt, false)

	syncBidirectional := func(label string, dryRun bool) (time.Duration, int) {
		t.Helper()
		mock.resetCounts()
		opts := tracker.SyncOptions{Pull: true, Push: true, DryRun: dryRun, ConflictResolution: tracker.ConflictTimestamp}
		started := time.Now()
		result, err := engine.Sync(ctx, opts)
		if err != nil {
			t.Fatalf("%s: Sync failed: %v", label, err)
		}
		wall := time.Since(started)

		// Post-sync reconcile passes, mirroring runLinearSync's unscoped
		// bidirectional flow.
		var warnings []string
		reconcileLinearParents(ctx, lt, dryRun, true, &warnings)
		reconcileLinearProjectMembership(ctx, lt, dryRun, true, &warnings)
		wall = time.Since(started)

		total := mock.reportProfile(t, label, wall)
		t.Logf("  sync stats: pulled=%d pushed=%d skipped=%d conflicts=%d errors=%d warnings=%d",
			result.Stats.Pulled, result.Stats.Pushed, result.Stats.Skipped, result.Stats.Conflicts,
			result.Stats.Errors, len(result.Warnings)+len(warnings))
		for i, w := range append(append([]string(nil), result.Warnings...), warnings...) {
			if i >= 5 {
				t.Logf("  warning[...]: %d more", len(result.Warnings)+len(warnings)-5)
				break
			}
			t.Logf("  warning[%d]: %s", i, w)
		}
		return wall, total
	}

	// --- Phase 1: initial full import (empty local DB, no last_sync). ---
	mock.resetCounts()
	importStart := time.Now()
	importResult, err := engine.Sync(ctx, tracker.SyncOptions{Pull: true})
	if err != nil {
		t.Fatalf("initial import: %v", err)
	}
	mock.reportProfile(t, fmt.Sprintf("initial full import (%d issues)", n), time.Since(importStart))
	if importResult.Stats.Created != n {
		t.Fatalf("initial import created %d local issues, want %d", importResult.Stats.Created, n)
	}

	// --- Phase 2: manufacture the hw phantom-dirty state. ---
	// The initial import recorded linear.last_sync at the next whole second;
	// wait past it so the touches below land strictly after last_sync.
	time.Sleep(1500 * time.Millisecond)

	locals, err := st.SearchIssues(ctx, "", types.IssueFilter{})
	if err != nil {
		t.Fatalf("SearchIssues: %v", err)
	}
	linked := 0
	for _, issue := range locals {
		if issue.ExternalRef == nil || *issue.ExternalRef == "" {
			continue
		}
		// Bump updated_at without changing any field the push/pull equality
		// checks or the field-scoped conflict diff compare — the same shape
		// as the metadata/external_ref write-backs that created the hw
		// phantom-dirty state.
		if err := st.UpdateIssue(ctx, issue.ID, map[string]interface{}{"metadata": json.RawMessage(`{"bdkqt":"phantom-touch"}`)}, "profile-actor"); err != nil {
			t.Fatalf("phantom touch %s: %v", issue.ID, err)
		}
		linked++
	}
	if linked != n {
		t.Fatalf("expected %d linked beads to touch, got %d", n, linked)
	}

	// A small genuine remote delta: freshen deltaSize issues, editing the
	// title on some (real content change to pull) and only the timestamp on
	// the rest (comment-style activity: pull-equal but sub-resources must
	// still be checked).
	freshTime := time.Now().UTC().Format(time.RFC3339)
	mock.mu.Lock()
	for i := 1; i <= deltaSize; i++ {
		issue := mock.issues[fmt.Sprintf("uuid-%d", i)]
		if i <= 3 {
			issue.Title = fmt.Sprintf("Profile issue %d (edited remotely)", i)
		}
		issue.UpdatedAt = freshTime
	}
	mock.mu.Unlock()

	// --- Phase 3: the acceptance scenario — bidirectional dry-run over the
	// phantom-dirty dataset. ---
	dryWall, dryTotal := syncBidirectional(
		fmt.Sprintf("bidirectional dry-run, %d linked beads all phantom-dirty, %d remote delta", n, deltaSize), true)

	// --- Phase 4: the same state, wet run. ---
	_, wetTotal := syncBidirectional(
		fmt.Sprintf("bidirectional wet run, %d linked beads all phantom-dirty, %d remote delta", n, deltaSize), false)

	// --- Phase 5: steady state — nothing changed since the wet run. ---
	_, steadyTotal := syncBidirectional("bidirectional dry-run, steady state (nothing dirty)", true)

	if assertBudgets {
		if dryTotal > 100 {
			t.Errorf("bidirectional dry-run used %d Linear API requests, budget is 100 (bd-kqt)", dryTotal)
		}
		if wetTotal > 150 {
			t.Errorf("bidirectional wet run used %d Linear API requests, budget is 150 (bd-kqt)", wetTotal)
		}
		if steadyTotal > 100 {
			t.Errorf("steady-state dry-run used %d Linear API requests, budget is 100 (bd-kqt)", steadyTotal)
		}
		// The bd-kqt 30s wall target is dominated by LOCAL store work under
		// a full phantom-dirty storm (per-candidate dolt_history scans in
		// field-scoped conflict detection — see
		// docs/LINEAR_SYNC_PROFILING.md), which varies with test hardware
		// and Dolt server contention. Assert a generous ceiling to catch
		// catastrophic regressions without flaking CI.
		if dryWall > 5*time.Minute {
			t.Errorf("bidirectional dry-run took %s, regression ceiling is 5m (bd-kqt)", dryWall.Round(time.Millisecond))
		}
		counts, _ := mock.snapshotCounts()
		if counts["issue-detail"] > 0 {
			t.Errorf("steady-state dry-run still issued %d per-issue detail fetches; expected batched fetches only", counts["issue-detail"])
		}
	}
}
