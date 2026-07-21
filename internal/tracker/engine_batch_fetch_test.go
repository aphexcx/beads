// Pure-Go tests for the bd-kqt batch-fetch engine paths. No cgo tag: these
// must run everywhere (the Dolt-image-gated cgo suite silently skips on many
// machines — bd-bhz).

package tracker

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/steveyegge/beads/internal/storage"
	"github.com/steveyegge/beads/internal/types"
)

// mockBatchFetchTracker layers BatchIssueFetcher over mockTracker and counts
// both batch and per-issue fetches. When batchErr is set, batch calls return
// it alongside an empty (partial) map, simulating a failed batch request.
type mockBatchFetchTracker struct {
	*mockTracker
	batchCalls      [][]string
	batchErr        error
	fetchIssueCalls int
}

func (m *mockBatchFetchTracker) BatchFetchIssues(_ context.Context, identifiers []string) (map[string]*TrackerIssue, error) {
	m.batchCalls = append(m.batchCalls, append([]string(nil), identifiers...))
	if m.batchErr != nil {
		return map[string]*TrackerIssue{}, m.batchErr
	}
	out := make(map[string]*TrackerIssue, len(identifiers))
	for _, identifier := range identifiers {
		for i := range m.issues {
			if m.issues[i].Identifier == identifier {
				issue := m.issues[i]
				out[identifier] = &issue
			}
		}
	}
	return out, nil
}

// fakeRateLimitErr satisfies the engine's rate-limit-exhausted detection.
type fakeRateLimitErr struct{}

func (fakeRateLimitErr) Error() string            { return "rate limit circuit breaker tripped" }
func (fakeRateLimitErr) RateLimitExhausted() bool { return true }

func (m *mockBatchFetchTracker) FetchIssue(ctx context.Context, identifier string) (*TrackerIssue, error) {
	m.fetchIssueCalls++
	return m.mockTracker.FetchIssue(ctx, identifier)
}

// countingFetchTracker counts per-issue fetches on a plain (non-batching)
// tracker, for the fallback-path assertions.
type countingFetchTracker struct {
	*mockTracker
	fetchIssueCalls int
}

func (m *countingFetchTracker) FetchIssue(ctx context.Context, identifier string) (*TrackerIssue, error) {
	m.fetchIssueCalls++
	return m.mockTracker.FetchIssue(ctx, identifier)
}

func TestDetectConflictsUsesBatchFetcher(t *testing.T) {
	ctx := context.Background()
	lastSync := time.Now().Add(-1 * time.Hour)

	dirtyRef := "https://mock.test/MOCK-1"
	cleanRef := "https://mock.test/MOCK-2"
	locals := []*types.Issue{
		{ID: "bd-1", Title: "dirty", ExternalRef: &dirtyRef, UpdatedAt: lastSync.Add(30 * time.Minute)},
		{ID: "bd-2", Title: "clean", ExternalRef: &cleanRef, UpdatedAt: lastSync.Add(-30 * time.Minute)},
	}

	mt := newMockTracker("mock")
	mt.issues = []TrackerIssue{
		{ID: "uuid-1", Identifier: "MOCK-1", Title: "dirty remote", UpdatedAt: lastSync.Add(45 * time.Minute)},
		{ID: "uuid-2", Identifier: "MOCK-2", Title: "clean remote", UpdatedAt: lastSync.Add(-45 * time.Minute)},
	}
	batcher := &mockBatchFetchTracker{mockTracker: mt}

	store := newPureTestStore(locals...)
	store.localMetadata["mock.last_sync"] = lastSync.UTC().Format(time.RFC3339Nano)

	e := NewEngine(batcher, store, "test-actor")
	conflicts, err := e.DetectConflicts(ctx, SyncOptions{})
	if err != nil {
		t.Fatalf("DetectConflicts: %v", err)
	}

	// Both sides of bd-1 changed after last_sync → one conflict. bd-2 is
	// clean locally and the store has no snapshot infra, so the legacy
	// fast-skip drops it before any fetch.
	if len(conflicts) != 1 || conflicts[0].IssueID != "bd-1" {
		t.Errorf("conflicts = %+v, want exactly bd-1", conflicts)
	}
	if len(batcher.batchCalls) != 1 {
		t.Fatalf("batch calls = %d, want 1", len(batcher.batchCalls))
	}
	if len(batcher.batchCalls[0]) != 1 || batcher.batchCalls[0][0] != "MOCK-1" {
		t.Errorf("batch identifiers = %v, want [MOCK-1]", batcher.batchCalls[0])
	}
	if batcher.fetchIssueCalls != 0 {
		t.Errorf("per-issue FetchIssue calls = %d, want 0 when batching is available", batcher.fetchIssueCalls)
	}
}

// TestDetectConflictsBatchErrorFallsBackPerIssue locks the codex bd-kqt
// round-1 MAJOR: a transient batch failure must not reclassify unresolved
// candidates as "absent remotely" — they get HEAD's per-issue fetch.
func TestDetectConflictsBatchErrorFallsBackPerIssue(t *testing.T) {
	ctx := context.Background()
	lastSync := time.Now().Add(-1 * time.Hour)

	dirtyRef := "https://mock.test/MOCK-1"
	locals := []*types.Issue{
		{ID: "bd-1", Title: "dirty", ExternalRef: &dirtyRef, UpdatedAt: lastSync.Add(30 * time.Minute)},
	}

	mt := newMockTracker("mock")
	mt.issues = []TrackerIssue{
		{ID: "uuid-1", Identifier: "MOCK-1", Title: "remote", UpdatedAt: lastSync.Add(45 * time.Minute)},
	}
	batcher := &mockBatchFetchTracker{mockTracker: mt, batchErr: fmt.Errorf("transient API failure")}

	store := newPureTestStore(locals...)
	store.localMetadata["mock.last_sync"] = lastSync.UTC().Format(time.RFC3339Nano)

	e := NewEngine(batcher, store, "test-actor")
	conflicts, err := e.DetectConflicts(ctx, SyncOptions{})
	if err != nil {
		t.Fatalf("DetectConflicts: %v", err)
	}
	if len(conflicts) != 1 {
		t.Errorf("conflicts = %+v, want 1 (per-issue fallback must still detect it)", conflicts)
	}
	if batcher.fetchIssueCalls != 1 {
		t.Errorf("per-issue FetchIssue calls = %d, want 1 after batch failure", batcher.fetchIssueCalls)
	}
}

// TestDetectConflictsAbortsOnRateLimitExhaustion: once the circuit breaker
// trips there is no budget for per-issue fallbacks — the phase must stop.
func TestDetectConflictsAbortsOnRateLimitExhaustion(t *testing.T) {
	ctx := context.Background()
	lastSync := time.Now().Add(-1 * time.Hour)

	dirtyRef := "https://mock.test/MOCK-1"
	locals := []*types.Issue{
		{ID: "bd-1", Title: "dirty", ExternalRef: &dirtyRef, UpdatedAt: lastSync.Add(30 * time.Minute)},
	}

	mt := newMockTracker("mock")
	mt.issues = []TrackerIssue{
		{ID: "uuid-1", Identifier: "MOCK-1", Title: "remote", UpdatedAt: lastSync.Add(45 * time.Minute)},
	}
	batcher := &mockBatchFetchTracker{mockTracker: mt, batchErr: fakeRateLimitErr{}}

	store := newPureTestStore(locals...)
	store.localMetadata["mock.last_sync"] = lastSync.UTC().Format(time.RFC3339Nano)

	e := NewEngine(batcher, store, "test-actor")
	if _, err := e.DetectConflicts(ctx, SyncOptions{}); err == nil {
		t.Fatal("DetectConflicts should propagate rate-limit exhaustion")
	}
	if batcher.fetchIssueCalls != 0 {
		t.Errorf("per-issue FetchIssue calls = %d, want 0 after exhaustion", batcher.fetchIssueCalls)
	}
}

// TestFetchPrelinkedIssuesBatchErrorFallsBackPerIssue mirrors the conflict
// fallback for hydration: transient batch failures degrade to the per-issue
// path (whose errors fail the pull, as on HEAD).
func TestFetchPrelinkedIssuesBatchErrorFallsBackPerIssue(t *testing.T) {
	ctx := context.Background()
	lastSync := time.Now().Add(-1 * time.Hour)

	dirtyRef := "https://mock.test/MOCK-1"
	locals := []*types.Issue{
		{ID: "bd-1", ExternalRef: &dirtyRef, UpdatedAt: lastSync.Add(10 * time.Minute)},
	}

	mt := newMockTracker("mock")
	mt.issues = []TrackerIssue{{ID: "uuid-1", Identifier: "MOCK-1", Title: "one", URL: dirtyRef}}
	batcher := &mockBatchFetchTracker{mockTracker: mt, batchErr: fmt.Errorf("transient API failure")}

	e := NewEngine(batcher, newPureTestStore(locals...), "test-actor")
	hydrated, hydratedIDs, err := e.fetchPrelinkedIssues(ctx, nil, locals, &lastSync)
	if err != nil {
		t.Fatalf("fetchPrelinkedIssues: %v", err)
	}
	if len(hydrated) != 1 || !hydratedIDs["bd-1"] {
		t.Errorf("hydrated = %+v ids=%v, want MOCK-1 via per-issue fallback", hydrated, hydratedIDs)
	}
	if batcher.fetchIssueCalls != 1 {
		t.Errorf("per-issue FetchIssue calls = %d, want 1 after batch failure", batcher.fetchIssueCalls)
	}
}

func TestFetchPrelinkedIssuesAbortsOnRateLimitExhaustion(t *testing.T) {
	ctx := context.Background()
	lastSync := time.Now().Add(-1 * time.Hour)

	dirtyRef := "https://mock.test/MOCK-1"
	locals := []*types.Issue{
		{ID: "bd-1", ExternalRef: &dirtyRef, UpdatedAt: lastSync.Add(10 * time.Minute)},
	}

	mt := newMockTracker("mock")
	mt.issues = []TrackerIssue{{ID: "uuid-1", Identifier: "MOCK-1", Title: "one", URL: dirtyRef}}
	batcher := &mockBatchFetchTracker{mockTracker: mt, batchErr: fakeRateLimitErr{}}

	e := NewEngine(batcher, newPureTestStore(locals...), "test-actor")
	if _, _, err := e.fetchPrelinkedIssues(ctx, nil, locals, &lastSync); err == nil {
		t.Fatal("fetchPrelinkedIssues should propagate rate-limit exhaustion")
	}
	if batcher.fetchIssueCalls != 0 {
		t.Errorf("per-issue FetchIssue calls = %d, want 0 after exhaustion", batcher.fetchIssueCalls)
	}
}

func TestDetectConflictsFallsBackToPerIssueFetch(t *testing.T) {
	ctx := context.Background()
	lastSync := time.Now().Add(-1 * time.Hour)

	dirtyRef := "https://mock.test/MOCK-1"
	locals := []*types.Issue{
		{ID: "bd-1", Title: "dirty", ExternalRef: &dirtyRef, UpdatedAt: lastSync.Add(30 * time.Minute)},
	}

	mt := newMockTracker("mock")
	mt.issues = []TrackerIssue{
		{ID: "uuid-1", Identifier: "MOCK-1", Title: "remote", UpdatedAt: lastSync.Add(45 * time.Minute)},
	}
	counter := &countingFetchTracker{mockTracker: mt}

	store := newPureTestStore(locals...)
	store.localMetadata["mock.last_sync"] = lastSync.UTC().Format(time.RFC3339Nano)

	e := NewEngine(counter, store, "test-actor")
	conflicts, err := e.DetectConflicts(ctx, SyncOptions{})
	if err != nil {
		t.Fatalf("DetectConflicts: %v", err)
	}
	if len(conflicts) != 1 {
		t.Errorf("conflicts = %+v, want 1", conflicts)
	}
	if counter.fetchIssueCalls != 1 {
		t.Errorf("per-issue FetchIssue calls = %d, want 1 on the fallback path", counter.fetchIssueCalls)
	}
}

func TestFetchPrelinkedIssuesBatchFetch(t *testing.T) {
	ctx := context.Background()
	lastSync := time.Now().Add(-1 * time.Hour)

	dirtyRef := "https://mock.test/MOCK-1"
	cleanRef := "https://mock.test/MOCK-2"
	seenRef := "https://mock.test/MOCK-3"
	locals := []*types.Issue{
		{ID: "bd-1", ExternalRef: &dirtyRef, UpdatedAt: lastSync.Add(10 * time.Minute)},
		{ID: "bd-2", ExternalRef: &cleanRef, UpdatedAt: lastSync.Add(-10 * time.Minute)},
		{ID: "bd-3", ExternalRef: &seenRef, UpdatedAt: lastSync.Add(10 * time.Minute)},
	}
	// MOCK-3 already came back in the incremental fetch → must not re-fetch.
	fetched := []TrackerIssue{{ID: "uuid-3", Identifier: "MOCK-3", URL: seenRef}}

	mt := newMockTracker("mock")
	mt.issues = []TrackerIssue{
		{ID: "uuid-1", Identifier: "MOCK-1", Title: "one", URL: dirtyRef},
		{ID: "uuid-2", Identifier: "MOCK-2", Title: "two", URL: cleanRef},
	}
	batcher := &mockBatchFetchTracker{mockTracker: mt}

	e := NewEngine(batcher, newPureTestStore(locals...), "test-actor")
	hydrated, hydratedIDs, err := e.fetchPrelinkedIssues(ctx, fetched, locals, &lastSync)
	if err != nil {
		t.Fatalf("fetchPrelinkedIssues: %v", err)
	}

	if len(hydrated) != 1 || hydrated[0].Identifier != "MOCK-1" {
		t.Errorf("hydrated = %+v, want exactly MOCK-1", hydrated)
	}
	if !hydratedIDs["bd-1"] || len(hydratedIDs) != 1 {
		t.Errorf("hydratedIDs = %v, want {bd-1}", hydratedIDs)
	}
	if len(batcher.batchCalls) != 1 || len(batcher.batchCalls[0]) != 1 || batcher.batchCalls[0][0] != "MOCK-1" {
		t.Errorf("batch calls = %v, want one call with [MOCK-1]", batcher.batchCalls)
	}
	if batcher.fetchIssueCalls != 0 {
		t.Errorf("per-issue FetchIssue calls = %d, want 0", batcher.fetchIssueCalls)
	}
}

func TestFetchPrelinkedIssuesFallbackPerIssue(t *testing.T) {
	ctx := context.Background()
	lastSync := time.Now().Add(-1 * time.Hour)

	dirtyRef := "https://mock.test/MOCK-1"
	locals := []*types.Issue{
		{ID: "bd-1", ExternalRef: &dirtyRef, UpdatedAt: lastSync.Add(10 * time.Minute)},
	}

	mt := newMockTracker("mock")
	mt.issues = []TrackerIssue{{ID: "uuid-1", Identifier: "MOCK-1", Title: "one", URL: dirtyRef}}
	counter := &countingFetchTracker{mockTracker: mt}

	e := NewEngine(counter, newPureTestStore(locals...), "test-actor")
	hydrated, _, err := e.fetchPrelinkedIssues(ctx, nil, locals, &lastSync)
	if err != nil {
		t.Fatalf("fetchPrelinkedIssues: %v", err)
	}
	if len(hydrated) != 1 {
		t.Errorf("hydrated = %+v, want 1", hydrated)
	}
	if counter.fetchIssueCalls != 1 {
		t.Errorf("per-issue FetchIssue calls = %d, want 1 on the fallback path", counter.fetchIssueCalls)
	}
}

func TestShouldSyncSubresources(t *testing.T) {
	e := NewEngine(newMockTracker("mock"), newPureTestStore(), "test-actor")
	lastSync := time.Now().Add(-1 * time.Hour)
	fresh := &TrackerIssue{UpdatedAt: lastSync.Add(10 * time.Minute)}
	stale := &TrackerIssue{UpdatedAt: lastSync.Add(-10 * time.Minute)}

	boundary := &TrackerIssue{UpdatedAt: lastSync}
	cases := []struct {
		name     string
		opts     SyncOptions
		ext      *TrackerIssue
		lastSync *time.Time
		hydrated bool
		want     bool
	}{
		{"no lastSync backfills", SyncOptions{}, stale, nil, false, true},
		{"explicit issue request refreshes", SyncOptions{IssueIDs: []string{"bd-1"}}, stale, &lastSync, false, true},
		{"remote fresh syncs", SyncOptions{}, fresh, &lastSync, false, true},
		{"just-linked backfills", SyncOptions{}, stale, &lastSync, true, true},
		{"stale remote skips", SyncOptions{}, stale, &lastSync, false, false},
		// The incremental pull filter is updatedAt >= last_sync, so the
		// exact-boundary issue is returned by the pull and must sync.
		{"boundary updatedAt syncs", SyncOptions{}, boundary, &lastSync, false, true},
	}
	for _, tc := range cases {
		if got := e.shouldSyncSubresources(tc.opts, tc.ext, tc.lastSync, tc.hydrated); got != tc.want {
			t.Errorf("%s: shouldSyncSubresources = %v, want %v", tc.name, got, tc.want)
		}
	}
}

// snapshotProbeStore layers storage.LinearIssueSnapshotStore over
// pureTestStore, recording reads and writes so tests can assert which
// issues paid the per-issue field-scoped detection cost and which received
// first-sync baselines. (The cgo-tagged suite has a richer
// recordingSnapshotStore; this pure variant exists so the bd-kqt fast-skip
// is locked on every platform.)
type snapshotProbeStore struct {
	*pureTestStore
	storage.LinearIssueSnapshotStore
	rows        map[string]*storage.LinearIssueSnapshot
	getCalls    []string
	upsertCalls []string
}

func (s *snapshotProbeStore) GetLinearIssueSnapshot(_ context.Context, issueID string) (*storage.LinearIssueSnapshot, error) {
	s.getCalls = append(s.getCalls, issueID)
	return s.rows[issueID], nil
}

func (s *snapshotProbeStore) UpsertLinearIssueSnapshot(_ context.Context, snap *storage.LinearIssueSnapshot) error {
	s.upsertCalls = append(s.upsertCalls, snap.IssueID)
	if s.rows == nil {
		s.rows = map[string]*storage.LinearIssueSnapshot{}
	}
	s.rows[snap.IssueID] = snap
	return nil
}

func (s *snapshotProbeStore) DeleteLinearIssueSnapshot(_ context.Context, _ string) error { return nil }

// snapshotProbeTracker adds PostPullSnapshotter to mockBatchFetchTracker so
// the engine takes the field-scoped conflict path. RecordPullSnapshot
// mirrors the real Linear tracker by routing the baseline into the store.
type snapshotProbeTracker struct {
	*mockBatchFetchTracker
	store *snapshotProbeStore
}

func (s *snapshotProbeTracker) RecordPullSnapshot(ctx context.Context, localBeadID string, fetched TrackerIssue) error {
	return s.store.UpsertLinearIssueSnapshot(ctx, &storage.LinearIssueSnapshot{
		IssueID: localBeadID,
		Title:   fetched.Title,
	})
}

// TestDetectConflictsSkipsFieldScopedWorkWhenBothSidesClean locks the bd-kqt
// wall-time fix and its codex round-3 correction: linked beads untouched on
// both sides since last_sync skip the field-scoped detection cost, but ONLY
// when a baseline snapshot already exists — a clean issue with no baseline
// must still get its first-sync baseline written, or a later both-sides
// change would re-baseline the changed remote and miss the conflict.
func TestDetectConflictsSkipsFieldScopedWorkWhenBothSidesClean(t *testing.T) {
	ctx := context.Background()
	lastSync := time.Now().Add(-1 * time.Hour)

	baselinedRef := "https://mock.test/MOCK-1"
	dirtyRef := "https://mock.test/MOCK-2"
	unbaselinedRef := "https://mock.test/MOCK-3"
	locals := []*types.Issue{
		{ID: "bd-clean-baselined", ExternalRef: &baselinedRef, UpdatedAt: lastSync.Add(-30 * time.Minute)},
		{ID: "bd-dirty", ExternalRef: &dirtyRef, UpdatedAt: lastSync.Add(30 * time.Minute)},
		{ID: "bd-clean-unbaselined", ExternalRef: &unbaselinedRef, UpdatedAt: lastSync.Add(-30 * time.Minute)},
	}

	mt := newMockTracker("mock")
	mt.issues = []TrackerIssue{
		{ID: "uuid-1", Identifier: "MOCK-1", Title: "clean", UpdatedAt: lastSync.Add(-45 * time.Minute)},
		{ID: "uuid-2", Identifier: "MOCK-2", Title: "dirty", UpdatedAt: lastSync.Add(-45 * time.Minute)},
		{ID: "uuid-3", Identifier: "MOCK-3", Title: "unbaselined", UpdatedAt: lastSync.Add(-45 * time.Minute)},
	}

	store := &snapshotProbeStore{
		pureTestStore: newPureTestStore(locals...),
		rows: map[string]*storage.LinearIssueSnapshot{
			"bd-clean-baselined": {IssueID: "bd-clean-baselined", Title: "clean"},
		},
	}
	store.localMetadata["mock.last_sync"] = lastSync.UTC().Format(time.RFC3339Nano)
	probe := &snapshotProbeTracker{&mockBatchFetchTracker{mockTracker: mt}, store}

	e := NewEngine(probe, store, "test-actor")
	conflicts, err := e.DetectConflicts(ctx, SyncOptions{})
	if err != nil {
		t.Fatalf("DetectConflicts: %v", err)
	}
	if len(conflicts) != 0 {
		t.Errorf("conflicts = %+v, want none (remote unchanged)", conflicts)
	}
	// The baselined clean issue is read once (the skip check) and never
	// enters field-scoped detection; the unbaselined clean issue falls
	// through and receives its first-sync baseline.
	baselinedReads := 0
	for _, id := range store.getCalls {
		if id == "bd-clean-baselined" {
			baselinedReads++
		}
	}
	if baselinedReads != 1 {
		t.Errorf("bd-clean-baselined snapshot reads = %d, want exactly 1 (the skip check); all reads: %v", baselinedReads, store.getCalls)
	}
	wroteUnbaselined := false
	for _, id := range store.upsertCalls {
		if id == "bd-clean-unbaselined" {
			wroteUnbaselined = true
		}
		if id == "bd-clean-baselined" {
			t.Errorf("bd-clean-baselined was re-baselined; upserts: %v", store.upsertCalls)
		}
	}
	if !wroteUnbaselined {
		t.Errorf("bd-clean-unbaselined did not receive its first-sync baseline; upserts: %v", store.upsertCalls)
	}
}

func TestExternalRefChangedAfterSkipsHistoryQueryWhenClean(t *testing.T) {
	ctx := context.Background()

	lastSync := time.Now()
	ref := "https://mock.test/MOCK-1"
	clean := &types.Issue{
		ID:          "bd-1",
		ExternalRef: &ref,
		CreatedAt:   lastSync.Add(-2 * time.Hour),
		UpdatedAt:   lastSync.Add(-1 * time.Hour),
	}

	store := &historyQuerierStore{prevRef: ref, prevFound: true}
	e := NewEngine(newMockTracker("mock"), store, "test-actor")
	changed, err := e.externalRefChangedAfter(ctx, clean, ref, lastSync)
	if err != nil {
		t.Fatalf("externalRefChangedAfter: %v", err)
	}
	if changed {
		t.Error("clean issue reported as ref-changed")
	}
	if store.calls != 0 {
		t.Errorf("clean issue issued %d history queries, want 0 (bd-kqt pre-filter)", store.calls)
	}

	// A dirty issue must still consult history.
	dirty := &types.Issue{
		ID:          "bd-2",
		ExternalRef: &ref,
		CreatedAt:   lastSync.Add(-2 * time.Hour),
		UpdatedAt:   lastSync.Add(1 * time.Minute),
	}
	changed, err = e.externalRefChangedAfter(ctx, dirty, ref, lastSync)
	if err != nil {
		t.Fatalf("externalRefChangedAfter(dirty): %v", err)
	}
	if changed {
		t.Error("dirty issue with unchanged ref reported as ref-changed")
	}
	if store.calls != 1 {
		t.Errorf("dirty issue issued %d history queries, want exactly 1", store.calls)
	}
}
