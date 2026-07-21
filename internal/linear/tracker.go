package linear

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/steveyegge/beads/internal/config"
	"github.com/steveyegge/beads/internal/storage"
	"github.com/steveyegge/beads/internal/tracker"
	"github.com/steveyegge/beads/internal/types"
)

var (
	_ tracker.BatchPushTracker   = (*Tracker)(nil)
	_ tracker.BatchPushDryRunner = (*Tracker)(nil)
	_ tracker.BatchIssueFetcher  = (*Tracker)(nil)
)

func init() {
	tracker.Register("linear", func() tracker.IssueTracker {
		return &Tracker{}
	})
}

// Tracker implements tracker.IssueTracker for Linear.
type Tracker struct {
	clients   map[string]*Client // keyed by team ID
	config    *MappingConfig
	store     storage.Storage
	teamIDs   []string // ordered list of configured team IDs
	projectID string

	// Label sync state (set via SetLabelSyncConfig from cmd/bd; defaults disable sync).
	labelSyncEnabled bool
	labelExclude     map[string]bool
	labelCreateScope LabelScope
	labelWarnFn      func(format string, args ...interface{}) // optional, for resolveLabelIDs warnings
}

// SetTeamIDs sets the team IDs before Init(). When set, Init() uses these
// instead of reading from config. This supports the --team CLI flag.
func (t *Tracker) SetTeamIDs(ids []string) {
	t.teamIDs = ids
}

// SetLabelSyncConfig configures bidirectional label sync. When enabled is
// false (the default), label-related code paths short-circuit and the legacy
// behavior is preserved. The warn callback receives messages for non-fatal
// failures (e.g., CreateLabel rate limits); pass nil to discard.
func (t *Tracker) SetLabelSyncConfig(enabled bool, exclude map[string]bool, scope LabelScope, warn func(string, ...interface{})) {
	t.labelSyncEnabled = enabled
	t.labelExclude = exclude
	t.labelCreateScope = scope
	t.labelWarnFn = warn
}

// LabelSyncEnabled is read by cmd/bd to decide whether to install the
// label-aware ContentEqual hook and PullHooks.ReconcileLabels callback.
func (t *Tracker) LabelSyncEnabled() bool { return t.labelSyncEnabled }

// LabelExclude is read by cmd/bd hooks to pass into the reconciler.
func (t *Tracker) LabelExclude() map[string]bool { return t.labelExclude }

// LoadSnapshot reads the persisted snapshot for an issue using the tracker's
// own store. Returns nil when no snapshot exists or no store is configured.
// Reconciler treats nil and empty-slice equivalently. Used by the pull/push
// hooks AND the dry-run gate in ContentEqual.
//
// This method does its own short-lived read transaction; it does NOT need to
// participate in any caller's transaction (snapshots are read-only here).
//
// Note: this opens a short read-only transaction per call. In a sync of N
// issues, ContentEqual + UpdateIssue collectively read snapshots N times.
// Acceptable for v1; a future cache could short-circuit repeated reads.
func (t *Tracker) LoadSnapshot(ctx context.Context, issueID string) ([]SnapshotEntry, error) {
	if t.store == nil {
		return nil, nil
	}
	var entries []storage.LinearLabelSnapshotEntry
	err := t.store.RunInTransaction(ctx, "linear: read snapshot", func(tx storage.Transaction) error {
		var err error
		entries, err = tx.GetLinearLabelSnapshot(ctx, issueID)
		return err
	})
	if err != nil {
		return nil, err
	}
	out := make([]SnapshotEntry, len(entries))
	for i, e := range entries {
		out[i] = SnapshotEntry{Name: e.LabelName, ID: e.LabelID}
	}
	return out, nil
}

func (t *Tracker) Name() string         { return "linear" }
func (t *Tracker) DisplayName() string  { return "Linear" }
func (t *Tracker) ConfigPrefix() string { return "linear" }

func (t *Tracker) Init(ctx context.Context, store storage.Storage) error {
	t.store = store

	// Resolve authentication: OAuth client-credentials takes precedence over API key.
	oauthClientID, _ := t.getConfig(ctx, "linear.oauth_client_id", "LINEAR_OAUTH_CLIENT_ID")
	oauthClientSecret, _ := t.getConfig(ctx, "linear.oauth_client_secret", "LINEAR_OAUTH_CLIENT_SECRET")
	hasOAuth := oauthClientID != "" && oauthClientSecret != ""

	var apiKey string
	if !hasOAuth {
		apiKey, _ = t.getConfig(ctx, "linear.api_key", "LINEAR_API_KEY")
		if apiKey == "" {
			return fmt.Errorf("Linear authentication not configured\n" +
				"Options:\n" +
				"  OAuth (for CI):  export LINEAR_OAUTH_CLIENT_ID=... LINEAR_OAUTH_CLIENT_SECRET=...\n" +
				"  API key (devs):  export LINEAR_API_KEY=... or bd config set linear.api_key \"YOUR_API_KEY\"")
		}
	}

	// Resolve team IDs: use pre-set IDs (from CLI), or fall back to config.
	if len(t.teamIDs) == 0 {
		pluralVal, _ := t.getConfig(ctx, "linear.team_ids", "LINEAR_TEAM_IDS")
		singularVal, _ := t.getConfig(ctx, "linear.team_id", "LINEAR_TEAM_ID")
		t.teamIDs = tracker.ResolveProjectIDs(nil, pluralVal, singularVal)
		if len(t.teamIDs) == 0 {
			return fmt.Errorf("Linear team ID not configured (set linear.team_id, linear.team_ids, or LINEAR_TEAM_ID)")
		}
	}

	// Read optional endpoint and project ID.
	var endpoint, projectID string
	if store != nil {
		endpoint, _ = store.GetConfig(ctx, "linear.api_endpoint")
		projectID, _ = store.GetConfig(ctx, "linear.project_id")
		if projectID != "" {
			t.projectID = projectID
		}
	}

	// Read optional rate-limit floor (LINEAR_RATE_LIMIT_FLOOR env or linear.rate_limit_floor config).
	var rateLimitFloor int
	if floorStr, _ := t.getConfig(ctx, "linear.rate_limit_floor", "LINEAR_RATE_LIMIT_FLOOR"); floorStr != "" {
		if v, err := strconv.Atoi(strings.TrimSpace(floorStr)); err == nil && v >= 0 {
			rateLimitFloor = v
		}
	}

	// Create per-team clients upfront for O(1) routing.
	t.clients = make(map[string]*Client, len(t.teamIDs))
	for _, teamID := range t.teamIDs {
		var client *Client
		if hasOAuth {
			client = NewOAuthClient(OAuthConfig{
				ClientID:     oauthClientID,
				ClientSecret: oauthClientSecret,
			}, teamID)
		} else {
			client = NewClient(apiKey, teamID)
		}
		if endpoint != "" {
			client = client.WithEndpoint(endpoint)
		}
		if projectID != "" {
			client = client.WithProjectID(projectID)
		}
		if rateLimitFloor > 0 {
			client = client.WithRateLimitFloor(rateLimitFloor)
		}
		t.clients[teamID] = client
	}

	t.config = LoadMappingConfig(&configLoaderAdapter{ctx: ctx, store: store})
	return nil
}

func (t *Tracker) Validate() error {
	if len(t.clients) == 0 {
		return fmt.Errorf("Linear tracker not initialized")
	}
	return nil
}

func (t *Tracker) Close() error { return nil }

func (t *Tracker) FetchIssues(ctx context.Context, opts tracker.FetchOptions) ([]tracker.TrackerIssue, error) {
	state := opts.State
	if state == "" {
		state = "all"
	}

	seen := make(map[string]bool)
	var result []tracker.TrackerIssue

	for _, teamID := range t.teamIDs {
		client := t.clients[teamID]
		if client == nil {
			continue
		}

		var issues []Issue
		var err error
		if opts.Since != nil {
			issues, err = client.FetchIssuesSince(ctx, state, *opts.Since)
		} else {
			issues, err = client.FetchIssues(ctx, state)
		}
		if err != nil {
			return result, fmt.Errorf("fetching issues from team %s: %w", teamID, err)
		}

		for _, li := range issues {
			if seen[li.ID] {
				continue
			}
			seen[li.ID] = true
			result = append(result, linearToTrackerIssue(&li))
		}
	}

	return result, nil
}

func (t *Tracker) FetchIssue(ctx context.Context, identifier string) (*tracker.TrackerIssue, error) {
	// Try the primary client first (first team), then others.
	for _, teamID := range t.teamIDs {
		client := t.clients[teamID]
		if client == nil {
			continue
		}
		li, err := client.FetchIssueByIdentifier(ctx, identifier)
		if err != nil {
			continue // Issue might belong to a different team.
		}
		if li != nil {
			ti := linearToTrackerIssue(li)
			return &ti, nil
		}
	}
	return nil, nil
}

// BatchFetchIssues resolves many Linear identifiers to their current remote
// state using batched number-in queries — ceil(N/MaxPageSize) requests per
// team instead of N (tracker.BatchIssueFetcher, bd-kqt). Team routing mirrors
// FetchIssue: the primary team is tried first, and identifiers it doesn't
// resolve are retried against the remaining teams. Identifiers not found in
// any team are absent from the result map.
func (t *Tracker) BatchFetchIssues(ctx context.Context, identifiers []string) (map[string]*tracker.TrackerIssue, error) {
	issues, _, err := t.batchFetchIssuesAcrossTeams(ctx, identifiers)
	result := make(map[string]*tracker.TrackerIssue, len(issues))
	for identifier, li := range issues {
		ti := linearToTrackerIssue(li)
		result[identifier] = &ti
	}
	if err != nil {
		return result, fmt.Errorf("batch fetching issues: %w", err)
	}
	return result, nil
}

func (t *Tracker) CreateIssue(ctx context.Context, issue *types.Issue) (*tracker.TrackerIssue, error) {
	client := t.primaryClient()
	if client == nil {
		return nil, fmt.Errorf("no Linear client available")
	}

	priority := PriorityToLinear(issue.Priority, t.config)

	stateID, err := t.findStateIDForIssue(ctx, client, issue)
	if err != nil {
		return nil, fmt.Errorf("finding state for status %s: %w", issue.Status, err)
	}

	// Label sync: filter excluded labels, resolve to IDs (auto-create as needed),
	// then pass to client.CreateIssue. Different from UpdateIssue's flow because
	// there's no fresh Linear state to reconcile against — the issue doesn't
	// exist yet. Just push what the bead has.
	var labelIDs []string
	var snapshotToWrite []storage.LinearLabelSnapshotEntry
	if t.labelSyncEnabled && len(issue.Labels) > 0 {
		toResolve := make([]string, 0, len(issue.Labels))
		for _, name := range issue.Labels {
			if t.labelExclude == nil || !t.labelExclude[strings.ToLower(name)] {
				toResolve = append(toResolve, name)
			}
		}
		resolved, err := resolveLabelIDs(ctx, client, toResolve, t.labelCreateScope, t.labelWarnFn)
		if err != nil {
			return nil, err
		}
		// Build labelIDs (deduplicated by ID — same case-mismatch concern as UpdateIssue)
		// and snapshot rows in lockstep.
		labelIDSet := make(map[string]bool)
		for _, n := range toResolve {
			label, ok := resolved[n]
			if !ok {
				continue // CreateLabel failed for this name; skip and let next sync retry
			}
			if labelIDSet[label.ID] {
				continue // dedupe
			}
			labelIDSet[label.ID] = true
			labelIDs = append(labelIDs, label.ID)
			snapshotToWrite = append(snapshotToWrite, storage.LinearLabelSnapshotEntry{
				LabelID:   label.ID,
				LabelName: label.Name, // Linear's display case (preserved via LabelsByName)
			})
		}
	}

	// Use issue.Description as-is: the sync engine's FormatDescription hook
	// (BuildLinearDescription) has already merged AcceptanceCriteria/Design/Notes
	// into the description before calling CreateIssue. Calling BuildLinearDescription
	// here a second time would duplicate those sections for issues with structured fields.
	description := issue.Description

	// Use idempotent creation when we have enough bead metadata to generate
	// a stable marker. This prevents duplicate Linear issues when sync is
	// interrupted between the API create call and the local external_ref
	// write-back. Both paths fall through to the shared post-create snapshot
	// writes below (label + issue snapshot baselines for field-scoped
	// conflict detection).
	var created *Issue
	if issue.ID != "" && issue.CreatedBy != "" {
		marker := GenerateIdempotencyMarker(issue.ID, issue.CreatedBy, issue.CreatedAt.UnixNano())
		var deduped bool
		created, deduped, err = client.CreateIssueIdempotent(ctx, issue.Title, description, priority, stateID, labelIDs, marker)
		if err == nil && deduped {
			fmt.Fprintf(os.Stderr, "linear: dedup — reusing existing issue %s for bead %s\n", created.Identifier, issue.ID)
		}
	} else {
		created, err = client.CreateIssue(ctx, issue.Title, description, priority, stateID, labelIDs)
	}
	if err != nil {
		return nil, err
	}

	if t.labelSyncEnabled && len(snapshotToWrite) > 0 {
		if err := t.writeSnapshot(ctx, issue.ID, snapshotToWrite); err != nil {
			if t.labelWarnFn != nil {
				t.labelWarnFn("snapshot write failed for new bead %s: %v", issue.ID, err)
			}
		}
	}

	// bd-ajn: persist the per-issue snapshot baseline for field-scoped
	// conflict detection on the next sync. The CreateIssue response is
	// the full post-create Linear state, so we know every field.
	if err := t.writeIssueSnapshot(ctx, issue.ID, created); err != nil && t.labelWarnFn != nil {
		t.labelWarnFn("issue snapshot write failed for new bead %s: %v", issue.ID, err)
	}

	ti := linearToTrackerIssue(created)
	return &ti, nil
}

// UpdateIssue pushes a bead's changes to Linear. When label sync is enabled,
// it also runs the label reconciler and pushes labelIds in the same mutation,
// then persists a fresh snapshot reflecting the post-push agreed state.
//
// Pull-side label reconciliation (PullHooks.ReconcileLabels in cmd/bd/linear.go)
// runs in the engine's pull-side transaction and writes its own snapshot
// reflecting only beads-side mutations. When push runs after pull in the same
// sync cycle, push's later snapshot write replaces pull's — they don't conflict
// because they cover the same set of labels with consistent IDs.
// UpdateIssue is the standard tracker.IssueTracker entry point. Delegates to
// UpdateIssueWithRemote with no pre-fetched remote — the implementation will
// fall back to fetching what it needs (or pushing stateId unconditionally
// when no remote is available).
//
// Engines that fetch the remote upfront should prefer UpdateIssueWithRemote
// (via the RemoteAwareUpdater capability) so this tracker can preserve
// remote-owned states like "In Review" driven by Linear's GitHub PR
// automation.
func (t *Tracker) UpdateIssue(ctx context.Context, externalID string, issue *types.Issue) (*tracker.TrackerIssue, error) {
	return t.UpdateIssueWithRemote(ctx, externalID, issue, nil)
}

// UpdateIssueWithRemote is the RemoteAwareUpdater entry point. When `remote`
// is non-nil and its mapped status matches the local bead's status, the
// stateId field is omitted from the GraphQL update — preserving the Linear
// state exactly as it is on the remote (e.g., "In Review" set by a GitHub
// PR opening). This stops bd from collapsing automation-owned states into
// the bead's coarser status when only metadata fields (title, description,
// priority, labels) actually changed.
//
// When `remote` is nil, the legacy behavior is preserved: stateId is always
// resolved and pushed. This is the conservative default for paths that
// don't have a fresh remote on hand.
//
// The caller's pre-fetched remote is also threaded into the label-sync
// reconciler, avoiding a second round-trip when label sync is enabled.
func (t *Tracker) UpdateIssueWithRemote(ctx context.Context, externalID string, issue *types.Issue, remote *tracker.TrackerIssue) (*tracker.TrackerIssue, error) {
	client := t.clientForExternalID(ctx, externalID)
	if client == nil {
		return nil, fmt.Errorf("cannot determine Linear team for issue %s", externalID)
	}

	// Label pushes are handled by the label-sync reconciler below (gated on
	// linear.label_sync_enabled, snapshot-based) rather than the plain
	// label-cache mapping — see reconcileAndBuildLabelUpdate.
	mapper := t.FieldMapper()
	updates := mapper.IssueToTracker(issue)

	// Skip stateId when the remote already maps to local status — preserves
	// Linear-owned states like "In Review" driven by GitHub PR automation.
	// Falls through to the legacy explicit-resolve path when remote is nil
	// or when its state genuinely differs from local.
	if !t.remoteStatusMatchesLocal(remote, issue) {
		stateID, err := t.findStateIDForIssue(ctx, client, issue)
		if err != nil {
			return nil, fmt.Errorf("finding state for status %s: %w", issue.Status, err)
		}
		if stateID != "" {
			updates["stateId"] = stateID
		}
	}

	// Label sync (decision #12: independent path, runs even when other fields are equal).
	// Threads the engine's pre-fetched remote into reconcileAndBuildLabelUpdate
	// to avoid a second FetchIssueByIdentifier round-trip.
	var snapshotToWrite []storage.LinearLabelSnapshotEntry
	if t.labelSyncEnabled {
		ids, snap, err := t.reconcileAndBuildLabelUpdate(ctx, client, externalID, issue, remote)
		if err != nil {
			return nil, err
		}
		updates["labelIds"] = ids
		snapshotToWrite = snap
	}

	updated, err := client.UpdateIssue(ctx, externalID, updates)
	if err != nil {
		return nil, err
	}

	// After successful push, persist the snapshot. Done OUTSIDE the local
	// transaction since the engine doesn't expose a tx here. If the snapshot
	// write fails, the push has already happened — log and move on; the next
	// sync's reconciler will compute correctly from prior snapshot state and
	// converge.
	if t.labelSyncEnabled {
		if err := t.writeSnapshot(ctx, issue.ID, snapshotToWrite); err != nil {
			if t.labelWarnFn != nil {
				t.labelWarnFn("snapshot write failed for %s: %v", issue.ID, err)
			}
		}
	}

	// bd-ajn: refresh the per-issue snapshot to the post-update state.
	// Without this, the next sync would diff against the pre-update
	// snapshot and incorrectly see "Linear changed these fields" for
	// the values we just pushed.
	if err := t.writeIssueSnapshot(ctx, issue.ID, updated); err != nil && t.labelWarnFn != nil {
		t.labelWarnFn("issue snapshot write failed for %s: %v", issue.ID, err)
	}

	ti := linearToTrackerIssue(updated)
	return &ti, nil
}

// PreviewUpdate implements tracker.DryRunPreviewer. Returns the categorization
// of what UpdateIssueWithRemote would do, so the engine's dry-run path prints
// accurate output (distinguishing "state preserved" from "state change") rather
// than the generic "Would update".
//
// Mirrors the state-skip decision in UpdateIssueWithRemote: when remote's
// mapped status equals local AND (for closed beads) the close-reason
// classification agrees with the remote state-type, state is preserved;
// otherwise it changes.
//
// Does NOT call Linear's API — pure local computation against the already-
// fetched remote.
func (t *Tracker) PreviewUpdate(_ context.Context, _ string, issue *types.Issue, remote *tracker.TrackerIssue) tracker.DryRunDecision {
	if remote == nil {
		// Engine couldn't fetch — UpdateIssueWithRemote will fall through to
		// legacy behavior (always push stateId). Treat as a state change for
		// dry-run accuracy.
		return tracker.DryRunStateChange
	}
	if t.remoteStatusMatchesLocal(remote, issue) {
		return tracker.DryRunStatePreserved
	}
	return tracker.DryRunStateChange
}

// remoteStatusMatchesLocal returns true when the remote Linear state, after
// being mapped through the tracker's state config, matches the local beads
// status. Lets UpdateIssueWithRemote skip pushing stateId when the remote
// already represents the same logical status — preserving Linear-owned
// states like "In Review" (driven by GitHub PR automation) when local
// metadata changes don't actually transition the issue.
//
// Returns false when remote is nil, when remote.State isn't a *State (lossy
// conversion path — shouldn't happen for Linear, but defensive), or when
// the mapped status genuinely differs from local.
//
// Closed-bead exception: both Linear "Done" (type=completed) and "Canceled"
// (type=canceled) map through StateToBeadsStatus to the coarse Beads
// `closed`. To prevent silently bypassing a Done↔Canceled transition driven
// by close_reason, this helper additionally requires that the remote state's
// type agree with what the local close_reason would route to. Mirrors the
// same check in PushFieldsEqual (mapping.go:633+).
func (t *Tracker) remoteStatusMatchesLocal(remote *tracker.TrackerIssue, issue *types.Issue) bool {
	if remote == nil || remote.State == nil || issue == nil {
		return false
	}
	state, ok := remote.State.(*State)
	if !ok || state == nil {
		return false
	}
	if StateToBeadsStatus(state, t.config) != issue.Status {
		return false
	}
	// For closed beads, require close-reason agreement with the remote
	// state-type so Done↔Canceled transitions still push stateId even
	// though both terminal states map to the same coarse Beads status.
	if issue.Status == types.StatusClosed {
		localIsCanceled := ClassifyCloseReason(issue.CloseReason) == CloseIntentCanceled
		remoteIsCanceled := strings.EqualFold(strings.TrimSpace(state.Type), "canceled")
		if localIsCanceled != remoteIsCanceled {
			return false
		}
	}
	return true
}

// reconcileAndBuildLabelUpdate fetches fresh Linear labels (or reuses the
// engine's pre-fetched remote when available), runs the reconciler, resolves
// new label IDs (with auto-create), deduplicates, and returns the labelIds
// slice to send to Linear AND the snapshot rows to persist after a successful
// push. Caller is responsible for actually persisting after push.
//
// Returns (nil, nil, nil) when label sync is disabled (caller should not call
// in that case, but defensive fallthrough is harmless).
//
// V1 behavior: if no pre-fetched remote is available AND FetchIssueByIdentifier
// fails (rate limit, network, missing issue), the entire UpdateIssue aborts —
// including non-label fields like status/title/description. We can't safely
// send labelIds without knowing the current Linear state. Future: degrade to
// "push other fields, skip labels, reconcile labels next sync" when the fetch
// fails for transient reasons.
func (t *Tracker) reconcileAndBuildLabelUpdate(
	ctx context.Context, client *Client, externalID string, issue *types.Issue, prefetched *tracker.TrackerIssue,
) (labelIDs []string, snapshotToWrite []storage.LinearLabelSnapshotEntry, err error) {
	// Prefer the engine's pre-fetched remote (raw type-asserted) to avoid an
	// extra API round-trip. Falls back to fetching when the engine didn't
	// provide one or the type assertion fails.
	var fresh *Issue
	if prefetched != nil {
		if linearIssue, ok := prefetched.Raw.(*Issue); ok && linearIssue != nil {
			fresh = linearIssue
		}
	}
	if fresh == nil {
		fresh, err = client.FetchIssueByIdentifier(ctx, externalID)
		if err != nil {
			return nil, nil, fmt.Errorf("fetch for label reconciliation: %w", err)
		}
	}
	if fresh == nil {
		return nil, nil, fmt.Errorf("label reconcile: issue %s not found in Linear", externalID)
	}

	linearLabels := make([]LinearLabel, 0)
	if fresh.Labels != nil {
		for _, l := range fresh.Labels.Nodes {
			linearLabels = append(linearLabels, LinearLabel{Name: l.Name, ID: l.ID})
		}
	}

	snap, err := t.LoadSnapshot(ctx, issue.ID)
	if err != nil {
		return nil, nil, fmt.Errorf("load snapshot: %w", err)
	}

	res := ReconcileLabels(LabelReconcileInput{
		Beads:    issue.Labels,
		Linear:   linearLabels,
		Snapshot: snap,
		Exclude:  t.labelExclude,
	})

	resolved, err := resolveLabelIDs(ctx, client, res.AddToLinear, t.labelCreateScope, t.labelWarnFn)
	if err != nil {
		return nil, nil, err
	}

	// Build labelIds, deduplicating by ID. Duplicates can occur when
	// case-insensitive resolution maps a bead label like "bug" to the
	// SAME Linear ID that's already in linearLabels under "Bug" — the
	// reconciler treated them as distinct (case-sensitive matching), but
	// LabelsByName resolved them to the same ID. Without dedup we'd send
	// `[L1, L1]` to Linear AND fail the snapshot insert on PK conflict.
	removeSet := make(map[string]bool, len(res.RemoveFromLinear))
	for _, id := range res.RemoveFromLinear {
		removeSet[id] = true
	}
	labelIDSet := make(map[string]bool)
	labelIDs = make([]string, 0)
	for _, l := range linearLabels {
		if !removeSet[l.ID] && !labelIDSet[l.ID] {
			labelIDs = append(labelIDs, l.ID)
			labelIDSet[l.ID] = true
		}
	}
	for _, n := range res.AddToLinear {
		if l, ok := resolved[n]; ok && !labelIDSet[l.ID] {
			labelIDs = append(labelIDs, l.ID)
			labelIDSet[l.ID] = true
		}
	}

	// Build the snapshot to persist. Reflects the post-push agreed state:
	// every labelID we just sent to Linear, with its name. Skipped labels
	// (CreateLabel failures) are absent — they retry next sync.
	//
	// CRITICAL: persist Linear's display case for the label name, NOT the
	// bead's spelling. Otherwise, when LabelsByName matches case-insensitively
	// (e.g. bead "bug" → Linear "Bug" with id L1), we'd persist {L1, "bug"}
	// in the snapshot. Next sync's reconciler sees snapshot.Name="bug" and
	// Linear.Name="Bug" with the same ID L1 → false rename detection → infinite
	// churn. We pre-populate nameByID with Linear's display case from the
	// fetched labels, then ONLY add resolved entries for IDs not already
	// known (i.e., labels that were freshly auto-created with the bead's
	// spelling — those names ARE Linear's spelling, since CreateLabel just
	// created them with that name).
	nameByID := make(map[string]string, len(linearLabels)+len(resolved))
	for _, l := range linearLabels {
		nameByID[l.ID] = l.Name // Linear's display case wins for known IDs
	}
	for _, label := range resolved {
		if _, alreadyKnown := nameByID[label.ID]; !alreadyKnown {
			nameByID[label.ID] = label.Name // For freshly-created, Name == bead's spelling, which is now Linear's spelling too.
		}
	}
	snapshotToWrite = make([]storage.LinearLabelSnapshotEntry, 0, len(labelIDs))
	for _, id := range labelIDs {
		snapshotToWrite = append(snapshotToWrite, storage.LinearLabelSnapshotEntry{
			LabelID:   id,
			LabelName: nameByID[id],
		})
	}
	return labelIDs, snapshotToWrite, nil
}

// writeSnapshot persists the post-sync label snapshot for an issue.
// Used by both UpdateIssue and CreateIssue after a successful push.
func (t *Tracker) writeSnapshot(ctx context.Context, issueID string, entries []storage.LinearLabelSnapshotEntry) error {
	if t.store == nil {
		return nil
	}
	return t.store.RunInTransaction(ctx, fmt.Sprintf("linear: snapshot labels %s", issueID), func(tx storage.Transaction) error {
		return tx.PutLinearLabelSnapshot(ctx, issueID, entries)
	})
}

// BatchPush implements tracker.BatchPushTracker. It partitions issues into
// creates and updates, uses issueBatchCreate for new issues (chunked at 50),
// and falls back to per-issue UpdateIssue for updates (since issueBatchUpdate
// applies the same fields to all IDs, which doesn't fit per-issue field diffs).
//
// Skip semantics: existing issues are fetched and compared with PushFieldsEqual
// before updating; unchanged issues are skipped. forceIDs bypasses this check.
//
// Multi-team: state IDs are resolved using the per-team workflow state cache,
// so updates to issues belonging to different teams use the correct state list.
//
// Result mapping: batch-create results are matched by title rather than array
// index, since Linear's API does not guarantee response order matches input order.
func (t *Tracker) BatchPush(ctx context.Context, issues []*types.Issue, forceIDs map[string]bool) (*tracker.BatchPushResult, error) {
	return t.executeBatchPush(ctx, issues, forceIDs, false)
}

// BatchPushDryRun implements tracker.BatchPushDryRunner. It runs the exact
// BatchPush pipeline — state-cache builds, batched remote prefetch, and the
// PushFieldsEqual unchanged-skip — but records outcomes instead of sending
// mutations (bd-q3y). Sharing executeBatchPush keeps preview and wet-run
// candidate outcomes from drifting (the bd-f0t failure mode): without this,
// the engine's fallback preview reported "Would update" for every ref-bearing
// bead, making the whole mirror look phantom-push-dirty on each dry-run.
func (t *Tracker) BatchPushDryRun(ctx context.Context, issues []*types.Issue, forceIDs map[string]bool) (*tracker.BatchPushResult, error) {
	return t.executeBatchPush(ctx, issues, forceIDs, true)
}

// executeBatchPush is the shared implementation behind BatchPush (dryRun =
// false) and BatchPushDryRun (dryRun = true). Dry-run mode issues the same
// read-side requests (per-team state caches, batched update-candidate
// prefetch) so the preview reflects real resolvability, but replaces every
// mutation with the result entry the wet run would produce.
func (t *Tracker) executeBatchPush(ctx context.Context, issues []*types.Issue, forceIDs map[string]bool, dryRun bool) (*tracker.BatchPushResult, error) {
	client := t.primaryClient()
	if client == nil {
		return nil, fmt.Errorf("no Linear client available")
	}

	// Build per-team state caches so that updates to issues belonging to different
	// teams resolve workflow state IDs against the correct team's state list.
	teamCaches := make(map[string]*StateCache, len(t.teamIDs))
	for _, teamID := range t.teamIDs {
		teamClient := t.clients[teamID]
		if teamClient == nil {
			continue
		}
		cache, err := BuildStateCache(ctx, teamClient)
		if err != nil {
			return nil, fmt.Errorf("building state cache for team %s: %w", teamID, err)
		}
		teamCaches[teamID] = cache
	}

	// The primary team's cache is used for creates, which always target the primary team.
	primaryCache := teamCaches[t.teamIDs[0]]
	if primaryCache == nil {
		return nil, fmt.Errorf("building state cache: no cache for primary team %s", t.teamIDs[0])
	}

	teamLabelCaches := make(map[string]*LabelCache, len(t.teamIDs))
	for _, teamID := range t.teamIDs {
		teamClient := t.clients[teamID]
		if teamClient == nil {
			continue
		}
		lc, err := BuildLabelCache(ctx, teamClient)
		if err != nil {
			return nil, fmt.Errorf("building label cache for team %s: %w", teamID, err)
		}
		teamLabelCaches[teamID] = lc
	}
	primaryLabelCache := teamLabelCaches[t.teamIDs[0]]
	if primaryLabelCache == nil {
		return nil, fmt.Errorf("building label cache: no cache for primary team %s", t.teamIDs[0])
	}

	result := &tracker.BatchPushResult{}

	var toCreate []*types.Issue
	var toUpdate []*types.Issue

	for _, issue := range issues {
		extRef := ""
		if issue.ExternalRef != nil {
			extRef = *issue.ExternalRef
		}
		if extRef == "" || !IsLinearExternalRef(extRef) {
			toCreate = append(toCreate, issue)
		} else {
			toUpdate = append(toUpdate, issue)
		}
	}

	// Batch create new issues.
	if len(toCreate) > 0 {
		// Partition into unique-title (safe for batch) and duplicate-title (single-create).
		// Title-based result correlation is only safe when titles are unique in the batch.
		titleCount := make(map[string]int, len(toCreate))
		for _, issue := range toCreate {
			titleCount[issue.Title]++
		}

		var batchIssues []*types.Issue
		var singleIssues []*types.Issue
		for _, issue := range toCreate {
			if titleCount[issue.Title] > 1 {
				singleIssues = append(singleIssues, issue)
			} else {
				batchIssues = append(batchIssues, issue)
			}
		}

		// Single-create path for duplicate-title issues using idempotency markers.
		for _, issue := range singleIssues {
			priority := PriorityToLinear(issue.Priority, t.config)
			stateID, stateErr := ResolveStateIDForBeadsStatus(primaryCache, issue.Status, t.config)
			if stateErr != nil {
				result.Errors = append(result.Errors, tracker.BatchPushError{
					LocalID: issue.ID,
					Message: fmt.Sprintf("resolving state for status %s: %v", issue.Status, stateErr),
				})
				continue
			}

			if dryRun {
				// State resolved — the wet run would create this issue. No
				// ExternalRef yet: it only exists after the real create.
				result.Created = append(result.Created, tracker.BatchPushItem{LocalID: issue.ID})
				continue
			}

			marker := GenerateIdempotencyMarker(issue.ID, issue.CreatedBy, issue.CreatedAt.UnixNano())
			labelIDs, unknown := ResolveLabelIDs(issue, primaryLabelCache, t.config)
			for _, name := range unknown {
				msg := fmt.Sprintf("linear: bead %s: label %q not found on Linear team (skipped)", issue.ID, name)
				fmt.Fprintf(os.Stderr, "%s\n", msg)
				result.Warnings = append(result.Warnings, msg)
			}
			created, _, createErr := client.CreateIssueIdempotent(ctx, issue.Title, issue.Description, priority, stateID, labelIDs, marker)
			if createErr != nil {
				result.Errors = append(result.Errors, tracker.BatchPushError{
					LocalID: issue.ID,
					Message: fmt.Sprintf("single create (dup title) for %q: %v", issue.Title, createErr),
				})
				continue
			}
			result.Created = append(result.Created, tracker.BatchPushItem{
				LocalID:     issue.ID,
				ExternalRef: created.URL,
			})
		}

		// Batch-create path for unique-title issues.
		var inputs []IssueCreateInput
		titleToIssue := make(map[string]*types.Issue, len(batchIssues))
		for _, issue := range batchIssues {
			priority := PriorityToLinear(issue.Priority, t.config)
			stateID, stateErr := ResolveStateIDForBeadsStatus(primaryCache, issue.Status, t.config)
			if stateErr != nil {
				result.Errors = append(result.Errors, tracker.BatchPushError{
					LocalID: issue.ID,
					Message: fmt.Sprintf("resolving state for status %s: %v", issue.Status, stateErr),
				})
				continue
			}

			if dryRun {
				// Same rationale as the single-create dry-run branch above.
				result.Created = append(result.Created, tracker.BatchPushItem{LocalID: issue.ID})
				continue
			}

			marker := GenerateIdempotencyMarker(issue.ID, issue.CreatedBy, issue.CreatedAt.UnixNano())
			desc := AppendIdempotencyMarker(issue.Description, marker)

			labelIDs, unknown := ResolveLabelIDs(issue, primaryLabelCache, t.config)
			for _, name := range unknown {
				msg := fmt.Sprintf("linear: bead %s: label %q not found on Linear team (skipped)", issue.ID, name)
				fmt.Fprintf(os.Stderr, "%s\n", msg)
				result.Warnings = append(result.Warnings, msg)
			}

			input := IssueCreateInput{
				TeamID:      client.TeamID,
				Title:       issue.Title,
				Description: desc,
				Priority:    priority,
				StateID:     stateID,
				LabelIDs:    labelIDs,
			}
			if client.ProjectID != "" {
				input.ProjectID = client.ProjectID
			}
			titleToIssue[issue.Title] = issue
			inputs = append(inputs, input)
		}

		if len(inputs) > 0 {
			created, createErr := client.BatchCreateIssues(ctx, inputs)
			if createErr != nil {
				result.Warnings = append(result.Warnings, fmt.Sprintf("batch create partial error: %v", createErr))
			}
			matched := make(map[string]bool, len(created))
			for _, li := range created {
				localIssue, ok := titleToIssue[li.Title]
				if !ok {
					result.Warnings = append(result.Warnings, fmt.Sprintf("batch create: response contained unexpected title %q", li.Title))
					continue
				}
				matched[li.Title] = true
				result.Created = append(result.Created, tracker.BatchPushItem{
					LocalID:     localIssue.ID,
					ExternalRef: li.URL,
				})
			}
			for title, localIssue := range titleToIssue {
				if !matched[title] {
					result.Errors = append(result.Errors, tracker.BatchPushError{
						LocalID: localIssue.ID,
						Message: fmt.Sprintf("not returned in batch create response (title: %q)", title),
					})
				}
			}
		}
	}

	// Resolve the remote state of every update candidate in batched queries
	// up front — one number-in query per page per team instead of one
	// FetchIssueByIdentifier per issue (bd-kqt). The prefetch serves three
	// consumers in the loop below: the unchanged-skip check, team routing
	// (which team resolved the identifier), and the identifier→UUID
	// resolution for the update mutation. Forced issues skip the equality
	// check but still need the UUID, so they are prefetched too.
	remoteByIdentifier, routeByIdentifier, prefetchErr := t.prefetchUpdateCandidates(ctx, toUpdate)
	if prefetchErr != nil {
		var exhausted *ErrRateLimitExhausted
		if errors.As(prefetchErr, &exhausted) {
			// No budget left — attempting the per-issue updates below would
			// just grind through more doomed requests. Abort the batch so
			// the engine's rate-limit handling stops the sync cleanly.
			return result, prefetchErr
		}
		// Degraded prefetch: unresolved candidates fall back to the
		// per-issue lookup path below (HEAD semantics) so a transient
		// batch failure doesn't cost them their skip check, UUID
		// resolution, or team routing.
		result.Warnings = append(result.Warnings, fmt.Sprintf("batch prefetch of update candidates failed (falling back to per-issue lookups): %v", prefetchErr))
	}
	prefetchDegraded := prefetchErr != nil

	// Update existing issues individually (each has different field values).
	for _, issue := range toUpdate {
		extRef := *issue.ExternalRef
		externalID := ExtractLinearIdentifier(extRef)
		if externalID == "" {
			externalID = extRef
		}

		routeClient := routeByIdentifier[externalID]
		if routeClient == nil {
			if prefetchDegraded && !dryRun {
				// The batch never resolved this identifier — probe teams
				// per-issue like the pre-batch code did. Wet-run only: a
				// preview must not fan out per-issue trial fetches (bd-kqt),
				// and routing is cosmetic when no mutation will be sent.
				routeClient = t.clientForExternalID(ctx, externalID)
			} else {
				// Batch ran cleanly and no team has this issue — fall back
				// to the primary client, matching clientForExternalID.
				routeClient = t.primaryClient()
			}
		}
		if routeClient == nil {
			result.Errors = append(result.Errors, tracker.BatchPushError{
				LocalID: issue.ID,
				Message: fmt.Sprintf("cannot determine Linear team for %s", externalID),
			})
			continue
		}

		// Use the per-team state cache so that multi-team setups resolve state IDs
		// against the correct team's workflow states, not the primary team's.
		teamCache, ok := teamCaches[routeClient.TeamID]
		if !ok || teamCache == nil {
			teamCache = primaryCache // defensive fallback
		}

		remoteIssue := remoteByIdentifier[externalID]
		if remoteIssue == nil && prefetchDegraded && !dryRun {
			// Lazy per-issue recovery for candidates the failed batch left
			// unresolved: without it, changed or forced updates would send
			// the mutation with the human identifier instead of the UUID.
			// Wet-run only — the preview accepts the degraded-prefetch
			// warning instead of grinding per-issue lookups (bd-kqt).
			if fetched, lookupErr := routeClient.FetchIssueByIdentifier(ctx, externalID); lookupErr == nil && fetched != nil {
				remoteIssue = fetched
			}
		}

		teamLabelCache := primaryLabelCache
		if lc, ok := teamLabelCaches[routeClient.TeamID]; ok && lc != nil {
			teamLabelCache = lc
		}

		// Skip issues that haven't changed since the last push, unless forced.
		// This mirrors the ContentEqual / UpdatedAt skip logic in the single-issue
		// push path (engine.go doPush) to avoid redundant API writes. The remote
		// comes from the batched prefetch above (bd-kqt), not a per-issue fetch.
		if !forceIDs[issue.ID] && remoteIssue != nil {
			// BatchPush receives pre-formatted descriptions from the sync
			// engine (FormatDescription hook). Clear structured fields before
			// comparison so PushFieldsEqual does not re-append them.
			comparableIssue := *issue
			comparableIssue.AcceptanceCriteria = ""
			comparableIssue.Design = ""
			comparableIssue.Notes = ""
			if PushFieldsEqual(&comparableIssue, remoteIssue, t.config, teamLabelCache) {
				result.Skipped = append(result.Skipped, issue.ID)
				continue
			}
		}

		mapper := &linearFieldMapper{config: t.config, labelCache: teamLabelCache}
		updates := mapper.IssueToTracker(issue)

		stateID, stateErr := ResolveStateIDForBeadsStatus(teamCache, issue.Status, t.config)
		if stateErr != nil {
			result.Errors = append(result.Errors, tracker.BatchPushError{
				LocalID: issue.ID,
				Message: fmt.Sprintf("resolving state for status %s: %v", issue.Status, stateErr),
			})
			continue
		}
		if stateID != "" {
			updates["stateId"] = stateID
		}

		if dryRun {
			// Record the outcome the wet run would produce for this issue,
			// without sending the mutation. Placed after state resolution so
			// state-mapping errors take the same precedence as the wet path.
			if remoteIssue == nil {
				if prefetchDegraded {
					// The failed prefetch left the remote unknown, so the
					// skip check can't run. Preview conservatively as an
					// update — the degraded-prefetch warning on the result
					// already tells the user the preview is incomplete.
					result.Updated = append(result.Updated, tracker.BatchPushItem{LocalID: issue.ID, ExternalRef: extRef})
					continue
				}
				// Clean prefetch and no configured team resolves the
				// identifier (deleted remotely, unconfigured team, or a
				// Project-URL ref): the wet run's mutation would fail
				// server-side and land in Errors. Report the same outcome.
				result.Errors = append(result.Errors, tracker.BatchPushError{
					LocalID: issue.ID,
					Message: fmt.Sprintf("%s not found in any configured Linear team; a push would fail", externalID),
				})
				continue
			}
			ref := extRef
			if remoteIssue.URL != "" {
				ref = remoteIssue.URL
			}
			result.Updated = append(result.Updated, tracker.BatchPushItem{LocalID: issue.ID, ExternalRef: ref})
			continue
		}

		// Prefer the UUID from the prefetch; unresolved identifiers keep the
		// identifier itself (the update will fail server-side and be recorded
		// per-issue, matching the old lookup-miss behavior).
		issueUUID := externalID
		if remoteIssue != nil {
			issueUUID = remoteIssue.ID
		}

		updated, updateErr := routeClient.UpdateIssue(ctx, issueUUID, updates)
		if updateErr != nil {
			result.Errors = append(result.Errors, tracker.BatchPushError{
				LocalID: issue.ID,
				Message: fmt.Sprintf("updating %s: %v", externalID, updateErr),
			})
			continue
		}

		result.Updated = append(result.Updated, tracker.BatchPushItem{
			LocalID:     issue.ID,
			ExternalRef: updated.URL,
		})
	}

	return result, nil
}

// prefetchUpdateCandidates batch-resolves the remote state of every issue in
// the update pass. Returns the remote issue and the team client that resolved
// each identifier. Identifiers no team resolves are absent from both maps.
// Like BatchFetchIssues, the primary team is tried first and only unresolved
// identifiers are retried against later teams, which also replaces
// clientForExternalID's per-issue trial fetches for routing.
//
// A non-nil error reports the first per-team batch failure; the maps still
// carry everything resolved before (and, for non-primary-team failures,
// after) it.
func (t *Tracker) prefetchUpdateCandidates(ctx context.Context, toUpdate []*types.Issue) (map[string]*Issue, map[string]*Client, error) {
	identifiers := make([]string, 0, len(toUpdate))
	for _, issue := range toUpdate {
		extRef := *issue.ExternalRef
		externalID := ExtractLinearIdentifier(extRef)
		if externalID == "" {
			externalID = extRef
		}
		identifiers = append(identifiers, externalID)
	}
	return t.batchFetchIssuesAcrossTeams(ctx, identifiers)
}

func (t *Tracker) FieldMapper() tracker.FieldMapper {
	return &linearFieldMapper{config: t.config}
}

// MappingConfig returns the resolved Linear mapping configuration.
func (t *Tracker) MappingConfig() *MappingConfig {
	return t.config
}

func (t *Tracker) IsExternalRef(ref string) bool {
	return IsLinearExternalRef(ref) // Recognizes both /issue/ and /project/ URLs
}

func (t *Tracker) ExtractIdentifier(ref string) string {
	return ExtractLinearIdentifier(ref)
}

func (t *Tracker) BuildExternalRef(issue *tracker.TrackerIssue) string {
	if issue.URL != "" {
		if canonical, ok := CanonicalizeLinearExternalRef(issue.URL); ok {
			return canonical
		}
		return issue.URL
	}
	return fmt.Sprintf("https://linear.app/issue/%s", issue.Identifier)
}

func skipOptionalPushStateMapping(status types.Status, err error, custom []types.CustomStatus) bool {
	if !strings.Contains(err.Error(), "has no configured Linear state") {
		return false
	}
	switch status {
	case types.StatusBlocked, types.StatusDeferred, types.StatusPinned, types.StatusHooked:
		return true
	}
	for _, cs := range custom {
		if types.Status(cs.Name) == status {
			return true
		}
	}
	return false
}

// ValidatePushStateMappings ensures push has explicit, non-ambiguous status
// mappings for every configured team before any mutation occurs.
//
// Validates two sets of statuses:
//  1. The core 4 (Open, InProgress, Blocked, Closed) — always checked, since
//     beads can produce any of these and push needs to know how to translate.
//  2. Every status referenced as a value in ExplicitStateMap — catches the
//     "Phase 2 deployed before Phase 1" failure mode where the rig config
//     references a Linear state name that doesn't exist on the team yet
//     (e.g., `linear.state_map.deferred = deferred` set before the team
//     gained a "Deferred" state). One clean validation error beats per-issue
//     push failures spread across a sync run.
//
// Blocked exception: missing-state errors for blocked are allowed when blocked
// is NOT explicitly mapped (legacy behavior — blocked issues fail at push time
// instead). When the user has opted in via `linear.state_map.blocked`, the
// exception lifts and validation is strict.
func (t *Tracker) ValidatePushStateMappings(ctx context.Context) error {
	if t.config == nil || len(t.config.ExplicitStateMap) == 0 {
		return fmt.Errorf("%s", missingExplicitStateMapMessage)
	}

	// Compute the set of statuses to validate: core 4 ∪ every status referenced
	// as an explicit-map value.
	explicitlyMapped := make(map[types.Status]bool)
	for _, value := range t.config.ExplicitStateMap {
		explicitlyMapped[ParseBeadsStatus(value)] = true
	}
	statusesToCheck := []types.Status{types.StatusOpen, types.StatusInProgress, types.StatusBlocked, types.StatusClosed}
	core := map[types.Status]bool{
		types.StatusOpen:       true,
		types.StatusInProgress: true,
		types.StatusBlocked:    true,
		types.StatusClosed:     true,
	}
	for s := range explicitlyMapped {
		if !core[s] {
			statusesToCheck = append(statusesToCheck, s)
		}
	}
	// Upstream #3772: also validate configured custom statuses (status.custom)
	// so a rig that defines them learns about unresolvable mappings up front.
	for _, cs := range t.config.CustomStatuses {
		st := types.Status(cs.Name)
		if !core[st] && !explicitlyMapped[st] {
			statusesToCheck = append(statusesToCheck, st)
		}
	}

	for _, teamID := range t.teamIDs {
		client := t.clients[teamID]
		if client == nil {
			continue
		}
		cache, err := BuildStateCache(ctx, client)
		if err != nil {
			return fmt.Errorf("fetching workflow states for team %s: %w", teamID, err)
		}
		for _, status := range statusesToCheck {
			var resolveErr error
			switch {
			case status == types.StatusClosed:
				// Closed uses close_reason-aware routing at push time, not
				// state_map. Validate both terminal paths (completed + canceled)
				// resolve to a state — a team missing a Done-type or
				// Canceled-type state should still be flagged here.
				synthDone := &types.Issue{Status: types.StatusClosed, CloseReason: ""}
				synthCancel := &types.Issue{Status: types.StatusClosed, CloseReason: "stale:"}
				if _, err := ResolveStateIDForIssue(cache, synthDone, t.config); err != nil {
					resolveErr = err
				} else if _, err := ResolveStateIDForIssue(cache, synthCancel, t.config); err != nil {
					resolveErr = err
				}
			case explicitlyMapped[status] && !core[status]:
				// Strict: the user opted into mapping a non-core status (e.g.,
				// linear.state_map.deferred = deferred). Require a direct state
				// match — NO canonical fallback. Catches Phase-2-before-Phase-1
				// rig migrations where the rig config references a Linear state
				// that doesn't exist on the team yet.
				_, resolveErr = resolveStateIDExact(cache, status, t.config)
			default:
				_, resolveErr = ResolveStateIDForBeadsStatus(cache, status, t.config)
			}
			if resolveErr != nil {
				// Optional-status exception (blocked/deferred/pinned/hooked/custom):
				// missing-state allowed when the status is NOT explicitly mapped.
				// With explicit opt-in via linear.state_map.<status>, fail loudly
				// here instead of waiting for the first push of that status.
				if !explicitlyMapped[status] &&
					skipOptionalPushStateMapping(status, resolveErr, t.config.CustomStatuses) {
					continue
				}
				return resolveErr
			}
		}
	}
	return nil
}

// findStateID looks up the Linear workflow state ID for a beads status
// using the given per-team client. Kept for status-only callers that have
// no issue context (e.g. pre-flight mapping validation).
func (t *Tracker) findStateID(ctx context.Context, client *Client, status types.Status) (string, error) {
	cache, err := BuildStateCache(ctx, client)
	if err != nil {
		return "", err
	}
	return ResolveStateIDForBeadsStatus(cache, status, t.config)
}

// findStateIDForIssue picks a Linear state ID for the given issue,
// honoring close_reason when the bead is closed so Done vs. Canceled is
// chosen correctly (GH#bd-1ob follow-up: was routing every closed bead
// to whichever terminal state the state_map happened to name-match, which
// in practice was always Canceled).
func (t *Tracker) findStateIDForIssue(ctx context.Context, client *Client, issue *types.Issue) (string, error) {
	cache, err := BuildStateCache(ctx, client)
	if err != nil {
		return "", err
	}
	return ResolveStateIDForIssue(cache, issue, t.config)
}

// primaryClient returns the client for the first configured team.
func (t *Tracker) primaryClient() *Client {
	if len(t.teamIDs) == 0 {
		return nil
	}
	return t.clients[t.teamIDs[0]]
}

// clientForExternalID resolves which per-team client should handle an issue
// identified by its Linear identifier (e.g., "TEAM-123").
func (t *Tracker) clientForExternalID(ctx context.Context, externalID string) *Client {
	if len(t.teamIDs) == 1 {
		return t.primaryClient()
	}

	// Try to fetch the issue from each team's client to find the owner.
	for _, teamID := range t.teamIDs {
		client := t.clients[teamID]
		if client == nil {
			continue
		}
		li, err := client.FetchIssueByIdentifier(ctx, externalID)
		if err == nil && li != nil {
			return client
		}
	}

	return t.primaryClient()
}

// TeamIDs returns the list of configured team IDs.
func (t *Tracker) TeamIDs() []string {
	return t.teamIDs
}

// PrimaryClient returns the client for the first configured team.
// Exported for CLI code that needs direct client access (e.g., push hooks).
func (t *Tracker) PrimaryClient() *Client {
	return t.primaryClient()
}

// getConfig reads a config value from storage, falling back to env var.
// For yaml-only keys (e.g. linear.api_key), reads from config.yaml first
// to match the behavior of cmd/bd/linear.go:getLinearConfig().
func (t *Tracker) getConfig(ctx context.Context, key, envVar string) (string, error) {
	// Secret keys are stored in config.yaml, not the Dolt database,
	// to avoid leaking secrets when pushing to remotes.
	if config.IsYamlOnlyKey(key) {
		if val := config.GetString(key); val != "" {
			return val, nil
		}
		if envVar != "" {
			if envVal := os.Getenv(envVar); envVal != "" {
				return envVal, nil
			}
		}
		return "", nil
	}

	val, err := t.store.GetConfig(ctx, key)
	if err == nil && val != "" {
		return val, nil
	}
	if envVar != "" {
		if envVal := os.Getenv(envVar); envVal != "" {
			return envVal, nil
		}
	}
	return "", nil
}

// linearToTrackerIssue converts a linear.Issue to a tracker.TrackerIssue.
func linearToTrackerIssue(li *Issue) tracker.TrackerIssue {
	ti := tracker.TrackerIssue{
		ID:          li.ID,
		Identifier:  li.Identifier,
		URL:         li.URL,
		Title:       li.Title,
		Description: li.Description,
		Priority:    li.Priority,
		Labels:      make([]string, 0),
		Raw:         li,
	}

	if li.State != nil {
		ti.State = li.State
	}

	if li.Labels != nil {
		for _, l := range li.Labels.Nodes {
			ti.Labels = append(ti.Labels, l.Name)
		}
	}

	if li.Assignee != nil {
		ti.Assignee = li.Assignee.Name
		ti.AssigneeEmail = li.Assignee.Email
		ti.AssigneeID = li.Assignee.ID
	}

	if li.Parent != nil {
		ti.ParentID = li.Parent.Identifier
		ti.ParentInternalID = li.Parent.ID
	}

	// bd-ajn: surface Linear's project UUID via Metadata for the
	// generic tracker layer's field-scoped diff. The bd-ajn
	// diffExternalFields path reads Metadata["project_id"] when
	// comparing against the snapshot's ProjectID column. Without this
	// wiring, every issue with a Project would falsely flag
	// FieldProject as changed on every sync.
	if li.Project != nil && li.Project.ID != "" {
		if ti.Metadata == nil {
			ti.Metadata = map[string]interface{}{}
		}
		ti.Metadata["project_id"] = li.Project.ID
	}

	if li.ProjectMilestone != nil {
		if ti.Metadata == nil {
			ti.Metadata = map[string]interface{}{}
		}
		ti.Metadata["linear"] = map[string]interface{}{
			"project_milestone": li.ProjectMilestone,
		}
	}

	if t, err := time.Parse(time.RFC3339, li.CreatedAt); err == nil {
		ti.CreatedAt = t
	}
	if t, err := time.Parse(time.RFC3339, li.UpdatedAt); err == nil {
		ti.UpdatedAt = t
	}
	if li.CompletedAt != "" {
		if t, err := time.Parse(time.RFC3339, li.CompletedAt); err == nil {
			ti.CompletedAt = &t
		}
	}

	return ti
}

// FetchComments retrieves comments for an issue from Linear.
// Implements tracker.CommentSyncer.
func (t *Tracker) FetchComments(ctx context.Context, externalIssueID string, since time.Time) ([]tracker.TrackerComment, error) {
	client := t.clientForExternalID(ctx, externalIssueID)
	if client == nil {
		return nil, fmt.Errorf("no Linear client available")
	}
	comments, err := client.FetchIssueComments(ctx, externalIssueID, since)
	if err != nil {
		return nil, err
	}

	result := make([]tracker.TrackerComment, 0, len(comments))
	for _, c := range comments {
		tc := tracker.TrackerComment{
			ID:   c.ID,
			Body: c.Body,
		}
		if c.User != nil {
			tc.Author = c.User.Name
		}
		if ts, err := time.Parse(time.RFC3339, c.CreatedAt); err == nil {
			tc.CreatedAt = ts
		}
		if ts, err := time.Parse(time.RFC3339, c.UpdatedAt); err == nil {
			tc.UpdatedAt = ts
		}
		result = append(result, tc)
	}
	return result, nil
}

// CreateComment creates a new comment on an issue in Linear.
// Implements tracker.CommentSyncer.
func (t *Tracker) CreateComment(ctx context.Context, externalIssueID, body string) (string, error) {
	client := t.clientForExternalID(ctx, externalIssueID)
	if client == nil {
		return "", fmt.Errorf("no Linear client available")
	}
	comment, err := client.CreateIssueComment(ctx, externalIssueID, body)
	if err != nil {
		return "", err
	}
	return comment.ID, nil
}

// FetchAttachments retrieves attachment metadata for an issue from Linear.
// Implements tracker.AttachmentFetcher.
func (t *Tracker) FetchAttachments(ctx context.Context, externalIssueID string) ([]tracker.TrackerAttachment, error) {
	client := t.clientForExternalID(ctx, externalIssueID)
	if client == nil {
		return nil, fmt.Errorf("no Linear client available")
	}
	attachments, err := client.FetchIssueAttachments(ctx, externalIssueID)
	if err != nil {
		return nil, err
	}

	result := make([]tracker.TrackerAttachment, 0, len(attachments))
	for _, a := range attachments {
		ta := tracker.TrackerAttachment{
			ID:       a.ID,
			Filename: a.Title,
			URL:      a.URL,
			// Note: MimeType is not populated because Linear's attachment
			// API does not expose metadata in the GraphQL schema.
		}
		if a.Creator != nil {
			ta.Creator = a.Creator.Name
		}
		if ts, err := time.Parse(time.RFC3339, a.CreatedAt); err == nil {
			ta.CreatedAt = ts
		}
		result = append(result, ta)
	}
	return result, nil
}

// CreateProject creates a new Linear project from a beads epic.
// Implements tracker.ProjectSyncer.
//
// Long descriptions are split: the truncated form (≤255 chars per
// Linear's ProjectCreateInput limit) goes into description; the full
// text goes into content (no length limit), preserving the bead's
// authored body on the Project page (bd-cs1).
func (t *Tracker) CreateProject(ctx context.Context, epic *types.Issue) (string, string, error) {
	client := t.primaryClient()
	if client == nil {
		return "", "", fmt.Errorf("no Linear client available")
	}

	state := MapEpicToProjectState(epic.Status)
	desc, content := splitEpicDescriptionForProject(epic.Description)
	project, err := client.CreateProject(ctx, epic.Title, desc, content, state)
	if err != nil {
		return "", "", err
	}

	// bd-6cl: persist the Project snapshot baseline for pull-side
	// field-scoped conflict detection. Without this, the next pull
	// would see "Linear has this Project" with no baseline and
	// trigger first-sync soft rollout — no detection on the very
	// first cycle. Writing here makes the second cycle correct.
	if err := t.writeProjectSnapshot(ctx, epic.ID, project); err != nil && t.labelWarnFn != nil {
		t.labelWarnFn("project snapshot write failed for new epic %s: %v", epic.ID, err)
	}

	return project.URL, project.ID, nil
}

// UpdateProject updates an existing Linear project from a beads epic.
// Implements tracker.ProjectSyncer.
//
// Long descriptions follow the same description/content split as
// CreateProject (bd-cs1): truncated summary in description, full text
// in content. Same Linear-side length limits apply.
//
// content is ALWAYS included in the update map — when the bead's
// description shortened from a long-truncated form to a short non-
// truncated form, the prior Project.content (rich body from the long
// run) would otherwise remain stale on Linear. Sending nil tells
// Linear's GraphQL to clear the field.
func (t *Tracker) UpdateProject(ctx context.Context, projectID string, epic *types.Issue) error {
	client := t.primaryClient()
	if client == nil {
		return fmt.Errorf("no Linear client available")
	}

	desc, content := splitEpicDescriptionForProject(epic.Description)
	updates := map[string]interface{}{
		"name":        epic.Title,
		"description": desc,
		"state":       MapEpicToProjectState(epic.Status),
	}
	if content != "" {
		updates["content"] = content
	} else {
		// Explicit clear: a previous long description that left content
		// populated must not linger if the user shortens the description.
		// Go nil → JSON null → Linear clears the field. (Same pattern as
		// clearLinearIssueParent for parentId.)
		updates["content"] = nil
	}

	updated, err := client.UpdateProject(ctx, projectID, updates)
	if err != nil {
		return err
	}

	// bd-6cl: refresh the snapshot to the post-update Project state.
	// Without this, the next pull-side diff would see the just-pushed
	// fields as "Linear changed them" and reverse them through the
	// conflict resolver. Same correctness invariant as bd-ajn's
	// post-UpdateIssue snapshot write.
	if err := t.writeProjectSnapshot(ctx, epic.ID, updated); err != nil && t.labelWarnFn != nil {
		t.labelWarnFn("project snapshot write failed for epic %s: %v", epic.ID, err)
	}
	return nil
}

// splitEpicDescriptionForProject splits an epic's description into the
// short (description) and long (content) forms Linear's Project model
// expects. When the source fits within Linear's 255-char limit,
// content is empty so the caller can omit it from the GraphQL input.
func splitEpicDescriptionForProject(full string) (description, content string) {
	cut, truncated := TruncateLinearProjectDescription(full)
	if !truncated {
		return cut, ""
	}
	return cut, full
}

// FetchProjects retrieves Linear projects and converts them to TrackerProjects.
// Implements tracker.ProjectSyncer.
func (t *Tracker) FetchProjects(ctx context.Context, state string) ([]tracker.TrackerProject, error) {
	var allProjects []tracker.TrackerProject

	for _, teamID := range t.teamIDs {
		client := t.clients[teamID]
		if client == nil {
			continue
		}

		projects, err := client.FetchProjects(ctx, state)
		if err != nil {
			return nil, fmt.Errorf("fetching projects from team %s: %w", teamID, err)
		}

		for _, p := range projects {
			tp := tracker.TrackerProject{
				ID:          p.ID,
				Name:        p.Name,
				Description: p.Description,
				Content:     p.Content, // bd-6cl: needed for pull-side description recombine
				URL:         p.URL,
				State:       p.State,
			}
			if updatedAt, err := time.Parse(time.RFC3339, p.UpdatedAt); err == nil {
				tp.UpdatedAt = updatedAt
			}
			allProjects = append(allProjects, tp)
		}
	}

	return allProjects, nil
}

// AssignIssueToProject assigns a Linear issue to a project.
// Implements tracker.ProjectSyncer.
//
// bd-ajn: callers that own the local bead ID should call
// RecordPostAssignSnapshot after this returns nil — that's the
// snapshot patch that prevents the next sync from seeing the
// just-pushed projectId as "Linear changed it." The interface
// signature stays Linear-identifier-only (matches the GraphQL
// mutation) because some callers (e.g., raw API users) don't have
// the local ID handy.
func (t *Tracker) AssignIssueToProject(ctx context.Context, issueExternalID, projectID string) error {
	client := t.clientForExternalID(ctx, issueExternalID)
	if client == nil {
		return fmt.Errorf("no Linear client available for issue %s", issueExternalID)
	}

	_, err := client.UpdateIssue(ctx, issueExternalID, map[string]interface{}{
		"projectId": projectID,
	})
	return err
}

// SetIssueParent sets the parent issue for sub-issue nesting in Linear.
// Implements tracker.ProjectSyncer.
//
// bd-ajn: see AssignIssueToProject — callers with the local ID should
// follow up with RecordPostSetParentSnapshot.
func (t *Tracker) SetIssueParent(ctx context.Context, issueExternalID, parentExternalID string) error {
	client := t.clientForExternalID(ctx, issueExternalID)
	if client == nil {
		return fmt.Errorf("no Linear client available for issue %s", issueExternalID)
	}

	_, err := client.UpdateIssue(ctx, issueExternalID, map[string]interface{}{
		"parentId": parentExternalID,
	})
	return err
}

// RecordPostAssignSnapshot patches the per-issue snapshot's project_id
// after a successful AssignIssueToProject. Callers invoke this when
// they have the local bead ID in hand. Silently no-ops when no
// baseline snapshot exists yet (first-sync soft rollout — see
// patchIssueSnapshotProjectID).
//
// Separate from AssignIssueToProject because the ProjectSyncer
// interface is Linear-identifier-only; the local bead ID isn't
// recoverable from a bare identifier without an extra lookup. Callers
// (migration tool, project reconciler) already have the mapping in
// scope at the point of the call, so the work falls to them.
func (t *Tracker) RecordPostAssignSnapshot(ctx context.Context, localBeadID, projectID string) error {
	return t.patchIssueSnapshotProjectID(ctx, localBeadID, projectID)
}

// RecordPostSetParentSnapshot patches the per-issue snapshot's
// parent_id after a successful SetIssueParent. See
// RecordPostAssignSnapshot for the rationale on why this is a
// separate call.
func (t *Tracker) RecordPostSetParentSnapshot(ctx context.Context, localBeadID, parentUUID string) error {
	return t.patchIssueSnapshotParentID(ctx, localBeadID, parentUUID)
}

// linearUpdateKeyForField is the per-ConflictField → Linear GraphQL
// update-map key. Used by UpdateIssueFields to restrict a full
// IssueToTracker payload down to only the resolver-approved fields.
//
// FieldStatus maps to stateId, not the bead's "status" string. The
// resolution happens inside UpdateIssueFields where state lookup is
// in scope.
//
// FieldProject and FieldParent map to projectId / parentId — but
// those fields are typically handled by AssignIssueToProject /
// SetIssueParent, not by UpdateIssue. The mapping is here for
// completeness; in practice the conflict resolver rarely flags these
// for field-scoped push because they don't pass through the bead
// store's issue update path.
var linearUpdateKeyForField = map[tracker.ConflictField]string{
	tracker.FieldTitle:       "title",
	tracker.FieldDescription: "description",
	tracker.FieldStatus:      "stateId",
	tracker.FieldPriority:    "priority",
	tracker.FieldAssignee:    "assigneeId",
	tracker.FieldProject:     "projectId",
	tracker.FieldParent:      "parentId",
}

// UpdateIssueFields implements tracker.FieldScopedUpdater. Builds a
// full update payload via the field mapper, then filters to only the
// requested ConflictFields. Status updates additionally resolve a
// stateId via findStateIDForIssue (same as UpdateIssueWithRemote's
// resolve path).
//
// Labels are NOT touched here. Label sync is independent of the
// other-fields conflict resolver — it has its own per-issue
// reconciler. Routing labels through this path would re-trigger the
// reconciler unnecessarily and duplicate the snapshot write below.
//
// After successful push, persists the issue snapshot with the
// post-update remote state — same as UpdateIssueWithRemote.
func (t *Tracker) UpdateIssueFields(ctx context.Context, externalID string, issue *types.Issue, remote *tracker.TrackerIssue, fields []tracker.ConflictField) (*tracker.TrackerIssue, error) {
	if len(fields) == 0 {
		return nil, fmt.Errorf("UpdateIssueFields: empty field list")
	}

	client := t.clientForExternalID(ctx, externalID)
	if client == nil {
		return nil, fmt.Errorf("cannot determine Linear team for issue %s", externalID)
	}

	// Build the field-restricted payload directly from issue state.
	updates := make(map[string]interface{}, len(fields))
	for _, f := range fields {
		key, ok := linearUpdateKeyForField[f]
		if !ok {
			continue // unknown field — skip rather than error
		}
		switch f {
		case tracker.FieldTitle:
			updates[key] = issue.Title
		case tracker.FieldDescription:
			updates[key] = issue.Description
		case tracker.FieldPriority:
			updates[key] = PriorityToLinear(issue.Priority, t.config)
		case tracker.FieldAssignee:
			// Linear's assigneeId field requires the user's UUID,
			// not the bead's assignee string (which is an email or
			// display name). The existing IssueToTracker path doesn't
			// push assignee at all for this reason — there's no
			// user-lookup wired up. Skip silently here too rather
			// than send the raw string and have Linear reject or
			// mis-assign (codex bd-ajn round-2 bug 7). When user-
			// resolution lands, this case can wire to it.
			continue
		case tracker.FieldProject, tracker.FieldParent:
			// These fields aren't pushed through UpdateIssue's main
			// path — they have dedicated AssignIssueToProject /
			// SetIssueParent endpoints. Skip silently; reconcilers
			// own them.
			continue
		case tracker.FieldStatus:
			// State requires a Linear state UUID lookup. Skip when
			// the remote already matches local (mirrors the
			// remoteStatusMatchesLocal logic in UpdateIssueWithRemote)
			// to preserve remote-owned states like "In Review".
			if t.remoteStatusMatchesLocal(remote, issue) {
				continue
			}
			stateID, err := t.findStateIDForIssue(ctx, client, issue)
			if err != nil {
				return nil, fmt.Errorf("finding state for status %s: %w", issue.Status, err)
			}
			if stateID != "" {
				updates[key] = stateID
			}
		}
	}

	if len(updates) == 0 {
		// All requested fields filtered out (e.g., status preserved
		// + no other fields). Treat as a no-op push — the caller's
		// pre-fetched remote already matches what we'd send.
		ti := *remote
		return &ti, nil
	}

	updated, err := client.UpdateIssue(ctx, externalID, updates)
	if err != nil {
		return nil, err
	}

	// bd-ajn: refresh the snapshot to the post-update state, same as
	// UpdateIssueWithRemote does for the full-issue path.
	if err := t.writeIssueSnapshot(ctx, issue.ID, updated); err != nil && t.labelWarnFn != nil {
		t.labelWarnFn("issue snapshot write failed for %s after field-scoped update: %v", issue.ID, err)
	}

	ti := linearToTrackerIssue(updated)
	return &ti, nil
}

// RecordPullSnapshot implements tracker.PostPullSnapshotter. Called
// by the engine after each successful pull-side import or update so
// the snapshot reflects the just-pulled Linear state. Without this,
// the next bidirectional sync would diff against the pre-pull
// snapshot and incorrectly see "Linear changed these fields" for the
// fields we just pulled into local.
//
// Derives the snapshot from `fetched.Raw` (which linearToTrackerIssue
// sets to the source *Issue), preserving Linear UUIDs that the
// TrackerIssue abstraction loses (state_id, project_id, etc.). When
// Raw isn't a *Issue (mock trackers, non-Linear adapters slipping
// through), silently no-ops — the snapshot machinery is best-effort
// at this layer; DetectConflicts's first-sync path handles missing
// snapshots safely.
func (t *Tracker) RecordPullSnapshot(ctx context.Context, localBeadID string, fetched tracker.TrackerIssue) error {
	li, ok := fetched.Raw.(*Issue)
	if !ok || li == nil {
		return nil
	}
	return t.writeIssueSnapshot(ctx, localBeadID, li)
}

// IsProjectRef checks if an external_ref is a Linear project URL.
// Implements tracker.ProjectSyncer.
func (t *Tracker) IsProjectRef(ref string) bool {
	return IsLinearProjectRef(ref)
}

// ExtractProjectID extracts the project ID from a Linear project URL or returns the ID directly.
// Implements tracker.ProjectSyncer.
func (t *Tracker) ExtractProjectID(ref string) string {
	// If it's a URL, we need to look up the project by slug to get the ID.
	// For simplicity, return the slug — callers needing the UUID should use FetchProject.
	if IsLinearProjectRef(ref) {
		return ExtractLinearProjectSlug(ref)
	}
	return ref
}

// BuildStateCacheFromTracker builds a StateCache using the tracker's primary client.
// This allows CLI code to set up PushHooks.BuildStateCache without accessing the client directly.
func BuildStateCacheFromTracker(ctx context.Context, t *Tracker) (*StateCache, error) {
	client := t.primaryClient()
	if client == nil {
		return nil, fmt.Errorf("Linear tracker not initialized")
	}
	return BuildStateCache(ctx, client)
}

// labelClient is the narrow interface resolveLabelIDs uses; *Client satisfies it.
// Defined as an interface so tests can stub without spinning up an HTTP server.
type labelClient interface {
	LabelsByName(ctx context.Context, names []string) (map[string]LinearLabel, error)
	CreateLabel(ctx context.Context, name string, scope LabelScope) (LinearLabel, error)
}

// resolveLabelIDs maps a set of beads label names to Linear label IDs, auto-
// creating any that don't exist. Per the spec (Atomicity & partial failure):
// a CreateLabel failure does NOT abort the whole push — the failed label is
// omitted from the result map, and the snapshot writer omits it too, so the
// next sync sees it as a fresh add and retries.
//
// LabelsByName ambiguity errors DO abort, since a duplicate label needs human
// resolution before further pushes are safe.
//
// LabelsByName returns lowercase-keyed results. We match case-insensitively
// (Linear matches that way; beads label casing may differ from Linear's).
// Output keys preserve the bead's original spelling so callers can use the
// map with their original list.
func resolveLabelIDs(ctx context.Context, c labelClient, names []string, scope LabelScope, warn func(format string, args ...interface{})) (map[string]LinearLabel, error) {
	if len(names) == 0 {
		return map[string]LinearLabel{}, nil
	}
	existing, err := c.LabelsByName(ctx, names)
	if err != nil {
		return nil, err // LabelsByName failure (incl. ambiguity) is fatal for this push
	}
	out := make(map[string]LinearLabel, len(names))
	for _, n := range names {
		if l, ok := existing[strings.ToLower(n)]; ok {
			out[n] = l // l is LinearLabel{Name: <Linear display case>, ID: <id>}
			continue
		}
		l, err := c.CreateLabel(ctx, n, scope)
		if err != nil {
			if warn != nil {
				warn("auto-create label %q failed; skipping for this sync (will retry next): %v", n, err)
			}
			continue // skip this label; do NOT abort the whole push
		}
		out[n] = l // l.Name == n (CreateLabel just stored it with this name)
	}
	return out, nil
}

// BuildLabelCacheFromTracker builds a LabelCache using the tracker's primary client.
// This allows CLI push hooks to compare label sets without reaching into the client.
func BuildLabelCacheFromTracker(ctx context.Context, t *Tracker) (*LabelCache, error) {
	client := t.primaryClient()
	if client == nil {
		return nil, fmt.Errorf("Linear tracker not initialized")
	}
	return BuildLabelCache(ctx, client)
}

// configLoaderAdapter wraps storage.Storage to implement linear.ConfigLoader.
type configLoaderAdapter struct {
	ctx   context.Context
	store storage.Storage
}

func (c *configLoaderAdapter) GetAllConfig() (map[string]string, error) {
	return c.store.GetAllConfig(c.ctx)
}
