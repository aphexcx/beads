package linear

import (
	"context"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/steveyegge/beads/internal/config"
	"github.com/steveyegge/beads/internal/storage"
	"github.com/steveyegge/beads/internal/tracker"
	"github.com/steveyegge/beads/internal/types"
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

	apiKey, err := t.getConfig(ctx, "linear.api_key", "LINEAR_API_KEY")
	if err != nil || apiKey == "" {
		return fmt.Errorf("Linear API key not configured (set linear.api_key or LINEAR_API_KEY)")
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

	// Create per-team clients upfront for O(1) routing.
	t.clients = make(map[string]*Client, len(t.teamIDs))
	for _, teamID := range t.teamIDs {
		client := NewClient(apiKey, teamID)
		if endpoint != "" {
			client = client.WithEndpoint(endpoint)
		}
		if projectID != "" {
			client = client.WithProjectID(projectID)
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

func (t *Tracker) CreateIssue(ctx context.Context, issue *types.Issue) (*tracker.TrackerIssue, error) {
	// Create on the primary (first) team.
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

	created, err := client.CreateIssue(ctx, issue.Title, issue.Description, priority, stateID, labelIDs)
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
func (t *Tracker) UpdateIssue(ctx context.Context, externalID string, issue *types.Issue) (*tracker.TrackerIssue, error) {
	client := t.clientForExternalID(ctx, externalID)
	if client == nil {
		return nil, fmt.Errorf("cannot determine Linear team for issue %s", externalID)
	}

	mapper := t.FieldMapper()
	updates := mapper.IssueToTracker(issue)

	// Resolve and include state so status changes are pushed to Linear.
	// Uses the issue-aware resolver so close_reason distinguishes
	// Done vs. Canceled for closed beads.
	stateID, err := t.findStateIDForIssue(ctx, client, issue)
	if err != nil {
		return nil, fmt.Errorf("finding state for status %s: %w", issue.Status, err)
	}
	if stateID != "" {
		updates["stateId"] = stateID
	}

	// Label sync (decision #12: independent path, runs even when other fields are equal).
	var snapshotToWrite []storage.LinearLabelSnapshotEntry
	if t.labelSyncEnabled {
		ids, snap, err := t.reconcileAndBuildLabelUpdate(ctx, client, externalID, issue)
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

	ti := linearToTrackerIssue(updated)
	return &ti, nil
}

// reconcileAndBuildLabelUpdate fetches fresh Linear labels, runs the reconciler,
// resolves new label IDs (with auto-create), deduplicates, and returns the
// labelIds slice to send to Linear AND the snapshot rows to persist after a
// successful push. Caller is responsible for actually persisting after push.
//
// Returns (nil, nil, nil) when label sync is disabled (caller should not call
// in that case, but defensive fallthrough is harmless).
func (t *Tracker) reconcileAndBuildLabelUpdate(
	ctx context.Context, client *Client, externalID string, issue *types.Issue,
) (labelIDs []string, snapshotToWrite []storage.LinearLabelSnapshotEntry, err error) {
	// Fetch current Linear labels for this issue so the reconciler has fresh state.
	// Note: the engine already fetched the issue once for ContentEqual; this is an
	// extra round-trip. Acceptable for v1.
	//
	// V1 behavior: if FetchIssueByIdentifier fails (rate limit, network, missing
	// issue), the entire UpdateIssue aborts — including non-label fields like
	// status/title/description. We can't safely send labelIds without knowing the
	// current Linear state. Future: degrade to "push other fields, skip labels,
	// reconcile labels next sync" when the fetch fails for transient reasons.
	fresh, err := client.FetchIssueByIdentifier(ctx, externalID)
	if err != nil {
		return nil, nil, fmt.Errorf("fetch for label reconciliation: %w", err)
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

// ValidatePushStateMappings ensures push has explicit, non-ambiguous status
// mappings for every configured team before any mutation occurs.
func (t *Tracker) ValidatePushStateMappings(ctx context.Context) error {
	if t.config == nil || len(t.config.ExplicitStateMap) == 0 {
		return fmt.Errorf("%s", missingExplicitStateMapMessage)
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
		for _, status := range []types.Status{types.StatusOpen, types.StatusInProgress, types.StatusBlocked, types.StatusClosed} {
			var resolveErr error
			if status == types.StatusClosed {
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
			} else {
				_, resolveErr = ResolveStateIDForBeadsStatus(cache, status, t.config)
			}
			if resolveErr != nil {
				// Only fail for statuses the config explicitly tries to map or when
				// mappings are entirely absent. Missing blocked mappings are allowed
				// until a blocked issue is actually pushed.
				if status == types.StatusBlocked && strings.Contains(resolveErr.Error(), "has no configured Linear state") {
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
func (t *Tracker) CreateProject(ctx context.Context, epic *types.Issue) (string, string, error) {
	client := t.primaryClient()
	if client == nil {
		return "", "", fmt.Errorf("no Linear client available")
	}

	state := MapEpicToProjectState(epic.Status)
	project, err := client.CreateProject(ctx, epic.Title, epic.Description, state)
	if err != nil {
		return "", "", err
	}

	return project.URL, project.ID, nil
}

// UpdateProject updates an existing Linear project from a beads epic.
// Implements tracker.ProjectSyncer.
func (t *Tracker) UpdateProject(ctx context.Context, projectID string, epic *types.Issue) error {
	client := t.primaryClient()
	if client == nil {
		return fmt.Errorf("no Linear client available")
	}

	updates := map[string]interface{}{
		"name":        epic.Title,
		"description": epic.Description,
		"state":       MapEpicToProjectState(epic.Status),
	}

	_, err := client.UpdateProject(ctx, projectID, updates)
	return err
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

// configLoaderAdapter wraps storage.Storage to implement linear.ConfigLoader.
type configLoaderAdapter struct {
	ctx   context.Context
	store storage.Storage
}

func (c *configLoaderAdapter) GetAllConfig() (map[string]string, error) {
	return c.store.GetAllConfig(c.ctx)
}
