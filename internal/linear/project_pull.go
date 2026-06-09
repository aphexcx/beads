package linear

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/steveyegge/beads/internal/storage"
	"github.com/steveyegge/beads/internal/tracker"
	"github.com/steveyegge/beads/internal/types"
)

// bd-6cl pull-side: detect changes on each fetched Linear Project,
// produce a per-epic update map honoring conflict-resolution policy
// + close-state preservation, and stage the new snapshot baseline.
//
// Mirrors bd-ajn's Issue-side flow:
//   - LOCAL diff: epic at lastSync (dolt_history_issues) vs current
//   - REMOTE diff: snapshot vs current TrackerProject
//   - intersect → conflicting fields
//   - resolver decides per-field, output is the apply-map
//
// First-sync soft rollout (mayor Q3 → option B): no snapshot →
// baseline + emit empty update (no conflict possible this run).
//
// Q1 close-state preservation: when the local epic is StatusClosed
// AND the Linear Project state would translate to anything other
// than StatusClosed, the status field is intentionally NOT
// included in the update (don't auto-reopen closed work).

// ProjectFieldKind identifies a Project field for per-field diff
// + resolver dispatch. Distinct from tracker.ConflictField (which
// is Issue-scoped) because Projects have a different shape
// (name/content/state vs title/description/status/priority/etc.).
type ProjectFieldKind string

const (
	ProjectFieldName        ProjectFieldKind = "project.name"
	ProjectFieldDescription ProjectFieldKind = "project.description"
	ProjectFieldContent     ProjectFieldKind = "project.content"
	ProjectFieldState       ProjectFieldKind = "project.state"
)

// ProjectPullDecision is the per-epic result of evaluating one
// Project against its local-side counterpart. Callers apply
// Updates to the local epic (when non-empty), write NewSnapshot
// to refresh the baseline, and respect SkipReason for
// observability (e.g. dry-run logging, first-sync log lines).
type ProjectPullDecision struct {
	LocalEpicID string
	// Updates is the bead-side update map (keys match
	// store.UpdateIssue's expected map). Empty means no apply
	// needed (no field changes, or all changes suppressed by
	// conflict resolution).
	Updates map[string]interface{}
	// NewSnapshot is the snapshot row to upsert after applying
	// Updates so the next sync's diff has a fresh baseline.
	// Always populated when the Project was fetched, regardless of
	// whether Updates is non-empty.
	NewSnapshot *storage.LinearProjectSnapshot
	// Conflicting lists fields where BOTH local and remote moved
	// since lastSync — surfaced for logging. Resolution policy
	// already applied; this is informational.
	Conflicting []ProjectFieldKind
	// SkipReason is set when the decision was a no-op for a
	// notable reason (first-sync baseline, close-state preserved).
	// Used by the caller to log instead of silently dropping.
	SkipReason string
}

// resolveProjectPull is the per-Project decision function. Inputs:
//   - localEpic: the matched local bead (TypeEpic, external_ref
//     points at this Project)
//   - remote: the just-fetched TrackerProject
//   - localAtSync: state of localEpic at lastSync from history;
//     nil when no committed history before lastSync (caller does
//     the dolt_history lookup; this function takes the result)
//   - snapshot: last-known Project snapshot; nil when first-sync
//   - policy: conflict resolution policy from SyncOptions
//   - syncedAt: the moment to stamp on the new baseline snapshot
//
// Returns a ProjectPullDecision with the update map + new
// snapshot. Caller applies them.
func resolveProjectPull(
	localEpic *types.Issue,
	remote tracker.TrackerProject,
	localAtSync *types.Issue,
	snapshot *storage.LinearProjectSnapshot,
	policy tracker.ConflictResolution,
	syncedAt time.Time,
) ProjectPullDecision {
	out := ProjectPullDecision{LocalEpicID: localEpic.ID}

	// First-sync soft rollout: snapshot baseline, no conflict
	// possible this run.
	if snapshot == nil {
		out.NewSnapshot = remoteToSnapshot(localEpic.ID, remote, syncedAt)
		out.SkipReason = "first sync - snapshotting baseline"
		return out
	}

	// Always stage a fresh snapshot reflecting the post-pull
	// remote state — the apply order is: write updates, write
	// snapshot. Even when Updates is empty, the snapshot bumps
	// its synced_at so prune/audit queries work.
	out.NewSnapshot = remoteToSnapshot(localEpic.ID, remote, syncedAt)

	// Per-field diff.
	localChanged := diffLocalEpicFields(localEpic, localAtSync)
	remoteChanged := diffProjectFields(remote, snapshot)
	conflicting := intersectProjectFields(localChanged, remoteChanged)
	out.Conflicting = conflicting

	if len(localChanged) == 0 && len(remoteChanged) == 0 {
		// Nothing changed on either side.
		return out
	}

	// Build the apply map by walking each field:
	//   - In conflicting set → policy decides (local|external|timestamp)
	//   - In remoteChanged only → pull
	//   - In localChanged only → ignore (push side owns it)
	updates := map[string]interface{}{}
	conflictSet := make(map[ProjectFieldKind]bool, len(conflicting))
	for _, f := range conflicting {
		conflictSet[f] = true
	}

	applyRemote := func(field ProjectFieldKind) {
		switch field {
		case ProjectFieldName:
			updates["title"] = remote.Name
		case ProjectFieldDescription, ProjectFieldContent:
			// Recombine description + content into bead.Description
			// (reverse of bd-cs1's push-side split). Set once even
			// if both fields were flagged.
			if _, already := updates["description"]; !already {
				updates["description"] = recombineProjectDescription(remote)
			}
		case ProjectFieldState:
			// Q1 close-state preservation: refuse to write status
			// when local is already closed AND remote-mapped status
			// is not closed (would auto-reopen historical work).
			mapped := MapProjectStateToBeads(remote.State)
			if localEpic.Status == types.StatusClosed && mapped.Status != types.StatusClosed {
				out.SkipReason = appendSkip(out.SkipReason,
					fmt.Sprintf("status preserved (local closed, Linear=%s)", remote.State))
				return
			}
			updates["status"] = string(mapped.Status)
			if mapped.Status == types.StatusClosed {
				updates["close_reason"] = mapped.CloseReason
			}
		}
	}

	// Iterate in deterministic order (test stability + log line
	// predictability across map iteration nondeterminism).
	for _, f := range canonicalProjectFields {
		// Conflicting → policy
		if conflictSet[f] {
			switch policy {
			case tracker.ConflictExternal:
				applyRemote(f)
			case tracker.ConflictLocal:
				// keep local; do nothing
			default: // timestamp / unset
				// Project doesn't have per-field updatedAt; fall
				// back to whole-issue comparison. Use
				// snapshot.SyncedAt as the boundary — if remote's
				// state semantically differs AND the snapshot is
				// the older record, that means Linear was edited
				// since lastSync. Same reasoning as bd-ajn Q7's
				// Linear-API-constraint acknowledgement.
				if remote.UpdatedAt.After(snapshot.SyncedAt) {
					applyRemote(f)
				}
			}
			continue
		}
		// Remote-only changed → pull unconditionally
		if remoteChanged[f] {
			applyRemote(f)
		}
		// Local-only changed → ignore (push side owns it)
	}

	out.Updates = updates
	return out
}

// canonicalProjectFields is the stable iteration order used by
// resolveProjectPull and assertion-friendly tests. Mirrors bd-ajn's
// conflictFieldKeys pattern.
var canonicalProjectFields = []ProjectFieldKind{
	ProjectFieldName,
	ProjectFieldDescription,
	ProjectFieldContent,
	ProjectFieldState,
}

// diffLocalEpicFields returns the set of LOCAL ProjectFieldKinds
// where current epic differs from its at-sync state. When
// atSync==nil (no prior committed state — issue created since
// lastSync), every populated field reports as "changed" so the
// caller treats the local side as authoritative.
func diffLocalEpicFields(current, atSync *types.Issue) map[ProjectFieldKind]bool {
	out := map[ProjectFieldKind]bool{}
	if current == nil {
		return out
	}
	if atSync == nil {
		if current.Title != "" {
			out[ProjectFieldName] = true
		}
		if current.Description != "" {
			out[ProjectFieldDescription] = true
			out[ProjectFieldContent] = true
		}
		out[ProjectFieldState] = true
		return out
	}
	if current.Title != atSync.Title {
		out[ProjectFieldName] = true
	}
	if current.Description != atSync.Description {
		out[ProjectFieldDescription] = true
		out[ProjectFieldContent] = true
	}
	if current.Status != atSync.Status {
		out[ProjectFieldState] = true
	}
	return out
}

// diffProjectFields returns the set of REMOTE ProjectFieldKinds
// where Linear's current Project differs from the snapshot. Empty
// fields on either side are treated as zero-values; the diff
// flags any inequality.
func diffProjectFields(remote tracker.TrackerProject, snap *storage.LinearProjectSnapshot) map[ProjectFieldKind]bool {
	out := map[ProjectFieldKind]bool{}
	if snap == nil {
		return out
	}
	if remote.Name != snap.Name {
		out[ProjectFieldName] = true
	}
	if remote.Description != snap.Description {
		out[ProjectFieldDescription] = true
	}
	if remote.Content != snap.Content {
		out[ProjectFieldContent] = true
	}
	if remote.State != snap.State {
		out[ProjectFieldState] = true
	}
	return out
}

// intersectProjectFields returns fields where BOTH local and
// remote moved since lastSync — the true-conflict subset.
func intersectProjectFields(local, remote map[ProjectFieldKind]bool) []ProjectFieldKind {
	if len(local) == 0 || len(remote) == 0 {
		return nil
	}
	var out []ProjectFieldKind
	for _, f := range canonicalProjectFields {
		if local[f] && remote[f] {
			out = append(out, f)
		}
	}
	return out
}

// recombineProjectDescription reverses bd-cs1's push-side split.
// Prefers Project.content (the long-form, authoritative body) when
// present; falls back to Project.description (the short summary,
// 255-char Linear cap). Pull-side authoritative on bead.Description.
func recombineProjectDescription(p tracker.TrackerProject) string {
	if strings.TrimSpace(p.Content) != "" {
		return p.Content
	}
	return p.Description
}

// remoteToSnapshot builds the snapshot row from a just-fetched
// TrackerProject. Used after every pull-side resolve so the next
// sync diffs against a fresh baseline.
func remoteToSnapshot(localEpicID string, remote tracker.TrackerProject, syncedAt time.Time) *storage.LinearProjectSnapshot {
	return &storage.LinearProjectSnapshot{
		IssueID:     localEpicID,
		ProjectID:   remote.ID,
		Name:        remote.Name,
		Description: remote.Description,
		Content:     remote.Content,
		State:       remote.State,
		SyncedAt:    syncedAt,
	}
}

// appendSkip joins a new reason onto an existing SkipReason
// string, using "; " as the separator. Keeps the reason field
// human-readable in log lines.
func appendSkip(existing, more string) string {
	if existing == "" {
		return more
	}
	return existing + "; " + more
}

// errProjectSnapshotStoreNotSupported signals a backend without
// Project-snapshot capability. Pull-side caller falls back to
// authoritative-overwrite for Projects (same severity as bd-ajn's
// legacy whole-issue fallback for Issues without a snapshot
// backend).
var errProjectSnapshotStoreNotSupported = errors.New("storage backend does not expose LinearProjectSnapshotStore")

// PullProjects implements tracker.ProjectPuller. The engine's
// doPull calls this at the top of its loop (when the tracker is
// also a ProjectSyncer — Linear satisfies both) to materialize
// remote Linear Projects as local epics before the per-Issue pull
// runs.
//
// Workflow per Project:
//  1. Match local epic by external_ref (Project URL → bead lookup)
//  2. Matched + snapshot exists → resolveProjectPull → apply
//     Updates → write new snapshot
//  3. Matched + no snapshot → first-sync soft rollout (baseline,
//     no apply, log)
//  4. Unmatched → CREATE local epic (TypeEpic) + write baseline
//     snapshot
//
// Errors from a single Project don't abort the pass; they're
// collected in stats.Errors. Snapshot-write failures land in
// stats.SnapshotWarnings (severity-distinct, per bd-ajn pattern).
//
// Dry-run: produces all decisions, logs them, but writes nothing
// to the store.
func (t *Tracker) PullProjects(ctx context.Context, opts tracker.ProjectPullOptions) (*tracker.ProjectPullStats, error) {
	stats := &tracker.ProjectPullStats{}
	if t.store == nil {
		return stats, fmt.Errorf("PullProjects: tracker has no store configured")
	}
	snapStore, snapOK := t.store.(storage.LinearProjectSnapshotStore)
	if !snapOK {
		// Backend doesn't support Project snapshots. Per mayor's
		// bd-6cl Q3 decision (option B), this is a fatal config gap
		// — the pull is unsafe without snapshots. Return a clear
		// error so the engine surfaces it as a warning rather than
		// silently dropping into the broken whole-issue fallback.
		return stats, errProjectSnapshotStoreNotSupported
	}

	remoteProjects, err := t.FetchProjects(ctx, "all")
	if err != nil {
		return stats, fmt.Errorf("PullProjects: FetchProjects: %w", err)
	}
	stats.Fetched = len(remoteProjects)
	if stats.Fetched == 0 {
		return stats, nil
	}

	// Build local-epic-by-Project-URL index. Walk all issues with
	// external_ref matching a Project URL. Bead lookup is O(N)
	// over the workspace; acceptable for v1 (rigs have tens to
	// hundreds of top-level epics, not thousands).
	//
	// Canonicalize both the indexed ref AND the remote.URL lookup
	// key (codex bd-6cl round-1 bug 2) so slug renames or trailing
	// /title differences don't cause a miss → duplicate-epic
	// creation. CanonicalizeLinearExternalRef strips trailing path
	// segments and normalizes form so both sides compare equal.
	localIssues, err := t.store.SearchIssues(ctx, "", types.IssueFilter{})
	if err != nil {
		return stats, fmt.Errorf("PullProjects: SearchIssues: %w", err)
	}
	localByProjectURL := make(map[string]*types.Issue)
	for _, issue := range localIssues {
		if issue == nil || issue.ExternalRef == nil {
			continue
		}
		ref := strings.TrimSpace(*issue.ExternalRef)
		if ref != "" && IsLinearProjectRef(ref) {
			canonical, ok := CanonicalizeLinearExternalRef(ref)
			if !ok {
				canonical = ref // fall back to raw if canonicalize couldn't normalize
			}
			localByProjectURL[canonical] = issue
		}
	}

	syncedAt := time.Now().UTC()
	stats.ProjectIDToLocalEpicID = make(map[string]string, len(remoteProjects))
	for _, remote := range remoteProjects {
		t.pullOneProject(ctx, remote, localByProjectURL, snapStore, opts, syncedAt, stats)
	}
	return stats, nil
}

// pullOneProject is the per-Project worker — extracted from
// PullProjects for clarity and unit-testability. Mutates stats in
// place.
func (t *Tracker) pullOneProject(
	ctx context.Context,
	remote tracker.TrackerProject,
	localByProjectURL map[string]*types.Issue,
	snapStore storage.LinearProjectSnapshotStore,
	opts tracker.ProjectPullOptions,
	syncedAt time.Time,
	stats *tracker.ProjectPullStats,
) {
	// Canonicalize remote.URL the same way the index was built so
	// the match doesn't miss on a slug or trailing-path difference
	// (codex bd-6cl round-1 bug 2).
	lookupKey := remote.URL
	if canonical, ok := CanonicalizeLinearExternalRef(remote.URL); ok {
		lookupKey = canonical
	}
	localEpic, matched := localByProjectURL[lookupKey]

	if !matched {
		// Unmatched: CREATE a new local epic. Snapshot baseline
		// writes after successful creation.
		if opts.DryRun {
			stats.PreviewLines = append(stats.PreviewLines,
				fmt.Sprintf("[dry-run] Would materialize Linear Project as new local epic: %s (%s)", remote.Name, remote.URL))
			stats.Created++
			return
		}
		newEpic, err := t.createEpicFromProject(ctx, remote, opts.Actor)
		if err != nil {
			stats.Errors = append(stats.Errors,
				fmt.Errorf("create epic from Project %s: %w", remote.URL, err))
			return
		}
		if sErr := snapStore.UpsertLinearProjectSnapshot(ctx,
			remoteToSnapshot(newEpic.ID, remote, syncedAt)); sErr != nil {
			stats.SnapshotWarnings = append(stats.SnapshotWarnings,
				fmt.Errorf("snapshot baseline for new epic %s (Project %s): %w",
					newEpic.ID, remote.URL, sErr))
		}
		if remote.ID != "" {
			stats.ProjectIDToLocalEpicID[remote.ID] = newEpic.ID
		}
		stats.Created++
		return
	}

	// Matched path always records the mapping so the post-pull
	// descendant-dep pass can look up the local epic ID from a
	// pulled Issue's projectId metadata.
	if remote.ID != "" {
		stats.ProjectIDToLocalEpicID[remote.ID] = localEpic.ID
	}

	// Matched: resolve against snapshot + local history.
	snap, err := snapStore.GetLinearProjectSnapshot(ctx, localEpic.ID)
	if err != nil {
		stats.Errors = append(stats.Errors,
			fmt.Errorf("load snapshot for %s: %w", localEpic.ID, err))
		return
	}

	// Codex bd-6cl round-1 bug 1: a snapshot pinned to a DIFFERENT
	// Project UUID than the one we just fetched means the epic was
	// repointed at a new Linear Project (rare — migration tooling or
	// manual external_ref rewrite). Trusting the stale snapshot
	// would produce a spurious diff or skip first-sync incorrectly.
	// Force the first-sync soft-rollout path by dropping the snap.
	if snap != nil && snap.ProjectID != "" && remote.ID != "" && snap.ProjectID != remote.ID {
		snap = nil
	}

	// Local at-sync from dolt_history. nil when issue had no
	// committed state at lastSync (created since) — resolver
	// treats that as "every populated field new locally".
	var localAtSync *types.Issue
	if !opts.LastSync.IsZero() {
		localAtSync, err = tracker.LoadLocalStateAtSync(ctx, t.store, localEpic.ID, opts.LastSync)
		if err != nil && !errors.Is(err, tracker.ErrHistoryNotSupported) {
			stats.Errors = append(stats.Errors,
				fmt.Errorf("history lookup for %s: %w", localEpic.ID, err))
			return
		}
	}

	decision := resolveProjectPull(localEpic, remote, localAtSync, snap, opts.Policy, syncedAt)

	if opts.DryRun {
		if len(decision.Updates) > 0 {
			stats.PreviewLines = append(stats.PreviewLines,
				fmt.Sprintf("[dry-run] Would update local epic %s from Linear Project %s (fields: %v)",
					localEpic.ID, remote.URL, sortedUpdateKeys(decision.Updates)))
			stats.Updated++
		} else if decision.SkipReason != "" {
			stats.PreviewLines = append(stats.PreviewLines,
				fmt.Sprintf("[dry-run] Project pull %s: %s", localEpic.ID, decision.SkipReason))
			if strings.Contains(decision.SkipReason, "first sync") {
				stats.FirstSync++
			} else {
				stats.Skipped++
			}
		} else {
			stats.Skipped++
		}
		return
	}

	if len(decision.Updates) > 0 {
		if uErr := t.store.UpdateIssue(ctx, localEpic.ID, decision.Updates, opts.Actor); uErr != nil {
			stats.Errors = append(stats.Errors,
				fmt.Errorf("apply Project pull update for %s: %w", localEpic.ID, uErr))
			return
		}
		stats.Updated++
	} else if strings.Contains(decision.SkipReason, "first sync") {
		stats.FirstSync++
	} else {
		stats.Skipped++
	}

	if decision.NewSnapshot != nil {
		if sErr := snapStore.UpsertLinearProjectSnapshot(ctx, decision.NewSnapshot); sErr != nil {
			stats.SnapshotWarnings = append(stats.SnapshotWarnings,
				fmt.Errorf("snapshot refresh for %s: %w", localEpic.ID, sErr))
		}
	}
}

// createEpicFromProject materializes a fresh local epic from a
// just-fetched Linear Project. Title, description (recombined from
// content/description per bd-cs1 reversal), status (via reverse
// map), and external_ref are set. The bead store assigns an ID.
//
// Status follows the reverse map: Linear "completed" → closed
// (no close_reason), "canceled" → closed + close_reason, etc.
// Q1's close-state preservation doesn't apply here because there's
// no prior local state to preserve — this is a fresh create.
func (t *Tracker) createEpicFromProject(ctx context.Context, remote tracker.TrackerProject, actor string) (*types.Issue, error) {
	mapped := MapProjectStateToBeads(remote.State)
	ref := remote.URL
	epic := &types.Issue{
		Title:       remote.Name,
		Description: recombineProjectDescription(remote),
		IssueType:   types.TypeEpic,
		Status:      mapped.Status,
		ExternalRef: &ref,
	}
	if mapped.CloseReason != "" {
		epic.CloseReason = mapped.CloseReason
	}
	if err := t.store.CreateIssue(ctx, epic, actor); err != nil {
		return nil, err
	}
	return epic, nil
}

// sortedUpdateKeys returns the keys of an update map in
// deterministic order for log lines. Pure helper.
func sortedUpdateKeys(m map[string]interface{}) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	// Inline sort.Strings to avoid a sort import for the helper.
	for i := 1; i < len(keys); i++ {
		for j := i; j > 0 && keys[j-1] > keys[j]; j-- {
			keys[j-1], keys[j] = keys[j], keys[j-1]
		}
	}
	return keys
}
