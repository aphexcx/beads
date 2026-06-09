package linear

import (
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
