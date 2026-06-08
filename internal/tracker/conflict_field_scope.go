package tracker

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/steveyegge/beads/internal/storage"
	"github.com/steveyegge/beads/internal/types"
)

// bd-ajn: field-scoped conflict detection. Replaces the old whole-
// issue timestamp comparison with a per-field diff against two
// snapshots:
//   - LOCAL side: dolt_history_issues at the most recent commit
//     before lastSync (the "as-of-lastSync" local state)
//   - REMOTE side: linear_issue_snapshots (the "as-of-lastSync" remote
//     state, written by every successful push/pull mutation)
//
// A field appears in LocalChanged when its current value differs
// from its lastSync value. Same for ExternalChanged. Conflicting is
// the intersection. The resolver (resolveConflictsFieldScoped) maps
// each set to per-field push/pull/conflict actions.
//
// First-sync soft rollout (mayor Q5): when no snapshot exists for an
// issue, the detector logs a baseline line, snapshots the current
// remote state, and emits NO conflict for that issue this run. The
// next sync sees a real baseline and field-scoping kicks in.

// loadLocalStateAtSync queries dolt_history_issues for the issue's
// state at the most recent commit before lastSync. Returns
// (nil, nil) when no such commit exists (issue created after
// lastSync — treated as "no prior state" by the caller).
//
// Requires the backend to expose a historical query path. Returns a
// sentinel error when the backend doesn't (mocks in tests); callers
// fall back to whole-issue timestamp resolution in that case.
func loadLocalStateAtSync(ctx context.Context, store storage.Storage, issueID string, lastSync time.Time) (*types.Issue, error) {
	hv, ok := store.(storage.HistoryViewer)
	if !ok {
		return nil, errHistoryNotSupported
	}
	entries, err := hv.History(ctx, issueID)
	if err != nil {
		// Empty history is a common case (issue never committed pre-lastSync);
		// don't surface as an error.
		if errors.Is(err, sql.ErrNoRows) {
			return nil, nil
		}
		return nil, fmt.Errorf("history lookup for %s: %w", issueID, err)
	}
	// Entries are ordered newest-first per the HistoryViewer contract.
	// Find the first entry whose commit time is <= lastSync.
	for _, e := range entries {
		if e == nil || e.Issue == nil {
			continue
		}
		if !e.CommitDate.After(lastSync) {
			return e.Issue, nil
		}
	}
	// All committed versions are post-lastSync (issue created/changed
	// only after the last sync). Caller treats as "no prior state".
	return nil, nil
}

// errHistoryNotSupported signals a backend without history capability.
// Callers fall back to whole-issue timestamp resolution.
var errHistoryNotSupported = errors.New("storage backend does not expose IssueHistoryViewer")

// diffLocalFields returns the set of fields whose value differs
// between `current` and `atSync`. When atSync is nil (issue had no
// committed state at lastSync — fresh local creation since), returns
// all populated fields as "changed" so the push path handles the new
// record.
func diffLocalFields(current, atSync *types.Issue) map[ConflictField]bool {
	out := make(map[ConflictField]bool)
	if current == nil {
		return out
	}
	if atSync == nil {
		// Newly created since lastSync — every populated field is "new".
		if current.Title != "" {
			out[FieldTitle] = true
		}
		if current.Description != "" {
			out[FieldDescription] = true
		}
		out[FieldStatus] = true
		out[FieldPriority] = true
		return out
	}
	if current.Title != atSync.Title {
		out[FieldTitle] = true
	}
	if current.Description != atSync.Description {
		out[FieldDescription] = true
	}
	if current.Status != atSync.Status {
		out[FieldStatus] = true
	}
	if current.Priority != atSync.Priority {
		out[FieldPriority] = true
	}
	if current.Assignee != atSync.Assignee {
		out[FieldAssignee] = true
	}
	// Project and Parent for local side aren't tracked at the issues-
	// table level — they're modeled through dependencies. v1 leaves
	// them out of the local diff; the Linear side captures them via
	// snapshot. This means a local-only parent/project change won't
	// register as a "local" change in DetectConflicts — but those
	// fields are typically owned by the reconciler, not by direct user
	// edits, so the gap is acceptable for v1. v2 would join against
	// the dependencies table.
	return out
}

// diffExternalFields returns the set of fields whose value on the
// external tracker (per the just-fetched extIssue) differs from the
// snapshot (the as-of-lastSync remote state). When snapshot is nil,
// returns nil to signal "first-sync — no baseline yet"; caller
// handles the soft-rollout path.
func diffExternalFields(extIssue *TrackerIssue, snapshot *storage.LinearIssueSnapshot) map[ConflictField]bool {
	if extIssue == nil {
		return nil
	}
	if snapshot == nil {
		return nil // first-sync signal
	}
	out := make(map[ConflictField]bool)
	if extIssue.Title != snapshot.Title {
		out[FieldTitle] = true
	}
	if extIssue.Description != snapshot.Description {
		out[FieldDescription] = true
	}
	// Status comparison: prefer the stable state_id when both sides
	// have one (snapshot stores it, extIssue.Raw carries it for
	// adapters that populate the State field). When state_id isn't
	// available, fall back to comparing the rendered status name —
	// best-effort. status_id is the authoritative comparator per
	// mayor's Q2.
	if currentStateID := extractStateID(extIssue); currentStateID != "" && snapshot.StateID != "" {
		if currentStateID != snapshot.StateID {
			out[FieldStatus] = true
		}
	} else if currentStatus := extractStatusName(extIssue); currentStatus != snapshot.Status {
		out[FieldStatus] = true
	}
	if extIssue.Priority != snapshot.Priority {
		out[FieldPriority] = true
	}
	if extIssue.AssigneeID != snapshot.AssigneeID {
		out[FieldAssignee] = true
	}
	if extractProjectID(extIssue) != snapshot.ProjectID {
		out[FieldProject] = true
	}
	if extIssue.ParentInternalID != snapshot.ParentID {
		out[FieldParent] = true
	}
	return out
}

// extractStateID pulls Linear's workflow-state UUID from a
// TrackerIssue when the adapter populates extIssue.State as a Linear-
// flavored object. Returns "" when the field isn't a recognized
// shape (other adapters, missing Raw, etc.).
//
// Uses an interface assertion to stay tracker-package-pure (no
// linear-package import).
func extractStateID(ti *TrackerIssue) string {
	if ti == nil || ti.State == nil {
		return ""
	}
	type idHolder interface {
		GetID() string
	}
	if h, ok := ti.State.(idHolder); ok {
		return h.GetID()
	}
	// Last resort: struct field via map (Linear's State{ID:...} works
	// via a JSON marshal/unmarshal round-trip but that's expensive).
	// Adapters that want field-scoped status comparison can implement
	// the GetID interface on their State type.
	return ""
}

// extractStatusName pulls a human-readable state name as a fallback
// comparator when state_id isn't available on either side.
func extractStatusName(ti *TrackerIssue) string {
	if ti == nil || ti.State == nil {
		return ""
	}
	type nameHolder interface {
		GetName() string
	}
	if h, ok := ti.State.(nameHolder); ok {
		return h.GetName()
	}
	return ""
}

// extractProjectID pulls a project UUID from a TrackerIssue. Today
// TrackerIssue has no first-class project field; the Linear adapter
// stores it in Metadata for round-trip preservation. Returns "" when
// not present.
func extractProjectID(ti *TrackerIssue) string {
	if ti == nil || ti.Metadata == nil {
		return ""
	}
	if v, ok := ti.Metadata["project_id"]; ok {
		if s, ok := v.(string); ok {
			return s
		}
	}
	return ""
}

// computeConflictingFields returns the intersection of two field
// sets — the fields where BOTH sides moved since lastSync.
func computeConflictingFields(local, external map[ConflictField]bool) []ConflictField {
	if len(local) == 0 || len(external) == 0 {
		return nil
	}
	var out []ConflictField
	for f := range local {
		if external[f] {
			out = append(out, f)
		}
	}
	return out
}
