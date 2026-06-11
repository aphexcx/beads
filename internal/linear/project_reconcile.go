package linear

import (
	"context"
	"errors"
	"fmt"
)

// ProjectMembershipLink describes a desired (issue → Project)
// membership to wire up on the Linear side. Issue identifier is what
// Tracker.ExtractIdentifier yields from a bead's external_ref (e.g.
// "HOU-167"); ProjectID is the Linear Project UUID (already resolved
// — caller must pass the UUID, not the Project URL or slug).
//
// LocalBeadID is bd-ajn glue: the reconciler doesn't use it, but
// post-success the caller iterates stats.Mutations and patches the
// per-issue snapshot via Tracker.RecordPostAssignSnapshot to prevent
// the next sync from misreading "Linear's projectId changed" for the
// projectId we just pushed. Optional — empty string means the caller
// doesn't care about snapshot upkeep (mocks, tests).
type ProjectMembershipLink struct {
	IssueIdentifier string
	ProjectID       string
	LocalBeadID     string
}

// ProjectMembershipStats summarizes a ReconcileProjectMembership run.
// Same shape as ParentReconcileStats (bd-ena) for callers that handle
// both passes uniformly.
type ProjectMembershipStats struct {
	// Updated is the count of Linear Issues whose projectId field was
	// changed (set, cleared, or rewired) by this pass. Zero in dry-run.
	Updated int
	// WouldUpdate is the count of mutations the pass WOULD have issued
	// in wet-run. Populated only in dry-run mode; zero in wet-run.
	WouldUpdate int
	// Mutations is the (issue, project) link list that was applied
	// (wet-run; only after IssueUpdate success) or would have been
	// applied (dry-run). Same set regardless of mode.
	Mutations []ProjectMembershipLink
	// Skipped is the count of links where Linear's projectId already
	// matches the desired Project — no API mutation was issued.
	Skipped int
	// NotFound is the list of issue identifiers that didn't resolve to
	// a Linear Issue (typically because the bead is unsynced or the
	// Linear Issue was deleted out-of-band). Their links are silently
	// skipped; the next sync will retry.
	NotFound []string
	// Errors collects per-link failures that did not abort the pass.
	Errors []error
	// SnapshotWarnings is bd-ajn glue: post-success snapshot patch
	// failures land here, not in Errors. Distinct severity — a
	// missed snapshot only costs ONE spurious conflict-gate on the
	// next sync (because DetectConflicts's first-sync path will
	// baseline at that point); the API mutation itself succeeded.
	// Callers should surface these as warnings, not errors.
	SnapshotWarnings []error
}

// ReconcileProjectMembership wires (issue → Project) membership from
// bead-side parent-child dependencies into Linear's projectId field.
// Symmetric to ReconcileParents (bd-ena) but for the Project case:
//
//   - bd-ena's ReconcileParents handles non-Project parents (parentId)
//   - this pass handles Project parents (projectId)
//
// Used as the post-sync second pass for bd-1ay's epic-as-Project
// orchestration. For each non-epic descendant whose nearest top-level-
// epic ancestor is a Linear Project, ensure projectId is set correctly.
// Idempotent: fetches each Issue's current projectId and only issues
// IssueUpdate when there's a delta.
//
// When dryRun is true, the read-only fetches still run (caller gets
// accurate Skipped / NotFound / Mutations preview) but the IssueUpdate
// mutation is skipped; counts go to WouldUpdate.
//
// Returns nil error when the pass completed (even if per-link errors
// were collected in Stats.Errors). A non-nil error indicates a setup-
// level failure (no client, rate-limit abort, etc.) that prevented any
// further work.
func (t *Tracker) ReconcileProjectMembership(ctx context.Context, links []ProjectMembershipLink, dryRun bool) (*ProjectMembershipStats, error) {
	stats := &ProjectMembershipStats{}
	if len(links) == 0 {
		return stats, nil
	}
	if t.primaryClient() == nil {
		return nil, errors.New("no Linear client available")
	}

	// Cache identifier → (issue, host client) so we don't refetch when
	// the same issue appears more than once (defensive) AND so the
	// update path uses the SAME client that successfully fetched the
	// issue. Same pattern as ReconcileParents.
	type entry struct {
		issue  *Issue
		client *Client
	}
	fetched := make(map[string]entry, len(links))
	fetchIssue := func(identifier string) (entry, error) {
		if cached, ok := fetched[identifier]; ok {
			return cached, nil
		}
		issue, client, err := t.fetchIssueAcrossTeams(ctx, identifier)
		if err != nil {
			return entry{}, err
		}
		e := entry{issue: issue, client: client}
		fetched[identifier] = e // cache nil too — repeated lookups are cheap
		return e, nil
	}

	for _, link := range links {
		if link.IssueIdentifier == "" || link.ProjectID == "" {
			continue
		}

		issueE, err := fetchIssue(link.IssueIdentifier)
		if err != nil {
			// Rate-limit circuit breaker tripped — stop now rather than
			// hammer the API for every remaining link.
			if isRateLimitExhausted(err) {
				return stats, fmt.Errorf("fetch issue %s: %w", link.IssueIdentifier, err)
			}
			stats.Errors = append(stats.Errors,
				fmt.Errorf("fetch issue %s: %w", link.IssueIdentifier, err))
			continue
		}
		if issueE.issue == nil {
			stats.NotFound = append(stats.NotFound, link.IssueIdentifier)
			continue
		}

		// Idempotency: skip if remote projectId already matches.
		if issueE.issue.Project != nil && issueE.issue.Project.ID == link.ProjectID {
			stats.Skipped++
			continue
		}

		if dryRun {
			stats.Mutations = append(stats.Mutations, link)
			stats.WouldUpdate++
			continue
		}

		_, err = issueE.client.UpdateIssue(ctx, issueE.issue.ID, map[string]interface{}{
			"projectId": link.ProjectID,
		})
		if err != nil {
			if isRateLimitExhausted(err) {
				return stats, fmt.Errorf("assign %s to Project %s: %w",
					link.IssueIdentifier, link.ProjectID, err)
			}
			stats.Errors = append(stats.Errors,
				fmt.Errorf("assign %s to Project %s: %w",
					link.IssueIdentifier, link.ProjectID, err))
			continue
		}
		// bd-ajn: patch the per-issue snapshot's project_id so the next
		// sync doesn't read this projectId-set as "Linear changed it"
		// and override unrelated local changes (the migration scenario
		// the bug ticket describes). Best-effort: failures route to
		// SnapshotWarnings (not Errors) per codex round-1 severity
		// note — a missed patch only costs one extra conflict-gate
		// next sync; the API mutation itself succeeded.
		if link.LocalBeadID != "" {
			if sErr := t.RecordPostAssignSnapshot(ctx, link.LocalBeadID, link.ProjectID); sErr != nil {
				stats.SnapshotWarnings = append(stats.SnapshotWarnings,
					fmt.Errorf("snapshot patch for %s after Project-assign: %w",
						link.IssueIdentifier, sErr))
			}
		}
		// Mutations is appended only after the IssueUpdate API call
		// succeeds, so the list reflects state actually propagated to
		// Linear — matches the bd-cs1-round-1 contract that ReconcileParents
		// adopted: callers can trust Mutations for post-sync reporting.
		stats.Mutations = append(stats.Mutations, link)
		stats.Updated++
	}

	return stats, nil
}
