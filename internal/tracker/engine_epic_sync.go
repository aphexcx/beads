package tracker

import (
	"context"
	"fmt"

	"github.com/steveyegge/beads/internal/types"
)

// doEpicSync iterates local top-level epics and ensures the external
// tracker has a Project for each. Returns epicProjectMap[bead.ID] = the
// Linear project ID. doPush uses the map to skip those epics (they're
// now Projects on Linear, not Issues) while continuing to push their
// non-epic descendants normally.
//
// Skipped entirely when the tracker doesn't implement ProjectSyncer
// (GitHub/Jira adapters unaffected). Pure no-op for trackers without
// the capability.
//
// Routing per top-level epic:
//   - external_ref empty → CreateProject, write back ref = Project URL.
//     Idempotency safety net: scan existing Projects by exact title
//     before creating, to catch a prior-run crash-between-create-and-
//     writeback window (same pattern as bd-go9's migration tool).
//   - external_ref is a Project URL → UpdateProject so state changes
//     (open→completed) propagate. MapEpicToProjectState handles the
//     mapping; no special-case code.
//   - external_ref is an Issue URL → skip. bd-go9's
//     `bd linear migrate-epic-to-project` is the user-initiated tool
//     for legacy Issue→Project conversion; doEpicSync MUST NOT
//     auto-convert (would be a destructive surprise on every sync).
//
// Multi-team Linear setups are not supported in v1 (bd-1ay scope) and
// will refuse loudly via the same paths bd-go9 uses. Cross-team Project
// routing is a separate design.
func (e *Engine) doEpicSync(ctx context.Context, opts SyncOptions) (map[string]string, error) {
	syncer, ok := e.Tracker.(ProjectSyncer)
	if !ok {
		// Adapter doesn't implement ProjectSyncer — silent no-op.
		// Returning an empty (non-nil) map keeps doPush's lookup logic
		// branch-free.
		return map[string]string{}, nil
	}

	// Fetch all local issues once. Includes wisps merge (default behavior
	// of SearchIssues when filter.Ephemeral is nil).
	issues, err := e.Store.SearchIssues(ctx, "", types.IssueFilter{})
	if err != nil {
		return nil, fmt.Errorf("loading issues for epic-sync: %w", err)
	}

	// Build a quick parent-lookup so isTopLevelEpic doesn't re-query
	// per-epic. Pre-pass: for each epic, walk its parent-child dep ONCE.
	// (For a typical rig with ~10 epics this is negligible.)
	epicProjectMap := make(map[string]string)

	for _, issue := range issues {
		if issue == nil || issue.IssueType != types.TypeEpic {
			continue
		}
		isTop, tErr := e.isTopLevelEpic(ctx, issue.ID)
		if tErr != nil {
			return nil, fmt.Errorf("checking top-level epic for %s: %w", issue.ID, tErr)
		}
		if !isTop {
			continue
		}

		extRef := derefStr(issue.ExternalRef)
		switch {
		case extRef == "":
			// New top-level epic: ensure Linear has a Project for it.
			projectID, cErr := e.ensureLinearProjectForEpic(ctx, syncer, issue, opts.DryRun)
			if cErr != nil {
				return nil, fmt.Errorf("create Project for %s: %w", issue.ID, cErr)
			}
			if projectID != "" {
				epicProjectMap[issue.ID] = projectID
			} else if opts.DryRun {
				// Dry-run path returns "" (no API mutation, no real UUID).
				// Still register the epic in the map with a sentinel so the
				// per-issue loop in doPush skips it — otherwise dry-run
				// would print BOTH "Would create Project" (from epic-sync)
				// AND "Would create Issue" (from doPush's create branch)
				// for the same epic. Sentinel value isn't a real UUID, but
				// doPush only uses the map for membership-check (skip-or-not),
				// not for the value.
				epicProjectMap[issue.ID] = "dry-run-pending"
			}

		case syncer.IsProjectRef(extRef):
			// Existing Project — update on every sync so state and
			// metadata propagate. Need the UUID for UpdateProject;
			// extRef is the URL, so resolve via the Project list.
			projectID, rErr := resolveProjectIDFromRef(ctx, syncer, extRef)
			if rErr != nil {
				return nil, fmt.Errorf("resolve Project ID for %s: %w", issue.ID, rErr)
			}
			if projectID == "" {
				e.warn("epic-sync: Project URL on %s no longer resolves on Linear (deleted out-of-band?); skipping", issue.ID)
				continue
			}
			if opts.DryRun {
				e.msg("[dry-run] Would update Project %s for epic %s (status=%s)",
					projectID, issue.ID, issue.Status)
			} else {
				if uErr := syncer.UpdateProject(ctx, projectID, issue); uErr != nil {
					return nil, fmt.Errorf("update Project for %s: %w", issue.ID, uErr)
				}
			}
			epicProjectMap[issue.ID] = projectID

		default:
			// external_ref points to an Issue URL — bd-go9 migration
			// tool handles legacy conversion. doEpicSync intentionally
			// does NOT auto-convert (mayor decision in bd-go9 scope).
			continue
		}
	}

	return epicProjectMap, nil
}

// isTopLevelEpic returns true when beadID is an epic with no parent-
// child dep to another epic. (Walks one level up; if the immediate
// parent is also an epic, beadID is a sub-epic.) Per bd-1ay design,
// only top-level epics map to Linear Projects.
func (e *Engine) isTopLevelEpic(ctx context.Context, beadID string) (bool, error) {
	deps, err := e.Store.GetDependenciesWithMetadata(ctx, beadID)
	if err != nil {
		return false, err
	}
	for _, d := range deps {
		if d == nil || d.DependencyType != types.DepParentChild {
			continue
		}
		if d.Issue.IssueType == types.TypeEpic {
			return false, nil
		}
	}
	return true, nil
}

// ensureLinearProjectForEpic creates a Linear Project for the given
// epic and writes the resulting URL back to bead.external_ref. Returns
// the Linear project UUID.
//
// Idempotency safety net: before CreateProject fires, scans existing
// Projects by exact title match. Catches the rare crash window where a
// prior CreateProject succeeded but the external_ref writeback didn't
// — without this check, a second run would create a duplicate Project.
//
// Dry-run: prints a preview line and returns "" (caller distinguishes
// real-vs-dry-run via the empty return). NOTE: the bd-1ay v1 dry-run
// path has a known cosmetic gap — descendants of a NEW (empty-extref)
// top-level epic appear under doPush's "Would create Issue" lines, and
// the post-sync ReconcileProjectMembership pass shows zero Project
// links for them. Reason: the reconciler rebuilds links from the
// persisted bead store + Linear FetchProjects (no in-memory carry-
// through from this function), so a Project that doesn't exist on
// Linear yet has no URL for the reconciler to resolve. Acceptable for
// v1 — wet-run shows the complete picture, and the dry-run still tells
// the user a Project WILL be created.
func (e *Engine) ensureLinearProjectForEpic(ctx context.Context, syncer ProjectSyncer, epic *types.Issue, dryRun bool) (string, error) {
	if dryRun {
		e.msg("[dry-run] Would create Project %q for epic %s", epic.Title, epic.ID)
		return "", nil
	}

	// Safety-net: prior-run crash recovery. Match by exact title in the
	// project list to catch orphaned Projects from a half-run sync.
	if existingID, existingURL, err := lookupProjectByExactTitle(ctx, syncer, epic.Title); err != nil {
		return "", fmt.Errorf("scanning Linear projects for duplicates: %w", err)
	} else if existingID != "" {
		// Found one. Adopt by writing the URL back to the bead and
		// returning the ID. Subsequent UpdateProject paths take over.
		e.msg("epic-sync: adopting pre-existing Linear Project for %s (recovered from prior crash)", epic.ID)
		if err := e.Store.UpdateIssue(ctx, epic.ID, map[string]interface{}{"external_ref": existingURL}, e.Actor); err != nil {
			return "", fmt.Errorf("writing recovered external_ref on %s: %w", epic.ID, err)
		}
		return existingID, nil
	}

	url, id, err := syncer.CreateProject(ctx, epic)
	if err != nil {
		return "", err
	}
	if err := e.Store.UpdateIssue(ctx, epic.ID, map[string]interface{}{"external_ref": url}, e.Actor); err != nil {
		// Project exists on Linear; bead reference write failed. Surface
		// loudly — the next run's safety net will adopt the orphaned
		// Project by title, but the user should know.
		e.warn("epic-sync: Project %s created on Linear but external_ref write to %s failed: %v",
			url, epic.ID, err)
		return id, fmt.Errorf("writing external_ref on %s after Project create: %w", epic.ID, err)
	}
	e.msg("epic-sync: created Linear Project for %s → %s", epic.ID, url)
	return id, nil
}

// lookupProjectByExactTitle scans the tracker's project list for one
// whose Name exactly matches title. Returns (id, url) of the first
// match, or ("", "") if none. Caveat: Project names are not guaranteed
// unique on Linear — a perfect collision would adopt the wrong Project.
// Mitigation: this only fires on a fresh CreateProject attempt for an
// epic that has no external_ref, which is itself rare; combined with
// the rarer-still title collision, the false-positive surface is small.
// v2 could embed a deterministic marker in the Project description.
func lookupProjectByExactTitle(ctx context.Context, syncer ProjectSyncer, title string) (id, url string, err error) {
	projects, err := syncer.FetchProjects(ctx, "all")
	if err != nil {
		return "", "", err
	}
	for _, p := range projects {
		if p.Name == title {
			return p.ID, p.URL, nil
		}
	}
	return "", "", nil
}

// resolveProjectIDFromRef returns the Linear project UUID for the given
// Project URL by scanning the project list. Linear's UpdateProject takes
// the UUID, not the slug; ExtractProjectID returns the slug. So we need
// a list-and-match here.
//
// Returns "" with no error when the URL no longer resolves (Project
// deleted on Linear since the bead's external_ref was set). Caller
// should surface a warning and skip the epic.
func resolveProjectIDFromRef(ctx context.Context, syncer ProjectSyncer, ref string) (string, error) {
	projects, err := syncer.FetchProjects(ctx, "all")
	if err != nil {
		return "", err
	}
	for _, p := range projects {
		if p.URL == ref {
			return p.ID, nil
		}
	}
	return "", nil
}
