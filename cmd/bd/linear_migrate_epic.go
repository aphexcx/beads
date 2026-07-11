package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"sort"
	"strings"
	"unicode/utf8"

	"github.com/spf13/cobra"
	"github.com/steveyegge/beads/internal/linear"
	"github.com/steveyegge/beads/internal/types"
)

// metadata key stored on the epic bead between CreateProject and the final
// external_ref rewrite. Lets a re-run after partial failure reuse the
// already-created Linear Project instead of creating a duplicate.
const linearMigrationMetadataKey = "bd:linear_migration_project_url"

// linearMigrateEpicCmd handles `bd linear migrate-epic-to-project <bead-id>`.
var linearMigrateEpicCmd = &cobra.Command{
	Use:   "migrate-epic-to-project <bead-id>",
	Short: "Convert a legacy Issue-backed top-level epic into a Linear Project",
	Long: `Convert a top-level epic that was pushed to Linear as a regular Issue
into a Linear Project, with all descendant Issues reassigned via projectId.

This is needed because Linear's data model says top-level epics belong as
Projects (which surface on the Projects page and group issues structurally),
while bd currently pushes every epic as an Issue. This tool fixes one epic
at a time.

The legacy Issue (HOU-N before the migration) is left in place by default —
its history, comments, attachments stay accessible. Use --legacy-issue=close
to close it once you've verified the Project shows the work correctly.

Workflow:
  1. Plan phase (always runs): enumerates descendants, validates refusal
     cases (must be epic, must have no parent epic, external_ref must be
     a Linear Issue URL, descendants must not span teams), prints
     per-descendant preview.
  2. Execute phase (when not --dry-run):
     a. CreateProject on Linear with the epic's title + description.
     b. Stash the new Project URL in bead metadata (for crash-resume).
     c. AssignIssueToProject on every descendant (skip if already correct).
     d. Handle legacy Issue per --legacy-issue mode.
     e. Update bead.external_ref to the new Project URL.

Idempotent on re-run: if the metadata-stashed Project URL exists, the
already-created Project is reused; descendant assignments and legacy
handling are skipped per their current state.

Refusal cases:
  - bead is not an epic
  - bead has a parent-child dep to another epic (i.e., it's a sub-epic)
  - bead's external_ref is already a Linear Project URL (already migrated)
  - bead has no external_ref (not yet pushed)
  - descendants span multiple Linear teams (v1 limitation)

Examples:
  bd linear migrate-epic-to-project hw-sv1v --dry-run
  bd linear migrate-epic-to-project hw-sv1v
  bd linear migrate-epic-to-project hw-sv1v --legacy-issue=close`,
	Args: cobra.ExactArgs(1),
	RunE: runLinearMigrateEpic,
}

func init() {
	linearMigrateEpicCmd.Flags().Bool("dry-run", false, "Preview the migration without making any Linear API mutations")
	linearMigrateEpicCmd.Flags().String("legacy-issue", "keep",
		"Disposition for the legacy Issue after Project creation: "+
			"keep (default — comment with link, leave open), "+
			"close (comment + close), "+
			"delete (NOT SUPPORTED in v1; use Linear UI manually)")
	linearCmd.AddCommand(linearMigrateEpicCmd)
}

// epicMigrationPlan captures the work the migration will perform. Computed
// during the plan phase and re-used by the execute phase.
type epicMigrationPlan struct {
	Bead              *types.Issue              // the epic bead being migrated
	LegacyIdentifier  string                    // e.g. "HOU-159"; empty after rewrite has completed
	TeamID            string                    // resolved single team for the descendants (URL slug)
	ExistingProjectID string                    // populated when resume detects a prior CreateProject
	ExistingProject   *linear.Project           // fetched when ExistingProjectID is set
	Descendants       []epicMigrationDescendant // stable order: input listing order
	DirectChildBeads  map[string]bool           // bead IDs of direct sub-epic children (for parentId clear)
	// Unsynced is the set of descendants found during the walk that
	// don't have a usable Linear external_ref yet. They're surfaced in
	// the plan output so the user can decide whether to push them
	// before running migration (recommended) or proceed and assign
	// them later. Migration won't touch these.
	Unsynced []unsyncedDescendant
	// CloseStateID is pre-resolved at plan time when --legacy-issue=close
	// so we fail loudly BEFORE any Linear mutations if linear.state_map
	// isn't configured. Empty when legacy mode is "keep".
	CloseStateID string
}

// epicMigrationDescendant is one bead in the tree under the root epic that
// will be assigned to the Project.
type epicMigrationDescendant struct {
	BeadID     string
	Identifier string // e.g. "HOU-167"
}

// unsyncedDescendant is a bead in the tree that doesn't have a Linear
// external_ref yet (not pushed). Captured separately so the plan output
// can surface them visibly — if the root rewrite completes before these
// descendants get a ref, they won't be assigned to the Project until
// some future bd-1ay-style projectId reconcile pass picks them up.
type unsyncedDescendant struct {
	BeadID string
	Reason string // "no external_ref" or "non-Linear external_ref"
}

func runLinearMigrateEpic(cmd *cobra.Command, args []string) error {
	beadID := args[0]
	dryRun, _ := cmd.Flags().GetBool("dry-run")
	legacyMode, _ := cmd.Flags().GetString("legacy-issue")

	switch legacyMode {
	case "keep", "close":
	case "delete":
		return HandleError("--legacy-issue=delete is not supported in v1 (no Linear DeleteIssue mutation wired); " +
			"use the Linear UI to delete manually, or use --legacy-issue=close")
	default:
		return HandleError("invalid --legacy-issue %q (use keep or close)", legacyMode)
	}
	if !dryRun {
		CheckReadonly("linear migrate-epic-to-project")
	}

	if err := ensureStoreActive(); err != nil {
		return HandleError("database not available: %v", err)
	}
	if err := validateLinearConfig(nil); err != nil {
		return HandleError("%v", err)
	}

	ctx := rootCtx
	teamIDs := getLinearTeamIDs(ctx, nil)
	lt := &linear.Tracker{}
	lt.SetTeamIDs(teamIDs)
	if err := lt.Init(ctx, store); err != nil {
		return HandleError("initializing Linear tracker: %v", err)
	}

	plan, err := planEpicMigration(ctx, lt, beadID, legacyMode, teamIDs)
	if err != nil {
		return HandleError("%v", err)
	}

	printEpicMigrationPlan(plan, dryRun, legacyMode)

	if dryRun {
		return nil
	}

	if err := executeEpicMigration(ctx, lt, plan, legacyMode, actor); err != nil {
		return HandleError("migration failed: %v", err)
	}

	fmt.Printf("\n✓ Migration complete: %s is now a Linear Project\n", plan.Bead.ID)
	return nil
}

// planEpicMigration loads the epic, validates refusal cases, enumerates
// descendants, and (if resumable) fetches the existing Project. Pure
// computation + read-only Linear fetches; no Linear mutations.
//
// configuredTeamIDs is the list of teams the Linear adapter is configured
// for. v1 refuses multi-team setups outright (codex bd-go9 review): if
// more than one team is configured, primaryClient() may not be the team
// that owns the bead's Issue, and CreateProject / retireLegacyIssue would
// silently target the wrong team. Multi-team is bd-1ay scope.
func planEpicMigration(ctx context.Context, lt *linear.Tracker, beadID, legacyMode string, configuredTeamIDs []string) (*epicMigrationPlan, error) {
	if len(configuredTeamIDs) > 1 {
		return nil, fmt.Errorf("multi-team Linear setups are not supported in v1 (have %d configured teams); "+
			"the migration tool routes via the primary client, which may not own the bead's Linear Issue. "+
			"Configure linear.team_id to a single team and retry", len(configuredTeamIDs))
	}

	bead, err := store.GetIssue(ctx, beadID)
	if err != nil {
		return nil, fmt.Errorf("loading bead %s: %w", beadID, err)
	}
	if bead == nil {
		return nil, fmt.Errorf("bead %s not found", beadID)
	}

	if bead.IssueType != types.TypeEpic {
		return nil, fmt.Errorf("bead %s is type %q, not %q — only epics can be migrated to Projects",
			beadID, bead.IssueType, types.TypeEpic)
	}
	if bead.ExternalRef == nil || strings.TrimSpace(*bead.ExternalRef) == "" {
		return nil, fmt.Errorf("bead %s has no external_ref; push it to Linear first, then migrate",
			beadID)
	}
	ref := strings.TrimSpace(*bead.ExternalRef)
	if !lt.IsExternalRef(ref) {
		return nil, fmt.Errorf("bead %s external_ref %q is not a Linear URL", beadID, ref)
	}

	// Refuse sub-epics: a bead with a parent-child dep TO another epic is
	// not a top-level epic.
	deps, err := store.GetDependenciesWithMetadata(ctx, beadID)
	if err != nil {
		return nil, fmt.Errorf("loading parent deps for %s: %w", beadID, err)
	}
	for _, d := range deps {
		if d == nil || d.DependencyType != types.DepParentChild {
			continue
		}
		if d.Issue.IssueType == types.TypeEpic {
			return nil, fmt.Errorf("bead %s is a sub-epic of %s — only top-level epics can be migrated; "+
				"migrate the root epic instead", beadID, d.Issue.ID)
		}
	}

	plan := &epicMigrationPlan{
		Bead:             bead,
		DirectChildBeads: make(map[string]bool),
	}

	// Resume detection. If bead.metadata holds the new-Project URL, the
	// migration was interrupted after CreateProject; reuse instead of
	// creating a duplicate. If the external_ref itself is already a
	// Project URL, migration completed; refuse re-run.
	storedProjectURL := extractStoredProjectURL(bead.Metadata)
	if lt.IsProjectRef(ref) {
		return nil, fmt.Errorf("bead %s external_ref %q is already a Project URL — "+
			"migration appears to have completed; nothing to do", beadID, ref)
	}
	plan.LegacyIdentifier = lt.ExtractIdentifier(ref)
	if plan.LegacyIdentifier == "" {
		return nil, fmt.Errorf("could not extract Linear identifier from %q", ref)
	}
	if storedProjectURL != "" {
		// Resume: fetch the stashed Project to confirm it still exists.
		project, fetchErr := findLinearProjectByURL(ctx, lt, storedProjectURL)
		if fetchErr != nil {
			return nil, fmt.Errorf("resume: fetching previously-created Project %q: %w",
				storedProjectURL, fetchErr)
		}
		if project == nil {
			return nil, fmt.Errorf("resume: stashed Project URL %q no longer resolves on Linear; "+
				"manually clear bd:linear_migration_project_url from bead %s metadata to start fresh",
				storedProjectURL, beadID)
		}
		plan.ExistingProjectID = project.ID
		plan.ExistingProject = project
	}

	// Enumerate descendants transitively via parent-child deps.
	descBeadIDs, directChildren, err := collectDescendantBeads(ctx, beadID)
	if err != nil {
		return nil, fmt.Errorf("walking descendants of %s: %w", beadID, err)
	}
	for _, id := range directChildren {
		plan.DirectChildBeads[id] = true
	}

	teamSeen := make(map[string]bool)
	for _, descID := range descBeadIDs {
		descBead, gErr := store.GetIssue(ctx, descID)
		if gErr != nil {
			return nil, fmt.Errorf("loading descendant %s: %w", descID, gErr)
		}
		if descBead == nil || descBead.ExternalRef == nil {
			// Surface, don't silently swallow (codex bd-go9 E2): the
			// migration can't assign these to the Project until they're
			// pushed. After root rewrite, a re-run of the migration
			// refuses ("already migrated") and these stay unassigned
			// until bd-1ay's projectId-reconcile pass.
			plan.Unsynced = append(plan.Unsynced, unsyncedDescendant{
				BeadID: descID,
				Reason: "no external_ref (not pushed to Linear yet)",
			})
			continue
		}
		descRef := strings.TrimSpace(*descBead.ExternalRef)
		if !lt.IsExternalRef(descRef) {
			plan.Unsynced = append(plan.Unsynced, unsyncedDescendant{
				BeadID: descID,
				Reason: fmt.Sprintf("non-Linear external_ref %q", descRef),
			})
			continue
		}
		ident := lt.ExtractIdentifier(descRef)
		if ident == "" {
			plan.Unsynced = append(plan.Unsynced, unsyncedDescendant{
				BeadID: descID,
				Reason: fmt.Sprintf("Linear external_ref %q has no extractable identifier", descRef),
			})
			continue
		}
		plan.Descendants = append(plan.Descendants, epicMigrationDescendant{
			BeadID:     descID,
			Identifier: ident,
		})
		// Use the team_id embedded in the Linear URL's path segment
		// (right after https://linear.app/) — same single-token check
		// used elsewhere in the adapter for routing.
		teamSeen[linearTeamSlugFromRef(descRef)] = true
	}

	if len(teamSeen) > 1 {
		teams := make([]string, 0, len(teamSeen))
		for t := range teamSeen {
			teams = append(teams, t)
		}
		sort.Strings(teams)
		return nil, fmt.Errorf("descendants span multiple Linear teams %v — v1 only supports single-team migration; "+
			"split the tree or migrate per-team manually", teams)
	}
	if len(teamSeen) == 1 {
		for t := range teamSeen {
			plan.TeamID = t
		}
	}

	// Stable order: by identifier suffix-number when both look like TEAM-N.
	sort.Slice(plan.Descendants, func(i, j int) bool {
		return plan.Descendants[i].Identifier < plan.Descendants[j].Identifier
	})

	// Pre-resolve the closed-state ID when --legacy-issue=close, so an
	// unconfigured linear.state_map fails LOUDLY here rather than after
	// CreateProject has already mutated Linear (codex bd-go9 review D).
	if legacyMode == "close" {
		cache, cErr := linear.BuildStateCacheFromTracker(ctx, lt)
		if cErr != nil {
			return nil, fmt.Errorf("building Linear state cache for --legacy-issue=close: %w", cErr)
		}
		stateID, sErr := linear.ResolveStateIDForBeadsStatus(cache, types.StatusClosed, lt.MappingConfig())
		if sErr != nil {
			return nil, fmt.Errorf("resolving closed-state ID for --legacy-issue=close (likely missing linear.state_map config): %w", sErr)
		}
		plan.CloseStateID = stateID
	}

	return plan, nil
}

// collectDescendantBeads walks the parent-child tree rooted at rootID and
// returns all transitively-reachable descendants AND the direct children.
// Both lists exclude rootID. Direct children are the beads whose parent-
// child dep targets rootID directly (used by execute phase to clear
// their parentId on Linear post-migration).
func collectDescendantBeads(ctx context.Context, rootID string) (all []string, directChildren []string, err error) {
	visited := map[string]bool{rootID: true}
	queue := []string{rootID}
	for len(queue) > 0 {
		current := queue[0]
		queue = queue[1:]

		dependents, depErr := store.GetDependentsWithMetadata(ctx, current)
		if depErr != nil {
			return nil, nil, fmt.Errorf("loading dependents of %s: %w", current, depErr)
		}
		for _, d := range dependents {
			if d == nil || d.DependencyType != types.DepParentChild {
				continue
			}
			childID := d.Issue.ID
			if visited[childID] {
				continue
			}
			visited[childID] = true
			all = append(all, childID)
			if current == rootID {
				directChildren = append(directChildren, childID)
			}
			queue = append(queue, childID)
		}
	}
	return all, directChildren, nil
}

// findLinearProjectByURL fetches all Linear projects and returns the one
// matching the given URL (by exact match). Returns nil if not found.
// Used only on the resume path — newly-created Projects are returned by
// CreateProject directly.
func findLinearProjectByURL(ctx context.Context, lt *linear.Tracker, projectURL string) (*linear.Project, error) {
	projects, err := lt.FetchProjects(ctx, "all")
	if err != nil {
		return nil, err
	}
	// Codex bd-6cl round-2 bug 4: tolerate a trailing-title-slug
	// difference between the caller's projectURL (often from a
	// bead's stored external_ref) and Linear's current Project URL.
	// Canonicalize both sides before comparing.
	needle := projectURL
	if canonical, ok := linear.CanonicalizeLinearExternalRef(projectURL); ok {
		needle = canonical
	}
	for _, p := range projects {
		key := p.URL
		if canonical, ok := linear.CanonicalizeLinearExternalRef(p.URL); ok {
			key = canonical
		}
		if key == needle {
			// Note: tracker.TrackerProject doesn't expose the underlying
			// linear.Project; re-fetch via the Linear-specific client to
			// get the typed *linear.Project. Cheap (~1 API call).
			return &linear.Project{
				ID:    p.ID,
				Name:  p.Name,
				URL:   p.URL,
				State: p.State,
			}, nil
		}
	}
	return nil, nil
}

// linearTeamSlugFromRef returns the team-slug path segment from a Linear
// URL (e.g. "https://linear.app/houmanoids/issue/HOU-1" → "houmanoids").
// Returns empty string for malformed input.
func linearTeamSlugFromRef(ref string) string {
	// Format: https://linear.app/<team-slug>/(issue|project)/<id>/...
	if !strings.Contains(ref, "linear.app/") {
		return ""
	}
	rest := ref[strings.Index(ref, "linear.app/")+len("linear.app/"):]
	if i := strings.Index(rest, "/"); i >= 0 {
		return rest[:i]
	}
	return ""
}

// extractStoredProjectURL pulls the migration-resume metadata field from
// bead.Metadata (json.RawMessage). Returns empty string when missing or
// malformed (not an error — fresh migrations have no metadata).
func extractStoredProjectURL(raw json.RawMessage) string {
	if len(raw) == 0 {
		return ""
	}
	var m map[string]interface{}
	if err := json.Unmarshal(raw, &m); err != nil {
		return ""
	}
	v, ok := m[linearMigrationMetadataKey].(string)
	if !ok {
		return ""
	}
	return strings.TrimSpace(v)
}

// setStoredProjectURL writes the migration-resume metadata field to a
// JSON bytes value preserving any existing fields. Returns the new
// json.RawMessage suitable for store.UpdateIssue("metadata", value).
func setStoredProjectURL(raw json.RawMessage, projectURL string) ([]byte, error) {
	m := map[string]interface{}{}
	if len(raw) > 0 {
		if err := json.Unmarshal(raw, &m); err != nil {
			return nil, fmt.Errorf("bead metadata is not a JSON object: %w", err)
		}
	}
	if projectURL == "" {
		delete(m, linearMigrationMetadataKey)
	} else {
		m[linearMigrationMetadataKey] = projectURL
	}
	return json.Marshal(m)
}

// printEpicMigrationPlan emits a human-readable plan. Same format for
// dry-run and execute (execute follows it with the actual mutations).
func printEpicMigrationPlan(plan *epicMigrationPlan, dryRun bool, legacyMode string) {
	prefix := ""
	if dryRun {
		prefix = "[dry-run] "
	}

	if plan.ExistingProject != nil {
		fmt.Printf("%sResume: reusing existing Project %s (%s) for bead %s\n",
			prefix, plan.ExistingProject.URL, plan.ExistingProject.ID, plan.Bead.ID)
	} else {
		fmt.Printf("%sWould create Project %q in team %s for bead %s (currently Issue %s)\n",
			prefix, plan.Bead.Title, plan.TeamID, plan.Bead.ID, plan.LegacyIdentifier)
	}
	// Surface the description-truncation behavior so users know what to
	// expect on Linear (bd-cs1). Linear's ProjectCreateInput.description
	// hard-caps at 255 chars; we truncate for the description summary
	// and pass the full text via content (rich body, no length limit).
	// Count runes (chars), not bytes — matches TruncateLinearProjectDescription's
	// rune-counted budget, so the user-facing number is consistent with the
	// 255 cap they're being told about.
	if _, truncated := linear.TruncateLinearProjectDescription(plan.Bead.Description); truncated {
		fmt.Printf("%sNote: bead description (%d chars) exceeds Linear's Project description limit (255); "+
			"the truncated summary will be the description, full text will be the Project content (rich body).\n",
			prefix, utf8.RuneCountInString(plan.Bead.Description))
	}
	fmt.Printf("%sWould assign %d descendant Issue(s) to the Project:\n",
		prefix, len(plan.Descendants))
	for _, d := range plan.Descendants {
		marker := ""
		if plan.DirectChildBeads[d.BeadID] {
			marker = "  (direct sub-epic — parentId will be cleared)"
		}
		fmt.Printf("%s  %s ← %s%s\n", prefix, d.Identifier, d.BeadID, marker)
	}
	if len(plan.Unsynced) > 0 {
		fmt.Fprintf(os.Stderr, "%sWARNING: %d descendant bead(s) lack a usable Linear external_ref and will NOT be assigned to the Project:\n",
			prefix, len(plan.Unsynced))
		for _, u := range plan.Unsynced {
			fmt.Fprintf(os.Stderr, "%s  %s — %s\n", prefix, u.BeadID, u.Reason)
		}
		fmt.Fprintf(os.Stderr, "%sPush these to Linear first (or accept that they'll require bd-1ay's projectId reconcile to wire up later).\n",
			prefix)
	}
	switch legacyMode {
	case "keep":
		fmt.Printf("%sLegacy Issue %s: add comment linking to Project, leave open (--legacy-issue=keep)\n",
			prefix, plan.LegacyIdentifier)
	case "close":
		fmt.Printf("%sLegacy Issue %s: add comment, then close (--legacy-issue=close)\n",
			prefix, plan.LegacyIdentifier)
	}
	fmt.Printf("%sBead %s external_ref will be rewritten from Issue URL to Project URL\n",
		prefix, plan.Bead.ID)
}

// executeEpicMigration performs the actual Linear and bd mutations per
// the plan. Order is failure-aware: CreateProject + stash metadata first
// (so resume can detect partial state), then idempotent descendant
// assignments, then legacy retirement, then external_ref rewrite as the
// final commit point.
func executeEpicMigration(ctx context.Context, lt *linear.Tracker, plan *epicMigrationPlan, legacyMode, actor string) error {
	// Step 1: ensure Project exists. Three paths:
	//   1a. Resume: plan.ExistingProject set from bead.Metadata stash.
	//   1b. Pre-create idempotency check: scan Linear's project list for a
	//       project with this epic's exact title. Catches the
	//       crash-between-CreateProject-and-stash window (codex bd-go9
	//       review A) — without this check, a second run after that
	//       narrow crash would create a duplicate Project.
	//   1c. Fresh: CreateProject.
	projectURL := ""
	projectID := ""
	switch {
	case plan.ExistingProject != nil:
		projectURL = plan.ExistingProject.URL
		projectID = plan.ExistingProject.ID
		fmt.Printf("Reusing existing Project %s\n", projectURL)
	default:
		existing, fErr := findLinearProjectByExactName(ctx, lt, plan.Bead.Title)
		if fErr != nil {
			return fmt.Errorf("scanning Linear projects for duplicate detection: %w", fErr)
		}
		if existing != nil {
			projectURL = existing.URL
			projectID = existing.ID
			fmt.Printf("✓ Found pre-existing Linear Project with matching title (recovering from prior crash): %s\n", projectURL)
		} else {
			url, id, err := lt.CreateProject(ctx, plan.Bead)
			if err != nil {
				return fmt.Errorf("creating Linear Project for %s: %w", plan.Bead.ID, err)
			}
			projectURL = url
			projectID = id
			fmt.Printf("✓ Created Linear Project: %s\n", projectURL)
		}

		// Step 2: stash the Project URL in bead.Metadata so a crash
		// mid-descendant-loop is resumable. Done for both the
		// freshly-created and pre-existing-found paths.
		newMeta, mErr := setStoredProjectURL(plan.Bead.Metadata, projectURL)
		if mErr != nil {
			return fmt.Errorf("marshaling resume metadata for %s: %w", plan.Bead.ID, mErr)
		}
		if uErr := store.UpdateIssue(ctx, plan.Bead.ID,
			map[string]interface{}{"metadata": string(newMeta)}, actor); uErr != nil {
			return fmt.Errorf("stashing resume metadata on %s: %w", plan.Bead.ID, uErr)
		}
	}

	// Step 3: AssignIssueToProject for each descendant + clear parentId
	// on direct sub-epic children. Idempotent: the projectId check skips
	// already-assigned descendants, but parentId clearing is checked
	// independently because the two API calls are separate (codex bd-go9
	// review C — if projectId was set on a prior run but the
	// SetIssueParent step failed mid-way, we still need to clear it).
	assigned, skippedAssign := 0, 0
	parentCleared, skippedClear := 0, 0
	for _, d := range plan.Descendants {
		issue, _, fErr := fetchLinearIssueForMigration(ctx, lt, d.Identifier)
		if fErr != nil {
			return fmt.Errorf("fetching %s: %w", d.Identifier, fErr)
		}
		if issue == nil {
			return fmt.Errorf("Linear issue %s not found (was the bead's external_ref rewritten externally?)", d.Identifier)
		}

		// projectId assignment.
		if issue.Project != nil && issue.Project.ID == projectID {
			skippedAssign++
		} else {
			if err := lt.AssignIssueToProject(ctx, d.Identifier, projectID); err != nil {
				return fmt.Errorf("assigning %s to Project: %w", d.Identifier, err)
			}
			// bd-ajn: patch the per-issue snapshot so the next sync
			// doesn't read this projectId-set as "Linear changed it"
			// and force-pull, reverting any unrelated local changes
			// (the specific scenario bd-ajn fixes — migration vs.
			// concurrent local status close).
			if sErr := lt.RecordPostAssignSnapshot(ctx, d.BeadID, projectID); sErr != nil {
				fmt.Printf("⚠ snapshot patch failed for %s after assign: %v (next sync will baseline)\n", d.BeadID, sErr)
			}
			assigned++
		}

		// parentId clear for direct sub-epic children. Send Go nil so
		// Linear's GraphQL receives null (clears the field). Empty
		// string would be rejected or no-op (codex bd-go9 review B).
		if plan.DirectChildBeads[d.BeadID] {
			if issue.Parent == nil {
				skippedClear++
			} else {
				if err := clearLinearIssueParent(ctx, lt, d.Identifier); err != nil {
					return fmt.Errorf("clearing parentId on direct sub-epic %s: %w", d.Identifier, err)
				}
				// bd-ajn: snapshot's parent_id needs to clear too
				// (empty string represents "no parent"). Without this,
				// next sync sees Linear's parent field as having
				// changed from <old uuid> to nil and force-pulls.
				if sErr := lt.RecordPostSetParentSnapshot(ctx, d.BeadID, ""); sErr != nil {
					fmt.Printf("⚠ snapshot patch failed for %s after parent clear: %v (next sync will baseline)\n", d.BeadID, sErr)
				}
				parentCleared++
			}
		}
	}
	fmt.Printf("✓ Descendant assignments: %d set, %d already correct\n", assigned, skippedAssign)
	if parentCleared+skippedClear > 0 {
		fmt.Printf("✓ Direct sub-epic parentId: %d cleared, %d already clear\n", parentCleared, skippedClear)
	}

	// Step 4: handle the legacy Issue per --legacy-issue mode. Uses the
	// CloseStateID resolved at plan time so a missing linear.state_map
	// can't sneak through to this point (codex bd-go9 review D).
	if plan.LegacyIdentifier != "" {
		if err := retireLegacyIssue(ctx, lt, plan.LegacyIdentifier, projectURL, legacyMode, plan.CloseStateID); err != nil {
			return fmt.Errorf("retiring legacy Issue %s: %w", plan.LegacyIdentifier, err)
		}
	}

	// Step 5: rewrite bead.external_ref to the Project URL AND clear the
	// resume metadata. This is the migration's commit point — once it
	// succeeds, subsequent runs see the bead as already-migrated.
	clearedMeta, mErr := setStoredProjectURL(plan.Bead.Metadata, "")
	if mErr != nil {
		return fmt.Errorf("clearing resume metadata on %s: %w", plan.Bead.ID, mErr)
	}
	if err := store.UpdateIssue(ctx, plan.Bead.ID, map[string]interface{}{
		"external_ref": projectURL,
		"metadata":     string(clearedMeta),
	}, actor); err != nil {
		return fmt.Errorf("rewriting external_ref on %s: %w", plan.Bead.ID, err)
	}
	fmt.Printf("✓ Bead %s external_ref now points at Project\n", plan.Bead.ID)
	return nil
}

// fetchLinearIssueForMigration is a thin wrapper around the tracker's
// primary client's FetchIssueByIdentifier. Kept separate from the parent
// reconciler's helper to avoid coupling the two features — if the
// reconciler's fetch caching changes, migration's read-once-per-call
// pattern is unaffected.
func fetchLinearIssueForMigration(ctx context.Context, lt *linear.Tracker, identifier string) (*linear.Issue, *linear.Client, error) {
	// Linear identifiers are unique across the workspace; primary client is
	// fine here because the multi-team refusal in plan phase guarantees a
	// single team. (Multi-team migration support is bd-1ay scope.)
	client := lt.PrimaryClient()
	if client == nil {
		return nil, nil, fmt.Errorf("no Linear client available")
	}
	issue, err := client.FetchIssueByIdentifier(ctx, identifier)
	return issue, client, err
}

// clearLinearIssueParent sets the issue's parentId to null on Linear.
// Linear's GraphQL requires the JSON value `null` (not an empty string)
// to clear the field — passing "" silently no-ops or returns an error
// from the server. We route through the raw Client.UpdateIssue with an
// explicit nil so JSON encoding produces `"parentId": null`.
func clearLinearIssueParent(ctx context.Context, lt *linear.Tracker, identifier string) error {
	client := lt.PrimaryClient()
	if client == nil {
		return fmt.Errorf("no Linear client available")
	}
	// Need the UUID, not the identifier, for IssueUpdate's `id` arg.
	issue, fErr := client.FetchIssueByIdentifier(ctx, identifier)
	if fErr != nil {
		return fErr
	}
	if issue == nil {
		return fmt.Errorf("issue %s not found", identifier)
	}
	_, err := client.UpdateIssue(ctx, issue.ID, map[string]interface{}{
		"parentId": nil,
	})
	return err
}

// findLinearProjectByExactName scans the team's Projects for one whose
// name exactly equals the given title. Used as a pre-CreateProject
// idempotency check: if a prior run created the Project but crashed
// before stashing the URL in bead.Metadata, this catches the duplicate.
// Returns nil when no match.
//
// Caveat: Project names are not guaranteed unique on Linear. If two
// different epics happen to share the same title (unusual), this could
// falsely identify the wrong Project. The tradeoff is real but narrow —
// the alternative (deterministic marker embedded in the description) is
// stronger but requires plumbing through CreateProject's description.
// v1 accepts the name-collision risk; v2 could add a marker.
func findLinearProjectByExactName(ctx context.Context, lt *linear.Tracker, name string) (*linear.Project, error) {
	projects, err := lt.FetchProjects(ctx, "all")
	if err != nil {
		return nil, err
	}
	for _, p := range projects {
		if p.Name == name {
			return &linear.Project{
				ID:    p.ID,
				Name:  p.Name,
				URL:   p.URL,
				State: p.State,
			}, nil
		}
	}
	return nil, nil
}

// retireLegacyIssue handles the legacy Issue per the user-selected mode.
// Comment with the Project URL is always added (for forensic context);
// closing is conditional on --legacy-issue=close. closeStateID must be
// non-empty when mode is "close" (pre-resolved at plan time).
func retireLegacyIssue(ctx context.Context, lt *linear.Tracker, identifier, projectURL, mode, closeStateID string) error {
	client := lt.PrimaryClient()
	if client == nil {
		return fmt.Errorf("no Linear client available")
	}
	issue, err := client.FetchIssueByIdentifier(ctx, identifier)
	if err != nil {
		return fmt.Errorf("fetching legacy issue %s: %w", identifier, err)
	}
	if issue == nil {
		// Already gone — treat as success.
		return nil
	}

	comment := fmt.Sprintf("This Issue has been migrated to a Project: %s\n\n"+
		"All sub-issues now nest under the Project; the work continues there.", projectURL)
	if _, err := client.CreateIssueComment(ctx, issue.ID, comment); err != nil {
		return fmt.Errorf("adding migration comment to %s: %w", identifier, err)
	}
	fmt.Printf("✓ Added migration comment to legacy Issue %s\n", identifier)

	if mode == "close" {
		if closeStateID == "" {
			// Defense-in-depth: should be pre-resolved at plan time.
			return fmt.Errorf("close-state ID not resolved (plan-phase validation skipped?)")
		}
		if _, err := client.UpdateIssue(ctx, issue.ID, map[string]interface{}{
			"stateId": closeStateID,
		}); err != nil {
			return fmt.Errorf("closing %s: %w", identifier, err)
		}
		fmt.Printf("✓ Closed legacy Issue %s\n", identifier)
	}
	return nil
}

// ensure os is used; keeps the import list explicit for the file.
var _ = os.Stderr
