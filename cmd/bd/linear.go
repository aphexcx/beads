package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/spf13/cobra"
	"github.com/steveyegge/beads/internal/beads"
	"github.com/steveyegge/beads/internal/config"
	"github.com/steveyegge/beads/internal/debug"
	"github.com/steveyegge/beads/internal/linear"
	"github.com/steveyegge/beads/internal/metrics"
	"github.com/steveyegge/beads/internal/storage"
	"github.com/steveyegge/beads/internal/tracker"
	"github.com/steveyegge/beads/internal/types"
)

// linearCmd is the root command for Linear integration.
var linearCmd = &cobra.Command{
	Use:     "linear",
	GroupID: "advanced",
	Short:   "Linear integration commands",
	Long: `Synchronize issues between beads and Linear.

Configuration:
  bd config set linear.api_key "YOUR_API_KEY"
  bd config set linear.team_id "TEAM_ID"
  bd config set linear.team_ids "TEAM_ID1,TEAM_ID2"  # Multiple teams (comma-separated)
  bd config set linear.project_id "PROJECT_ID"  # Optional: sync only this project

Environment variables (alternative to config):
  LINEAR_API_KEY  - Linear API key (for individual developers)
  LINEAR_TEAM_ID  - Linear team ID (UUID, singular)
  LINEAR_TEAM_IDS - Linear team IDs (comma-separated UUIDs)

OAuth (for CI workers / automated sync):
  LINEAR_OAUTH_CLIENT_ID     - OAuth app client ID
  LINEAR_OAUTH_CLIENT_SECRET - OAuth app client secret

  When both OAuth env vars are set, OAuth client_credentials flow is used
  instead of the API key. This allows CI workers to authenticate as an
  application (actor=application) rather than impersonating a user.
  Precedence: OAuth > LINEAR_API_KEY > config file.

Data Mapping (optional, sensible defaults provided):
  Priority mapping (Linear 0-4 to Beads 0-4):
    bd config set linear.priority_map.0 4    # No priority -> Backlog
    bd config set linear.priority_map.1 0    # Urgent -> Critical
    bd config set linear.priority_map.2 1    # High -> High
    bd config set linear.priority_map.3 2    # Medium -> Medium
    bd config set linear.priority_map.4 3    # Low -> Low

  State mapping (Linear state type to Beads status):
    bd config set linear.state_map.backlog open
    bd config set linear.state_map.unstarted open
    bd config set linear.state_map.started in_progress
    bd config set linear.state_map.completed closed
    bd config set linear.state_map.canceled closed
    bd config set linear.state_map.my_custom_state in_progress  # Custom state names

  Label to issue type mapping:
    bd config set linear.label_type_map.bug bug
    bd config set linear.label_type_map.feature feature
    bd config set linear.label_type_map.epic epic

  Relation type mapping (Linear relations to Beads dependencies):
    bd config set linear.relation_map.blocks blocks
    bd config set linear.relation_map.blockedBy blocks
    bd config set linear.relation_map.duplicate duplicates
    bd config set linear.relation_map.related related

  Bidirectional label sync (opt-in):
    bd config set linear.label_sync_enabled true
    bd config set linear.label_sync_exclude "internal-tag,gt:agent"
    bd config set linear.label_create_scope team   # or workspace

  When enabled, label changes propagate in both directions:
  - Adding/removing a label in Linear flows to the bead on next sync.
  - Adding/removing a label in beads flows to Linear on next sync.
  - Renaming a label in Linear (same ID) flows as a name change.
  - The first sync after enabling never removes labels on either side;
    both sides' labels merge into a unified set.

  Note: enabling this changes pull semantics for existing beads. Today the
  pull-side already mirrors Linear labels destructively (beads labels not on
  Linear are silently removed). With label sync enabled, removal becomes
  intent-aware: a label is only removed when one side actively removes it.

  ID generation (optional, hash IDs to match bd/Jira hash mode):
    bd config set linear.id_mode "hash"      # hash (default)
    bd config set linear.hash_length "6"     # hash length 3-8 (default: 6)

Examples:
  bd linear sync --pull         # Import issues from Linear
  bd linear sync --push         # Export issues to Linear
  bd linear sync                # Bidirectional sync (pull then push)
  bd linear sync --dry-run      # Preview sync without changes
  bd create "Fix login" --external-ref https://linear.app/team/issue/TEAM-123
                              # Link a local issue to an existing Linear issue
  bd linear status              # Show sync status`,
}

// linearSyncCmd handles synchronization with Linear.
var linearSyncCmd = &cobra.Command{
	Use:   "sync",
	Short: "Synchronize issues with Linear",
	Long: `Synchronize issues between beads and Linear.

Modes:
  --pull              Import issues from Linear into beads
  --push              Export issues from beads to Linear
  --pull-if-stale     Pull only if data is stale (skip if fresh)
  (no flags)          Bidirectional sync: pull then push, with conflict resolution

Staleness (--pull-if-stale):
  --threshold 20m     How old data must be before pulling (default 20m)
  A 5-minute debounce prevents agent loops: if a pull completed within 5 minutes,
  data is always treated as fresh regardless of the threshold.

Team Selection:
  --team ID1,ID2  Override configured team IDs for this sync
  Multiple teams can be configured via linear.team_ids (comma-separated).
  Falls back to linear.team_id for backward compatibility.
  Push requires explicit --team when multiple teams are configured.

Pull Options:
  --milestones       Reconstruct Linear project milestones as local epic parents

Type Filtering (--push only):
  --type task,feature       Only sync issues of these types
  --exclude-type wisp       Exclude issues of these types
  --include-ephemeral       Include ephemeral issues (wisps, etc.); default is to exclude
  --parent TICKET           Only push this ticket and its descendants
  --since 15d               Only push issues updated in the last 15 days
  --since 2026-03-27        Only push issues updated after this date

  Persistent exclude types (merged with --exclude-type):
    bd config set linear.exclude_types "molecule,event"

  Persistent exclude by label (comma-separated, matched against issue.Labels):
    bd config set linear.exclude_labels "gt:agent"
  --relations               Import Linear relations as bd dependencies on pull

Persistent push-direction ID filters (workflow artifacts, sandbox beads, etc.):
  bd config set linear.exclude_id_prefix "hw-mol-"
  bd config set linear.exclude_id_patterns "-wisp-,sandbox-,scratch-"

  exclude_id_prefix is a single case-sensitive prefix on the bead ID.
  exclude_id_patterns is a comma-separated list of case-sensitive substrings
  (matched anywhere in the ID). Both are combined as a union: a bead
  matching either rule is skipped from push (no create, no update). Beads
  with an existing external_ref that NOW match are silently skipped on
  future syncs; the Linear-side issue persists — archive/delete it manually
  if desired.

Conflict Resolution:
  By default, newer timestamp wins. Override with:
  --prefer-local    Always prefer local beads version
  --prefer-linear   Always prefer Linear version

Examples:
  bd linear sync --pull                         # Import from Linear
  bd linear sync --pull-if-stale                # Pull only if data is stale
  bd linear sync --pull-if-stale --threshold 5m # Pull if older than 5 minutes
  bd linear sync --pull --relations             # Import Linear blocking relations as bd deps
  bd linear sync --push --create-only           # Push new issues only
  bd linear sync --push --type=task,feature     # Push only tasks and features
  bd linear sync --push --exclude-type=wisp     # Push all except wisps
  bd linear sync --push --parent=bd-abc123      # Push one ticket tree
  bd linear sync --dry-run                      # Preview without changes
  bd linear sync --prefer-local                 # Bidirectional, local wins`,
	SilenceUsage:  true,
	SilenceErrors: true,
	RunE:          runLinearSync,
}

// linearStatusCmd shows the current sync status.
var linearStatusCmd = &cobra.Command{
	Use:   "status",
	Short: "Show Linear sync status",
	Long: `Show the current Linear sync status, including:
  - Last sync timestamp
  - Configuration status
  - Number of issues with Linear links
  - Issues pending push (no external_ref)`,
	SilenceUsage:  true,
	SilenceErrors: true,
	RunE:          runLinearStatus,
}

// linearTeamsCmd lists available teams.
var linearTeamsCmd = &cobra.Command{
	Use:   "teams",
	Short: "List available Linear teams",
	Long: `List all teams accessible with your Linear API key.

Use this to find the team ID (UUID) needed for configuration.

Example:
  bd linear teams
  bd config set linear.team_id "12345678-1234-1234-1234-123456789abc"`,
	SilenceUsage:  true,
	SilenceErrors: true,
	RunE:          runLinearTeams,
}

func init() {
	linearSyncCmd.Flags().Bool("pull", false, "Pull issues from Linear")
	linearSyncCmd.Flags().Bool("push", false, "Push issues to Linear")
	linearSyncCmd.Flags().Bool("dry-run", false, "Preview sync without making changes")
	linearSyncCmd.Flags().Bool("prefer-local", false, "Prefer local version on conflicts")
	linearSyncCmd.Flags().Bool("prefer-linear", false, "Prefer Linear version on conflicts")
	linearSyncCmd.Flags().Bool("create-only", false, "Only create new issues, don't update existing")
	linearSyncCmd.Flags().Bool("create-closed", false, "Push closed local beads with no external ref as new Linear issues (for historical backfill; skipped by default)")
	linearSyncCmd.Flags().Bool("verbose-diff", false, "In --dry-run, show per-field differences for each would-be update")
	linearSyncCmd.Flags().Bool("update-refs", true, "Update external_ref after creating Linear issues")
	linearSyncCmd.Flags().Bool("milestones", false, "Reconstruct Linear project milestones as local epic parents when pulling")
	linearSyncCmd.Flags().String("state", "all", "Issue state to sync: open, closed, all")
	linearSyncCmd.Flags().StringSlice("type", nil, "Only sync issues of these types (can be repeated)")
	linearSyncCmd.Flags().StringSlice("exclude-type", nil, "Exclude issues of these types (can be repeated)")
	linearSyncCmd.Flags().Bool("include-ephemeral", false, "Include ephemeral issues (wisps, etc.) when pushing to Linear")
	linearSyncCmd.Flags().String("parent", "", "Limit push to this beads ticket and its descendants")
	linearSyncCmd.Flags().String("since", "", "Only push issues updated after this date (e.g. 2026-03-27, 15d for 15 days ago)")
	linearSyncCmd.Flags().StringSlice("team", nil, "Team ID(s) to sync (overrides configured team_id/team_ids)")
	linearSyncCmd.Flags().Bool("relations", false, "Import Linear relations as bd dependencies when pulling")
	linearSyncCmd.Flags().Bool("pull-if-stale", false, "Pull only if Linear data is stale (skip if fresh)")
	linearSyncCmd.Flags().Duration("threshold", linear.DefaultStaleThreshold, "Staleness threshold for --pull-if-stale (default 20m)")
	linearSyncCmd.Flags().Bool("no-wait", false, "Fail immediately if another sync is running instead of waiting")
	registerSelectiveSyncFlags(linearSyncCmd)

	linearCmd.AddCommand(linearSyncCmd)
	linearCmd.AddCommand(linearStatusCmd)
	linearCmd.AddCommand(linearTeamsCmd)
	rootCmd.AddCommand(linearCmd)
}

func runLinearSync(cmd *cobra.Command, args []string) error {
	evt := metrics.NewCommandEvent("linear-sync")
	defer func() {
		if c := metrics.Global(); c != nil {
			c.CloseEventAndAdd(evt)
		}
	}()

	pull, _ := cmd.Flags().GetBool("pull")
	push, _ := cmd.Flags().GetBool("push")
	dryRun, _ := cmd.Flags().GetBool("dry-run")
	preferLocal, _ := cmd.Flags().GetBool("prefer-local")
	preferLinear, _ := cmd.Flags().GetBool("prefer-linear")
	createOnly, _ := cmd.Flags().GetBool("create-only")
	createClosed, _ := cmd.Flags().GetBool("create-closed")
	verboseDiff, _ := cmd.Flags().GetBool("verbose-diff")
	milestones, _ := cmd.Flags().GetBool("milestones")
	state, _ := cmd.Flags().GetString("state")
	typeFilters, _ := cmd.Flags().GetStringSlice("type")
	excludeTypes, _ := cmd.Flags().GetStringSlice("exclude-type")
	includeEphemeral, _ := cmd.Flags().GetBool("include-ephemeral")
	sinceStr, _ := cmd.Flags().GetString("since")
	cliTeams, _ := cmd.Flags().GetStringSlice("team")
	relations, _ := cmd.Flags().GetBool("relations")
	pullIfStale, _ := cmd.Flags().GetBool("pull-if-stale")
	threshold, _ := cmd.Flags().GetDuration("threshold")
	noWait, _ := cmd.Flags().GetBool("no-wait")

	if pullIfStale {
		beadsDir := resolveBeadsDirForStaleness()
		if beadsDir != "" {
			info := linear.GetStalenessInfoWithFallback(beadsDir, threshold, linearStorePullFallback(rootCtx))
			if info.WithinDebounce {
				if jsonOutput {
					return outputJSON(map[string]interface{}{
						"is_fresh":  true,
						"last_pull": info.LastPull.Format(time.RFC3339),
						"age":       linear.FormatAge(info.Age),
						"skipped":   true,
					})
				}
				fmt.Printf("Linear data is fresh (last pull %s ago, within debounce)\n", linear.FormatAge(info.Age))
				return nil
			}

			if info.IsFresh {
				if jsonOutput {
					return outputJSON(map[string]interface{}{
						"is_fresh":  true,
						"last_pull": info.LastPull.Format(time.RFC3339),
						"age":       linear.FormatAge(info.Age),
						"skipped":   true,
					})
				}
				fmt.Printf("Linear data is fresh (last pull %s ago)\n", linear.FormatAge(info.Age))
				return nil
			}
		}
		pull = true
	}

	if lockDir := beads.FindBeadsDir(); lockDir != "" {
		wait := !noWait
		if !wait {
			fmt.Fprintln(os.Stderr, "Acquiring sync lock (non-blocking)...")
		} else {
			fmt.Fprintln(os.Stderr, "Acquiring sync lock...")
		}
		syncLock, err := linear.AcquireSyncLock(lockDir, wait)
		if err != nil {
			if held, ok := err.(*linear.SyncLockHeldError); ok {
				if held.Info != nil {
					return HandleError("another bd linear sync is already running (PID %d, started %s)",
						held.Info.PID, held.Info.Started.Format("15:04:05"))
				}
				return HandleError("another bd linear sync is already running")
			}
			return HandleError("acquiring sync lock: %v", err)
		}
		defer func() {
			if err := syncLock.Release(); err != nil {
				fmt.Fprintf(os.Stderr, "Warning: failed to release sync lock: %v\n", err)
			}
		}()
	}

	if !dryRun {
		CheckReadonly("linear sync")
	}

	if preferLocal && preferLinear {
		return HandleErrorRespectJSON("cannot use both --prefer-local and --prefer-linear")
	}
	if milestones && push && !pull {
		return HandleErrorRespectJSON("--milestones only applies when pulling from Linear")
	}

	if err := ensureStoreActive(); err != nil {
		return HandleErrorRespectJSON("database not available: %v", err)
	}

	if err := validateLinearConfig(cliTeams); err != nil {
		return HandleErrorRespectJSON("%v", err)
	}

	ctx := rootCtx
	teamIDs := getLinearTeamIDs(ctx, cliTeams)
	willPush := push || !pull

	if willPush && len(teamIDs) > 1 && len(cliTeams) == 0 {
		return HandleErrorRespectJSON("push requires explicit --team flag when multiple teams are configured\n" +
			"Use: bd linear sync --push --team <TEAM_ID>")
	}

	lt := &linear.Tracker{}
	lt.SetTeamIDs(teamIDs)
	if err := lt.Init(ctx, store); err != nil {
		return HandleErrorRespectJSON("initializing Linear tracker: %v", err)
	}

	wireLinearLabelSyncConfig(ctx, lt)

	if willPush {
		if err := lt.ValidatePushStateMappings(ctx); err != nil {
			return HandleErrorRespectJSON("%v", err)
		}
	}

	engine := tracker.NewEngine(lt, store, actor)
	engine.OnMessage = func(msg string) { fmt.Println("  " + msg) }
	engine.OnWarning = func(msg string) { fmt.Fprintf(os.Stderr, "Warning: %s\n", msg) }

	// Set up Linear-specific pull hooks
	engine.PullHooks = buildLinearPullHooks(ctx, lt, linearPullHookOptions{
		Milestones: milestones,
		DryRun:     dryRun,
		Actor:      actor,
	})

	opts := tracker.SyncOptions{
		Pull:         pull,
		Push:         push,
		DryRun:       dryRun,
		CreateOnly:   createOnly,
		CreateClosed: createClosed,
		VerboseDiff:  verboseDiff,
		State:        state,
	}
	opts.DependencySources = linearPullDependencySources(relations)

	for _, t := range typeFilters {
		opts.TypeFilter = append(opts.TypeFilter, types.IssueType(strings.ToLower(t)))
	}
	// Merge CLI --exclude-type with config linear.exclude_types
	configExclude, _ := store.GetConfig(ctx, "linear.exclude_types")
	if configExclude != "" {
		for _, t := range strings.Split(configExclude, ",") {
			t = strings.TrimSpace(t)
			if t != "" {
				opts.ExcludeTypes = append(opts.ExcludeTypes, types.IssueType(strings.ToLower(t)))
			}
		}
	}
	for _, t := range excludeTypes {
		opts.ExcludeTypes = append(opts.ExcludeTypes, types.IssueType(strings.ToLower(t)))
	}
	// Read config linear.exclude_labels — comma-separated list of labels to
	// skip from push (e.g. "gt:agent" filters out polecat agent beads).
	configExcludeLabels, _ := store.GetConfig(ctx, "linear.exclude_labels")
	if configExcludeLabels != "" {
		for _, l := range strings.Split(configExcludeLabels, ",") {
			l = strings.TrimSpace(l)
			if l != "" {
				opts.ExcludeLabels = append(opts.ExcludeLabels, l)
			}
		}
	}
	applyLinearExcludeIDConfig(ctx, store, &opts)
	if !includeEphemeral {
		opts.ExcludeEphemeral = true
	}

	// Parse --since flag. Return (not FatalError/os.Exit): the sync lock
	// acquired above releases via defer, which an exit would bypass.
	if sinceStr != "" {
		sinceTime, err := parseSinceFlag(sinceStr)
		if err != nil {
			return HandleErrorRespectJSON("invalid --since value %q: %v", sinceStr, err)
		}
		opts.Since = sinceTime
	}

	if err := applySelectiveSyncFlags(cmd, &opts, push); err != nil {
		return HandleErrorRespectJSON("%v", err)
	}
	allowProjectCreates := opts.ParentID != "" || len(opts.IssueIDs) > 0

	engine.PushHooks = buildLinearPushHooks(ctx, lt, allowProjectCreates)

	if preferLocal {
		opts.ConflictResolution = tracker.ConflictLocal
	} else if preferLinear {
		opts.ConflictResolution = tracker.ConflictExternal
	} else {
		opts.ConflictResolution = tracker.ConflictTimestamp
	}

	result, err := engine.Sync(ctx, opts)
	if err != nil {
		if jsonOutput {
			if jerr := outputJSON(result); jerr != nil {
				return jerr
			}
			return SilentExit()
		}
		return HandleError("%v", err)
	}

	// Post-sync: reconcile parent-child relationships into Linear's parent
	// field. The per-issue create/update path can't always set parentId
	// (children are sometimes pushed before parents have an external_ref),
	// and previously-pushed orphan trees need a backfill. This pass is
	// idempotent — no API mutation when remote parent already matches.
	//
	// In dry-run mode the pass still runs (read-only fetches) so the user
	// gets a preview of which parents would be set; the IssueUpdate
	// mutation is skipped per-link.
	//
	// Skipped on scoped syncs (--parent / --type / --exclude-type / --issue-id)
	// since the reconciler walks ALL local beads with external_refs.
	//
	// `effectivePush` mirrors engine.Sync's bidirectional default so
	// `bd linear sync --dry-run` (no direction flag) still previews the
	// reconcile. opts.Push isn't readable after engine.Sync (passed by value).
	effectivePush := push || (!push && !pull)
	if effectivePush && result.Success && !syncIsScoped(&opts) {
		reconcileLinearParents(ctx, lt, dryRun, jsonOutput, &result.Warnings)
		// bd-1ay: separate pass for Project membership. Runs after
		// the parent reconciler (since bd-9w3's buildLinearParentLinks
		// silently skips Project-URL parents — those links are this
		// pass's responsibility). Same scoped-sync skip applies.
		reconcileLinearProjectMembership(ctx, lt, dryRun, jsonOutput, &result.Warnings)
	}

	// Record successful pull timestamp
	if (pull || !push) && !dryRun {
		if beadsDir := resolveBeadsDirForStaleness(); beadsDir != "" {
			_ = linear.WriteLastPullTimestamp(beadsDir)
		}
		_ = linear.RecordLastPullMetadata(ctx, getStore())
	}

	if jsonOutput {
		if pullIfStale {
			return outputJSON(map[string]interface{}{
				"stats":    result.Stats,
				"warnings": result.Warnings,
				"is_fresh": true,
				"skipped":  false,
			})
		}
		return outputJSON(result)
	}
	if dryRun {
		fmt.Println("\n✓ Dry run complete (no changes made)")
		return nil
	}
	if result.Stats.Pulled > 0 {
		fmt.Printf("✓ Pulled %d issues from Linear (%d created, %d updated locally)\n",
			result.Stats.Pulled, result.PullStats.Created, result.PullStats.Updated)
	}
	if result.Stats.Pushed > 0 {
		fmt.Printf("✓ Pushed %d issues to Linear (%d created, %d updated)\n",
			result.Stats.Pushed, result.PushStats.Created, result.PushStats.Updated)
	}
	if result.Stats.Skipped > 0 {
		fmt.Printf("  Skipped %d (no changes needed)\n", result.Stats.Skipped)
	}
	if result.Stats.Conflicts > 0 {
		fmt.Printf("→ Resolved %d conflicts\n", result.Stats.Conflicts)
	}
	fmt.Println("\n✓ Linear sync complete")
	if len(result.Warnings) > 0 {
		fmt.Println("\nWarnings:")
		for _, w := range result.Warnings {
			fmt.Printf("  - %s\n", w)
		}
	}
	return nil
}

func linearPullDependencySources(includeRelations bool) []tracker.DependencySource {
	if includeRelations {
		return nil
	}
	return []tracker.DependencySource{tracker.DependencySourceParent}
}

type linearPullHookOptions struct {
	Milestones bool
	DryRun     bool
	Actor      string
}

// wireLinearLabelSyncConfig loads label-sync settings from store config and
// installs them on the tracker, so PushHooks/PullHooks builders see the right
// LabelSyncEnabled() value when they install the label-aware hooks. Every
// entry point that builds Linear hooks (sync, and the pull/push shortcuts in
// sync_push_pull.go) must call this after lt.Init — otherwise label sync
// silently degrades to off for that path.
func wireLinearLabelSyncConfig(ctx context.Context, lt *linear.Tracker) {
	allCfg, _ := store.GetAllConfig(ctx)
	lsCfg := loadLinearLabelSyncConfig(allCfg)
	lt.SetLabelSyncConfig(lsCfg.Enabled, lsCfg.Exclude, lsCfg.CreateScope, func(format string, args ...interface{}) {
		fmt.Fprintf(os.Stderr, "Warning: linear label sync: "+format+"\n", args...)
	})
}

// buildLinearPullHooks creates PullHooks for Linear-specific pull behavior.
func buildLinearPullHooks(ctx context.Context, lt *linear.Tracker, opts linearPullHookOptions) *tracker.PullHooks {
	return buildLinearPullHooksForStore(ctx, lt, store, opts)
}

func buildLinearPullHooksForStore(ctx context.Context, lt *linear.Tracker, st storage.Storage, opts linearPullHookOptions) *tracker.PullHooks {
	idMode := getLinearIDMode(ctx)
	hashLength := getLinearHashLength(ctx)

	hooks := &tracker.PullHooks{}
	hookActor := opts.Actor
	if hookActor == "" {
		hookActor = actor
	}

	hooks.SyncComments = buildCommentPullHook(ctx, lt)
	hooks.SyncAttachments = buildAttachmentPullHook(ctx, lt)

	// ContentEqual: Linear builds its description by merging local's
	// description with acceptance_criteria/design/notes. A byte compare
	// between local.Description and remote.Description always fails when
	// local has any of those fields populated. Compare the rebuilt local
	// form (with markdown noise normalized) against the normalized remote.
	hooks.ContentEqual = func(local, remote *types.Issue) bool {
		if local == nil || remote == nil {
			return false
		}
		if local.Title != remote.Title ||
			local.Priority != remote.Priority ||
			local.Status != remote.Status ||
			local.IssueType != remote.IssueType {
			return false
		}
		localDesc := linear.NormalizeLinearMarkdown(linear.BuildLinearDescription(local))
		remoteDesc := linear.NormalizeLinearMarkdown(remote.Description)
		if localDesc != remoteDesc {
			return false
		}
		// Closed-beads close_reason logic mirrors pullIssueEqual:
		if local.Status == types.StatusClosed && local.CloseReason != remote.CloseReason {
			return false
		}
		if remote.Status != types.StatusClosed && strings.TrimSpace(local.CloseReason) != "" {
			return false
		}
		// Decision #12 (pull-side mirror): when label sync is enabled, a label
		// delta forces ContentEqual to false so the engine enters the update
		// transaction at engine.go:492 and our ReconcileLabels callback runs.
		// Without this, Linear-only label adds would be silently skipped.
		if lt.LabelSyncEnabled() && pullHasLabelDelta(lt, local, remote) {
			return false
		}
		return true
	}

	var generateID func(context.Context, *types.Issue) error
	if idMode == "hash" && st != nil {
		// Pre-load existing IDs for collision avoidance
		existingIssues, err := st.SearchIssues(ctx, "", types.IssueFilter{})
		usedIDs := make(map[string]bool)
		if err == nil {
			for _, issue := range existingIssues {
				if issue.ID != "" {
					usedIDs[issue.ID] = true
				}
			}
		}

		// YAML config takes precedence — in shared-server mode the DB
		// may belong to a different project (GH#2469).
		prefix := config.GetString("issue-prefix")
		if prefix == "" {
			var err error
			prefix, err = st.GetConfig(ctx, "issue_prefix")
			if err != nil || prefix == "" {
				prefix = "bd"
			}
		}

		generateID = func(_ context.Context, issue *types.Issue) error {
			ids := []*types.Issue{issue}
			idOpts := linear.IDGenerationOptions{
				BaseLength: hashLength,
				MaxLength:  8,
				UsedIDs:    usedIDs,
			}
			if err := linear.GenerateIssueIDs(ids, prefix, "linear-import", idOpts); err != nil {
				return err
			}
			// Track the newly generated ID for future collision avoidance
			usedIDs[issue.ID] = true
			return nil
		}
		hooks.GenerateID = generateID
	}

	if opts.Milestones && st != nil {
		hooks.AfterConvert = func(ctx context.Context, extIssue *tracker.TrackerIssue, conv *tracker.IssueConversion, ref string, _ *types.Issue, syncOpts tracker.SyncOptions) error {
			li, ok := extIssue.Raw.(*linear.Issue)
			if !ok || li == nil || li.ProjectMilestone == nil {
				return nil
			}
			if syncOpts.DryRun || opts.DryRun {
				return nil
			}
			milestoneRef, err := ensureLinearMilestoneEpic(ctx, st, li.ProjectMilestone, hookActor, generateID)
			if err != nil {
				return err
			}
			if strings.TrimSpace(ref) == "" {
				return fmt.Errorf("missing external ref for Linear issue %s", extIssue.Identifier)
			}
			conv.Dependencies = append(conv.Dependencies, tracker.DependencyInfo{
				FromExternalID: ref,
				ToExternalID:   milestoneRef,
				Type:           string(types.DepParentChild),
				Source:         tracker.DependencySourceParent,
			})
			return nil
		}
	}

	// Bidirectional label sync — Linear-specific reconciliation, gated on config.
	if lt.LabelSyncEnabled() {
		hooks.ReconcileLabels = func(ctx context.Context, tx storage.Transaction, issueID string, desired []string, extIssue *tracker.TrackerIssue, actor string) error {
			remoteIssue, ok := extIssue.Raw.(*linear.Issue)
			if !ok || remoteIssue == nil {
				return fmt.Errorf("ReconcileLabels: unexpected raw type %T", extIssue.Raw)
			}

			linearLabels := make([]linear.LinearLabel, 0)
			if remoteIssue.Labels != nil {
				for _, l := range remoteIssue.Labels.Nodes {
					linearLabels = append(linearLabels, linear.LinearLabel{Name: l.Name, ID: l.ID})
				}
			}

			snap, err := tx.GetLinearLabelSnapshot(ctx, issueID)
			if err != nil {
				return fmt.Errorf("load snapshot: %w", err)
			}
			snapEntries := make([]linear.SnapshotEntry, len(snap))
			for i, s := range snap {
				snapEntries[i] = linear.SnapshotEntry{Name: s.LabelName, ID: s.LabelID}
			}

			currentLabels, err := tx.GetLabels(ctx, issueID)
			if err != nil {
				return fmt.Errorf("get current labels: %w", err)
			}

			res := linear.ReconcileLabels(linear.LabelReconcileInput{
				Beads:    currentLabels,
				Linear:   linearLabels,
				Snapshot: snapEntries,
				Exclude:  lt.LabelExclude(),
			})

			// Apply Beads-side mutations only — Linear-side mutations
			// (RemoveFromLinear, AddToLinear) are owned by the push path
			// (Tracker.UpdateIssue), which runs in a separate transaction
			// and writes its own snapshot after a successful API call.
			for _, n := range res.RemoveFromBeads {
				if err := tx.RemoveLabel(ctx, issueID, n, actor); err != nil {
					return fmt.Errorf("remove label %q: %w", n, err)
				}
			}
			for _, n := range res.AddToBeads {
				if err := tx.AddLabel(ctx, issueID, n, actor); err != nil {
					return fmt.Errorf("add label %q: %w", n, err)
				}
			}

			// Snapshot reflects the agreed state after pull-side mutations.
			snapshotEntries := make([]storage.LinearLabelSnapshotEntry, len(res.NewSnapshot))
			for i, e := range res.NewSnapshot {
				snapshotEntries[i] = storage.LinearLabelSnapshotEntry{LabelID: e.ID, LabelName: e.Name}
			}
			return tx.PutLinearLabelSnapshot(ctx, issueID, snapshotEntries)
		}
	}

	return hooks
}

const linearMilestoneExternalRefPrefix = "linear:project-milestone:"

func linearMilestoneExternalRef(id string) string {
	return linearMilestoneExternalRefPrefix + strings.TrimSpace(id)
}

func isLinearMilestoneExternalRef(ref string) bool {
	return strings.HasPrefix(strings.TrimSpace(ref), linearMilestoneExternalRefPrefix)
}

func ensureLinearMilestoneEpic(ctx context.Context, st storage.Storage, ms *linear.ProjectMilestone, actor string, generateID func(context.Context, *types.Issue) error) (string, error) {
	milestoneID := strings.TrimSpace(ms.ID)
	if milestoneID == "" {
		return "", fmt.Errorf("Linear project milestone is missing id")
	}
	title := strings.TrimSpace(ms.Name)
	if title == "" {
		title = milestoneID
	}
	description := ms.Description
	ref := linearMilestoneExternalRef(milestoneID)

	metadata, err := mergedLinearMilestoneMetadata(nil, ms)
	if err != nil {
		return "", err
	}

	existing, err := findLinearMilestoneEpic(ctx, st, ref, milestoneID, title)
	if err != nil {
		return "", err
	}
	if existing != nil {
		updates := map[string]interface{}{}
		if existing.Title != title {
			updates["title"] = title
		}
		if existing.Description != description {
			updates["description"] = description
		}
		if existing.IssueType != types.TypeEpic {
			updates["issue_type"] = string(types.TypeEpic)
		}
		if existing.ExternalRef == nil || strings.TrimSpace(*existing.ExternalRef) != ref {
			updates["external_ref"] = ref
		}
		mergedMetadata, err := mergedLinearMilestoneMetadata(existing.Metadata, ms)
		if err != nil {
			return "", err
		}
		if string(existing.Metadata) != string(mergedMetadata) {
			updates["metadata"] = mergedMetadata
		}
		if len(updates) > 0 {
			if err := st.UpdateIssue(ctx, existing.ID, updates, actor); err != nil {
				return "", fmt.Errorf("updating Linear milestone epic %s: %w", existing.ID, err)
			}
		}
		return ref, nil
	}

	externalRef := ref
	epic := &types.Issue{
		Title:       title,
		Description: description,
		Status:      types.StatusOpen,
		Priority:    2,
		IssueType:   types.TypeEpic,
		ExternalRef: &externalRef,
		Metadata:    metadata,
	}
	if generateID != nil {
		if err := generateID(ctx, epic); err != nil {
			return "", fmt.Errorf("generating Linear milestone epic ID: %w", err)
		}
	}
	if err := st.CreateIssue(ctx, epic, actor); err != nil {
		return "", fmt.Errorf("creating Linear milestone epic %q: %w", title, err)
	}
	return ref, nil
}

func findLinearMilestoneEpic(ctx context.Context, st storage.Storage, ref, milestoneID, title string) (*types.Issue, error) {
	if existing, err := st.GetIssueByExternalRef(ctx, ref); err == nil {
		return existing, nil
	} else if !errors.Is(err, storage.ErrNotFound) {
		return nil, err
	}

	issues, err := st.SearchIssues(ctx, "", types.IssueFilter{})
	if err != nil {
		return nil, fmt.Errorf("searching local issues for Linear milestone %s: %w", milestoneID, err)
	}
	for _, issue := range issues {
		if issueHasLinearMilestoneID(issue, milestoneID) {
			return issue, nil
		}
	}

	for _, issue := range issues {
		if issue.IssueType != types.TypeEpic || !strings.EqualFold(strings.TrimSpace(issue.Title), title) {
			continue
		}
		ref := ""
		if issue.ExternalRef != nil {
			ref = strings.TrimSpace(*issue.ExternalRef)
		}
		if ref == "" {
			return issue, nil
		}
	}
	return nil, nil
}

func mergedLinearMilestoneMetadata(existing json.RawMessage, ms *linear.ProjectMilestone) (json.RawMessage, error) {
	data := make(map[string]interface{})
	if len(existing) > 0 {
		trimmed := strings.TrimSpace(string(existing))
		if trimmed != "" && trimmed != "null" {
			if err := json.Unmarshal(existing, &data); err != nil {
				return nil, fmt.Errorf("existing milestone metadata is not a JSON object: %w", err)
			}
		}
	}

	linearMeta, _ := data["linear"].(map[string]interface{})
	if linearMeta == nil {
		linearMeta = make(map[string]interface{})
	}
	linearMeta["kind"] = "project_milestone"
	linearMeta["project_milestone"] = map[string]interface{}{
		"id":          strings.TrimSpace(ms.ID),
		"name":        ms.Name,
		"description": ms.Description,
		"progress":    ms.Progress,
		"targetDate":  ms.TargetDate,
	}
	data["linear"] = linearMeta

	raw, err := json.Marshal(data)
	if err != nil {
		return nil, fmt.Errorf("marshaling Linear milestone metadata: %w", err)
	}
	return json.RawMessage(raw), nil
}

func issueHasLinearMilestoneID(issue *types.Issue, milestoneID string) bool {
	if issue == nil || len(issue.Metadata) == 0 {
		return false
	}
	var data struct {
		Linear struct {
			Kind             string `json:"kind"`
			ProjectMilestone struct {
				ID string `json:"id"`
			} `json:"project_milestone"`
		} `json:"linear"`
	}
	if err := json.Unmarshal(issue.Metadata, &data); err != nil {
		return false
	}
	return data.Linear.Kind == "project_milestone" &&
		strings.TrimSpace(data.Linear.ProjectMilestone.ID) == strings.TrimSpace(milestoneID)
}

func isLinearMilestoneIssue(issue *types.Issue) bool {
	if issue == nil {
		return false
	}
	if issue.ExternalRef != nil && isLinearMilestoneExternalRef(*issue.ExternalRef) {
		return true
	}
	var data struct {
		Linear struct {
			Kind string `json:"kind"`
		} `json:"linear"`
	}
	if len(issue.Metadata) == 0 || json.Unmarshal(issue.Metadata, &data) != nil {
		return false
	}
	return data.Linear.Kind == "project_milestone"
}

// buildLinearPushHooks creates PushHooks for Linear-specific push behavior.
func buildLinearPushHooks(ctx context.Context, lt *linear.Tracker, allowProjectCreates bool) *tracker.PushHooks {
	config := lt.MappingConfig()
	return &tracker.PushHooks{
		FormatDescription: func(issue *types.Issue) string {
			return linear.BuildLinearDescription(issue)
		},
		ContentEqual: func(local *types.Issue, remote *tracker.TrackerIssue) bool {
			remoteIssue, ok := remote.Raw.(*linear.Issue)
			if ok && remoteIssue != nil {
				// Label equality is handled by the label-sync gate below
				// (push-direction delta via snapshots) rather than
				// PushFieldsEqual's plain set comparison — pass nil cache.
				if !linear.PushFieldsEqual(local, remoteIssue, config, nil) {
					return false
				}
				// Decision #12: label sync gate. When label sync is enabled,
				// non-empty PUSH-DIRECTION label delta forces a push even if all
				// other fields are equal.
				if lt.LabelSyncEnabled() && hasLabelDelta(ctx, lt, local, remoteIssue) {
					return false
				}
				return true
			}
			remoteConv := lt.FieldMapper().IssueToBeads(remote)
			if remoteConv == nil || remoteConv.Issue == nil {
				return false
			}
			return linear.PushFieldsEqualToBeads(local, remoteConv.Issue)
		},
		DescribeDiff: func(local *types.Issue, remote *tracker.TrackerIssue) []string {
			remoteIssue, ok := remote.Raw.(*linear.Issue)
			if !ok || remoteIssue == nil {
				return nil
			}
			diffs := linear.PushFieldsDiff(local, remoteIssue, config)

			// Append label diff when label sync is enabled.
			if lt.LabelSyncEnabled() {
				linearLabels := make([]linear.LinearLabel, 0)
				if remoteIssue.Labels != nil {
					for _, l := range remoteIssue.Labels.Nodes {
						linearLabels = append(linearLabels, linear.LinearLabel{Name: l.Name, ID: l.ID})
					}
				}
				snap, snapErr := lt.LoadSnapshot(ctx, local.ID)
				if snapErr != nil {
					debug.Logf("DescribeDiff: LoadSnapshot(%s) failed: %v — proceeding with nil snap", local.ID, snapErr)
				}
				res := linear.ReconcileLabels(linear.LabelReconcileInput{
					Beads:    local.Labels,
					Linear:   linearLabels,
					Snapshot: snap,
					Exclude:  lt.LabelExclude(),
				})

				// Build an ID→name lookup so RemoveFromLinear (which is []ID per
				// the reconciler design) displays as names. Snapshot is the
				// canonical source — every removed ID came from there. Linear's
				// current labels are a fallback for IDs still present remotely.
				nameByID := make(map[string]string, len(snap)+len(linearLabels))
				for _, s := range snap {
					nameByID[s.ID] = s.Name
				}
				for _, l := range linearLabels {
					if _, ok := nameByID[l.ID]; !ok {
						nameByID[l.ID] = l.Name
					}
				}
				removeNames := make([]string, len(res.RemoveFromLinear))
				for i, id := range res.RemoveFromLinear {
					if name, ok := nameByID[id]; ok {
						removeNames[i] = name
					} else {
						removeNames[i] = id // fallback — shouldn't happen for ids sourced from snapshot
					}
				}

				if len(res.AddToLinear) > 0 {
					diffs = append(diffs, fmt.Sprintf("labels +%v (push to Linear)", res.AddToLinear))
				}
				if len(removeNames) > 0 {
					diffs = append(diffs, fmt.Sprintf("labels -%v (remove from Linear)", removeNames))
				}
				if len(res.AddToBeads) > 0 {
					diffs = append(diffs, fmt.Sprintf("labels +%v (add to bead)", res.AddToBeads))
				}
				if len(res.RemoveFromBeads) > 0 {
					diffs = append(diffs, fmt.Sprintf("labels -%v (remove from bead)", res.RemoveFromBeads))
				}
			}
			return diffs
		},
		BuildStateCache: func(ctx context.Context) (interface{}, error) {
			return linear.BuildStateCacheFromTracker(ctx, lt)
		},
		ResolveState: func(cache interface{}, status types.Status) (string, bool) {
			sc, ok := cache.(*linear.StateCache)
			if !ok || sc == nil {
				return "", false
			}
			id := sc.FindStateForBeadsStatus(status)
			return id, id != ""
		},
		ShouldPush: func(issue *types.Issue) bool {
			if isLinearMilestoneIssue(issue) {
				return false
			}
			if projectID, _ := store.GetConfig(ctx, "linear.project_id"); projectID != "" {
				if issue.ExternalRef == nil || strings.TrimSpace(*issue.ExternalRef) == "" {
					if !allowProjectCreates {
						return false
					}
				}
			}

			// Apply push prefix filtering if configured
			pushPrefix, _ := store.GetConfig(ctx, "linear.push_prefix")
			if pushPrefix == "" {
				return true
			}
			for _, prefix := range strings.Split(pushPrefix, ",") {
				prefix = strings.TrimSpace(prefix)
				prefix = strings.TrimSuffix(prefix, "-")
				if prefix != "" && strings.HasPrefix(issue.ID, prefix+"-") {
					return true
				}
			}
			return false
		},
		SyncComments: buildCommentPushHook(ctx, lt),
	}
}

// buildCommentPullHook creates a hook that pulls comments from Linear and imports them locally.
// Uses the tracker's CommentSyncer interface for fetching. The hook unwraps the
// HookFiringStore decorator to reach the concrete CommentRefStore implementation.
func buildCommentPullHook(_ context.Context, lt *linear.Tracker) func(context.Context, string, string) error {
	return func(ctx context.Context, localIssueID, externalIssueID string) error {
		refStore, ok := storage.UnwrapStore(store).(storage.CommentRefStore)
		if !ok {
			return nil
		}

		comments, err := lt.FetchComments(ctx, externalIssueID, time.Time{})
		if err != nil {
			return fmt.Errorf("fetch comments: %w", err)
		}

		for _, c := range comments {
			extRef := "linear:comment:" + c.ID
			existing, err := refStore.GetCommentByExternalRef(ctx, localIssueID, extRef)
			if err != nil {
				continue
			}
			if existing != nil {
				continue
			}
			if _, err := refStore.ImportCommentWithRef(ctx, localIssueID, c.Author, c.Body, extRef, c.CreatedAt); err != nil {
				debug.Logf("comment import failed for %s: %v", localIssueID, err)
			}
		}
		return nil
	}
}

// buildAttachmentPullHook creates a hook that pulls attachment metadata from Linear.
// Pull-only: beads does not push attachments to Linear.
func buildAttachmentPullHook(_ context.Context, lt *linear.Tracker) func(context.Context, string, string) error {
	return func(ctx context.Context, localIssueID, externalIssueID string) error {
		attStore, ok := storage.UnwrapStore(store).(storage.AttachmentStore)
		if !ok {
			return nil
		}

		attachments, err := lt.FetchAttachments(ctx, externalIssueID)
		if err != nil {
			return fmt.Errorf("fetch attachments: %w", err)
		}

		for _, a := range attachments {
			extRef := "linear:attachment:" + a.ID
			existing, err := attStore.GetAttachmentByExternalRef(ctx, localIssueID, extRef)
			if err != nil {
				continue
			}
			if existing != nil {
				continue
			}
			att := &types.Attachment{
				IssueID:     localIssueID,
				ExternalRef: extRef,
				Filename:    a.Filename,
				URL:         a.URL,
				MimeType:    a.MimeType,
				SizeBytes:   a.SizeBytes,
				Source:      "linear",
				Creator:     a.Creator,
				CreatedAt:   a.CreatedAt,
			}
			if _, err := attStore.CreateAttachment(ctx, att); err != nil {
				debug.Logf("attachment import failed for %s: %v", localIssueID, err)
			}
		}
		return nil
	}
}

// buildCommentPushHook creates a hook that pushes local comments to Linear.
// Only comments without an existing linear:comment:* external_ref are pushed
// (already-synced comments are skipped). Newly created Linear comment IDs are
// recorded on the local comment so subsequent syncs don't duplicate.
func buildCommentPushHook(_ context.Context, lt *linear.Tracker) func(context.Context, string, string) error {
	return func(ctx context.Context, localIssueID, externalIssueID string) error {
		refStore, ok := storage.UnwrapStore(store).(storage.CommentRefStore)
		if !ok {
			return nil
		}

		localComments, err := store.GetIssueComments(ctx, localIssueID)
		if err != nil {
			return fmt.Errorf("list local comments: %w", err)
		}

		for _, c := range localComments {
			if strings.HasPrefix(c.ExternalRef, "linear:comment:") {
				continue
			}
			remoteID, err := lt.CreateComment(ctx, externalIssueID, c.Text)
			if err != nil {
				debug.Logf("push comment to %s failed: %v", externalIssueID, err)
				continue
			}
			extRef := "linear:comment:" + remoteID
			if err := refStore.UpdateCommentExternalRef(ctx, localIssueID, c.ID, extRef); err != nil {
				debug.Logf("record external_ref on %s failed: %v", c.ID, err)
			}
		}
		return nil
	}
}

// parseSinceFlag parses a --since value as either a duration shorthand
// (e.g. "15d") or an absolute date (e.g. "2026-03-27"). Returns the cutoff
// time; UpdatedAt strictly after this cutoff passes the filter.
func parseSinceFlag(s string) (time.Time, error) {
	s = strings.TrimSpace(s)
	if strings.HasSuffix(s, "d") {
		days, err := strconv.Atoi(strings.TrimSuffix(s, "d"))
		if err == nil && days > 0 {
			return time.Now().UTC().AddDate(0, 0, -days), nil
		}
	}
	for _, layout := range []string{"2006-01-02", "2006-01-02T15:04:05Z", time.RFC3339} {
		if t, err := time.Parse(layout, s); err == nil {
			return t, nil
		}
	}
	return time.Time{}, fmt.Errorf("expected date (2006-01-02) or duration (15d)")
}

func runLinearStatus(cmd *cobra.Command, args []string) error {
	evt := metrics.NewCommandEvent("linear-status")
	defer func() {
		if c := metrics.Global(); c != nil {
			c.CloseEventAndAdd(evt)
		}
	}()

	ctx := rootCtx

	if err := ensureStoreActive(); err != nil {
		return HandleErrorRespectJSON("%v", err)
	}

	apiKey, _ := getLinearConfig(ctx, "linear.api_key")
	oauthClientID, _ := getLinearConfig(ctx, "linear.oauth_client_id")
	oauthClientSecret, _ := getLinearConfig(ctx, "linear.oauth_client_secret")
	teamIDs := getLinearTeamIDs(ctx, nil)
	lastSync := tracker.LastSync(ctx, store, "linear")

	hasOAuth := oauthClientID != "" && oauthClientSecret != ""
	configured := (apiKey != "" || hasOAuth) && len(teamIDs) > 0

	allIssues, err := store.SearchIssues(ctx, "", types.IssueFilter{})
	if err != nil {
		return HandleErrorRespectJSON("%v", err)
	}

	withLinearRef := 0
	pendingPush := 0
	for _, issue := range allIssues {
		if issue.ExternalRef != nil && linear.IsLinearExternalRef(*issue.ExternalRef) {
			withLinearRef++
		} else if issue.ExternalRef == nil {
			pendingPush++
		}
	}

	if jsonOutput {
		hasAPIKey := apiKey != ""
		teamID := ""
		if len(teamIDs) > 0 {
			teamID = teamIDs[0]
		}
		authMode := "none"
		if hasOAuth {
			authMode = "oauth"
		} else if hasAPIKey {
			authMode = "api_key"
		}
		return outputJSON(map[string]interface{}{
			"configured":      configured,
			"has_api_key":     hasAPIKey,
			"has_oauth":       hasOAuth,
			"auth_mode":       authMode,
			"team_id":         teamID,
			"team_ids":        teamIDs,
			"last_sync":       lastSync,
			"total_issues":    len(allIssues),
			"with_linear_ref": withLinearRef,
			"pending_push":    pendingPush,
		})
	}

	fmt.Println("Linear Sync Status")
	fmt.Println("==================")
	fmt.Println()

	if !configured {
		fmt.Println("Status: Not configured")
		fmt.Println()
		fmt.Println("To configure Linear integration:")
		fmt.Println("  bd config set linear.api_key \"YOUR_API_KEY\"")
		fmt.Println("  bd config set linear.team_id \"TEAM_ID\"")
		fmt.Println("  bd config set linear.team_ids \"TEAM_ID1,TEAM_ID2\"  # multiple teams")
		fmt.Println()
		fmt.Println("Or use environment variables:")
		fmt.Println("  export LINEAR_API_KEY=\"YOUR_API_KEY\"")
		fmt.Println("  export LINEAR_TEAM_ID=\"TEAM_ID\"")
		fmt.Println()
		fmt.Println("For CI/OAuth authentication:")
		fmt.Println("  export LINEAR_OAUTH_CLIENT_ID=\"...\"")
		fmt.Println("  export LINEAR_OAUTH_CLIENT_SECRET=\"...\"")
		return nil
	}

	if len(teamIDs) == 1 {
		fmt.Printf("Team ID:      %s\n", teamIDs[0])
	} else {
		fmt.Printf("Team IDs:     %s (%d teams)\n", strings.Join(teamIDs, ", "), len(teamIDs))
	}
	if hasOAuth {
		fmt.Printf("Auth:         OAuth (client_credentials)\n")
	} else {
		fmt.Printf("API Key:      %s\n", maskAPIKey(apiKey))
	}
	if lastSync != "" {
		fmt.Printf("Last Sync:    %s\n", lastSync)
	} else {
		fmt.Println("Last Sync:    Never")
	}
	fmt.Println()
	fmt.Printf("Total Issues: %d\n", len(allIssues))
	fmt.Printf("With Linear:  %d\n", withLinearRef)
	fmt.Printf("Local Only:   %d\n", pendingPush)

	if pendingPush > 0 {
		fmt.Println()
		fmt.Printf("Run 'bd linear sync --push' to push %d local issue(s) to Linear\n", pendingPush)
	}
	return nil
}

func runLinearTeams(cmd *cobra.Command, args []string) error {
	evt := metrics.NewCommandEvent("linear-teams")
	defer func() {
		if c := metrics.Global(); c != nil {
			c.CloseEventAndAdd(evt)
		}
	}()

	ctx := rootCtx

	client, err := buildLinearClient(ctx, "")
	if err != nil {
		return HandleError("%v", err)
	}

	teams, err := client.FetchTeams(ctx)
	if err != nil {
		return HandleError("fetching teams: %v", err)
	}

	if len(teams) == 0 {
		fmt.Println("No teams found (check your API key permissions)")
		return nil
	}

	if jsonOutput {
		return outputJSON(teams)
	}

	fmt.Println("Available Linear Teams")
	fmt.Println("======================")
	fmt.Println()
	fmt.Printf("%-40s  %-6s  %s\n", "ID (use this for linear.team_id)", "Key", "Name")
	fmt.Printf("%-40s  %-6s  %s\n", "----------------------------------------", "------", "----")
	for _, team := range teams {
		fmt.Printf("%-40s  %-6s  %s\n", team.ID, team.Key, team.Name)
	}
	fmt.Println()
	fmt.Println("To configure:")
	fmt.Println("  bd config set linear.team_id \"<ID>\"")
	fmt.Println("  bd config set linear.team_ids \"<ID1>,<ID2>\"  # multiple teams")
	return nil
}

// resolveBeadsDirForStaleness returns the active beads directory for
// staleness tracking. Falls back to BEADS_DIR env, then dbPath resolution.
func resolveBeadsDirForStaleness() string {
	if dir := os.Getenv("BEADS_DIR"); dir != "" {
		return dir
	}
	if dbPath != "" {
		return resolveCommandBeadsDir(dbPath)
	}
	return ""
}

// linearStorePullFallback returns a LastPullFallback that consults store
// evidence of past pulls when .beads/last_pull is absent — databases synced
// by bd versions that predate the file's stamps, and fresh clones, since the
// file is per-machine and gitignored. Without it every such database reports
// NeverPulled and forces a full network pull on each --pull-if-stale
// (bd-stc). The store is opened lazily inside the closure so the
// file-present path stays DB-free.
func linearStorePullFallback(ctx context.Context) linear.LastPullFallback {
	return func() time.Time {
		if err := ensureStoreActive(); err != nil {
			return time.Time{}
		}
		return linear.StoreLastPullFallback(ctx, getStore())()
	}
}

// uuidRegex matches valid UUID format (with or without hyphens).
var uuidRegex = regexp.MustCompile(`^[0-9a-fA-F]{8}-?[0-9a-fA-F]{4}-?[0-9a-fA-F]{4}-?[0-9a-fA-F]{4}-?[0-9a-fA-F]{12}$`)

func isValidUUID(s string) bool {
	return uuidRegex.MatchString(s)
}

// validateLinearConfig checks that required Linear configuration is present.
// cliTeams is the list of team IDs from the --team flag (may be nil).
func validateLinearConfig(cliTeams []string) error {
	if err := ensureStoreActive(); err != nil {
		return fmt.Errorf("database not available: %w", err)
	}

	ctx := rootCtx

	// Accept either OAuth credentials or API key.
	oauthClientID, _ := getLinearConfig(ctx, "linear.oauth_client_id")
	oauthClientSecret, _ := getLinearConfig(ctx, "linear.oauth_client_secret")
	hasOAuth := oauthClientID != "" && oauthClientSecret != ""

	if !hasOAuth {
		apiKey, _ := getLinearConfig(ctx, "linear.api_key")
		if apiKey == "" {
			return fmt.Errorf("Linear authentication not configured\n" +
				"Options:\n" +
				"  OAuth (for CI):  export LINEAR_OAUTH_CLIENT_ID=... LINEAR_OAUTH_CLIENT_SECRET=...\n" +
				"  API key (devs):  export LINEAR_API_KEY=... or bd config set linear.api_key \"YOUR_API_KEY\"")
		}
	}

	teamIDs := getLinearTeamIDs(ctx, cliTeams)
	if len(teamIDs) == 0 {
		return fmt.Errorf("no Linear team ID configured\nRun: bd config set linear.team_id \"TEAM_ID\"\nOr:  bd config set linear.team_ids \"TEAM_ID1,TEAM_ID2\"\nOr: export LINEAR_TEAM_ID=TEAM_ID")
	}

	for _, id := range teamIDs {
		if !isValidUUID(id) {
			return fmt.Errorf("invalid Linear team ID (expected UUID format like '12345678-1234-1234-1234-123456789abc')\nInvalid value: %s", id)
		}
	}

	return nil
}

// maskAPIKey returns a masked version of an API key for display.
// Shows first 4 and last 4 characters, with dots in between.
func maskAPIKey(key string) string {
	if len(key) <= 8 {
		return "****"
	}
	return key[:4] + "..." + key[len(key)-4:]
}

// getLinearConfig reads a Linear configuration value. Returns the value and its source.
// Priority: environment variable > project config.
// Env vars take precedence so CI workers can override config without modifying config.yaml.
func getLinearConfig(ctx context.Context, key string) (value string, source string) {
	// Secret keys (e.g. linear.api_key) are stored in config.yaml, not the
	// Dolt database, to avoid leaking secrets when pushing to remotes.
	// Env vars are checked first so that LINEAR_OAUTH_CLIENT_ID/SECRET etc.
	// override whatever is in config.yaml.
	if config.IsYamlOnlyKey(key) {
		envKey := linearConfigToEnvVar(key)
		if envKey != "" {
			if value := os.Getenv(envKey); value != "" {
				return value, fmt.Sprintf("environment variable (%s)", envKey)
			}
		}
		if value := config.GetString(key); value != "" {
			return value, "project config (config.yaml)"
		}
		return "", ""
	}

	// Try to read from store (works in direct mode)
	if store != nil {
		value, _ = store.GetConfig(ctx, key) // Best effort: empty value is valid fallback
		if value != "" {
			return value, "project config (bd config)"
		}
	} else if dbPath != "" {
		tempStore, err := openReadOnlyStoreForDBPath(ctx, dbPath)
		if err == nil {
			defer func() { _ = tempStore.Close() }()
			value, _ = tempStore.GetConfig(ctx, key) // Best effort: empty value is valid fallback
			if value != "" {
				return value, "project config (bd config)"
			}
		}
	}

	// Fall back to environment variable
	envKey := linearConfigToEnvVar(key)
	if envKey != "" {
		value = os.Getenv(envKey)
		if value != "" {
			return value, fmt.Sprintf("environment variable (%s)", envKey)
		}
	}

	return "", ""
}

// linearConfigToEnvVar maps Linear config keys to their environment variable names.
func linearConfigToEnvVar(key string) string {
	switch key {
	case "linear.api_key":
		return "LINEAR_API_KEY"
	case "linear.team_id":
		return "LINEAR_TEAM_ID"
	case "linear.team_ids":
		return "LINEAR_TEAM_IDS"
	case "linear.oauth_client_id":
		return "LINEAR_OAUTH_CLIENT_ID"
	case "linear.oauth_client_secret":
		return "LINEAR_OAUTH_CLIENT_SECRET"
	default:
		return ""
	}
}

// getLinearTeamIDs resolves the effective team IDs from all config sources.
// Precedence: cliTeams (--team flag) > linear.team_ids > LINEAR_TEAM_IDS > linear.team_id > LINEAR_TEAM_ID
func getLinearTeamIDs(ctx context.Context, cliTeams []string) []string {
	pluralVal, _ := getLinearConfig(ctx, "linear.team_ids")
	singularVal, _ := getLinearConfig(ctx, "linear.team_id")
	return tracker.ResolveProjectIDs(cliTeams, pluralVal, singularVal)
}

// getLinearClient creates a configured Linear client from beads config.
// Uses the first configured team ID for operations that require a single team.
//
// Auth precedence:
//  1. OAuth env vars (LINEAR_OAUTH_CLIENT_ID + LINEAR_OAUTH_CLIENT_SECRET)
//  2. LINEAR_API_KEY env var
//  3. linear.oauth_client_id + linear.oauth_client_secret in config
//  4. linear.api_key in config
func getLinearClient(ctx context.Context) (*linear.Client, error) {
	teamIDs := getLinearTeamIDs(ctx, nil)
	if len(teamIDs) == 0 {
		return nil, fmt.Errorf("Linear team ID not configured")
	}

	client, err := buildLinearClient(ctx, teamIDs[0])
	if err != nil {
		return nil, err
	}

	if store != nil {
		if endpoint, _ := store.GetConfig(ctx, "linear.api_endpoint"); endpoint != "" {
			client = client.WithEndpoint(endpoint)
		}
		if projectID, _ := store.GetConfig(ctx, "linear.project_id"); projectID != "" {
			client = client.WithProjectID(projectID)
		}
		// Apply optional rate-limit circuit-breaker floor.
		// Readable/settable via `bd config get/set linear.rate_limit_floor`.
		// Also honored via the LINEAR_RATE_LIMIT_FLOOR environment variable.
		floorStr, _ := getLinearConfig(ctx, "linear.rate_limit_floor")
		if floorStr == "" {
			floorStr = os.Getenv("LINEAR_RATE_LIMIT_FLOOR")
		}
		if floorStr != "" {
			if v, err := strconv.Atoi(strings.TrimSpace(floorStr)); err == nil && v >= 0 {
				client = client.WithRateLimitFloor(v)
			}
		}
	}

	return client, nil
}

// buildLinearClient resolves auth credentials and returns an appropriately
// configured Linear client. OAuth takes precedence over API key.
func buildLinearClient(ctx context.Context, teamID string) (*linear.Client, error) {
	oauthClientID, _ := getLinearConfig(ctx, "linear.oauth_client_id")
	oauthClientSecret, _ := getLinearConfig(ctx, "linear.oauth_client_secret")

	if oauthClientID != "" && oauthClientSecret != "" {
		debug.Logf("Linear: using OAuth client-credentials authentication")
		oauthCfg := linear.OAuthConfig{
			ClientID:     oauthClientID,
			ClientSecret: oauthClientSecret,
		}
		return linear.NewOAuthClient(oauthCfg, teamID), nil
	}

	apiKey, _ := getLinearConfig(ctx, "linear.api_key")
	if apiKey == "" {
		return nil, fmt.Errorf("Linear authentication not configured\n" +
			"Options:\n" +
			"  OAuth (for CI):  export LINEAR_OAUTH_CLIENT_ID=... LINEAR_OAUTH_CLIENT_SECRET=...\n" +
			"  API key (devs):  export LINEAR_API_KEY=... or bd config set linear.api_key \"...\"")
	}

	return linear.NewClient(apiKey, teamID), nil
}

// storeConfigLoader adapts the store to the linear.ConfigLoader interface.
type storeConfigLoader struct {
	ctx context.Context
}

func (l *storeConfigLoader) GetAllConfig() (map[string]string, error) {
	return store.GetAllConfig(l.ctx)
}

// loadLinearMappingConfig loads mapping configuration from beads config.
func loadLinearMappingConfig(ctx context.Context) *linear.MappingConfig {
	if store == nil {
		return linear.DefaultMappingConfig()
	}
	return linear.LoadMappingConfig(&storeConfigLoader{ctx: ctx})
}

// linearLabelSyncConfig holds the parsed label-sync configuration that the
// Linear tracker needs to enable bidirectional label reconciliation.
type linearLabelSyncConfig struct {
	Enabled     bool
	Exclude     map[string]bool
	CreateScope linear.LabelScope
}

// loadLinearLabelSyncConfig parses the three label-sync config keys from a
// flat config map. Pass `store.GetAllConfig()` results in.
func loadLinearLabelSyncConfig(cfg map[string]string) linearLabelSyncConfig {
	out := linearLabelSyncConfig{
		Exclude:     map[string]bool{},
		CreateScope: linear.LabelScopeTeam,
	}
	if v := cfg["linear.label_sync_enabled"]; strings.EqualFold(strings.TrimSpace(v), "true") {
		out.Enabled = true
	}
	if v := cfg["linear.label_sync_exclude"]; v != "" {
		for _, raw := range strings.Split(v, ",") {
			n := strings.ToLower(strings.TrimSpace(raw))
			if n != "" {
				out.Exclude[n] = true
			}
		}
	}
	switch strings.ToLower(strings.TrimSpace(cfg["linear.label_create_scope"])) {
	case "workspace":
		out.CreateScope = linear.LabelScopeWorkspace
	default:
		out.CreateScope = linear.LabelScopeTeam
	}
	return out
}

// getLinearIDMode returns the configured ID mode for Linear imports.
// Supported values: "hash" (default) or "db".
func getLinearIDMode(ctx context.Context) string {
	mode, _ := getLinearConfig(ctx, "linear.id_mode")
	mode = strings.ToLower(strings.TrimSpace(mode))
	if mode == "" {
		return "hash"
	}
	return mode
}

// linearConfigReader is the minimal slice of storage.Storage that the
// linear-config helpers depend on. Lets tests inject a fake without
// spinning up a Dolt server.
type linearConfigReader interface {
	GetConfig(ctx context.Context, key string) (string, error)
}

// applyLinearExcludeIDConfig reads linear.exclude_id_prefix and
// linear.exclude_id_patterns from the given config reader and applies them
// to opts. Both keys are push-direction-only filters; see the help text on
// linearSyncCmd for the user-facing semantics.
//
// Empty values are no-ops. Patterns are comma-split, trimmed, with empty
// entries dropped. If reader is nil (no store configured), this is a no-op.
func applyLinearExcludeIDConfig(ctx context.Context, reader linearConfigReader, opts *tracker.SyncOptions) {
	if reader == nil || opts == nil {
		return
	}
	if v, _ := reader.GetConfig(ctx, "linear.exclude_id_prefix"); v != "" {
		opts.ExcludeIDPrefix = strings.TrimSpace(v)
	}
	if v, _ := reader.GetConfig(ctx, "linear.exclude_id_patterns"); v != "" {
		for _, p := range strings.Split(v, ",") {
			p = strings.TrimSpace(p)
			if p != "" {
				opts.ExcludeIDPatterns = append(opts.ExcludeIDPatterns, p)
			}
		}
	}
}

// getLinearHashLength returns the configured hash length for Linear imports.
// Values are clamped to the supported range 3-8.
func getLinearHashLength(ctx context.Context) int {
	raw, _ := getLinearConfig(ctx, "linear.hash_length")
	if raw == "" {
		return 6
	}
	value, err := strconv.Atoi(strings.TrimSpace(raw))
	if err != nil {
		return 6
	}
	if value < 3 {
		return 3
	}
	if value > 8 {
		return 8
	}
	return value
}

// hasLabelDelta runs the reconciler in dry-run mode against the persisted
// snapshot and returns true if any **push-direction** label adds/removes
// would fire. Used by the push-side ContentEqual to bypass the engine-level
// skip when only labels differ in the push direction.
//
// Pull-direction deltas (AddToBeads, RemoveFromBeads) are deliberately NOT
// checked here — those are the pull path's concern. Including them here would
// cause push to issue an IssueUpdate carrying labelIds identical to Linear's
// current state (a wasted API call) just because the bead is missing labels
// Linear has.
//
// On LoadSnapshot error: log loudly and fall through with a nil snap rather
// than short-circuiting to true. The reconciler's first-sync synthesis treats
// nil snap as "use intersection of beads and Linear" — labels that genuinely
// agree produce no delta, and labels that disagree still surface as adds.
// Returning true here would force a push that DescribeDiff (which ignores
// the same error) would then describe as having no diff — a UX inconsistency
// that masks the true cause and produces no-op API calls in a loop.
func hasLabelDelta(ctx context.Context, lt *linear.Tracker, local *types.Issue, remoteIssue *linear.Issue) bool {
	linearLabels := make([]linear.LinearLabel, 0)
	if remoteIssue.Labels != nil {
		for _, l := range remoteIssue.Labels.Nodes {
			linearLabels = append(linearLabels, linear.LinearLabel{Name: l.Name, ID: l.ID})
		}
	}
	snap, err := lt.LoadSnapshot(ctx, local.ID)
	if err != nil {
		debug.Logf("hasLabelDelta: LoadSnapshot(%s) failed: %v — proceeding with nil snap", local.ID, err)
	}
	res := linear.ReconcileLabels(linear.LabelReconcileInput{
		Beads:    local.Labels,
		Linear:   linearLabels,
		Snapshot: snap,
		Exclude:  lt.LabelExclude(),
	})
	return len(res.AddToLinear) > 0 || len(res.RemoveFromLinear) > 0
}

// pullHasLabelDelta is the pull-side counterpart to hasLabelDelta. It compares
// local.Labels (the bead's current label set) against remote.Labels (the
// converted-from-Linear bead's label set) by name, and returns true if any
// name differs.
//
// The pull-side hook only applies beads-side mutations, so a name-set diff
// is sufficient to decide whether to enter the update path. The full
// reconciler runs inside the transaction with full IDs.
//
// Excluded labels are ignored on both sides (case-insensitive).
// Name comparison is case-insensitive — Linear treats labels case-insensitively,
// and bead casing may diverge from Linear's display casing.
func pullHasLabelDelta(lt *linear.Tracker, local *types.Issue, remote *types.Issue) bool {
	localLower := make(map[string]bool, len(local.Labels))
	for _, n := range local.Labels {
		localLower[strings.ToLower(n)] = true
	}
	remoteLower := make(map[string]bool, len(remote.Labels))
	for _, n := range remote.Labels {
		remoteLower[strings.ToLower(n)] = true
	}
	excluded := lt.LabelExclude()
	for k := range remoteLower {
		if excluded != nil && excluded[k] {
			continue
		}
		if !localLower[k] {
			return true // Linear has a label beads doesn't
		}
	}
	for k := range localLower {
		if excluded != nil && excluded[k] {
			continue
		}
		if !remoteLower[k] {
			return true // beads has a label Linear doesn't
		}
	}
	return false
}

// syncIsScoped returns true when the user explicitly constrained THIS
// invocation to a specific subset of beads (via --parent, --issues, or
// --type). The parent reconcile pass is skipped on scoped syncs because
// it walks the full local tree, which could mutate Linear-side state
// outside the scope the user asked for.
//
// Notably ExcludeTypes is NOT a scoping signal: it merges with persistent
// config (linear.exclude_types), and rigs that set it default-on (e.g.
// "molecule,event") would otherwise have the reconcile pass permanently
// disabled — bd-9w3 root cause. Reconcile only ever touches the parent
// field on the child issue, so excluding types from push doesn't really
// conflict with wiring up parent-child for the remaining types.
//
// TypeFilter IS kept as a scoping signal because --type is set only via
// the CLI flag for this invocation; the user's intent to push a specific
// subset is explicit.
func syncIsScoped(opts *tracker.SyncOptions) bool {
	if opts == nil {
		return false
	}
	if opts.ParentID != "" || len(opts.IssueIDs) > 0 {
		return true
	}
	if len(opts.TypeFilter) > 0 {
		return true
	}
	return false
}

// reconcileLinearParents runs as a post-sync pass to wire parent-child bead
// dependencies into Linear's parent issue field. Idempotent — no API call
// when the remote parent already matches.
//
// Two scenarios this fixes:
//
//  1. Fresh tree push: when a child is pushed before its parent in the same
//     sync, the create call has no parentId to send. After all issues have
//     external_refs, this pass closes the loop.
//  2. Orphan repair: existing Linear issues created in earlier bd versions
//     (or by interrupted syncs) without a parent get wired up retroactively.
//
// In dry-run mode the read-only fetches still run and the per-link mutation
// plan is printed as [dry-run] lines, but no IssueUpdate is issued. Lets
// users preview the orphan-repair scope before committing to a wet sync.
//
// Human-readable output is suppressed when jsonOutput is true so the
// caller's JSON serialization (in runLinearSync's output section) isn't
// polluted with stray fmt.Printf lines. Warnings and errors still go
// through the warnings slice, which IS surfaced in JSON output via
// SyncResult.Warnings.
//
// Warnings (per-link failures, missing refs) are appended to the engine's
// warning slice so the user sees them in the standard sync output.
func reconcileLinearParents(ctx context.Context, lt *linear.Tracker, dryRun, jsonOutput bool, warnings *[]string) {
	if lt == nil || store == nil {
		return
	}
	links, err := buildLinearParentLinks(ctx, lt)
	if err != nil {
		*warnings = append(*warnings, fmt.Sprintf("parent reconcile: building link set failed: %v", err))
		return
	}
	if len(links) == 0 {
		return
	}
	stats, err := lt.ReconcileParents(ctx, links, dryRun)
	// Print mutation summary first; an abort (e.g. rate-limit circuit
	// breaker) may have completed some updates before bailing, and the
	// user should see that work wasn't lost. Suppress when --json is
	// requested so the JSON envelope stays clean.
	if stats != nil && !jsonOutput {
		if dryRun {
			if stats.WouldUpdate > 0 {
				fmt.Printf("[dry-run] Would reconcile %d Linear parent link%s\n",
					stats.WouldUpdate, plural(stats.WouldUpdate))
				for _, link := range stats.Mutations {
					fmt.Printf("[dry-run] Would set parent of %s → %s\n",
						link.ChildIdentifier, link.ParentIdentifier)
				}
			}
		} else if stats.Updated > 0 {
			fmt.Printf("✓ Reconciled %d Linear parent link%s\n",
				stats.Updated, plural(stats.Updated))
		}
	}
	if err != nil {
		*warnings = append(*warnings, fmt.Sprintf("parent reconcile: %v", err))
		return
	}
	for _, e := range stats.Errors {
		*warnings = append(*warnings, fmt.Sprintf("parent reconcile: %v", e))
	}
	// bd-ajn: snapshot patch failures are separate severity — surface
	// but don't conflate with API errors.
	for _, e := range stats.SnapshotWarnings {
		*warnings = append(*warnings, fmt.Sprintf("parent reconcile (snapshot): %v", e))
	}
}

// buildLinearParentLinks enumerates local beads with a Linear external_ref
// and a parent-child dependency to a parent that also has a Linear
// external_ref. Beads whose parent isn't yet synced to Linear are silently
// skipped — they'll get picked up on a subsequent sync.
//
// bd-go9 carve-out: when the parent's external_ref is a Linear Project URL
// (set by `bd linear migrate-epic-to-project`), the parent isn't an Issue
// and setting parentId on the child would be semantically wrong (and the
// Linear API would reject it). Such links are silently skipped here.
// The projectId-reconcile loop that mirrors this for the Project case is
// bd-1ay scope; in the meantime, the migration tool itself sets projectId
// on the descendants at conversion time.
func buildLinearParentLinks(ctx context.Context, lt *linear.Tracker) ([]linear.ParentLink, error) {
	issues, err := store.SearchIssues(ctx, "", types.IssueFilter{})
	if err != nil {
		return nil, err
	}
	type refInfo struct {
		identifier string
		isProject  bool
	}
	idToInfo := make(map[string]refInfo, len(issues))
	for _, issue := range issues {
		if issue.ExternalRef == nil {
			continue
		}
		ref := strings.TrimSpace(*issue.ExternalRef)
		if !lt.IsExternalRef(ref) {
			continue
		}
		ident := lt.ExtractIdentifier(ref)
		if ident == "" {
			continue
		}
		idToInfo[issue.ID] = refInfo{
			identifier: ident,
			isProject:  lt.IsProjectRef(ref),
		}
	}
	if len(idToInfo) == 0 {
		return nil, nil
	}
	links := make([]linear.ParentLink, 0)
	for _, issue := range issues {
		childInfo, ok := idToInfo[issue.ID]
		if !ok {
			continue
		}
		if childInfo.isProject {
			// A child that is itself a Project is invalid for parentId
			// assignment — skip defensively. (Shouldn't occur in normal
			// trees; only matters if a Project bead ends up with a
			// parent-child dep, which the migration tool doesn't create.)
			continue
		}
		deps, err := store.GetDependenciesWithMetadata(ctx, issue.ID)
		if err != nil {
			return nil, fmt.Errorf("loading deps for %s: %w", issue.ID, err)
		}
		for _, d := range deps {
			if d == nil || d.DependencyType != types.DepParentChild {
				continue
			}
			parentInfo, ok := idToInfo[d.Issue.ID]
			if !ok {
				continue
			}
			if parentInfo.isProject {
				// Parent has been migrated to a Linear Project — the
				// child's relationship is via projectId, not parentId.
				// Migration tool set it; bd-1ay will reconcile it on
				// ongoing sync.
				continue
			}
			links = append(links, linear.ParentLink{
				ChildIdentifier:  childInfo.identifier,
				ParentIdentifier: parentInfo.identifier,
				ChildLocalBeadID: issue.ID, // bd-ajn: lets reconciler patch snapshot post-success
			})
		}
	}
	return links, nil
}

func plural(n int) string {
	if n == 1 {
		return ""
	}
	return "s"
}

// reconcileLinearProjectMembership runs as a post-sync pass to wire
// non-epic descendants' projectId field to their nearest top-level-
// epic ancestor's Linear Project. Bd-1ay's symmetric counterpart to
// the parent reconciler (bd-ena): parents are Issues → parentId via
// ReconcileParents; parents are Projects → projectId via this pass.
//
// Idempotent: only fires IssueUpdate when remote projectId differs
// from the desired Project. Dry-run prints per-link preview lines;
// suppressed under --json so the output envelope stays clean.
//
// Skipped on the same scoped-sync conditions as the parent reconciler
// (called only when !syncIsScoped(&opts)).
func reconcileLinearProjectMembership(ctx context.Context, lt *linear.Tracker, dryRun, jsonOutput bool, warnings *[]string) {
	if lt == nil || store == nil {
		return
	}
	links, err := buildLinearProjectMembershipLinks(ctx, lt)
	if err != nil {
		*warnings = append(*warnings, fmt.Sprintf("project reconcile: building link set failed: %v", err))
		return
	}
	if len(links) == 0 {
		return
	}
	stats, err := lt.ReconcileProjectMembership(ctx, links, dryRun)
	// Print mutation summary; suppress when --json is requested.
	if stats != nil && !jsonOutput {
		if dryRun {
			if stats.WouldUpdate > 0 {
				fmt.Printf("[dry-run] Would reconcile %d Linear Project membership link%s\n",
					stats.WouldUpdate, plural(stats.WouldUpdate))
				for _, link := range stats.Mutations {
					fmt.Printf("[dry-run] Would assign %s to Project %s\n",
						link.IssueIdentifier, link.ProjectID)
				}
			}
		} else if stats.Updated > 0 {
			fmt.Printf("✓ Reconciled %d Linear Project membership link%s\n",
				stats.Updated, plural(stats.Updated))
		}
	}
	if err != nil {
		*warnings = append(*warnings, fmt.Sprintf("project reconcile: %v", err))
		return
	}
	for _, e := range stats.Errors {
		*warnings = append(*warnings, fmt.Sprintf("project reconcile: %v", e))
	}
	// bd-ajn: snapshot patch failures are separate severity — surface
	// but don't conflate with API errors.
	for _, e := range stats.SnapshotWarnings {
		*warnings = append(*warnings, fmt.Sprintf("project reconcile (snapshot): %v", e))
	}
}

// buildLinearProjectMembershipLinks enumerates non-epic beads with a
// Linear Issue external_ref whose nearest top-level-epic ancestor has
// a Linear Project external_ref. For each such (issue, project) pair,
// emits a ProjectMembershipLink. Walks parent-child deps to find the
// ancestor.
//
// Linear Project URLs on the ancestor are resolved to UUIDs via a
// single FetchProjects call (one round-trip regardless of tree size).
// Issues whose ancestor's Project URL no longer resolves on Linear
// (deleted out-of-band) are silently skipped — surfaced as a warning
// via the reconciler if the user re-runs.
//
// Beads without a Linear Issue external_ref are skipped (same shape
// as ReconcileParents — bd-1ay-pull-side (bd-6cl) will handle them).
func buildLinearProjectMembershipLinks(ctx context.Context, lt *linear.Tracker) ([]linear.ProjectMembershipLink, error) {
	issues, err := store.SearchIssues(ctx, "", types.IssueFilter{})
	if err != nil {
		return nil, err
	}
	// Index beads by ID for O(1) ancestor walk.
	byID := make(map[string]*types.Issue, len(issues))
	for _, issue := range issues {
		if issue != nil {
			byID[issue.ID] = issue
		}
	}
	// Resolve all Project URLs once up front. Map URL → UUID.
	urlToProjectID, err := buildLinearProjectURLIndex(ctx, lt)
	if err != nil {
		return nil, err
	}

	links := make([]linear.ProjectMembershipLink, 0)
	for _, issue := range issues {
		if issue == nil || issue.IssueType == types.TypeEpic {
			// Epics themselves don't get projectId; their CHILDREN do.
			continue
		}
		if issue.ExternalRef == nil {
			continue
		}
		ref := strings.TrimSpace(*issue.ExternalRef)
		if !lt.IsExternalRef(ref) || lt.IsProjectRef(ref) {
			// Skip non-Linear refs and Project refs (the latter would
			// mean a non-epic with a Project external_ref, which is
			// a state inconsistency the migration tool wouldn't produce).
			continue
		}
		ident := lt.ExtractIdentifier(ref)
		if ident == "" {
			continue
		}
		// Walk up parent-child chain to find the nearest top-level epic.
		ancestorBeadID, walkErr := findNearestTopLevelEpicAncestor(ctx, byID, issue.ID)
		if walkErr != nil {
			return nil, fmt.Errorf("walking ancestors of %s: %w", issue.ID, walkErr)
		}
		if ancestorBeadID == "" {
			continue
		}
		ancestor := byID[ancestorBeadID]
		if ancestor == nil || ancestor.ExternalRef == nil {
			continue
		}
		ancestorRef := strings.TrimSpace(*ancestor.ExternalRef)
		if !lt.IsProjectRef(ancestorRef) {
			// Ancestor epic isn't (yet) a Linear Project — either it's
			// still an Issue (bd-go9 hasn't been run) or it's never
			// been pushed. Reconciler doesn't fire for these; the
			// parent reconciler (bd-ena) handles Issue-parent links.
			continue
		}
		// Codex bd-6cl round-2 bug 4: canonicalize the bead's
		// stored external_ref before lookup so a trailing-title-slug
		// rename on Linear doesn't silently break the resolution.
		// buildLinearProjectURLIndex also canonicalizes its keys, so
		// both sides compare in the same shape.
		lookupKey := ancestorRef
		if canonical, ok := linear.CanonicalizeLinearExternalRef(ancestorRef); ok {
			lookupKey = canonical
		}
		projectID, ok := urlToProjectID[lookupKey]
		if !ok || projectID == "" {
			// Project URL doesn't resolve on Linear (deleted
			// out-of-band). Silently skip; would be too noisy to
			// warn on every such issue.
			continue
		}
		links = append(links, linear.ProjectMembershipLink{
			IssueIdentifier: ident,
			ProjectID:       projectID,
			LocalBeadID:     issue.ID, // bd-ajn: lets reconciler patch snapshot post-success
		})
	}
	return links, nil
}

// buildLinearProjectURLIndex fetches all Linear Projects once and
// returns a URL → UUID map. Cached per reconcile invocation; used by
// buildLinearProjectMembershipLinks to translate epic external_refs
// (which are URLs) into the UUIDs that IssueUpdate's projectId field
// requires.
func buildLinearProjectURLIndex(ctx context.Context, lt *linear.Tracker) (map[string]string, error) {
	projects, err := lt.FetchProjects(ctx, "all")
	if err != nil {
		return nil, fmt.Errorf("fetching Linear projects: %w", err)
	}
	idx := make(map[string]string, len(projects))
	for _, p := range projects {
		if p.URL == "" || p.ID == "" {
			continue
		}
		// Codex bd-6cl round-2 bug 4: canonicalize the URL so the
		// lookup at the call site can compare canonicalized
		// references symmetrically — a bead's stored external_ref
		// may carry a different trailing title slug than Linear's
		// current Project URL.
		key := p.URL
		if canonical, ok := linear.CanonicalizeLinearExternalRef(p.URL); ok {
			key = canonical
		}
		idx[key] = p.ID
	}
	return idx, nil
}

// findNearestTopLevelEpicAncestor walks UP the parent-child dep chain
// from issueID until reaching either (a) a bead with no parent — return
// it iff it's an epic; or (b) cycle-detection bailout (defensive). Per
// bd-1ay design, ALL descendants of a top-level epic land in the same
// Project, not their immediate sub-epic. So we walk past intermediate
// epics too.
//
// Returns "" when the chain leads to a non-epic root (i.e., no epic
// anywhere in the ancestor chain).
func findNearestTopLevelEpicAncestor(ctx context.Context, byID map[string]*types.Issue, issueID string) (string, error) {
	current := issueID
	visited := map[string]bool{current: true}
	for {
		deps, err := store.GetDependenciesWithMetadata(ctx, current)
		if err != nil {
			return "", err
		}
		var parentID string
		for _, d := range deps {
			if d == nil || d.DependencyType != types.DepParentChild {
				continue
			}
			parentID = d.Issue.ID
			break
		}
		if parentID == "" {
			// Reached the root. Top-level only if it's an epic.
			currentBead := byID[current]
			if currentBead != nil && currentBead.IssueType == types.TypeEpic {
				return current, nil
			}
			return "", nil
		}
		if visited[parentID] {
			// Cycle (shouldn't happen for valid parent-child trees, but
			// don't loop forever). Treat as no top-level epic.
			return "", nil
		}
		visited[parentID] = true
		current = parentID
	}
}
