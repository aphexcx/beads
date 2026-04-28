package main

import (
	"context"
	"fmt"
	"os"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/spf13/cobra"
	"github.com/steveyegge/beads/internal/config"
	"github.com/steveyegge/beads/internal/debug"
	"github.com/steveyegge/beads/internal/linear"
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
  LINEAR_API_KEY  - Linear API key
  LINEAR_TEAM_ID  - Linear team ID (UUID, singular)
  LINEAR_TEAM_IDS - Linear team IDs (comma-separated UUIDs)

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
  bd linear status              # Show sync status`,
}

// linearSyncCmd handles synchronization with Linear.
var linearSyncCmd = &cobra.Command{
	Use:   "sync",
	Short: "Synchronize issues with Linear",
	Long: `Synchronize issues between beads and Linear.

Modes:
  --pull         Import issues from Linear into beads
  --push         Export issues from beads to Linear
  (no flags)     Bidirectional sync: pull then push, with conflict resolution

Team Selection:
  --team ID1,ID2  Override configured team IDs for this sync
  Multiple teams can be configured via linear.team_ids (comma-separated).
  Falls back to linear.team_id for backward compatibility.
  Push requires explicit --team when multiple teams are configured.

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

Conflict Resolution:
  By default, newer timestamp wins. Override with:
  --prefer-local    Always prefer local beads version
  --prefer-linear   Always prefer Linear version

Examples:
  bd linear sync --pull                         # Import from Linear
  bd linear sync --push --create-only           # Push new issues only
  bd linear sync --push --type=task,feature     # Push only tasks and features
  bd linear sync --push --exclude-type=wisp     # Push all except wisps
  bd linear sync --push --parent=bd-abc123      # Push one ticket tree
  bd linear sync --dry-run                      # Preview without changes
  bd linear sync --prefer-local                 # Bidirectional, local wins`,
	Run: runLinearSync,
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
	Run: runLinearStatus,
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
	Run: runLinearTeams,
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
	linearSyncCmd.Flags().String("state", "all", "Issue state to sync: open, closed, all")
	linearSyncCmd.Flags().StringSlice("type", nil, "Only sync issues of these types (can be repeated)")
	linearSyncCmd.Flags().StringSlice("exclude-type", nil, "Exclude issues of these types (can be repeated)")
	linearSyncCmd.Flags().Bool("include-ephemeral", false, "Include ephemeral issues (wisps, etc.) when pushing to Linear")
	linearSyncCmd.Flags().String("parent", "", "Limit push to this beads ticket and its descendants")
	linearSyncCmd.Flags().String("since", "", "Only push issues updated after this date (e.g. 2026-03-27, 15d for 15 days ago)")
	linearSyncCmd.Flags().StringSlice("team", nil, "Team ID(s) to sync (overrides configured team_id/team_ids)")
	registerSelectiveSyncFlags(linearSyncCmd)

	linearCmd.AddCommand(linearSyncCmd)
	linearCmd.AddCommand(linearStatusCmd)
	linearCmd.AddCommand(linearTeamsCmd)
	rootCmd.AddCommand(linearCmd)
}

func runLinearSync(cmd *cobra.Command, args []string) {
	pull, _ := cmd.Flags().GetBool("pull")
	push, _ := cmd.Flags().GetBool("push")
	dryRun, _ := cmd.Flags().GetBool("dry-run")
	preferLocal, _ := cmd.Flags().GetBool("prefer-local")
	preferLinear, _ := cmd.Flags().GetBool("prefer-linear")
	createOnly, _ := cmd.Flags().GetBool("create-only")
	createClosed, _ := cmd.Flags().GetBool("create-closed")
	verboseDiff, _ := cmd.Flags().GetBool("verbose-diff")
	state, _ := cmd.Flags().GetString("state")
	typeFilters, _ := cmd.Flags().GetStringSlice("type")
	excludeTypes, _ := cmd.Flags().GetStringSlice("exclude-type")
	includeEphemeral, _ := cmd.Flags().GetBool("include-ephemeral")
	sinceStr, _ := cmd.Flags().GetString("since")
	cliTeams, _ := cmd.Flags().GetStringSlice("team")

	if !dryRun {
		CheckReadonly("linear sync")
	}

	if preferLocal && preferLinear {
		FatalError("cannot use both --prefer-local and --prefer-linear")
	}

	if err := ensureStoreActive(); err != nil {
		FatalError("database not available: %v", err)
	}

	if err := validateLinearConfig(cliTeams); err != nil {
		FatalError("%v", err)
	}

	ctx := rootCtx
	teamIDs := getLinearTeamIDs(ctx, cliTeams)
	willPush := push || !pull

	// Require explicit --team for push when multiple teams are configured.
	if willPush && len(teamIDs) > 1 && len(cliTeams) == 0 {
		FatalError("push requires explicit --team flag when multiple teams are configured\n" +
			"Use: bd linear sync --push --team <TEAM_ID>")
	}

	// Create and initialize the Linear tracker
	lt := &linear.Tracker{}
	lt.SetTeamIDs(teamIDs)
	if err := lt.Init(ctx, store); err != nil {
		FatalError("initializing Linear tracker: %v", err)
	}

	// Wire label-sync config so PushHooks/PullHooks builders see the right
	// LabelSyncEnabled() value when they install the label-aware hooks below.
	allCfg, _ := store.GetAllConfig(ctx)
	lsCfg := loadLinearLabelSyncConfig(allCfg)
	lt.SetLabelSyncConfig(lsCfg.Enabled, lsCfg.Exclude, lsCfg.CreateScope, func(format string, args ...interface{}) {
		fmt.Fprintf(os.Stderr, "Warning: linear label sync: "+format+"\n", args...)
	})

	if willPush {
		if err := lt.ValidatePushStateMappings(ctx); err != nil {
			FatalError("%v", err)
		}
	}

	// Create the sync engine
	engine := tracker.NewEngine(lt, store, actor)
	engine.OnMessage = func(msg string) { fmt.Println("  " + msg) }
	engine.OnWarning = func(msg string) { fmt.Fprintf(os.Stderr, "Warning: %s\n", msg) }

	// Set up Linear-specific pull hooks
	engine.PullHooks = buildLinearPullHooks(ctx, lt)

	// Build sync options from CLI flags
	opts := tracker.SyncOptions{
		Pull:         pull,
		Push:         push,
		DryRun:       dryRun,
		CreateOnly:   createOnly,
		CreateClosed: createClosed,
		VerboseDiff:  verboseDiff,
		State:        state,
	}

	// Convert type filters
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
	if !includeEphemeral {
		opts.ExcludeEphemeral = true
	}

	// Parse --since flag
	if sinceStr != "" {
		sinceTime, err := parseSinceFlag(sinceStr)
		if err != nil {
			FatalError("invalid --since value %q: %v", sinceStr, err)
		}
		opts.Since = sinceTime
	}

	if err := applySelectiveSyncFlags(cmd, &opts, push); err != nil {
		FatalError("%v", err)
	}
	allowProjectCreates := opts.ParentID != "" || len(opts.IssueIDs) > 0

	// Set up Linear-specific push hooks
	engine.PushHooks = buildLinearPushHooks(ctx, lt, allowProjectCreates)

	// Map conflict resolution
	if preferLocal {
		opts.ConflictResolution = tracker.ConflictLocal
	} else if preferLinear {
		opts.ConflictResolution = tracker.ConflictExternal
	} else {
		opts.ConflictResolution = tracker.ConflictTimestamp
	}

	// Run sync
	result, err := engine.Sync(ctx, opts)
	if err != nil {
		if jsonOutput {
			outputJSON(result)
		} else {
			fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		}
		os.Exit(1)
	}

	// Output results
	if jsonOutput {
		outputJSON(result)
	} else if dryRun {
		fmt.Println("\n✓ Dry run complete (no changes made)")
	} else {
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
	}
}

// buildLinearPullHooks creates PullHooks for Linear-specific pull behavior.
func buildLinearPullHooks(ctx context.Context, lt *linear.Tracker) *tracker.PullHooks {
	idMode := getLinearIDMode(ctx)
	hashLength := getLinearHashLength(ctx)

	hooks := &tracker.PullHooks{}

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

	if idMode == "hash" {
		// Pre-load existing IDs for collision avoidance
		existingIssues, err := store.SearchIssues(ctx, "", types.IssueFilter{})
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
			prefix, err = store.GetConfig(ctx, "issue_prefix")
			if err != nil || prefix == "" {
				prefix = "bd"
			}
		}

		hooks.GenerateID = func(_ context.Context, issue *types.Issue) error {
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
				if !linear.PushFieldsEqual(local, remoteIssue, config) {
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
				snap, _ := lt.LoadSnapshot(ctx, local.ID)
				res := linear.ReconcileLabels(linear.LabelReconcileInput{
					Beads:    local.Labels,
					Linear:   linearLabels,
					Snapshot: snap,
					Exclude:  lt.LabelExclude(),
				})
				if len(res.AddToLinear) > 0 {
					diffs = append(diffs, fmt.Sprintf("labels +%v (push to Linear)", res.AddToLinear))
				}
				if len(res.RemoveFromLinear) > 0 {
					diffs = append(diffs, fmt.Sprintf("labels -%v (remove from Linear)", res.RemoveFromLinear))
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

func runLinearStatus(cmd *cobra.Command, args []string) {
	ctx := rootCtx

	if err := ensureStoreActive(); err != nil {
		FatalError("%v", err)
	}

	apiKey, _ := getLinearConfig(ctx, "linear.api_key")
	teamIDs := getLinearTeamIDs(ctx, nil)
	lastSync, _ := store.GetConfig(ctx, "linear.last_sync")

	configured := apiKey != "" && len(teamIDs) > 0

	allIssues, err := store.SearchIssues(ctx, "", types.IssueFilter{})
	if err != nil {
		FatalError("%v", err)
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
		// Backward compat: include team_id as first team, plus full list.
		teamID := ""
		if len(teamIDs) > 0 {
			teamID = teamIDs[0]
		}
		outputJSON(map[string]interface{}{
			"configured":      configured,
			"has_api_key":     hasAPIKey,
			"team_id":         teamID,
			"team_ids":        teamIDs,
			"last_sync":       lastSync,
			"total_issues":    len(allIssues),
			"with_linear_ref": withLinearRef,
			"pending_push":    pendingPush,
		})
		return
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
		return
	}

	if len(teamIDs) == 1 {
		fmt.Printf("Team ID:      %s\n", teamIDs[0])
	} else {
		fmt.Printf("Team IDs:     %s (%d teams)\n", strings.Join(teamIDs, ", "), len(teamIDs))
	}
	fmt.Printf("API Key:      %s\n", maskAPIKey(apiKey))
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
}

func runLinearTeams(cmd *cobra.Command, args []string) {
	ctx := rootCtx

	apiKey, apiKeySource := getLinearConfig(ctx, "linear.api_key")
	if apiKey == "" {
		fmt.Fprintf(os.Stderr, "Error: Linear API key not configured\n")
		fmt.Fprintf(os.Stderr, "Run: bd config set linear.api_key \"YOUR_API_KEY\"\n")
		fmt.Fprintf(os.Stderr, "Or:  export LINEAR_API_KEY=YOUR_API_KEY\n")
		os.Exit(1)
	}

	debug.Logf("Using API key from %s", apiKeySource)

	client := linear.NewClient(apiKey, "")

	teams, err := client.FetchTeams(ctx)
	if err != nil {
		FatalError("fetching teams: %v", err)
	}

	if len(teams) == 0 {
		fmt.Println("No teams found (check your API key permissions)")
		return
	}

	if jsonOutput {
		outputJSON(teams)
		return
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

	apiKey, _ := getLinearConfig(ctx, "linear.api_key")
	if apiKey == "" {
		return fmt.Errorf("Linear API key not configured\nRun: bd config set linear.api_key \"YOUR_API_KEY\"\nOr: export LINEAR_API_KEY=YOUR_API_KEY")
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
// Priority: project config > environment variable.
func getLinearConfig(ctx context.Context, key string) (value string, source string) {
	// Secret keys (e.g. linear.api_key) are stored in config.yaml, not the
	// Dolt database, to avoid leaking secrets when pushing to remotes.
	if config.IsYamlOnlyKey(key) {
		if value := config.GetString(key); value != "" {
			return value, "project config (config.yaml)"
		}
		envKey := linearConfigToEnvVar(key)
		if envKey != "" {
			if value := os.Getenv(envKey); value != "" {
				return value, fmt.Sprintf("environment variable (%s)", envKey)
			}
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
func getLinearClient(ctx context.Context) (*linear.Client, error) {
	apiKey, _ := getLinearConfig(ctx, "linear.api_key")
	if apiKey == "" {
		return nil, fmt.Errorf("Linear API key not configured")
	}

	teamIDs := getLinearTeamIDs(ctx, nil)
	if len(teamIDs) == 0 {
		return nil, fmt.Errorf("Linear team ID not configured")
	}

	client := linear.NewClient(apiKey, teamIDs[0])

	if store != nil {
		if endpoint, _ := store.GetConfig(ctx, "linear.api_endpoint"); endpoint != "" {
			client = client.WithEndpoint(endpoint)
		}
		// Filter to specific project if configured
		if projectID, _ := store.GetConfig(ctx, "linear.project_id"); projectID != "" {
			client = client.WithProjectID(projectID)
		}
	}

	return client, nil
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
// Fail-safe: if the snapshot read errors, return true to force a push;
// worse to silently swallow a label change than to issue a no-op mutation.
func hasLabelDelta(ctx context.Context, lt *linear.Tracker, local *types.Issue, remoteIssue *linear.Issue) bool {
	linearLabels := make([]linear.LinearLabel, 0)
	if remoteIssue.Labels != nil {
		for _, l := range remoteIssue.Labels.Nodes {
			linearLabels = append(linearLabels, linear.LinearLabel{Name: l.Name, ID: l.ID})
		}
	}
	snap, err := lt.LoadSnapshot(ctx, local.ID)
	if err != nil {
		return true
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
func pullHasLabelDelta(lt *linear.Tracker, local *types.Issue, remote *types.Issue) bool {
	localSet := make(map[string]bool, len(local.Labels))
	for _, n := range local.Labels {
		localSet[n] = true
	}
	remoteSet := make(map[string]bool, len(remote.Labels))
	for _, n := range remote.Labels {
		remoteSet[n] = true
	}
	excluded := lt.LabelExclude()
	for n := range remoteSet {
		if excluded != nil && excluded[strings.ToLower(n)] {
			continue
		}
		if !localSet[n] {
			return true // Linear has a label beads doesn't
		}
	}
	for n := range localSet {
		if excluded != nil && excluded[strings.ToLower(n)] {
			continue
		}
		if !remoteSet[n] {
			return true // beads has a label Linear doesn't
		}
	}
	return false
}
