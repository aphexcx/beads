package tracker

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"

	"github.com/steveyegge/beads/internal/storage"
	"github.com/steveyegge/beads/internal/types"
	"github.com/steveyegge/beads/internal/ui"
)

// syncTracer is the OTel tracer for tracker sync spans.
var syncTracer = otel.Tracer("github.com/steveyegge/beads/tracker")

// rateLimitExhaustedError is implemented by tracker errors (e.g.
// linear.ErrRateLimitExhausted) that signal the API quota floor has been
// hit and the sync loop should abort immediately rather than cascade the
// error across every remaining issue.
type rateLimitExhaustedError interface {
	RateLimitExhausted() bool
}

// isRateLimitExhausted reports whether err (or any error it wraps) signals
// that the API rate-limit circuit breaker has tripped.
func isRateLimitExhausted(err error) bool {
	var rle rateLimitExhaustedError
	return errors.As(err, &rle) && rle.RateLimitExhausted()
}

// PullHooks contains optional callbacks that customize pull (import) behavior.
// Trackers opt into behaviors by setting the hooks they need.
type PullHooks struct {
	// GenerateID assigns an ID to a newly-pulled issue before import.
	// If nil, issues keep whatever ID the storage layer assigns.
	// The hook receives the issue (with converted fields) and should set issue.ID.
	// Callers typically pre-load used IDs into the closure for collision avoidance.
	GenerateID func(ctx context.Context, issue *types.Issue) error

	// TransformIssue is called after FieldMapper.IssueToBeads() and before storage.
	// Use for description formatting, field normalization, etc.
	TransformIssue func(issue *types.Issue)

	// ShouldImport filters issues during pull. Return false to skip.
	// Called on the raw TrackerIssue before conversion to beads format.
	// If nil, all issues are imported.
	ShouldImport func(issue *TrackerIssue) bool

	// SyncComments is called per-issue after import to sync comments from the external tracker.
	// If nil, comment sync is skipped during pull.
	SyncComments func(ctx context.Context, localIssueID string, externalIssueID string) error

	// SyncAttachments is called per-issue after import to sync attachment metadata from the external tracker.
	// If nil, attachment sync is skipped during pull.
	SyncAttachments func(ctx context.Context, localIssueID string, externalIssueID string) error

	// ContentEqual overrides the generic pullIssueEqual check when the
	// tracker needs custom comparison logic (e.g. Linear builds its
	// description by merging local.description with acceptance/notes fields,
	// so a byte-exact compare between local.Description and remote.Description
	// always fails even when the content is semantically identical).
	// Returns true if local and the converted-remote are equal and the pull
	// should skip. If nil, pullIssueEqual is used.
	ContentEqual func(local, remote *types.Issue) bool

	// ReconcileLabels overrides the legacy "Linear-authoritative" label sync
	// at engine.go:496. When set, the hook owns reading current labels,
	// running its own reconciliation logic, and writing back through tx
	// (including any per-tracker snapshot tables).
	//
	// When nil, the engine falls back to legacySyncIssueLabels (the prior
	// behavior, renamed in this commit). This keeps non-Linear trackers
	// (GitHub/GitLab) unaffected — they have no opinion about label
	// reconciliation and rely on the legacy flow.
	ReconcileLabels func(ctx context.Context, tx storage.Transaction, issueID string, desired []string, extIssue *TrackerIssue, actor string) error
	// AfterConvert is called after the external issue has been converted to
	// a beads issue, transformed, and assigned an ID, but before it is stored.
	// Hooks may mutate the conversion, for example by adding dependencies that
	// should be created after all pulled issues have been saved.
	AfterConvert func(ctx context.Context, extIssue *TrackerIssue, conv *IssueConversion, ref string, existing *types.Issue, opts SyncOptions) error
}

// PushHooks contains optional callbacks that customize push (export) behavior.
// Trackers opt into behaviors by setting the hooks they need.
type PushHooks struct {
	// FormatDescription transforms the description before sending to tracker.
	// Linear uses this for BuildLinearDescription (merging structured fields).
	// If nil, issue.Description is used as-is.
	FormatDescription func(issue *types.Issue) string

	// ContentEqual compares local and remote issues to skip unnecessary API calls.
	// Returns true if content is identical (skip update). If nil, uses timestamp comparison.
	ContentEqual func(local *types.Issue, remote *TrackerIssue) bool

	// DescribeDiff returns human-readable field-level differences between local
	// and remote for dry-run verbose output. Each entry is a short description
	// like "title: \"old\" → \"new\"". Return empty slice if no differences.
	// Optional — if nil, dry-run output just reports that an update would happen
	// without enumerating fields.
	DescribeDiff func(local *types.Issue, remote *TrackerIssue) []string

	// ShouldPush filters issues during push. Return false to skip.
	// Called in addition to type/state/ephemeral filters. Use for prefix filtering, etc.
	// If nil, all issues (matching other filters) are pushed.
	ShouldPush func(issue *types.Issue) bool

	// BuildStateCache is called once before the push loop to pre-cache workflow states.
	// Returns an opaque cache value passed to ResolveState on each issue.
	// If nil, no caching is done.
	BuildStateCache func(ctx context.Context) (interface{}, error)

	// ResolveState maps a beads status to a tracker state ID using the cached state.
	// Only called if BuildStateCache is set. Returns (stateID, ok).
	ResolveState func(cache interface{}, status types.Status) (string, bool)

	// SyncComments is called per-issue after push to sync local comments to the external tracker.
	// If nil, comment sync is skipped during push.
	SyncComments func(ctx context.Context, localIssueID string, externalIssueID string) error
}

// Engine orchestrates synchronization between beads and an external tracker.
// It implements the shared Pull→Detect→Resolve→Push pattern that all tracker
// integrations follow, eliminating duplication between Linear, GitLab, etc.
type Engine struct {
	Tracker   IssueTracker
	Store     storage.Storage
	Actor     string
	PullHooks *PullHooks
	PushHooks *PushHooks

	// Callbacks for UI feedback (optional).
	OnMessage func(msg string)
	OnWarning func(msg string)

	// stateCache holds the opaque value from PushHooks.BuildStateCache during a push.
	// Tracker adapters access it via ResolveState().
	stateCache interface{}

	// warnings collects warning messages during a Sync() call for inclusion in SyncResult.
	warnings []string
}

// NewEngine creates a new sync engine for the given tracker and storage.
func NewEngine(tracker IssueTracker, store storage.Storage, actor string) *Engine {
	return &Engine{
		Tracker: tracker,
		Store:   store,
		Actor:   actor,
	}
}

// Sync performs a complete synchronization operation based on the given options.
func (e *Engine) Sync(ctx context.Context, opts SyncOptions) (*SyncResult, error) {
	ctx, span := syncTracer.Start(ctx, "tracker.sync",
		trace.WithAttributes(
			attribute.String("sync.tracker", e.Tracker.DisplayName()),
			attribute.Bool("sync.pull", opts.Pull || (!opts.Pull && !opts.Push)),
			attribute.Bool("sync.push", opts.Push || (!opts.Pull && !opts.Push)),
			attribute.Bool("sync.dry_run", opts.DryRun),
		),
	)
	defer span.End()

	result := &SyncResult{Success: true}
	e.warnings = nil

	// Default to bidirectional if neither specified
	if !opts.Pull && !opts.Push {
		opts.Pull = true
		opts.Push = true
	}

	// bd-3p8: hard-fail when a tracker advertises a capability that
	// REQUIRES a matching store interface, but the configured store
	// doesn't implement it. Mayor's design principle: capability-
	// degrade is acceptable for OPTIONAL features (label sync,
	// comment sync); REQUIRED features (Project pull when
	// ProjectSyncer is configured, field-scoped conflicts when the
	// tracker supports them) should hard-error instead of warn —
	// otherwise the feature ships silently disabled on backends
	// without the matching impl, exactly the bd-3p8 failure mode.
	if err := e.checkRequiredStoreCapabilities(); err != nil {
		result.Success = false
		result.Error = err.Error()
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return result, err
	}

	// Track IDs to skip/force during push based on conflict resolution
	skipPushIDs := make(map[string]bool)
	forcePushIDs := make(map[string]bool)

	allowPullOverwriteIDs := make(map[string]bool)

	// bd-ajn field-scoped per-issue field maps. Keyed by issue.ID;
	// each inner map lists the ConflictFields to push or pull. Empty
	// (or missing entry) means "no scoping — push/pull whole issue"
	// for legacy compatibility.
	pushFieldScopes := make(map[string]map[ConflictField]bool)
	pullFieldScopes := make(map[string]map[ConflictField]bool)

	// Phase 1: Detect conflicts (only for bidirectional sync)
	if opts.Pull && opts.Push {
		conflicts, err := e.DetectConflicts(ctx, opts)
		if err != nil {
			e.warn("Failed to detect conflicts: %v", err)
		} else if len(conflicts) > 0 {
			result.Stats.Conflicts = len(conflicts)
			e.resolveConflictsWithFieldScopes(opts, conflicts,
				skipPushIDs, forcePushIDs, allowPullOverwriteIDs,
				pushFieldScopes, pullFieldScopes)
		}
	}

	// Phase 2: Pull
	if opts.Pull {
		pullStats, err := e.doPull(ctx, opts, allowPullOverwriteIDs, skipPushIDs, pullFieldScopes)
		if err != nil {
			result.Success = false
			result.Error = fmt.Sprintf("pull failed: %v", err)
			span.RecordError(err)
			span.SetStatus(codes.Error, result.Error)
			return result, err
		}
		result.PullStats = *pullStats
		result.Stats.Pulled = pullStats.Created + pullStats.Updated
		result.Stats.Created += pullStats.Created
		result.Stats.Updated += pullStats.Updated
		result.Stats.Skipped += pullStats.Skipped
		result.Stats.Errors += pullStats.Errors
	}

	// Phase 3: Push
	if opts.Push {
		pushStats, err := e.doPush(ctx, opts, skipPushIDs, forcePushIDs, pushFieldScopes)
		if err != nil {
			result.Success = false
			result.Error = fmt.Sprintf("push failed: %v", err)
			span.RecordError(err)
			span.SetStatus(codes.Error, result.Error)
			return result, err
		}
		result.PushStats = *pushStats
		result.Stats.Pushed = pushStats.Created + pushStats.Updated
		result.Stats.Created += pushStats.Created
		result.Stats.Updated += pushStats.Updated
		result.Stats.Skipped += pushStats.Skipped
		result.Stats.Errors += pushStats.Errors
		result.Warnings = append(result.Warnings, pushStats.Warnings...)
	}

	// Record final stats as span attributes.
	span.SetAttributes(
		attribute.Int("sync.pulled", result.Stats.Pulled),
		attribute.Int("sync.pushed", result.Stats.Pushed),
		attribute.Int("sync.conflicts", result.Stats.Conflicts),
		attribute.Int("sync.created", result.Stats.Created),
		attribute.Int("sync.updated", result.Stats.Updated),
		attribute.Int("sync.skipped", result.Stats.Skipped),
		attribute.Int("sync.errors", result.Stats.Errors),
	)

	// Update last_sync timestamp. Dolt DATETIME columns round sub-second
	// values, so rows this sync just wrote can carry updated_at values up
	// to half a second in the future of wall clock. Record last_sync at
	// the next whole second so the engine's own writes are never misread
	// as local edits by the next pull's conflict guard.
	if !opts.DryRun {
		lastSync := time.Now().UTC().Truncate(time.Second).Add(time.Second).Format(time.RFC3339Nano)
		key := e.Tracker.ConfigPrefix() + ".last_sync"
		if err := e.Store.SetLocalMetadata(ctx, key, lastSync); err != nil {
			e.warn("Failed to update last_sync: %v", err)
		}
		result.LastSync = lastSync
	}

	// Batch-push warnings were already appended above; e.warn-collected
	// warnings join them rather than replacing them.
	result.Warnings = append(result.Warnings, e.warnings...)
	return result, nil
}

// DetectConflicts identifies issues that need cross-side attention since
// the last sync. bd-ajn extended this from whole-issue timestamp
// comparison to per-field diff backed by snapshots:
//
//   - LOCAL side: dolt_history_issues for state at lastSync
//   - REMOTE side: linear_issue_snapshots (LinearIssueSnapshotStore)
//
// The resulting Conflict carries LocalChanged / ExternalChanged /
// Conflicting field maps. resolveConflicts consumes them to dispatch
// per-field push / pull / true-conflict actions.
//
// Falls back to whole-issue timestamp behavior when:
//   - The storage backend doesn't expose HistoryViewer
//   - The tracker doesn't expose a snapshot store
//   - The snapshot row doesn't exist yet (first-sync soft rollout —
//     mayor's Q5: baseline + emit no conflict, next sync gets the
//     real field-scoped path)
func (e *Engine) DetectConflicts(ctx context.Context, opts SyncOptions) ([]Conflict, error) {
	ctx, span := syncTracer.Start(ctx, "tracker.detect_conflicts",
		trace.WithAttributes(attribute.String("sync.tracker", e.Tracker.DisplayName())),
	)
	defer span.End()

	// Get last sync time
	key := e.Tracker.ConfigPrefix() + ".last_sync"
	lastSyncStr, err := e.Store.GetLocalMetadata(ctx, key)
	if err != nil || lastSyncStr == "" {
		return nil, nil // No previous sync, no conflicts possible
	}

	lastSync, err := parseSyncTime(lastSyncStr)
	if err != nil {
		return nil, fmt.Errorf("invalid last_sync timestamp %q: %w", lastSyncStr, err)
	}

	// Find local issues with external refs for this tracker
	filter := types.IssueFilter{}
	issues, err := e.Store.SearchIssues(ctx, "", filter)
	if err != nil {
		return nil, fmt.Errorf("searching issues: %w", err)
	}

	// bd-ajn: check capability surface once up front so the per-issue
	// loop doesn't repeat type assertions.
	snapStore, snapStoreOK := e.Store.(storage.LinearIssueSnapshotStore)
	snapshotter, snapshotterOK := e.Tracker.(PostPullSnapshotter)

	// First pass: select detection candidates without touching the API.
	type conflictCandidate struct {
		issue  *types.Issue
		extRef string
		extID  string
	}
	var candidates []conflictCandidate
	for _, issue := range issues {
		extRef := derefStr(issue.ExternalRef)
		if extRef == "" || !e.Tracker.IsExternalRef(extRef) {
			continue
		}

		// Whole-issue fast-skip: no local change AND we'll only know
		// about external changes after fetching. The old code skipped
		// here unconditionally; field-scoping needs to fetch even when
		// !localChanged because the external side may have changed
		// fields we still want to pull. The conservative compromise:
		// keep the fast-skip when neither snapshot infra is available
		// (legacy behavior preserved).
		if !issue.UpdatedAt.After(lastSync) && !(snapStoreOK && snapshotterOK) {
			continue
		}

		extID := e.Tracker.ExtractIdentifier(extRef)
		if extID == "" {
			continue
		}
		candidates = append(candidates, conflictCandidate{issue: issue, extRef: extRef, extID: extID})
	}

	// Resolve remote state for all candidates in batched requests when the
	// tracker supports it (bd-kqt: the per-issue fallback costs one API
	// request per linked issue, which alone can exhaust an hourly budget on
	// ~1k-issue repos).
	identifiers := make([]string, len(candidates))
	for i, cand := range candidates {
		identifiers[i] = cand.extID
	}
	remoteByID, batched, batchErr := e.batchFetchRemoteIssues(ctx, identifiers)
	if batchErr != nil && isRateLimitExhausted(batchErr) {
		// No budget left — stop instead of letting later phases grind
		// against a tripped circuit breaker.
		return nil, fmt.Errorf("batch fetching conflict candidates: %w", batchErr)
	}
	// Transient batch failure: restore HEAD's per-issue semantics for the
	// unresolved remainder (fetch each; per-issue errors skip that issue
	// only) instead of misreading them as "absent remotely".
	perIssueFallback := batchErr != nil

	var conflicts []Conflict
	for _, cand := range candidates {
		issue, extRef, extID := cand.issue, cand.extRef, cand.extID
		localChanged := issue.UpdatedAt.After(lastSync)

		// Fetch external version (from the batch map when available).
		var extIssue *TrackerIssue
		if batched {
			extIssue = remoteByID[extID]
			if extIssue == nil && perIssueFallback {
				var err error
				extIssue, err = e.Tracker.FetchIssue(ctx, extID)
				if err != nil {
					continue
				}
			}
		} else {
			var err error
			extIssue, err = e.Tracker.FetchIssue(ctx, extID)
			if err != nil {
				continue
			}
		}
		if extIssue == nil {
			continue
		}

		// Both-sides-clean fast-skip: neither side has been touched since
		// lastSync, so no conflict is possible and there is nothing new to
		// field-scope. Skipping here saves the per-issue dolt_history scan
		// that detectFieldScopedConflict performs for every baselined
		// issue — which otherwise runs for EVERY linked bead on every
		// bidirectional sync and dominates wall time on ~1k-issue repos
		// (bd-kqt). The skip requires an existing baseline snapshot: a
		// clean issue with NO baseline must still flow into
		// detectFieldScopedConflict so the first-sync soft rollout records
		// one — otherwise a later both-sides change would baseline the
		// already-changed remote and let the push overwrite it without
		// conflict resolution (codex bd-kqt round-3).
		if !localChanged && !extIssue.UpdatedAt.After(lastSync) && snapStoreOK && snapshotterOK {
			if snap, snapErr := snapStore.GetLinearIssueSnapshot(ctx, issue.ID); snapErr == nil && snap != nil {
				continue
			}
		}

		// Try field-scoped path. Falls through to whole-issue logic on
		// any of the documented fallback conditions.
		if snapStoreOK && snapshotterOK {
			emitted, fieldErr := e.detectFieldScopedConflict(ctx, issue, extIssue, extRef, lastSync, snapStore, snapshotter, opts.DryRun)
			if fieldErr == nil {
				if emitted != nil {
					conflicts = append(conflicts, *emitted)
				}
				continue
			}
			// Field-scoped detection error → fall through to legacy.
			e.warn("Field-scoped conflict detection failed for %s; falling back to whole-issue: %v", issue.ID, fieldErr)
		}

		// Legacy whole-issue timestamp path. Reached when the backend
		// doesn't expose snapshot infra OR when field-scoped detection
		// errored. Preserves pre-bd-ajn behavior for trackers that
		// haven't opted into the snapshot pattern.
		if !localChanged {
			continue
		}
		if extIssue.UpdatedAt.After(lastSync) {
			conflicts = append(conflicts, Conflict{
				IssueID:            issue.ID,
				LocalUpdated:       issue.UpdatedAt,
				ExternalUpdated:    extIssue.UpdatedAt,
				ExternalRef:        extRef,
				ExternalIdentifier: extIssue.Identifier,
				ExternalInternalID: extIssue.ID,
			})
		}
	}

	span.SetAttributes(attribute.Int("sync.conflicts", len(conflicts)))
	return conflicts, nil
}

// detectFieldScopedConflict runs the per-field diff for a single
// issue. Returns (conflict, nil) when a conflict should be emitted,
// (nil, nil) when no action is needed (no changes on either side),
// or (nil, err) to signal the caller to fall back to whole-issue
// logic.
//
// First-sync soft rollout (mayor's Q5): a missing snapshot triggers
// a baseline write + log line, and we return (nil, nil) — no
// conflict this run, next sync gets real field-scoping.
//
// bd-p4m: dryRun gates the baseline write. In dry-run mode we
// preview the would-be write but do NOT touch the snapshot table —
// otherwise --dry-run consumes the soft-rollout grace period and
// the next wet-run takes a different code path than it would have
// without the dry-run. Dry-run must be a pure preview.
func (e *Engine) detectFieldScopedConflict(
	ctx context.Context,
	issue *types.Issue,
	extIssue *TrackerIssue,
	extRef string,
	lastSync time.Time,
	snapStore storage.LinearIssueSnapshotStore,
	snapshotter PostPullSnapshotter,
	dryRun bool,
) (*Conflict, error) {
	snap, err := snapStore.GetLinearIssueSnapshot(ctx, issue.ID)
	if err != nil {
		return nil, fmt.Errorf("read snapshot: %w", err)
	}
	if snap == nil {
		// First-sync soft rollout.
		if dryRun {
			// bd-p4m: preview only; don't write. Skipping the write
			// preserves dry-run idempotency AND keeps the soft-
			// rollout grace period available for the next wet-run.
			e.msg("[dry-run] Would snapshot baseline for issue %s (no conflict possible this run)", issue.ID)
			return nil, nil
		}
		e.msg("first sync — snapshotting baseline for issue %s (no conflict possible this run)", issue.ID)
		if writeErr := snapshotter.RecordPullSnapshot(ctx, issue.ID, *extIssue); writeErr != nil {
			// Codex bd-ajn round-2 bug 6: a persistent baseline write
			// failure would loop forever — every sync sees nil
			// snapshot, retries, fails, and the user's conflicts are
			// never surfaced. Escalate to the caller as a real error
			// so it falls through to legacy whole-issue handling.
			// That path uses pure timestamp comparison (no snapshot
			// dependency) and at least gives the conflict a chance
			// to be detected and resolved under existing behavior.
			return nil, fmt.Errorf("baseline snapshot write failed for %s: %w", issue.ID, writeErr)
		}
		return nil, nil
	}

	// Local-side state at lastSync via dolt_history. nil result means
	// the issue had no committed state at lastSync (created since);
	// diffLocalFields treats that as "all populated fields new".
	localAtSync, err := loadLocalStateAtSync(ctx, e.Store, issue.ID, lastSync)
	if err != nil {
		if errors.Is(err, errHistoryNotSupported) {
			// Backend without history capability — signal fallback.
			return nil, err
		}
		return nil, fmt.Errorf("load local state at sync: %w", err)
	}

	localChanged := diffLocalFields(issue, localAtSync)
	externalChanged := diffExternalFields(extIssue, snap)
	conflicting := computeConflictingFields(localChanged, externalChanged)

	// No changes on either side → no work for the resolver.
	if len(localChanged) == 0 && len(externalChanged) == 0 {
		return nil, nil
	}

	return &Conflict{
		IssueID:            issue.ID,
		LocalUpdated:       issue.UpdatedAt,
		ExternalUpdated:    extIssue.UpdatedAt,
		ExternalRef:        extRef,
		ExternalIdentifier: extIssue.Identifier,
		ExternalInternalID: extIssue.ID,
		LocalChanged:       localChanged,
		ExternalChanged:    externalChanged,
		Conflicting:        conflicting,
	}, nil
}

// doPull imports issues from the external tracker into beads. IDs of issues
// it creates or updates are added to pulledIDs so a bidirectional sync's push
// phase does not echo the freshly pulled content straight back to the tracker.
//
// bd-ajn: pullFieldScopes carries per-issue field restrictions from the
// conflict resolver. When set for an issue, buildPullIssueUpdates
// returns ONLY those fields so the pull doesn't clobber locally-
// changed fields that aren't part of the resolver's pull set.
// Empty / missing entry → whole-issue update (legacy path).
func (e *Engine) doPull(ctx context.Context, opts SyncOptions, allowOverwriteIDs, pulledIDs map[string]bool, pullFieldScopes map[string]map[ConflictField]bool) (*PullStats, error) {
	ctx, span := syncTracer.Start(ctx, "tracker.pull",
		trace.WithAttributes(
			attribute.String("sync.tracker", e.Tracker.DisplayName()),
			attribute.Bool("sync.dry_run", opts.DryRun),
		),
	)
	defer span.End()

	stats := &PullStats{}

	// Determine if incremental sync is possible
	fetchOpts := FetchOptions{State: opts.State}
	var lastSync *time.Time
	key := e.Tracker.ConfigPrefix() + ".last_sync"
	if lastSyncStr, err := e.Store.GetLocalMetadata(ctx, key); err == nil && lastSyncStr != "" {
		if t, err := parseSyncTime(lastSyncStr); err == nil {
			fetchOpts.Since = &t
			lastSync = &t
			stats.Incremental = true
			stats.SyncedSince = lastSyncStr
		}
	}

	// bd-6cl: pull-side Project materialization. Run BEFORE the
	// per-Issue pull so that new local epics created here exist
	// when the post-pull descendant-projectId-wiring pass (P5)
	// walks the just-pulled Issues. Skipped silently for trackers
	// that don't implement ProjectPuller (GitHub, Jira, etc.).
	projectIDToLocalEpicID := map[string]string{}
	if pp, ok := e.Tracker.(ProjectPuller); ok {
		projectOpts := ProjectPullOptions{
			DryRun: opts.DryRun,
			Policy: opts.ConflictResolution,
			Actor:  e.Actor,
		}
		if lastSync != nil {
			projectOpts.LastSync = *lastSync
		}
		projectStats, err := pp.PullProjects(ctx, projectOpts)
		if err != nil {
			e.warn("Project pull failed: %v", err)
		} else if projectStats != nil {
			for _, line := range projectStats.PreviewLines {
				e.msg("%s", line)
			}
			for _, perr := range projectStats.Errors {
				e.warn("Project pull error: %v", perr)
			}
			for _, swarn := range projectStats.SnapshotWarnings {
				e.warn("Project pull (snapshot): %v", swarn)
			}
			stats.Created += projectStats.Created
			stats.Updated += projectStats.Updated
			// FirstSync (codex bd-6cl round-1 nit) folds into Skipped
			// for engine display: from the user's perspective, a
			// first-sync baseline produced no apply work — it's a
			// skip with extra observability captured in PreviewLines.
			stats.Skipped += projectStats.Skipped + projectStats.FirstSync
			projectIDToLocalEpicID = projectStats.ProjectIDToLocalEpicID
		}
	}

	localIssues, err := e.Store.SearchIssues(ctx, "", types.IssueFilter{})
	if err != nil {
		return nil, fmt.Errorf("searching local issues: %w", err)
	}
	localByExternalIdentifier := make(map[string]*types.Issue, len(localIssues))
	localByID := make(map[string]*types.Issue, len(localIssues))
	for _, localIssue := range localIssues {
		if localIssue == nil {
			continue
		}
		if localID := strings.TrimSpace(localIssue.ID); localID != "" {
			localByID[localID] = localIssue
		}
		if localIssue == nil || localIssue.ExternalRef == nil {
			continue
		}
		localRef := strings.TrimSpace(*localIssue.ExternalRef)
		if localRef == "" || !e.Tracker.IsExternalRef(localRef) {
			continue
		}
		identifier := e.Tracker.ExtractIdentifier(localRef)
		if identifier == "" {
			continue
		}
		localByExternalIdentifier[identifier] = localIssue
	}

	prelinkedHydrateIDs := make(map[string]bool)

	// Fetch issues from external tracker
	var extIssues []TrackerIssue
	if len(opts.IssueIDs) > 0 {
		// Selective pull: fetch only requested issues via FetchIssue()
		prefix, _ := e.Store.GetConfig(ctx, "issue_prefix")
		for _, id := range opts.IssueIDs {
			var identifier string
			if isBeadID(id, prefix) {
				// Look up the local issue to find its external ref
				if local, ok := localByID[id]; ok && local.ExternalRef != nil {
					identifier = e.Tracker.ExtractIdentifier(*local.ExternalRef)
				}
				if identifier == "" {
					e.warn("No external ref found for local issue %s, skipping pull", id)
					stats.Skipped++
					continue
				}
			} else {
				identifier = id
			}
			extIssue, err := e.Tracker.FetchIssue(ctx, identifier)
			if err != nil {
				e.warn("Failed to fetch %s: %v", identifier, err)
				stats.Errors++
				continue
			}
			if extIssue == nil {
				e.warn("Issue %s not found in %s", identifier, e.Tracker.DisplayName())
				stats.Skipped++
				continue
			}
			extIssues = append(extIssues, *extIssue)
		}
		stats.Candidates = len(extIssues)
	} else {
		// Bulk pull: fetch all issues matching filters
		extIssues, err = e.Tracker.FetchIssues(ctx, fetchOpts)
		if err != nil {
			return nil, fmt.Errorf("fetching issues: %w", err)
		}
		stats.Candidates = len(extIssues)
		if provider, ok := e.Tracker.(PullStatsProvider); ok {
			stats.Queried, stats.Candidates = provider.LastPullStats()
		}
		hydrated, hydratedLocalIDs, err := e.fetchPrelinkedIssues(ctx, extIssues, localIssues, lastSync)
		if err != nil {
			return nil, fmt.Errorf("hydrating pre-linked %s issues: %w", e.Tracker.DisplayName(), err)
		}
		extIssues = append(extIssues, hydrated...)
		stats.Candidates += len(hydrated)
		for id := range hydratedLocalIDs {
			prelinkedHydrateIDs[id] = true
		}
	}

	mapper := e.Tracker.FieldMapper()
	var pendingDeps []DependencyInfo
	var dryRunIssues []*types.Issue

	for _, extIssue := range extIssues {
		// ShouldImport hook: filter before conversion
		if e.PullHooks != nil && e.PullHooks.ShouldImport != nil {
			if !e.PullHooks.ShouldImport(&extIssue) {
				stats.Skipped++
				continue
			}
		}

		// Check if we already have this issue before dry-run so preview stats
		// distinguish creates from updates.
		ref := e.Tracker.BuildExternalRef(&extIssue)
		existing, _ := e.Store.GetIssueByExternalRef(ctx, ref)
		if existing == nil && ref != "" {
			identifier := e.Tracker.ExtractIdentifier(ref)
			if identifier != "" {
				existing = localByExternalIdentifier[identifier]
			}
		}
		conv := mapper.IssueToBeads(&extIssue)
		if conv == nil || conv.Issue == nil {
			stats.Skipped++
			continue
		}
		if existing == nil {
			if localID := strings.TrimSpace(conv.Issue.ID); localID != "" {
				existing = localByID[localID]
			}
		}

		// TransformIssue hook: description formatting, field normalization
		if e.PullHooks != nil && e.PullHooks.TransformIssue != nil {
			e.PullHooks.TransformIssue(conv.Issue)
		}

		// GenerateID hook: hash-based ID generation
		if e.PullHooks != nil && e.PullHooks.GenerateID != nil {
			if err := e.PullHooks.GenerateID(ctx, conv.Issue); err != nil {
				e.warn("Failed to generate ID for %s: %v", extIssue.Identifier, err)
				stats.Skipped++
				continue
			}
		}

		if existing != nil {
			// Conflict-aware pull: skip updating issues that were locally
			// modified since last sync. Conflict detection (Phase 2) will
			// handle these per the configured resolution strategy.
			// Without this guard, pull silently overwrites local changes
			// before conflict detection can compare timestamps.
			if lastSync != nil && existing.UpdatedAt.After(*lastSync) && !allowOverwriteIDs[existing.ID] && !prelinkedHydrateIDs[existing.ID] {
				stats.Skipped++
				continue
			}
		}

		if e.PullHooks != nil && e.PullHooks.AfterConvert != nil {
			if err := e.PullHooks.AfterConvert(ctx, &extIssue, conv, ref, existing, opts); err != nil {
				e.warn("Failed to prepare %s: %v", extIssue.Identifier, err)
				stats.Skipped++
				continue
			}
		}

		// Collect dependencies before the content-equal skip: an issue whose
		// fields are unchanged can still have gained dependencies remotely.
		pendingDeps = appendFilteredDependencies(pendingDeps, conv.Dependencies, opts.DependencyTypes, opts.DependencySources)
		if opts.DryRun {
			dryRunIssue := *conv.Issue
			if strings.TrimSpace(ref) != "" {
				dryRunIssue.ExternalRef = strPtr(ref)
			}
			dryRunIssues = append(dryRunIssues, &dryRunIssue)
		}

		pullEqual := false
		if existing != nil {
			if e.PullHooks != nil && e.PullHooks.ContentEqual != nil {
				pullEqual = e.PullHooks.ContentEqual(existing, conv.Issue) && referencesMatch(existing, ref)
			} else {
				pullEqual = pullIssueEqual(existing, conv.Issue, ref)
			}
		}
		if pullEqual {
			// Issue unchanged, but still sync comments/attachments
			// (they may have been added externally since last sync).
			//
			// bd-p4m round 1: dry-run guard MUST precede these hooks.
			// SyncComments / SyncAttachments write to the local store
			// (ImportCommentWithRef + CreateAttachment); firing them
			// during --dry-run violates the read-only contract. Same
			// class of bug as the snapshot baseline write this PR
			// fixed in detectFieldScopedConflict — caught by codex
			// round-1's wider audit.
			if e.shouldSyncSubresources(opts, &extIssue, lastSync, prelinkedHydrateIDs[existing.ID]) {
				if e.PullHooks != nil && e.PullHooks.SyncComments != nil && extIssue.ID != "" && !opts.DryRun {
					if err := e.PullHooks.SyncComments(ctx, existing.ID, extIssue.ID); err != nil {
						e.warn("Comment sync failed for %s: %v", existing.ID, err)
					}
				}
				if e.PullHooks != nil && e.PullHooks.SyncAttachments != nil && extIssue.ID != "" && !opts.DryRun {
					if err := e.PullHooks.SyncAttachments(ctx, existing.ID, extIssue.ID); err != nil {
						e.warn("Attachment sync failed for %s: %v", existing.ID, err)
					}
				}
			}
			stats.Skipped++
			continue
		}

		if opts.DryRun {
			if existing != nil {
				e.msg("[dry-run] Would update local issue: %s - %s", extIssue.Identifier, ui.SanitizeForTerminal(extIssue.Title))
				stats.Updated++
			} else {
				e.msg("[dry-run] Would import: %s - %s", extIssue.Identifier, ui.SanitizeForTerminal(extIssue.Title))
				stats.Created++
			}
			continue
		}

		if existing != nil {
			updates := buildPullIssueUpdates(existing, conv.Issue, ref)
			if scope := pullFieldScopes[existing.ID]; len(scope) > 0 {
				// bd-ajn field-scoped pull: keep only the resolver-
				// approved fields + ref (which is metadata, not a
				// user-editable field). external_ref keys stay because
				// pull is the path that backfills them.
				updates = restrictPullUpdatesToFields(updates, scope)
			}
			if raw, ok := marshalTrackerMetadata(extIssue.Metadata); ok {
				updates["metadata"] = raw
			}

			if err := e.Store.RunInTransaction(ctx, fmt.Sprintf("bd: pull update %s", existing.ID), func(tx storage.Transaction) error {
				if err := tx.UpdateIssue(ctx, existing.ID, updates, e.Actor); err != nil {
					return err
				}
				if e.PullHooks != nil && e.PullHooks.ReconcileLabels != nil {
					return e.PullHooks.ReconcileLabels(ctx, tx, existing.ID, conv.Issue.Labels, &extIssue, e.Actor)
				}
				return legacySyncIssueLabels(ctx, tx, existing.ID, conv.Issue.Labels, e.Actor)
			}); err != nil {
				e.warn("Failed to update %s: %v", existing.ID, err)
				continue
			}
			stats.Updated++
			if pulledIDs != nil {
				pulledIDs[existing.ID] = true
			}
		} else {
			// Create new issue
			conv.Issue.ExternalRef = strPtr(ref)
			if raw, ok := marshalTrackerMetadata(extIssue.Metadata); ok {
				conv.Issue.Metadata = raw
			}
			if err := e.Store.CreateIssue(ctx, conv.Issue, e.Actor); err != nil {
				e.warn("Failed to create issue for %s: %v", extIssue.Identifier, err)
				continue
			}
			stats.Created++
			if pulledIDs != nil {
				pulledIDs[conv.Issue.ID] = true
			}
		}

		// Sync comments/attachments after import (new or updated).
		localID := conv.Issue.ID
		if existing != nil {
			localID = existing.ID
		}
		// bd-ajn: record the per-issue snapshot for trackers that
		// support PostPullSnapshotter. Captures the just-pulled remote
		// state so the NEXT DetectConflicts run can correctly diff
		// "did Linear change this field?" without confusing pulled
		// state with newly-changed remote state. Best-effort: a
		// failure here surfaces as a warning but does not abort
		// import.
		if snapshotter, ok := e.Tracker.(PostPullSnapshotter); ok && localID != "" {
			if err := snapshotter.RecordPullSnapshot(ctx, localID, extIssue); err != nil {
				e.warn("Snapshot write failed for %s after pull: %v", localID, err)
			}
		}
		if e.shouldSyncSubresources(opts, &extIssue, lastSync, prelinkedHydrateIDs[localID]) {
			if e.PullHooks != nil && e.PullHooks.SyncComments != nil && extIssue.ID != "" {
				if err := e.PullHooks.SyncComments(ctx, localID, extIssue.ID); err != nil {
					e.warn("Comment sync failed for %s: %v", localID, err)
				}
			}
			if e.PullHooks != nil && e.PullHooks.SyncAttachments != nil && extIssue.ID != "" {
				if err := e.PullHooks.SyncAttachments(ctx, localID, extIssue.ID); err != nil {
					e.warn("Attachment sync failed for %s: %v", localID, err)
				}
			}
		}

		// bd-6cl: when the just-pulled Issue's remote payload
		// includes a projectId (Linear sets Metadata["project_id"]
		// for issues belonging to a Project), wire a parent-child
		// dep from this issue's bead to the local epic that
		// materializes that Project. Idempotent — skip if dep
		// already exists. Skipped on dry-run (no creates ran, the
		// epic might not exist locally yet). Skipped when the
		// projectId isn't mapped to a known local epic (out-of-band
		// Project, or our PullProjects didn't see it for any reason).
		if !opts.DryRun && localID != "" && len(projectIDToLocalEpicID) > 0 {
			if pid := pulledIssueProjectID(extIssue); pid != "" {
				if epicID, ok := projectIDToLocalEpicID[pid]; ok && epicID != "" && epicID != localID {
					if err := e.ensureParentChildDep(ctx, localID, epicID); err != nil {
						e.warn("Failed to wire Project parent-child dep for %s → %s: %v", localID, epicID, err)
					}
				}
			}
		}

	}

	// Create dependencies after all issues are imported
	depErrors := 0
	if opts.DryRun {
		depErrors = e.previewDependencies(ctx, pendingDeps, dryRunIssues)
	} else {
		depErrors = e.createDependencies(ctx, pendingDeps)
	}
	stats.Skipped += depErrors

	span.SetAttributes(
		attribute.Int("sync.created", stats.Created),
		attribute.Int("sync.updated", stats.Updated),
		attribute.Int("sync.skipped", stats.Skipped),
	)
	return stats, nil
}

// shouldSyncSubresources gates the per-issue comment/attachment pull hooks,
// each of which costs one API request per issue (bd-kqt). Sub-resources are
// synced when there is evidence they could have changed:
//
//   - no lastSync — first/full pull, backfill everything;
//   - explicitly requested issues (--issues) — the user asked for a refresh;
//   - the remote changed since lastSync — comments/attachments bump the
//     remote issue's updatedAt, so a stale updatedAt proves stale
//     sub-resources;
//   - the local bead was just (re-)linked to this remote issue — its
//     comments were never imported, regardless of remote staleness.
//
// Issues hydrated only because of local churn (e.g. every bead looking dirty
// after a sync write-back storm) fail all four conditions and skip the two
// per-issue requests.
func (e *Engine) shouldSyncSubresources(opts SyncOptions, extIssue *TrackerIssue, lastSync *time.Time, hydratedForRefChange bool) bool {
	if lastSync == nil || len(opts.IssueIDs) > 0 || hydratedForRefChange {
		return true
	}
	// >= rather than > : the incremental pull filter is updatedAt gte
	// last_sync, so an issue updated exactly at the boundary is returned by
	// the pull and its sub-resources must not be skipped (codex bd-kqt
	// round-1 MINOR).
	return !extIssue.UpdatedAt.Before(*lastSync)
}

// referencesMatch reports whether the local external_ref matches the provided
// ref string (after trimming). Used by the pull-equal fast-path to require
// both content equality AND ref alignment before skipping an update.
func referencesMatch(local *types.Issue, ref string) bool {
	if local == nil {
		return false
	}
	localRef := ""
	if local.ExternalRef != nil {
		localRef = strings.TrimSpace(*local.ExternalRef)
	}
	return localRef == strings.TrimSpace(ref)
}

func pullIssueEqual(local *types.Issue, remote *types.Issue, ref string) bool {
	if local == nil || remote == nil {
		return false
	}
	if local.Title != remote.Title ||
		local.Description != remote.Description ||
		local.Priority != remote.Priority ||
		local.Status != remote.Status ||
		local.IssueType != remote.IssueType ||
		strings.TrimSpace(local.Assignee) != strings.TrimSpace(remote.Assignee) ||
		!equalNormalizedStrings(local.Labels, remote.Labels) {
		return false
	}
	// For closed beads also compare close_reason — the tracker's field
	// mapper populates remote.CloseReason to reflect Linear's terminal
	// state intent (Canceled → marker, Done → empty). Treating them as
	// equal when close_reason drifts lets a bead remain marked
	// cancel-intent locally after Linear flipped Canceled→Done, which
	// then re-clobbers on the next push.
	if local.Status == types.StatusClosed && local.CloseReason != remote.CloseReason {
		return false
	}
	// Reopen case: Linear moved a closed issue back to open/in_progress.
	// If local still carries a close_reason from the prior closure, a
	// future re-close would re-fire that stale reason and route the push
	// to the wrong terminal state. Force an update so close_reason gets
	// cleared.
	if remote.Status != types.StatusClosed && strings.TrimSpace(local.CloseReason) != "" {
		return false
	}
	localRef := ""
	if local.ExternalRef != nil {
		localRef = strings.TrimSpace(*local.ExternalRef)
	}
	return localRef == strings.TrimSpace(ref)
}

func buildPullIssueUpdates(existing *types.Issue, remote *types.Issue, ref string) map[string]interface{} {
	updates := map[string]interface{}{
		"title":       remote.Title,
		"description": remote.Description,
		"priority":    remote.Priority,
		"status":      string(remote.Status),
		"issue_type":  string(remote.IssueType),
		"assignee":    remote.Assignee,
	}
	// Sync close_reason on any closed pull so Linear's terminal-state
	// intent (Done vs Canceled) survives round-trip. Without this, a
	// bead that was reaper-auto-closed locally and then manually moved
	// to Done in Linear would keep its "stale:" close_reason, and the
	// next push would route it back to Canceled via close_reason-based
	// state resolution. The tracker's field mapper is responsible for
	// setting remote.CloseReason: non-empty for Canceled/Duplicate-type
	// states, empty for Done. Overwriting free-form user close_reasons
	// on closed beads is intentional — the same clobber model is
	// already used for title/description/priority/status/assignee.
	if remote.Status == types.StatusClosed {
		updates["close_reason"] = remote.CloseReason
	} else {
		// Reopen: clear any stale close_reason so it can't mis-route a
		// future push back to Canceled via close_reason resolution.
		updates["close_reason"] = ""
	}
	trimmedRef := strings.TrimSpace(ref)
	if trimmedRef == "" {
		return updates
	}
	if existing.ExternalRef == nil || strings.TrimSpace(*existing.ExternalRef) != trimmedRef {
		updates["external_ref"] = trimmedRef
	}
	return updates
}

func marshalTrackerMetadata(metadata interface{}) (json.RawMessage, bool) {
	if metadata == nil {
		return nil, false
	}
	raw, err := json.Marshal(metadata)
	if err != nil {
		return nil, false
	}
	return json.RawMessage(raw), true
}

func appendFilteredDependencies(dst []DependencyInfo, deps []DependencyInfo, allowedTypes []types.DependencyType, allowedSources []DependencySource) []DependencyInfo {
	if len(deps) == 0 {
		return dst
	}
	if len(allowedTypes) == 0 && len(allowedSources) == 0 {
		return append(dst, deps...)
	}
	allowed := make(map[string]struct{}, len(allowedTypes))
	for _, depType := range allowedTypes {
		allowed[string(depType)] = struct{}{}
	}
	allowedSourceSet := make(map[DependencySource]struct{}, len(allowedSources))
	for _, source := range allowedSources {
		allowedSourceSet[source] = struct{}{}
	}
	for _, dep := range deps {
		if len(allowed) > 0 {
			if _, ok := allowed[dep.Type]; !ok {
				continue
			}
		}
		if len(allowedSourceSet) > 0 {
			if _, ok := allowedSourceSet[dep.Source]; !ok {
				continue
			}
		}
		dst = append(dst, dep)
	}
	return dst
}

// batchFetchRemoteIssues resolves identifiers to remote issues through the
// tracker's BatchIssueFetcher capability. Returns batched=false when the
// tracker doesn't support batching — callers then use their per-issue
// FetchIssue paths for everything.
//
// Error contract (codex bd-kqt round-1 MAJOR): a batch failure must not
// silently reclassify unresolved identifiers as "absent remotely". The
// partial map plus the error are returned so callers can (a) abort the
// phase on rate-limit exhaustion instead of issuing further doomed
// requests, and (b) restore HEAD's per-issue fetch semantics for the
// unresolved remainder on transient failures.
func (e *Engine) batchFetchRemoteIssues(ctx context.Context, identifiers []string) (map[string]*TrackerIssue, bool, error) {
	fetcher, ok := e.Tracker.(BatchIssueFetcher)
	if !ok {
		return nil, false, nil
	}
	if len(identifiers) == 0 {
		return map[string]*TrackerIssue{}, true, nil
	}
	remote, err := fetcher.BatchFetchIssues(ctx, identifiers)
	if err != nil {
		e.warn("Batch fetch of %d remote issues failed (resolved %d before the error): %v", len(identifiers), len(remote), err)
	}
	if remote == nil {
		remote = map[string]*TrackerIssue{}
	}
	return remote, true, err
}

func (e *Engine) fetchPrelinkedIssues(ctx context.Context, fetched []TrackerIssue, localIssues []*types.Issue, lastSync *time.Time) ([]TrackerIssue, map[string]bool, error) {
	hydratedLocalIDs := make(map[string]bool)
	if lastSync == nil {
		return nil, hydratedLocalIDs, nil
	}

	seen := make(map[string]struct{}, len(fetched))
	for _, issue := range fetched {
		for _, id := range []string{
			strings.TrimSpace(issue.Identifier),
			strings.TrimSpace(e.Tracker.ExtractIdentifier(e.Tracker.BuildExternalRef(&issue))),
		} {
			if id != "" {
				seen[id] = struct{}{}
				seen[strings.ToLower(id)] = struct{}{}
			}
		}
	}

	// First pass: decide which local issues need hydration without touching
	// the API, so the fetches can be batched (bd-kqt).
	type hydrateCandidate struct {
		local      *types.Issue
		identifier string
	}
	var candidates []hydrateCandidate
	for _, local := range localIssues {
		if local == nil || local.ExternalRef == nil {
			continue
		}
		ref := strings.TrimSpace(*local.ExternalRef)
		if ref == "" || !e.Tracker.IsExternalRef(ref) {
			continue
		}
		changedAfterLastSync, err := e.externalRefChangedAfter(ctx, local, ref, *lastSync)
		if err != nil {
			return nil, hydratedLocalIDs, fmt.Errorf("checking pre-linked local issue %s: %w", local.ID, err)
		}
		if !changedAfterLastSync {
			continue
		}
		identifier := strings.TrimSpace(e.Tracker.ExtractIdentifier(ref))
		if identifier == "" {
			continue
		}
		if _, ok := seen[identifier]; ok {
			continue
		}
		if _, ok := seen[strings.ToLower(identifier)]; ok {
			continue
		}
		seen[identifier] = struct{}{}
		seen[strings.ToLower(identifier)] = struct{}{}
		candidates = append(candidates, hydrateCandidate{local: local, identifier: identifier})
	}

	identifiers := make([]string, len(candidates))
	for i, cand := range candidates {
		identifiers[i] = cand.identifier
	}
	remoteByID, batched, batchErr := e.batchFetchRemoteIssues(ctx, identifiers)
	if batchErr != nil && isRateLimitExhausted(batchErr) {
		// Match the per-issue path's contract: a hydration fetch failure
		// fails the pull. Aborting here also keeps the sync from issuing
		// further requests against a tripped circuit breaker.
		return nil, hydratedLocalIDs, fmt.Errorf("hydrating pre-linked issues: %w", batchErr)
	}
	// Transient batch failure: fall back to per-issue fetches for the
	// unresolved remainder — HEAD semantics, where a per-issue error fails
	// the pull rather than silently skipping hydration.
	perIssueFallback := batchErr != nil

	var hydrated []TrackerIssue
	for _, cand := range candidates {
		var extIssue *TrackerIssue
		if batched {
			extIssue = remoteByID[cand.identifier]
			if extIssue == nil && perIssueFallback {
				var err error
				extIssue, err = e.Tracker.FetchIssue(ctx, cand.identifier)
				if err != nil {
					return hydrated, hydratedLocalIDs, err
				}
			}
		} else {
			var err error
			extIssue, err = e.Tracker.FetchIssue(ctx, cand.identifier)
			if err != nil {
				return hydrated, hydratedLocalIDs, err
			}
		}
		if extIssue == nil {
			continue
		}
		hydrated = append(hydrated, *extIssue)
		hydratedLocalIDs[cand.local.ID] = true
	}
	return hydrated, hydratedLocalIDs, nil
}

type dbProvider interface {
	DB() *sql.DB
}

func (e *Engine) externalRefChangedAfter(ctx context.Context, local *types.Issue, currentRef string, lastSync time.Time) (bool, error) {
	if local == nil {
		return false, nil
	}
	// Cheap pre-filter: any external_ref change bumps updated_at, so an
	// issue untouched since lastSync cannot have a changed ref. This keeps
	// the dolt_history query below off the per-issue hot path — without it,
	// every linked issue pays a history-table scan on every sync (bd-kqt).
	//
	// Store timestamps are DATETIME with second granularity and round to
	// the nearest second, so a ref linked moments after lastSync (which
	// keeps sub-second precision) can be stored up to half a second BEFORE
	// it. Compare against a one-second guard band and let the authoritative
	// history check below decide the near-lastSync cases — the raw
	// comparison misread those updates as "untouched" and silently skipped
	// hydration (bd-21h).
	cutoff := lastSync.Add(-time.Second)
	if !local.CreatedAt.After(cutoff) && !local.UpdatedAt.After(cutoff) {
		return false, nil
	}
	provider, ok := e.Store.(dbProvider)
	if !ok || provider.DB() == nil {
		// No history support: the guard-banded pre-filter above already
		// found a possible change, and possible must mean "hydrate" here —
		// a spurious fetch is batched and cheap, a missed one loses data.
		return true, nil
	}

	var previousRef sql.NullString
	err := provider.DB().QueryRowContext(ctx, `
		SELECT external_ref
		FROM (
			SELECT id, external_ref, commit_date FROM dolt_history_issues
		) h
		WHERE h.id = ? AND h.commit_date <= ?
		ORDER BY h.commit_date DESC
		LIMIT 1
	`, local.ID, lastSync.UTC()).Scan(&previousRef)
	if err == sql.ErrNoRows {
		return true, nil
	}
	if err != nil {
		return false, err
	}
	return !previousRef.Valid || strings.TrimSpace(previousRef.String) != strings.TrimSpace(currentRef), nil
}

func legacySyncIssueLabels(ctx context.Context, tx storage.Transaction, issueID string, desired []string, actor string) error {
	current, err := tx.GetLabels(ctx, issueID)
	if err != nil {
		return err
	}
	currentSet := normalizedStringSet(current)
	desiredSet := normalizedStringSet(desired)
	for label := range currentSet {
		if _, ok := desiredSet[label]; ok {
			continue
		}
		if err := tx.RemoveLabel(ctx, issueID, label, actor); err != nil {
			return err
		}
	}
	for label := range desiredSet {
		if _, ok := currentSet[label]; ok {
			continue
		}
		if err := tx.AddLabel(ctx, issueID, label, actor); err != nil {
			return err
		}
	}
	return nil
}

func equalNormalizedStrings(a, b []string) bool {
	an := normalizedStringSlice(a)
	bn := normalizedStringSlice(b)
	if len(an) != len(bn) {
		return false
	}
	for i := range an {
		if an[i] != bn[i] {
			return false
		}
	}
	return true
}

func normalizedStringSet(values []string) map[string]struct{} {
	result := make(map[string]struct{}, len(values))
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value == "" {
			continue
		}
		result[value] = struct{}{}
	}
	return result
}

func normalizedStringSlice(values []string) []string {
	set := normalizedStringSet(values)
	result := make([]string, 0, len(set))
	for value := range set {
		result = append(result, value)
	}
	sort.Strings(result)
	return result
}

func parseSyncTime(value string) (time.Time, error) {
	if value == "" {
		return time.Time{}, fmt.Errorf("empty sync timestamp")
	}
	if parsed, err := time.Parse(time.RFC3339Nano, value); err == nil {
		return parsed, nil
	}
	return time.Parse(time.RFC3339, value)
}

// LastSync returns the recorded last-sync timestamp for a tracker config
// prefix (e.g. "linear", "jira"), or "" if the tracker has never synced.
// The sync engine records the timestamp in local metadata; older bd versions
// wrote it to config, so fall back there for databases synced before the
// refactor.
func LastSync(ctx context.Context, store storage.Storage, configPrefix string) string {
	key := configPrefix + ".last_sync"
	if value, err := store.GetLocalMetadata(ctx, key); err == nil && value != "" {
		return value
	}
	value, _ := store.GetConfig(ctx, key)
	return value
}

// LastSyncTime returns LastSync parsed as a time.Time, or the zero time
// when the tracker has never synced or the recorded value is unparseable.
func LastSyncTime(ctx context.Context, store storage.Storage, configPrefix string) time.Time {
	raw := LastSync(ctx, store, configPrefix)
	if raw == "" {
		return time.Time{}
	}
	parsed, err := parseSyncTime(raw)
	if err != nil {
		return time.Time{}
	}
	return parsed
}

// doPush exports beads issues to the external tracker.
//
// bd-ajn: pushFieldScopes carries per-issue field restrictions
// produced by resolveConflictsWithFieldScopes. When pushFieldScopes
// has an entry for an issue and the tracker implements
// FieldScopedUpdater, only those fields are sent in the update —
// preventing whole-issue overwrites that would clobber concurrent
// remote changes to fields the local side didn't touch. Trackers
// without FieldScopedUpdater fall back to full-issue UpdateIssue.
func (e *Engine) doPush(ctx context.Context, opts SyncOptions, skipIDs, forceIDs map[string]bool, pushFieldScopes map[string]map[ConflictField]bool) (*PushStats, error) {
	ctx, span := syncTracer.Start(ctx, "tracker.push",
		trace.WithAttributes(
			attribute.String("sync.tracker", e.Tracker.DisplayName()),
			attribute.Bool("sync.dry_run", opts.DryRun),
		),
	)
	defer span.End()

	stats := &PushStats{}

	// BuildStateCache hook: pre-cache workflow states once before the loop.
	// Stored on Engine so tracker adapters can call ResolveState() during push.
	e.stateCache = nil
	if e.PushHooks != nil && e.PushHooks.BuildStateCache != nil {
		var err error
		e.stateCache, err = e.PushHooks.BuildStateCache(ctx)
		if err != nil {
			return nil, fmt.Errorf("building state cache: %w", err)
		}
	}

	// bd-1ay: Epic-sync pass. For trackers implementing ProjectSyncer
	// (Linear), ensure each top-level epic has a Linear Project. doPush's
	// per-issue loop below uses epicProjectMap to skip those epics so
	// they don't double-create as Issues.
	//
	// Scoped to top-level epics with empty or Project-URL external_ref;
	// Issue-URL external_refs are left to the bd-go9 migration tool.
	// Skipped silently for trackers that don't implement ProjectSyncer.
	epicProjectMap, err := e.doEpicSync(ctx, opts)
	if err != nil {
		return nil, fmt.Errorf("epic-sync: %w", err)
	}

	// Fetch local issues
	filter := types.IssueFilter{}
	issues, err := e.Store.SearchIssues(ctx, "", filter)
	if err != nil {
		return nil, fmt.Errorf("searching local issues: %w", err)
	}

	// Filter to specific IssueIDs if requested.
	if issueIDSet := buildIssueIDSet(opts.IssueIDs); issueIDSet != nil {
		filtered := make([]*types.Issue, 0, len(opts.IssueIDs))
		for _, issue := range issues {
			if issueIDSet[issue.ID] {
				filtered = append(filtered, issue)
			}
		}
		issues = filtered
	}

	// Build descendant set if --parent was specified.
	var descendantSet map[string]bool
	if opts.ParentID != "" {
		descendantSet, err = e.buildDescendantSet(ctx, opts.ParentID)
		if err != nil {
			return nil, fmt.Errorf("resolving parent %s: %w", opts.ParentID, err)
		}
	}

	// bd-1ay TODO (forward-risk note from codex round 2): the
	// BatchPushTracker fast path below bypasses the per-issue loop
	// where epicProjectMap is consulted. Linear (the only ProjectSyncer
	// today) does NOT implement BatchPushTracker, so this is safe right
	// now. If a future ProjectSyncer adapter ALSO implements
	// BatchPushTracker, the batch path would need an epic-skip layer
	// at the collectBatchPushIssues stage (parallel to the existing
	// per-issue filter), or epic Projects would double-create as Issues
	// in the batch.
	if batchTracker, ok := e.Tracker.(BatchPushTracker); ok {
		candidates, skipped := e.collectBatchPushIssues(issues, opts, descendantSet, skipIDs, forceIDs)
		stats.Skipped += skipped
		if len(candidates) == 0 {
			return stats, nil
		}
		pushIssues := make([]*types.Issue, len(candidates))
		for i, c := range candidates {
			pushIssues[i] = c.issue
		}
		if opts.DryRun {
			if dryRunner, ok := e.Tracker.(BatchPushDryRunner); ok {
				batchResult, err := dryRunner.BatchPushDryRun(ctx, pushIssues, forceIDs)
				if err != nil {
					return nil, fmt.Errorf("previewing batch push: %w", err)
				}
				e.renderBatchDryRun(pushIssues, batchResult)
				stats.Created += len(batchResult.Created)
				stats.Updated += len(batchResult.Updated)
				stats.Skipped += len(batchResult.Skipped)
				stats.Errors += len(batchResult.Errors)
				stats.Warnings = append(stats.Warnings, batchResult.Warnings...)
				for _, item := range batchResult.Errors {
					if item.LocalID != "" {
						e.warn("Failed to preview push %s in %s: %s", item.LocalID, e.Tracker.DisplayName(), item.Message)
						continue
					}
					e.warn("Failed to preview pushes in %s: %s", e.Tracker.DisplayName(), item.Message)
				}
				return stats, nil
			}
			// The tracker cannot preview a batch push itself, so preview from
			// the exact candidate set BatchPush would receive. Falling through
			// to the per-issue loop here would let the preview drift from the
			// real run whenever the two paths' filters differ (bd-f0t).
			for _, c := range candidates {
				if c.action == pushActionCreate {
					e.msg("[dry-run] Would create in %s: %s", e.Tracker.DisplayName(), ui.SanitizeForTerminal(c.issue.Title))
					stats.Created++
				} else {
					e.msg("[dry-run] Would update in %s: %s", e.Tracker.DisplayName(), ui.SanitizeForTerminal(c.issue.Title))
					stats.Updated++
				}
			}
			return stats, nil
		}
		batchResult, err := batchTracker.BatchPush(ctx, pushIssues, forceIDs)
		if err != nil {
			return nil, fmt.Errorf("batch pushing issues: %w", err)
		}
		e.applyBatchPushResult(ctx, batchResult)
		stats.Created += len(batchResult.Created)
		stats.Updated += len(batchResult.Updated)
		stats.Skipped += len(batchResult.Skipped)
		stats.Errors += len(batchResult.Errors)
		stats.Warnings = append(stats.Warnings, batchResult.Warnings...)
		for _, item := range batchResult.Errors {
			if item.LocalID != "" {
				e.warn("Failed to push %s in %s: %s", item.LocalID, e.Tracker.DisplayName(), item.Message)
				continue
			}
			e.warn("Failed to push issues in %s: %s", e.Tracker.DisplayName(), item.Message)
		}
		return stats, nil
	}

	for _, issue := range issues {
		action := e.classifyPushIssue(issue, opts, descendantSet, skipIDs, forceIDs)
		if action == pushActionSkip {
			stats.Skipped++
			continue
		}
		// bd-1ay: top-level epics handled as Projects by doEpicSync are
		// skipped here so they don't double-create as Issues. The
		// post-sync ReconcileProjectMembership pass (Linear-specific,
		// fired from cmd/bd/linear.go) walks descendants and assigns
		// them to the right Project via projectId.
		if _, isEpicProject := epicProjectMap[issue.ID]; isEpicProject {
			stats.Skipped++
			continue
		}

		willCreate := action == pushActionCreate

		// Skip tombstone creates: a bead that's already closed locally with no
		// external ref was done without ever flowing through the external
		// tracker. Creating a new terminal-state ticket for it just adds noise
		// in Linear/Jira/GitHub. Opt back in with --create-closed for one-off
		// historical backfills.
		if willCreate && issue.Status == types.StatusClosed && !opts.CreateClosed {
			stats.Skipped++
			continue
		}

		// FormatDescription hook: apply to a copy so we don't mutate local data.
		pushIssue := e.formatPushIssue(issue)

		if willCreate {
			if opts.DryRun {
				e.msg("[dry-run] Would create in %s: %s", e.Tracker.DisplayName(), ui.SanitizeForTerminal(issue.Title))
				stats.Created++
				continue
			}
			// Create in external tracker
			created, err := e.Tracker.CreateIssue(ctx, pushIssue)
			if err != nil {
				if isRateLimitExhausted(err) {
					return stats, fmt.Errorf("sync aborted: %w", err)
				}
				e.warn("Failed to create %s in %s: %v", issue.ID, e.Tracker.DisplayName(), err)
				stats.Errors++
				if isRateLimitedErr(err) {
					e.warnRateLimitAbort(err, len(issues)-stats.Created-stats.Updated-stats.Skipped-stats.Errors)
					return stats, nil
				}
				continue
			}

			// Update local issue with external ref
			ref := e.Tracker.BuildExternalRef(created)
			updates := map[string]interface{}{"external_ref": ref}
			if err := e.Store.UpdateIssue(ctx, issue.ID, updates, e.Actor); err != nil {
				e.warn("Failed to update external_ref for %s: %v", issue.ID, err)
				stats.Errors++
				// Note: issue WAS created externally, so we still count Created
				// but also flag the error so the user knows the link is broken
			}
			// Surface any partial-success warnings from the create (e.g. a
			// follow-up state change that failed) through the sync result so a
			// degraded push is visible rather than silently swallowed.
			for _, w := range created.Warnings {
				e.warn("%s (%s)", w, issue.ID)
			}
			stats.Created++

			// Sync comments after create (push direction).
			if e.PushHooks != nil && e.PushHooks.SyncComments != nil && created != nil && created.ID != "" {
				if err := e.PushHooks.SyncComments(ctx, issue.ID, created.ID); err != nil {
					e.warn("Comment push failed for %s: %v", issue.ID, err)
				}
			}
		} else {
			// Update existing external issue
			extID := e.Tracker.ExtractIdentifier(derefStr(issue.ExternalRef))
			if extID == "" {
				stats.Skipped++
				continue
			}

			// Check if update is needed
			var fetchedExt *TrackerIssue
			// Force-pushed issues normally skip the remote fetch (the user has
			// already decided to overwrite). But when the tracker implements
			// RemoteAwareUpdater, we still want the remote payload so the
			// implementation can preserve remote-owned state (e.g., Linear's
			// "In Review" driven by GitHub PR automation). One extra API call
			// per forced issue, bounded by conflict count — not amplified for
			// bulk syncs.
			_, trackerIsRemoteAware := e.Tracker.(RemoteAwareUpdater)
			fetchForForce := forceIDs[issue.ID] && trackerIsRemoteAware
			if !forceIDs[issue.ID] || fetchForForce {
				extIssue, err := e.Tracker.FetchIssue(ctx, extID)
				if isRateLimitExhausted(err) {
					return stats, fmt.Errorf("sync aborted: %w", err)
				}
				if err == nil && extIssue != nil {
					fetchedExt = extIssue
					// ContentEqual hook: content-hash dedup to skip unnecessary API calls.
					if !forceIDs[issue.ID] {
						if e.PushHooks != nil && e.PushHooks.ContentEqual != nil {
							if e.PushHooks.ContentEqual(issue, extIssue) {
								stats.Skipped++
								continue
							}
						} else if !extIssue.UpdatedAt.Before(issue.UpdatedAt) {
							stats.Skipped++ // Default: external is same or newer
							continue
						}
					} else if e.PushHooks != nil && e.PushHooks.ContentEqual != nil {
						// Forced path: the conflict resolver flagged this bead as
						// "local newer" based on timestamps, but timestamps drift
						// (a status bulk-update bumps UpdatedAt without changing
						// any field Linear would store). When we have the fetched
						// remote AND ContentEqual reports they're truly equal,
						// skip the no-op push so the wet-run doesn't spam the
						// tracker with unchanged-payload mutations and the dry-
						// run doesn't print "Would push field change" with no
						// surfacing diff. Only runs when fetchForForce is true
						// (i.e., RemoteAwareUpdater trackers); other forced paths
						// preserve the existing intentional-overwrite semantics.
						if e.PushHooks.ContentEqual(issue, extIssue) {
							stats.Skipped++
							continue
						}
					}
				}
			}

			if opts.DryRun {
				// Categorize what wet-run would do — distinguishes "state
				// preserved" (Linear's "In Review" survives a metadata edit
				// because the bead's status maps to the remote's current state)
				// from "state change" (real status transition). Falls back to
				// generic "Would update" for trackers that don't implement
				// DryRunPreviewer or when no remote was fetched.
				label := "Would update"
				if dp, ok := e.Tracker.(DryRunPreviewer); ok && fetchedExt != nil {
					switch dp.PreviewUpdate(ctx, extID, pushIssue, fetchedExt) {
					case DryRunStatePreserved:
						label = "Would push field change (state preserved)"
					case DryRunStateChange:
						label = "Would push state change"
					case DryRunNoDiff:
						// Should usually be filtered by ContentEqual upstream,
						// but if we reach here, the tracker's own preview says
						// nothing would happen.
						label = "Would skip (no diff)"
					}
				}
				e.msg("[dry-run] %s in %s: %s — %s", label, e.Tracker.DisplayName(), issue.ID, ui.SanitizeForTerminal(issue.Title))
				if opts.VerboseDiff && e.PushHooks != nil && e.PushHooks.DescribeDiff != nil && fetchedExt != nil {
					for _, d := range e.PushHooks.DescribeDiff(issue, fetchedExt) {
						e.msg("    · %s", d)
					}
				}
				stats.Updated++
				continue
			}

			// Prefer FieldScopedUpdater + RemoteAwareUpdater when both
			// the scope is set and the tracker supports field scoping
			// (bd-ajn). Otherwise fall through to the legacy ordering:
			// RemoteAwareUpdater preferred over plain UpdateIssue.
			var updateErr error
			scope := pushFieldScopes[issue.ID]
			fsu, fsuOK := e.Tracker.(FieldScopedUpdater)
			switch {
			case len(scope) > 0 && fsuOK:
				// Field-scoped path: only the conflict-resolver-approved
				// fields go to Linear. Other fields (which may have been
				// touched on Linear's side without local change) stay
				// untouched.
				_, updateErr = fsu.UpdateIssueFields(ctx, extID, pushIssue, fetchedExt, conflictFieldKeys(scope))
			default:
				if rau, ok := e.Tracker.(RemoteAwareUpdater); ok {
					_, updateErr = rau.UpdateIssueWithRemote(ctx, extID, pushIssue, fetchedExt)
				} else {
					_, updateErr = e.Tracker.UpdateIssue(ctx, extID, pushIssue)
				}
			}
			if updateErr != nil {
				if isRateLimitExhausted(updateErr) {
					return stats, fmt.Errorf("sync aborted: %w", updateErr)
				}
				e.warn("Failed to update %s in %s: %v", issue.ID, e.Tracker.DisplayName(), updateErr)
				stats.Errors++
				if isRateLimitedErr(updateErr) {
					e.warnRateLimitAbort(updateErr, len(issues)-stats.Created-stats.Updated-stats.Skipped-stats.Errors)
					return stats, nil
				}
				continue
			}
			stats.Updated++

			// Sync comments after update (push direction).
			if e.PushHooks != nil && e.PushHooks.SyncComments != nil {
				if err := e.PushHooks.SyncComments(ctx, issue.ID, extID); err != nil {
					e.warn("Comment push failed for %s: %v", issue.ID, err)
				}
			}
		}
	}

	span.SetAttributes(
		attribute.Int("sync.created", stats.Created),
		attribute.Int("sync.updated", stats.Updated),
		attribute.Int("sync.skipped", stats.Skipped),
		attribute.Int("sync.errors", stats.Errors),
	)
	return stats, nil
}

// pushAction classifies what the push path will do with an issue.
type pushAction int

const (
	pushActionSkip pushAction = iota
	pushActionCreate
	pushActionUpdate
)

// pushCandidate pairs an issue selected for push with the action the push
// path will take on it.
type pushCandidate struct {
	issue  *types.Issue
	action pushAction
}

// classifyPushIssue applies the push candidate-selection filters. Both the
// per-issue loop and the batch path must route through this single helper so
// dry-run previews and real pushes always operate on the same candidate set
// (bd-f0t: duplicated filters drifted, making dry-run under-report creates).
func (e *Engine) classifyPushIssue(issue *types.Issue, opts SyncOptions, descendantSet, skipIDs, forceIDs map[string]bool) pushAction {
	// Limit to parent and its descendants if requested.
	if descendantSet != nil && !descendantSet[issue.ID] {
		return pushActionSkip
	}
	// Skip filtered types/states/ephemeral
	if !e.shouldPushIssue(issue, opts) {
		return pushActionSkip
	}
	// ShouldPush hook: custom filtering (prefix filtering, etc.)
	if e.PushHooks != nil && e.PushHooks.ShouldPush != nil && !e.PushHooks.ShouldPush(issue) {
		return pushActionSkip
	}
	// Skip conflict-excluded issues
	if skipIDs[issue.ID] {
		return pushActionSkip
	}

	extRef := derefStr(issue.ExternalRef)
	if extRef == "" || !e.Tracker.IsExternalRef(extRef) {
		return pushActionCreate
	}
	if opts.CreateOnly && !forceIDs[issue.ID] {
		return pushActionSkip
	}
	return pushActionUpdate
}

func (e *Engine) collectBatchPushIssues(issues []*types.Issue, opts SyncOptions, descendantSet, skipIDs, forceIDs map[string]bool) ([]pushCandidate, int) {
	candidates := make([]pushCandidate, 0, len(issues))
	skipped := 0
	for _, issue := range issues {
		action := e.classifyPushIssue(issue, opts, descendantSet, skipIDs, forceIDs)
		if action == pushActionSkip {
			skipped++
			continue
		}
		candidates = append(candidates, pushCandidate{issue: e.formatPushIssue(issue), action: action})
	}
	return candidates, skipped
}

func (e *Engine) formatPushIssue(issue *types.Issue) *types.Issue {
	if e.PushHooks == nil || e.PushHooks.FormatDescription == nil {
		return issue
	}
	copy := *issue
	copy.Description = e.PushHooks.FormatDescription(issue)
	return &copy
}

func (e *Engine) applyBatchPushResult(ctx context.Context, result *BatchPushResult) {
	if result == nil {
		return
	}
	items := append(append([]BatchPushItem(nil), result.Created...), result.Updated...)
	for _, item := range items {
		if item.LocalID == "" || strings.TrimSpace(item.ExternalRef) == "" {
			continue
		}
		updates := map[string]interface{}{"external_ref": strings.TrimSpace(item.ExternalRef)}
		if err := e.Store.UpdateIssue(ctx, item.LocalID, updates, e.Actor); err != nil {
			e.warn("Failed to update external_ref for %s: %v", item.LocalID, err)
		}
	}
}

func (e *Engine) renderBatchDryRun(issues []*types.Issue, result *BatchPushResult) {
	if result == nil {
		return
	}
	titles := make(map[string]string, len(issues))
	for _, issue := range issues {
		if issue == nil || issue.ID == "" {
			continue
		}
		titles[issue.ID] = issue.Title
	}
	for _, item := range result.Created {
		e.msg("[dry-run] Would create in %s: %s", e.Tracker.DisplayName(), titles[item.LocalID])
	}
	for _, item := range result.Updated {
		e.msg("[dry-run] Would update in %s: %s", e.Tracker.DisplayName(), titles[item.LocalID])
	}
}

// resolveConflicts applies the configured conflict resolution strategy.
//
// bd-ajn: each conflict is now per-field-aware. When the conflict
// carries field-scoped diff data (HasFieldScopedDiff), this routine
// emits per-field decisions:
//
//   - Field in LocalChanged only → push that field (no skip, no force
//     at the whole-issue level — the push path field-scopes itself
//     via pushFieldScopes)
//   - Field in ExternalChanged only → pull that field (pullFieldScopes
//     tells the pull path which fields to apply)
//   - Field in Conflicting → policy decides (ConflictLocal /
//     ConflictExternal / ConflictTimestamp at whole-issue scope, since
//     Linear doesn't expose per-field updatedAt — mayor's Q7
//     acknowledgment).
//
// When the conflict lacks field-scoped data (legacy path —
// HasFieldScopedDiff returns false), falls through to the old
// whole-issue logic for backward compatibility.
//
// The field-scope maps are nil-safe — callers (doPush, doPull) check
// presence before consuming.
func (e *Engine) resolveConflicts(opts SyncOptions, conflicts []Conflict, skipIDs, forceIDs, allowPullOverwriteIDs map[string]bool) {
	e.resolveConflictsWithFieldScopes(opts, conflicts, skipIDs, forceIDs, allowPullOverwriteIDs, nil, nil)
}

// resolveConflictsWithFieldScopes is the field-aware extension.
// pushFieldScopes and pullFieldScopes, when non-nil, are populated
// with per-field action maps. Tests and the field-scoped doPush /
// doPull paths consume these; the legacy wrapper (resolveConflicts)
// passes nil and ignores them.
func (e *Engine) resolveConflictsWithFieldScopes(
	opts SyncOptions,
	conflicts []Conflict,
	skipIDs, forceIDs, allowPullOverwriteIDs map[string]bool,
	pushFieldScopes, pullFieldScopes map[string]map[ConflictField]bool,
) {
	for _, c := range conflicts {
		if c.HasFieldScopedDiff() {
			e.resolveFieldScopedConflict(opts, c, skipIDs, forceIDs, allowPullOverwriteIDs, pushFieldScopes, pullFieldScopes)
			continue
		}

		// Legacy whole-issue path (no snapshot infra, or first-sync
		// fallback).
		switch opts.ConflictResolution {
		case ConflictLocal:
			forceIDs[c.IssueID] = true
			e.msg("Conflict on %s: keeping local version", c.IssueID)

		case ConflictExternal:
			skipIDs[c.IssueID] = true
			allowPullOverwriteIDs[c.IssueID] = true
			e.msg("Conflict on %s: keeping external version", c.IssueID)

		default: // ConflictTimestamp or unset
			if c.LocalUpdated.After(c.ExternalUpdated) {
				forceIDs[c.IssueID] = true
				e.msg("Conflict on %s: local is newer, pushing", c.IssueID)
			} else {
				skipIDs[c.IssueID] = true
				allowPullOverwriteIDs[c.IssueID] = true
				e.msg("Conflict on %s: external is newer, importing", c.IssueID)
			}
		}
	}
}

// resolveFieldScopedConflict dispatches per-field decisions for a
// single conflict that carries snapshot-backed diff data.
func (e *Engine) resolveFieldScopedConflict(
	opts SyncOptions,
	c Conflict,
	skipIDs, forceIDs, allowPullOverwriteIDs map[string]bool,
	pushFieldScopes, pullFieldScopes map[string]map[ConflictField]bool,
) {
	// Step 1: auto-merge non-conflicting fields.
	//   - LocalChanged \ Conflicting → push these fields
	//   - ExternalChanged \ Conflicting → pull these fields
	conflictSet := make(map[ConflictField]bool, len(c.Conflicting))
	for _, f := range c.Conflicting {
		conflictSet[f] = true
	}
	pushFields := make(map[ConflictField]bool)
	pullFields := make(map[ConflictField]bool)
	for f := range c.LocalChanged {
		if !conflictSet[f] {
			pushFields[f] = true
		}
	}
	for f := range c.ExternalChanged {
		if !conflictSet[f] {
			pullFields[f] = true
		}
	}

	// Step 2: dispatch the truly-conflicting fields per policy.
	for _, f := range c.Conflicting {
		var winner string // for logging only
		switch opts.ConflictResolution {
		case ConflictLocal:
			pushFields[f] = true
			winner = "local"
		case ConflictExternal:
			pullFields[f] = true
			winner = "external"
		default: // timestamp — falls back to whole-issue updatedAt per Q7
			if c.LocalUpdated.After(c.ExternalUpdated) {
				pushFields[f] = true
				winner = "local (newer)"
			} else {
				pullFields[f] = true
				winner = "external (newer)"
			}
		}
		e.msg("Conflict on %s.%s: keeping %s version", c.IssueID, f, winner)
	}

	// Step 3: publish to the caller's scope maps (when provided).
	if pushFieldScopes != nil && len(pushFields) > 0 {
		pushFieldScopes[c.IssueID] = pushFields
	}
	if pullFieldScopes != nil && len(pullFields) > 0 {
		pullFieldScopes[c.IssueID] = pullFields
	}

	// Step 4: gate the whole-issue push/pull paths so the resolution
	// decisions take effect at the engine level.
	//
	// Rules:
	//   - Any pushFields present → force the push path to run for
	//     this issue (so locally-changed fields propagate).
	//   - Any pullFields present → allow the pull path to overwrite
	//     local (so externally-changed fields land).
	//   - Mixed (both push and pull): set forceIDs +
	//     allowPullOverwriteIDs but NOT skipIDs. The skipIDs map
	//     short-circuits the push path entirely before
	//     pushFieldScopes can dispatch field-scoped — using it for
	//     the pull side of a mixed conflict would silently skip the
	//     push side (codex bd-ajn round-1 bug).
	//   - Pull-only (no pushFields): set skipIDs so the legacy push
	//     path doesn't fire for this issue at all. The pull side
	//     handles the work via allowPullOverwriteIDs +
	//     pullFieldScopes.
	if len(pushFields) > 0 {
		forceIDs[c.IssueID] = true
	}
	if len(pullFields) > 0 {
		allowPullOverwriteIDs[c.IssueID] = true
		if len(pushFields) == 0 {
			// Pull-only — block the push path. Mixed scenarios skip
			// this so doPush can run with pushFieldScopes scoping.
			skipIDs[c.IssueID] = true
		}
	}
}

// reimportIssue fetches the external version and updates the local issue.
func (e *Engine) reimportIssue(ctx context.Context, c Conflict) {
	extIssue, err := e.Tracker.FetchIssue(ctx, c.ExternalIdentifier)
	if err != nil || extIssue == nil {
		e.warn("Failed to re-import %s: %v", c.IssueID, err)
		return
	}

	conv := e.Tracker.FieldMapper().IssueToBeads(extIssue)
	if conv == nil || conv.Issue == nil {
		return
	}

	updates := map[string]interface{}{
		"title":       conv.Issue.Title,
		"description": conv.Issue.Description,
		"priority":    conv.Issue.Priority,
		"status":      string(conv.Issue.Status),
	}
	if extIssue.Metadata != nil {
		if raw, err := json.Marshal(extIssue.Metadata); err == nil {
			updates["metadata"] = json.RawMessage(raw)
		}
	}

	if err := e.Store.UpdateIssue(ctx, c.IssueID, updates, e.Actor); err != nil {
		e.warn("Failed to update %s during reimport: %v", c.IssueID, err)
		return
	}
	// bd-ajn: refresh the snapshot to the just-pulled state, matching
	// the doPull import path. Without this, the next sync would see
	// stale snapshot fields as "Linear changed them" after a
	// conflict-resolution import.
	if snapshotter, ok := e.Tracker.(PostPullSnapshotter); ok {
		if err := snapshotter.RecordPullSnapshot(ctx, c.IssueID, *extIssue); err != nil {
			e.warn("Snapshot write failed for %s after reimport: %v", c.IssueID, err)
		}
	}
}

// createDependencies creates dependencies from the pending list, matching
// external IDs to local issue IDs. Returns the number of dependencies that
// failed to resolve or create.
func (e *Engine) createDependencies(ctx context.Context, deps []DependencyInfo) int {
	if len(deps) == 0 {
		return 0
	}

	resolveIssue, err := e.dependencyIssueResolver(ctx, nil)
	if err != nil {
		e.warn("Failed to build dependency resolver: %v", err)
		return len(deps)
	}

	errCount := 0
	for _, dep := range deps {
		fromIssue, err := resolveIssue(ctx, dep.FromExternalID)
		if err != nil {
			e.warn("Failed to resolve dependency source %s: %v", dep.FromExternalID, err)
			errCount++
			continue
		}
		toIssue, err := resolveIssue(ctx, dep.ToExternalID)
		if err != nil {
			e.warn("Failed to resolve dependency target %s: %v", dep.ToExternalID, err)
			errCount++
			continue
		}

		if fromIssue == nil || toIssue == nil {
			continue // Not found (no error) — expected if issue wasn't imported
		}

		d := &types.Dependency{
			IssueID:     fromIssue.ID,
			DependsOnID: toIssue.ID,
			Type:        types.DependencyType(dep.Type),
		}
		if err := e.Store.AddDependency(ctx, d, e.Actor); err != nil {
			e.warn("Failed to create dependency %s -> %s: %v", fromIssue.ID, toIssue.ID, err)
			errCount++
		}
	}
	return errCount
}

// checkRequiredStoreCapabilities (bd-3p8) enforces mayor's design
// principle: tracker capabilities that REQUIRE a matching store
// capability must hard-fail at sync-start when the store lacks the
// impl, not silently degrade to a no-op.
//
// Current required pairings:
//   - Tracker implements PostPullSnapshotter (bd-ajn field-scoped
//     conflicts) → Store MUST implement LinearIssueSnapshotStore.
//   - Tracker implements ProjectPuller (bd-6cl pull-side Project
//     materialization) → Store MUST implement
//     LinearProjectSnapshotStore.
//
// Trackers that don't advertise these capabilities (GitHub, Jira,
// mocks) pass through unaffected — the check only fires for
// configured tracker→store mismatches.
func (e *Engine) checkRequiredStoreCapabilities() error {
	if _, ok := e.Tracker.(PostPullSnapshotter); ok {
		if _, storeOK := e.Store.(storage.LinearIssueSnapshotStore); !storeOK {
			// Lead with the interface name (durable anchor — survives
			// the bead system's archival cycle). Bead reference is the
			// secondary historical anchor.
			return fmt.Errorf(
				"storage backend does not implement storage.LinearIssueSnapshotStore "+
					"required by tracker %q (PostPullSnapshotter capability); "+
					"field-scoped conflict resolution cannot operate safely "+
					"(historical context: bd-ajn, bd-3p8)",
				e.Tracker.DisplayName())
		}
	}
	if _, ok := e.Tracker.(ProjectPuller); ok {
		if _, storeOK := e.Store.(storage.LinearProjectSnapshotStore); !storeOK {
			return fmt.Errorf(
				"storage backend does not implement storage.LinearProjectSnapshotStore "+
					"required by tracker %q (ProjectPuller capability); "+
					"pull-side Project materialization cannot operate safely "+
					"(historical context: bd-6cl, bd-3p8)",
				e.Tracker.DisplayName())
		}
	}
	return nil
}

// pulledIssueProjectID extracts the Linear Project UUID from a
// TrackerIssue's Metadata map (set by bd-ajn round-1 wiring in
// linearToTrackerIssue). Returns "" when the issue has no
// projectId or the value isn't a string. bd-6cl uses this in the
// per-Issue pull loop to decide whether to wire a parent-child
// dep to the materialized local epic.
func pulledIssueProjectID(ti TrackerIssue) string {
	if ti.Metadata == nil {
		return ""
	}
	if v, ok := ti.Metadata["project_id"]; ok {
		if s, ok := v.(string); ok {
			return s
		}
	}
	return ""
}

// ensureParentChildDep adds a parent-child dependency from childID
// to parentID, idempotently. Skips when the dep already exists
// (per bead store semantics: AddDependency returns an error on
// duplicate, which we treat as success). bd-6cl's pull-side
// descendant wiring calls this for each just-pulled Issue whose
// Linear projectId matches a materialized local epic.
func (e *Engine) ensureParentChildDep(ctx context.Context, childID, parentID string) error {
	// Pre-check: skip the AddDependency call when the dep is
	// already present locally. Avoids generating an event row +
	// hides the storage-layer "duplicate dependency" error.
	existing, err := e.Store.GetDependenciesWithMetadata(ctx, childID)
	if err != nil {
		return fmt.Errorf("get dependencies for %s: %w", childID, err)
	}
	for _, d := range existing {
		if d == nil {
			continue
		}
		if d.Issue.ID == parentID && d.DependencyType == types.DepParentChild {
			return nil // already wired
		}
	}
	dep := &types.Dependency{
		IssueID:     childID,
		DependsOnID: parentID,
		Type:        types.DepParentChild,
	}
	return e.Store.AddDependency(ctx, dep, e.Actor)
}

func (e *Engine) previewDependencies(ctx context.Context, deps []DependencyInfo, dryRunIssues []*types.Issue) int {
	if len(deps) == 0 {
		return 0
	}

	resolveIssue, err := e.dependencyIssueResolver(ctx, dryRunIssues)
	if err != nil {
		e.warn("Failed to build dependency resolver: %v", err)
		return len(deps)
	}

	wouldCreate := 0
	pending := make(map[string]struct{}, len(deps))
	for _, dep := range deps {
		fromIssue, err := resolveIssue(ctx, dep.FromExternalID)
		if err != nil {
			e.warn("Failed to resolve dependency source %s: %v", dep.FromExternalID, err)
			continue
		}
		toIssue, err := resolveIssue(ctx, dep.ToExternalID)
		if err != nil {
			e.warn("Failed to resolve dependency target %s: %v", dep.ToExternalID, err)
			continue
		}
		if fromIssue == nil || toIssue == nil {
			continue
		}
		if dependencyExists(ctx, e.Store, fromIssue.ID, toIssue.ID, types.DependencyType(dep.Type)) {
			continue
		}
		key := pendingDependencyPreviewKey(fromIssue.ID, toIssue.ID, dep.Type)
		if _, ok := pending[key]; ok {
			continue
		}
		pending[key] = struct{}{}
		fromDisplay := firstNonEmpty(fromIssue.ID, dep.FromExternalID)
		toDisplay := firstNonEmpty(toIssue.ID, dep.ToExternalID)
		e.msg("[dry-run] Would create dependency: %s -> %s (%s)", fromDisplay, toDisplay, dep.Type)
		wouldCreate++
	}
	if wouldCreate > 0 {
		e.msg("[dry-run] Would create %d dependencies", wouldCreate)
	}
	return 0
}

func pendingDependencyPreviewKey(fromID, toID, depType string) string {
	return strings.Join([]string{
		strings.TrimSpace(fromID),
		strings.TrimSpace(toID),
		strings.TrimSpace(depType),
	}, "\x00")
}

func (e *Engine) dependencyIssueResolver(ctx context.Context, extraIssues []*types.Issue) (func(context.Context, string) (*types.Issue, error), error) {
	issues, searchErr := e.Store.SearchIssues(ctx, "", types.IssueFilter{})
	if searchErr != nil {
		return nil, searchErr
	}
	issues = append(issues, extraIssues...)

	byExternal := make(map[string]*types.Issue, len(issues)*2)
	for _, candidate := range issues {
		if candidate == nil || candidate.ExternalRef == nil {
			continue
		}
		ref := strings.TrimSpace(*candidate.ExternalRef)
		if ref == "" {
			continue
		}
		if _, exists := byExternal[ref]; !exists {
			byExternal[ref] = candidate
		}
		if !e.Tracker.IsExternalRef(ref) {
			continue
		}
		identifier := strings.TrimSpace(e.Tracker.ExtractIdentifier(ref))
		if identifier != "" {
			if _, exists := byExternal[identifier]; !exists {
				byExternal[identifier] = candidate
			}
			lowerIdentifier := strings.ToLower(identifier)
			if _, exists := byExternal[lowerIdentifier]; !exists {
				byExternal[lowerIdentifier] = candidate
			}
		}
	}

	return func(ctx context.Context, externalID string) (*types.Issue, error) {
		externalID = strings.TrimSpace(externalID)
		if externalID == "" {
			return nil, nil
		}
		if issue := byExternal[externalID]; issue != nil {
			return issue, nil
		}
		if issue := byExternal[strings.ToLower(externalID)]; issue != nil {
			return issue, nil
		}
		// Tracker refs come in URL variants (e.g. Linear URLs with and
		// without the title slug); match on the extracted identifier the
		// same way the index above was built.
		if e.Tracker.IsExternalRef(externalID) {
			if identifier := strings.TrimSpace(e.Tracker.ExtractIdentifier(externalID)); identifier != "" {
				if issue := byExternal[identifier]; issue != nil {
					return issue, nil
				}
				if issue := byExternal[strings.ToLower(identifier)]; issue != nil {
					return issue, nil
				}
			}
		}
		if strings.Contains(externalID, "://") {
			return e.Store.GetIssueByExternalRef(ctx, externalID)
		}
		return nil, nil
	}, nil
}

func dependencyExists(ctx context.Context, store storage.Storage, issueID, dependsOnID string, depType types.DependencyType) bool {
	if strings.TrimSpace(issueID) == "" || strings.TrimSpace(dependsOnID) == "" {
		return false
	}
	records, err := store.GetDependenciesWithMetadata(ctx, issueID)
	if err != nil {
		return false
	}
	for _, record := range records {
		if record.ID == dependsOnID && record.DependencyType == depType {
			return true
		}
	}
	return false
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return value
		}
	}
	return ""
}

// buildDescendantSet returns the set of issue IDs consisting of the given parent
// and all its transitive descendants via parent-child dependencies.
func (e *Engine) buildDescendantSet(ctx context.Context, parentID string) (map[string]bool, error) {
	result := map[string]bool{parentID: true}
	queue := []string{parentID}
	for len(queue) > 0 {
		current := queue[0]
		queue = queue[1:]
		dependents, err := e.Store.GetDependentsWithMetadata(ctx, current)
		if err != nil {
			return nil, fmt.Errorf("getting dependents of %s: %w", current, err)
		}
		for _, dep := range dependents {
			if dep.DependencyType == types.DepParentChild && !result[dep.Issue.ID] {
				result[dep.Issue.ID] = true
				queue = append(queue, dep.Issue.ID)
			}
		}
	}
	return result, nil
}

// shouldPushIssue checks if an issue should be included in push based on filters.
func (e *Engine) shouldPushIssue(issue *types.Issue, opts SyncOptions) bool {
	// Skip ephemeral issues (wisps, etc.) if requested
	if opts.ExcludeEphemeral && issue.Ephemeral {
		return false
	}

	if len(opts.TypeFilter) > 0 {
		found := false
		for _, t := range opts.TypeFilter {
			if issue.IssueType == t {
				found = true
				break
			}
		}
		if !found {
			return false
		}
	}

	for _, t := range opts.ExcludeTypes {
		if issue.IssueType == t {
			return false
		}
	}

	// Linear scan matches ExcludeTypes' style above. Both ExcludeLabels and
	// issue.Labels are typically short (<=5), so the nested loop is faster
	// than allocating a per-issue map.
	for _, ex := range opts.ExcludeLabels {
		for _, label := range issue.Labels {
			if label == ex {
				return false
			}
		}
	}

	// ExcludeIDPrefix: case-sensitive prefix match on the bead ID. Filters
	// workflow-artifact beads (e.g. "hw-mol-foo") from external sync without
	// requiring them to share a type or label.
	if opts.ExcludeIDPrefix != "" && strings.HasPrefix(issue.ID, opts.ExcludeIDPrefix) {
		return false
	}
	// ExcludeIDPatterns: case-sensitive substring match anywhere in the ID.
	// Union with ExcludeIDPrefix — matching either rule excludes the issue.
	for _, p := range opts.ExcludeIDPatterns {
		if p != "" && strings.Contains(issue.ID, p) {
			return false
		}
	}

	if opts.State == "open" && issue.Status == types.StatusClosed {
		return false
	}

	// Skip issues not updated since the --since cutoff.
	if !opts.Since.IsZero() && !issue.UpdatedAt.After(opts.Since) {
		return false
	}

	return true
}

// ResolveState maps a beads status to a tracker state ID using the push state cache.
// Returns (stateID, ok). Only usable during a push operation after BuildStateCache has run.
func (e *Engine) ResolveState(status types.Status) (string, bool) {
	if e.PushHooks == nil || e.PushHooks.ResolveState == nil || e.stateCache == nil {
		return "", false
	}
	return e.PushHooks.ResolveState(e.stateCache, status)
}

// strPtr returns a pointer to the given string.
func strPtr(s string) *string { return &s }

// derefStr safely dereferences a *string, returning "" for nil.
func derefStr(s *string) string {
	if s == nil {
		return ""
	}
	return *s
}

// isBeadID returns true if the given string looks like a local bead ID
// (i.e. it starts with the configured prefix followed by a hyphen, like "bd-123").
// External tracker refs (URLs, "EXT-1", etc.) will return false.
func isBeadID(id, prefix string) bool {
	if prefix == "" || id == "" {
		return false
	}
	return strings.HasPrefix(id, prefix+"-")
}

// buildIssueIDSet converts a slice of IDs into a set for O(1) lookup.
func buildIssueIDSet(ids []string) map[string]bool {
	if len(ids) == 0 {
		return nil
	}
	set := make(map[string]bool, len(ids))
	for _, id := range ids {
		set[id] = true
	}
	return set
}

func (e *Engine) msg(format string, args ...interface{}) {
	if e.OnMessage != nil {
		e.OnMessage(fmt.Sprintf(format, args...))
	}
}

func (e *Engine) warn(format string, args ...interface{}) {
	msg := fmt.Sprintf(format, args...)
	e.warnings = append(e.warnings, msg)
	if e.OnWarning != nil {
		e.OnWarning(msg)
	}
}
