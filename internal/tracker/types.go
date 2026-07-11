// Package tracker provides a plugin framework for external issue tracker integrations.
//
// It defines interfaces (IssueTracker, FieldMapper) and a shared SyncEngine that
// eliminates duplication between tracker integrations (Linear, GitLab, Jira, etc.).
//
// Design based on GitHub issue #1150 and PRs #1564-#1567, updated for Dolt-only storage.
package tracker

import (
	"time"

	"github.com/steveyegge/beads/internal/types"
)

// TrackerIssue represents an issue from an external tracker in a generic format.
// Each tracker adapter converts its native issue type to/from this intermediate form.
type TrackerIssue struct {
	// Core identification
	ID         string // External tracker's internal ID (e.g., UUID)
	Identifier string // Human-readable identifier (e.g., "TEAM-123", "PROJ-456")
	URL        string // Web URL to the issue

	// Content
	Title       string
	Description string

	// Classification
	Priority int         // Priority value (tracker-specific, mapped via FieldMapper)
	State    interface{} // Tracker-specific state object (mapped via FieldMapper)
	Type     interface{} // Tracker-specific type (mapped via FieldMapper)
	Labels   []string    // Labels/tags

	// Assignment
	Assignee      string // Assignee name or email
	AssigneeID    string // Assignee's tracker-specific ID
	AssigneeEmail string // Assignee email if available

	// Timestamps
	CreatedAt   time.Time
	UpdatedAt   time.Time
	CompletedAt *time.Time

	// Relationships
	ParentID         string // Parent issue identifier (for subtasks/children)
	ParentInternalID string // Parent issue internal ID

	// Warnings carries non-fatal, partial-success messages from a create/update
	// (e.g. the issue was created but a follow-up state change failed). The sync
	// engine drains these into the sync result's warnings so a degraded push is
	// visible in --json output instead of being silently swallowed.
	Warnings []string

	// Raw data for tracker-specific processing
	Raw interface{} // Original API response for tracker-specific access

	// Metadata for tracker-specific fields that don't map to core Issue fields.
	// Stored in Issue.Metadata for round-trip preservation.
	Metadata map[string]interface{}
}

// FetchOptions specifies options for fetching issues from an external tracker.
type FetchOptions struct {
	// State filter: "open", "closed", or "all" (default)
	State string

	// Incremental sync: only fetch issues updated since this time.
	Since *time.Time

	// Maximum number of issues to fetch (0 = no limit).
	Limit int
}

// SyncOptions configures the behavior of a sync operation.
type SyncOptions struct {
	// Pull imports issues from the external tracker.
	Pull bool
	// Push exports issues to the external tracker.
	Push bool
	// DryRun previews sync without making changes.
	DryRun bool
	// CreateOnly only creates new issues, doesn't update existing.
	CreateOnly bool
	// CreateClosed allows pushing closed local beads with no external ref as
	// new external issues (tombstone creates). Default is false — closed
	// beads without an external ref are skipped so ongoing syncs don't
	// retroactively register completed local-only work. Set true for one-off
	// historical backfills.
	CreateClosed bool
	// VerboseDiff prints field-level differences for each would-be update in
	// dry-run output. Requires the tracker's PushHooks.DescribeDiff hook to
	// be set; otherwise is a no-op.
	VerboseDiff bool
	// State filters issues: "open", "closed", or "all".
	State string
	// ConflictResolution specifies how to handle bidirectional conflicts.
	ConflictResolution ConflictResolution
	// TypeFilter limits which issue types are synced (empty = all).
	TypeFilter []types.IssueType
	// ExcludeTypes excludes specific issue types from sync.
	ExcludeTypes []types.IssueType
	// ExcludeLabels excludes issues that carry any of the listed labels.
	// Useful for filtering out internal-infrastructure beads (e.g. agent
	// beads tagged "gt:agent") that shouldn't be exported to the external
	// tracker. Matched case-sensitively against issue.Labels.
	ExcludeLabels []string
	// ExcludeIDPrefix skips issues whose ID starts with this prefix (case-
	// sensitive). Used to filter workflow-artifact beads (e.g. "hw-mol-")
	// from external sync. Empty means no prefix filter.
	ExcludeIDPrefix string
	// ExcludeIDPatterns skips issues whose ID contains any of these
	// substrings (case-sensitive, anywhere in the ID). Empty means no
	// pattern filter. Combined with ExcludeIDPrefix as a union (matches
	// either rule → excluded).
	ExcludeIDPatterns []string
	// ExcludeEphemeral skips ephemeral/wisp issues from push (default behavior in CLI).
	ExcludeEphemeral bool
	// ParentID limits push to this beads issue and all its descendants via
	// parent-child dependencies. Empty means no restriction.
	ParentID string
	// IssueIDs restricts sync to only these issues. Accepts bead IDs (e.g. "bd-123")
	// or external refs (e.g. "EXT-456"). When non-empty, push filters local issues
	// by ID and pull uses FetchIssue() for targeted retrieval instead of bulk fetch.
	IssueIDs []string
	// Since limits push to issues updated after this time. Zero value means no restriction.
	Since time.Time
	// DependencyTypes limits which dependency types pull creates from tracker
	// mapper output. Empty means all dependency types are created.
	DependencyTypes []types.DependencyType
	// DependencySources limits which dependency sources pull creates from tracker
	// mapper output. Empty means all dependency sources are created.
	DependencySources []DependencySource
}

// SyncResult is the complete result of a sync operation.
type SyncResult struct {
	Success   bool      `json:"success"`
	Stats     SyncStats `json:"stats"`
	LastSync  string    `json:"last_sync,omitempty"` // RFC3339 timestamp
	Error     string    `json:"error,omitempty"`
	Warnings  []string  `json:"warnings,omitempty"`
	PullStats PullStats `json:"-"`
	PushStats PushStats `json:"-"`
}

// SyncStats accumulates sync statistics.
type SyncStats struct {
	Pulled    int `json:"pulled"`
	Pushed    int `json:"pushed"`
	Created   int `json:"created"`
	Updated   int `json:"updated"`
	Skipped   int `json:"skipped"`
	Errors    int `json:"errors"`
	Conflicts int `json:"conflicts"`
}

// PullStats tracks pull operation results.
type PullStats struct {
	Queried     int
	Candidates  int
	Created     int
	Updated     int
	Skipped     int
	Errors      int
	Incremental bool
	SyncedSince string
}

// PushStats tracks push operation results.
type PushStats struct {
	Created  int
	Updated  int
	Skipped  int
	Errors   int
	Warnings []string
}

// BatchPushItem describes one local issue handled by a tracker batch push.
type BatchPushItem struct {
	LocalID     string
	ExternalRef string
}

// BatchPushError describes one issue-level failure from a tracker batch push.
type BatchPushError struct {
	LocalID string
	Message string
}

// BatchPushResult is the normalized result of a tracker batch push.
type BatchPushResult struct {
	Created  []BatchPushItem
	Updated  []BatchPushItem
	Skipped  []string
	Errors   []BatchPushError
	Warnings []string
}

// ConflictField identifies an issue field that can be diffed between
// the local bead and its external tracker counterpart. bd-ajn's
// field-scoped conflict detection emits one of these per actual
// change, replacing the old whole-issue boolean.
type ConflictField string

const (
	FieldTitle       ConflictField = "title"
	FieldDescription ConflictField = "description"
	FieldStatus      ConflictField = "status"
	FieldPriority    ConflictField = "priority"
	FieldAssignee    ConflictField = "assignee"
	FieldProject     ConflictField = "project"
	FieldParent      ConflictField = "parent"
)

// Conflict represents a bidirectional modification conflict.
//
// bd-ajn extends the type with per-field diff results. Backends that
// support snapshot-based detection populate LocalChanged /
// ExternalChanged / Conflicting; the resolver consumes them to
// decide push/pull/conflict on a per-field basis. Backends that
// don't (no snapshot capability, or first-sync window before
// baselines exist) leave the maps empty and the resolver falls
// back to whole-issue timestamp behavior — same semantics as before
// bd-ajn.
type Conflict struct {
	IssueID            string    // Beads issue ID
	LocalUpdated       time.Time // When the local version was last modified
	ExternalUpdated    time.Time // When the external version was last modified
	ExternalRef        string    // URL or identifier for the external issue
	ExternalIdentifier string    // External tracker's identifier (e.g., "TEAM-123")
	ExternalInternalID string    // External tracker's internal ID (for API calls)

	// bd-ajn field-scoped diff results. Empty maps + empty slice =
	// fall back to whole-issue timestamp comparison (legacy path,
	// also used when no snapshot exists yet).
	LocalChanged    map[ConflictField]bool // fields the LOCAL side moved since lastSync
	ExternalChanged map[ConflictField]bool // fields the EXTERNAL side moved since lastSync
	Conflicting     []ConflictField        // intersect(LocalChanged, ExternalChanged)
}

// HasFieldScopedDiff reports whether this conflict carries snapshot-
// backed per-field information. When false, the resolver MUST treat
// the conflict as whole-issue (legacy behavior). Distinguishing the
// two states explicitly avoids ambiguity when both maps happen to be
// empty for a genuine "no fields changed" case.
func (c *Conflict) HasFieldScopedDiff() bool {
	return c.LocalChanged != nil || c.ExternalChanged != nil
}

// ConflictResolution specifies how to handle sync conflicts.
type ConflictResolution string

const (
	// ConflictTimestamp resolves conflicts by keeping the newer version.
	ConflictTimestamp ConflictResolution = "timestamp"
	// ConflictLocal always keeps the local beads version.
	ConflictLocal ConflictResolution = "local"
	// ConflictExternal always keeps the external tracker's version.
	ConflictExternal ConflictResolution = "external"
)

// IssueConversion holds the result of converting an external tracker issue to beads.
type IssueConversion struct {
	Issue        *types.Issue
	Dependencies []DependencyInfo
}

// DependencyInfo describes a dependency to create after all issues are imported.
type DependencyInfo struct {
	FromExternalID string // External identifier of the dependent issue
	ToExternalID   string // External identifier of the dependency target
	Type           string // Beads dependency type (blocks, related, duplicates, parent-child)
	Source         DependencySource
}

// DependencySource identifies which tracker relationship produced a dependency.
type DependencySource string

const (
	DependencySourceParent   DependencySource = "parent"
	DependencySourceRelation DependencySource = "relation"
)
