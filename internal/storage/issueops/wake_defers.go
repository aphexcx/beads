package issueops

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/steveyegge/beads/internal/config"
	"github.com/steveyegge/beads/internal/debug"
	"github.com/steveyegge/beads/internal/storage/dberrors"
	"github.com/steveyegge/beads/internal/storage/sqlbuild"
	"github.com/steveyegge/beads/internal/types"
)

// DeferWakeActor is the actor recorded on the status_changed event when the
// wake sweep returns an expired dated defer to open. A constant rather than
// the invoking session's actor: the wake is the system honoring the defer
// date, not something the reader who happened to trigger it did.
const DeferWakeActor = "bd-defer-wake"

// OwnerLabelPrefix is the label that names the node (city) a row belongs to
// in a federated store: owner:citadel. Gas City stamps it at create time from
// its [federation] identity; the wake sweep reads it to find the one node
// allowed to wake the row.
const OwnerLabelPrefix = "owner:"

// DeferWakeSkip is one expired dated defer the owner scope left alone on this
// node: another node's row, reported and never written.
type DeferWakeSkip struct {
	ID string
	// Owner is the node the row resolved to — its owner label, else its
	// prefix's declared home — or "" when neither names one.
	Owner string
}

// WakeDefersResult reports what one sweep woke, per table.
type WakeDefersResult struct {
	// Issues are the permanent-table issue ids returned to open; only these
	// affect Dolt-versioned tables, so only these decide whether the caller
	// mints a dolt commit.
	Issues []string
	// Wisps are the woken wisp ids. Wisp tables are dolt_ignored, so a
	// wisp-only wake never needs a version commit.
	Wisps []string
	// Skipped are the expired dated issue defers the owner scope declined on
	// this node (see WakeExpiredDefersInTx): reported, never written, so they
	// never decide a commit. Wisps are never scoped, so never skipped.
	Skipped []DeferWakeSkip
}

// WakeDefersCommitMessage names a sweep's dolt commit. n is the number of
// permanent issues woken (len(result.Issues)); callers with n == 0 should not
// commit at all.
func WakeDefersCommitMessage(n int) string {
	return fmt.Sprintf("bd: wake %d expired defer(s)", n)
}

// deferWakeConfigErrorOnce keeps the unreadable-config advisory to one line
// per process: the sweep runs on every ready read.
var deferWakeConfigErrorOnce sync.Once

// WakeExpiredDefersInTx returns every DATED defer whose date has passed to
// open: status='deferred' AND defer_until <= now flips to status='open',
// defer_until=NULL — byte-identical to what `bd undefer` writes, so a later
// dateless `bd defer` cannot inherit a stale past date and instantly re-wake.
// A DATELESS defer (defer_until IS NULL) is the indefinite icebox and is
// deliberately never touched: `bd undefer` stays its only exit.
//
// This is the lazy half of the defer contract. `bd defer --until` and
// `bd update --defer` promise "hidden until <date>", but nothing ever flipped
// the status back, so an expired defer stayed invisible to the ready front
// forever. Callers run this sweep at the top of ready-work reads and claims;
// the snapshot-then-recheck shape mirrors ReclaimExpiredLeasesInTx, so a bead
// re-deferred or claimed between the snapshot and its UPDATE is simply skipped.
//
// OWNER SCOPE. In a federated store every replica runs this sweep over the
// same issues rows, and a wake is a write: two replicas waking the same row
// mint divergent commits on the same cells and the next pull conflicts. So
// in a store whose own config declares federation.prefix_home.<prefix>
// (deferWakeScopeFor) an issue wakes only on the node its owner:<node> label
// names, else on its prefix's declared home; a row with neither wakes nowhere
// until one is set, and every other node reports it skipped and writes
// nothing. A store that declares no prefix home runs the legacy sweep byte
// for byte, whatever node_id says. A store whose config cannot be read may be
// federated, so its sweep is skipped whole, with one advisory, rather than
// run unscoped. The scope is applied to the snapshot SELECT only, like the
// reclaim scope, and to the permanent issues table alone: wisps are
// clone-local (dolt_ignored), so only the node that made a wisp can wake it.
//
// The caller owns Dolt versioning (commit iff len(result.Issues) > 0) and must
// treat sweep failure as advisory — a ready listing never fails because the
// wake could not run.
func WakeExpiredDefersInTx(ctx context.Context, tx DBTX) (WakeDefersResult, error) {
	var result WakeDefersResult
	scope, err := deferWakeScopeFor(ctx)
	if err != nil {
		deferWakeConfigErrorOnce.Do(func() {
			warnReplica("warning: defer-wake sweep skipped, this store's federation config is unusable: %v\n", err)
		})
		return result, nil
	}
	issues, skipped, err := wakeExpiredDefersInTable(ctx, tx, sqlbuild.IssuesFilterTables, "events", scope)
	if err != nil {
		return result, err
	}
	result.Issues = issues
	result.Skipped = skipped
	// Wisps carry the same status/defer_until columns and `bd defer` reaches
	// them through the same UpdateIssue routing. No owner scope here (nil):
	// wisp tables are dolt_ignored, so a wisp exists only on the node that
	// made it and no peer could ever wake it instead. The unscoped shape
	// names no labels table, so a database mid-migration with wisps but no
	// wisp_labels is still swept, and IsTableNotExist below can only mean the
	// wisps table itself is absent: a pre-wisp database, tolerated like every
	// other wisp probe.
	wisps, _, err := wakeExpiredDefersInTable(ctx, tx, sqlbuild.WispsFilterTables, "wisp_events", nil)
	switch {
	case err == nil:
		result.Wisps = wisps
	case dberrors.IsTableNotExist(err):
		// Pre-wisp database: nothing there to wake.
	default:
		return result, err
	}
	if scope != nil {
		reportDeferWakeSkips(result.Skipped, scope.localNode)
	}
	return result, nil
}

// deferWakeScope is the owner scope of one store: who this node is and where
// each prefix lives. nil means the store is not federated and the sweep is
// the legacy sweep.
type deferWakeScope struct {
	localNode string
	homes     map[string]string
}

// deferWakeWorkspaceKey carries the .beads directory of the store a sweep
// runs against, so the owner scope is read from THAT store's config.
type deferWakeWorkspaceKey struct{}

// WithDeferWakeWorkspace names the store's .beads directory for the owner
// scope of the defer-wake sweep, so a library consumer that opened the
// workspace without config.Initialize (the public beads.OpenBestAvailable /
// OpenFromConfig) resolves the same scope bd's own process does, and a
// cross-workspace open reads the target store, not the launch workspace.
func WithDeferWakeWorkspace(ctx context.Context, beadsDir string) context.Context {
	return context.WithValue(ctx, deferWakeWorkspaceKey{}, beadsDir)
}

// DeferWakeWorkspace returns the store directory WithDeferWakeWorkspace put
// on the context, if any.
func DeferWakeWorkspace(ctx context.Context) (string, bool) {
	dir, ok := ctx.Value(deferWakeWorkspaceKey{}).(string)
	return dir, ok && dir != ""
}

// deferWakeScopeFor resolves the owner scope of the store the sweep runs
// against from that store's own files alone (config.ReadWorkspaceFederation),
// read once per sweep. Three outcomes: no federation.prefix_home key, a nil
// scope and the legacy sweep; keys present and usable, a scope; a file that
// cannot be read or parsed, or a key with no usable value, an error. Neither
// the process environment nor the user-level config can arm or steer it, and
// node_id alone never arms it: it is per machine and sits under every store
// there. The node is resolved for the wake only, leaving config.NodeID and
// the lease guard untouched; WithNodeID still overrides it for tests.
//
// A sweep whose context names no store is the unscoped primitive. The entry
// layers that own a store keep it from running by accident: the dolt and
// embedded wrappers always attach theirs, and uow.WakeExpiredDefers refuses
// to sweep for a provider that names none. beads.Open(dbPath) with a dbPath
// outside a .beads directory names that directory, where no config lives, so
// such a store reads as not federated.
func deferWakeScopeFor(ctx context.Context) (*deferWakeScope, error) {
	dir, ok := DeferWakeWorkspace(ctx)
	if !ok {
		return nil, nil
	}
	fed, err := config.ReadWorkspaceFederation(dir)
	if err != nil || len(fed.PrefixHomes) == 0 {
		return nil, err
	}
	scope := &deferWakeScope{localNode: fed.NodeID, homes: fed.PrefixHomes}
	if node, ok := ctx.Value(nodeIDContextKey{}).(string); ok {
		scope.localNode = node
	}
	return scope, nil
}

func wakeExpiredDefersInTable(ctx context.Context, tx DBTX, tables sqlbuild.FilterTables, eventsTable string, scope *deferWakeScope) (woken []string, skipped []DeferWakeSkip, err error) {
	// Snapshot first so each genuinely-woken row gets its own event. The
	// UPDATE below repeats the whole predicate, so a row rescued between the
	// SELECT and its UPDATE (re-deferred further out, claimed, closed) matches
	// nothing and is skipped rather than clobbered. In a scoped store the
	// owner label rides along so the scope decides per row before any UPDATE
	// is issued; unscoped, the snapshot names no labels table at all. An owner
	// label with an empty or blank value ("owner:", "owner: ") is left out of
	// the pick so MIN() lands on a real owner when both exist.
	ownerCol, args := "''", []any(nil)
	if scope != nil {
		ownerCol = fmt.Sprintf("COALESCE((SELECT MIN(l.label) FROM %s l WHERE l.issue_id = t.id AND l.label LIKE ? AND TRIM(SUBSTRING(l.label, ?)) <> ''), '')", tables.Labels)
		args = []any{OwnerLabelPrefix + "%", len(OwnerLabelPrefix) + 1}
	}
	//nolint:gosec // G201: table names are the hardcoded sqlbuild constants from the caller above.
	rows, err := tx.QueryContext(ctx, fmt.Sprintf(`
		SELECT t.id, %s
		FROM %s t
		WHERE t.status = 'deferred' AND t.defer_until IS NOT NULL
		  AND t.defer_until <= UTC_TIMESTAMP()
	`, ownerCol, tables.Main), args...)
	if err != nil {
		return nil, nil, fmt.Errorf("wake expired defers: scan %s: %w", tables.Main, err)
	}
	var expired []string
	for rows.Next() {
		var id, ownerLabel string
		if err := rows.Scan(&id, &ownerLabel); err != nil {
			_ = rows.Close()
			return nil, nil, fmt.Errorf("wake expired defers: scan %s row: %w", tables.Main, err)
		}
		if scope != nil {
			owner := deferWakeOwner(id, ownerLabel, scope.homes)
			if owner == "" || owner != scope.localNode {
				skipped = append(skipped, DeferWakeSkip{ID: id, Owner: owner})
				continue
			}
		}
		expired = append(expired, id)
	}
	if err := rows.Err(); err != nil {
		_ = rows.Close()
		return nil, nil, fmt.Errorf("wake expired defers: iterate %s: %w", tables.Main, err)
	}
	if err := rows.Close(); err != nil {
		return nil, nil, fmt.Errorf("wake expired defers: close %s rows: %w", tables.Main, err)
	}
	if len(expired) == 0 {
		return nil, skipped, nil
	}

	now := time.Now().UTC()
	for _, id := range expired {
		// row_lock is rewritten so a concurrent claim/update conflicts at
		// commit time instead of cell-merging with this write — the same
		// invariant the lease scheme depends on.
		//nolint:gosec // G201: table name is the hardcoded sqlbuild constant from the caller above.
		res, err := tx.ExecContext(ctx, fmt.Sprintf(`
			UPDATE %s
			SET status = 'open', defer_until = NULL, updated_at = ?, row_lock = ?
			WHERE id = ? AND status = 'deferred' AND defer_until IS NOT NULL
			  AND defer_until <= UTC_TIMESTAMP()
		`, tables.Main), now, freshRowLock(), id)
		if err != nil {
			return woken, skipped, fmt.Errorf("wake expired defer %s: %w", id, err)
		}
		n, err := res.RowsAffected()
		if err != nil {
			return woken, skipped, fmt.Errorf("wake expired defer %s rows affected: %w", id, err)
		}
		if n == 0 {
			continue // rescued concurrently — leave it be
		}
		if err := RecordFullEventInTable(ctx, tx, eventsTable, id, types.EventStatusChanged,
			DeferWakeActor, string(types.StatusDeferred), string(types.StatusOpen)); err != nil {
			return woken, skipped, fmt.Errorf("record wake event for %s: %w", id, err)
		}
		// A wake is a status change, so it journals as an update. Emitted past
		// the rows-affected re-check, so a concurrently-rescued bead records
		// nothing.
		if err := RecordEventInTx(ctx, tx, EventUpdate, id, DeferWakeActor); err != nil {
			return woken, skipped, err
		}
		woken = append(woken, id)
	}
	return woken, skipped, nil
}

// deferWakeOwner resolves the node an expired defer belongs to: the node its
// owner:<node> label names, else the declared home of its prefix — the
// longest declared prefix wins, so beads-vscode-1 asks for beads-vscode
// before beads — else "". An owner label with an empty value ("owner:") is
// no label, so the prefix home still applies.
func deferWakeOwner(id, ownerLabel string, homes map[string]string) string {
	if owner := strings.TrimSpace(strings.TrimPrefix(ownerLabel, OwnerLabelPrefix)); owner != "" {
		return owner
	}
	parts := strings.Split(id, "-")
	for n := len(parts) - 1; n >= 1; n-- {
		if home := homes[strings.Join(parts[:n], "-")]; home != "" {
			return home
		}
	}
	return ""
}

// deferWakeSkipDetailRows caps the bd -v per-row listing of skipped defers.
const deferWakeSkipDetailRows = 20

// deferWakeSkipReportOnce keeps the skip audit to one report per process: a
// skipped row stays deferred here until its owner's wake syncs over, and a
// long-lived reader would otherwise repeat the line on every ready read.
var deferWakeSkipReportOnce sync.Once

// reportDeferWakeSkips audits the expired defers the owner scope declined:
// one stderr line (never stdout — bd ready --json owns it), the per-row
// detail behind bd -v / BD_DEBUG, nothing under --quiet, once per process.
func reportDeferWakeSkips(skipped []DeferWakeSkip, localNode string) {
	if len(skipped) == 0 || debug.IsQuiet() {
		return
	}
	deferWakeSkipReportOnce.Do(func() {
		warnReplica("%s", formatDeferWakeSkipSummary(skipped, localNode))
		if !debug.Enabled() {
			return
		}
		for i, s := range skipped {
			if i == deferWakeSkipDetailRows {
				warnReplica("  ... and %d more\n", len(skipped)-i)
				break
			}
			owner := s.Owner
			if owner == "" {
				owner = "no owner label, no prefix home"
			}
			warnReplica("  %s (%s)\n", s.ID, owner)
		}
	})
}

// formatDeferWakeSkipSummary renders the one-line audit: a count per owning
// node, most first, with the rows nothing places named by the config key
// that would place them.
func formatDeferWakeSkipSummary(skipped []DeferWakeSkip, localNode string) string {
	counts := map[string]int{}
	for _, s := range skipped {
		counts[s.Owner]++
	}
	owners := make([]string, 0, len(counts))
	for owner := range counts {
		owners = append(owners, owner)
	}
	sort.Slice(owners, func(i, j int) bool {
		if counts[owners[i]] != counts[owners[j]] {
			return counts[owners[i]] > counts[owners[j]]
		}
		return owners[i] < owners[j]
	})
	var b strings.Builder
	for i, owner := range owners {
		if i > 0 {
			b.WriteString(", ")
		}
		if owner == "" {
			fmt.Fprintf(&b, "no owner label and no %s<prefix> (%d)", config.FederationPrefixHomeKey, counts[owner])
			continue
		}
		fmt.Fprintf(&b, "%q (%d)", owner, counts[owner])
	}
	node := fmt.Sprintf("%q", localNode)
	if localNode == "" {
		node = "node_id unset"
	}
	return fmt.Sprintf("defer-wake: skipped %d expired dated %s not owned by this node (%s): %s. "+
		"Only the owning node wakes them; label the row %s<node> or set %s<prefix> on every clone. (bd -v lists up to %d.)\n",
		len(skipped), pluralWord(len(skipped), "defer", "defers"), node, b.String(),
		OwnerLabelPrefix, config.FederationPrefixHomeKey, deferWakeSkipDetailRows)
}
