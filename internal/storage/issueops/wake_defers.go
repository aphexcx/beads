package issueops

import (
	"context"
	"fmt"
	"sort"
	"strings"
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

// PrefixHomeConfigKey is the config.yaml namespace that names the home node
// of a bead prefix: federation.prefix_home.hw: citadel. An expired defer with
// no owner label wakes only on that node. The key lives in the git-tracked
// .beads/config.yaml on purpose: every clone of the store carries the same
// answer, and only the node whose node_id matches acts on it.
const PrefixHomeConfigKey = "federation.prefix_home."

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
// on a replica that names itself (config.NodeID, the identity the lease
// guard already uses) an issue wakes only where it belongs: on the node its
// owner:<node> label names, else on the node federation.prefix_home.<prefix>
// declares home for its prefix. A row with neither wakes nowhere until one of
// them is set, and every other node reports it skipped and writes nothing. A
// deployment with no node_id keeps the old behavior: it has said nothing about
// being one replica among several, and waking every expired defer there is
// the whole contract. The scope is applied to the snapshot SELECT only, like
// the reclaim scope; the per-row UPDATE re-checks by id. It governs the
// permanent issues table alone: wisps are clone-local (dolt_ignored) and never
// federate, so the node that made a wisp is the only node that has it, and
// the only one that can wake it.
//
// The caller owns Dolt versioning (commit iff len(result.Issues) > 0) and must
// treat sweep failure as advisory — a ready listing never fails because the
// wake could not run.
func WakeExpiredDefersInTx(ctx context.Context, tx DBTX) (WakeDefersResult, error) {
	var result WakeDefersResult
	localNode := NodeID(ctx)
	issues, skipped, err := wakeExpiredDefersInTable(ctx, tx, sqlbuild.IssuesFilterTables, "events", localNode)
	if err != nil {
		return result, err
	}
	result.Issues = issues
	result.Skipped = skipped
	// Wisps carry the same status/defer_until columns and `bd defer` reaches
	// them through the same UpdateIssue routing. The table is tolerated absent
	// for pre-wisp databases, like every other wisp probe. No owner scope
	// here (localNode ""): wisp tables are dolt_ignored, so a wisp exists
	// only on the node that made it and no peer could ever wake it instead.
	wisps, _, err := wakeExpiredDefersInTable(ctx, tx, sqlbuild.WispsFilterTables, "wisp_events", "")
	switch {
	case err == nil:
		result.Wisps = wisps
	case dberrors.IsTableNotExist(err):
		// Pre-wisp database: nothing there to wake.
	default:
		return result, err
	}
	reportDeferWakeSkips(result.Skipped, localNode)
	return result, nil
}

func wakeExpiredDefersInTable(ctx context.Context, tx DBTX, tables sqlbuild.FilterTables, eventsTable, localNode string) (woken []string, skipped []DeferWakeSkip, err error) {
	// Snapshot first so each genuinely-woken row gets its own event. The
	// UPDATE below repeats the whole predicate, so a row rescued between the
	// SELECT and its UPDATE (re-deferred further out, claimed, closed) matches
	// nothing and is skipped rather than clobbered. The owner label rides
	// along so the owner scope decides per row before any UPDATE is issued.
	//nolint:gosec // G201: table names are the hardcoded sqlbuild constants from the caller above.
	rows, err := tx.QueryContext(ctx, fmt.Sprintf(`
		SELECT t.id, COALESCE((SELECT MIN(l.label) FROM %s l WHERE l.issue_id = t.id AND l.label LIKE ?), '')
		FROM %s t
		WHERE t.status = 'deferred' AND t.defer_until IS NOT NULL
		  AND t.defer_until <= UTC_TIMESTAMP()
	`, tables.Labels, tables.Main), OwnerLabelPrefix+"%")
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
		if localNode != "" {
			if owner := deferWakeOwner(id, ownerLabel, prefixHomeFromConfig); owner != localNode {
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
// before beads — else "". homeOf answers federation.prefix_home.<prefix>.
func deferWakeOwner(id, ownerLabel string, homeOf func(prefix string) string) string {
	if ownerLabel != "" {
		return strings.TrimSpace(strings.TrimPrefix(ownerLabel, OwnerLabelPrefix))
	}
	parts := strings.Split(id, "-")
	for n := len(parts) - 1; n >= 1; n-- {
		if home := strings.TrimSpace(homeOf(strings.Join(parts[:n], "-"))); home != "" {
			return home
		}
	}
	return ""
}

func prefixHomeFromConfig(prefix string) string {
	return config.GetString(PrefixHomeConfigKey + prefix)
}

// deferWakeSkipDetailRows caps the bd -v per-row listing of skipped defers.
const deferWakeSkipDetailRows = 20

// reportDeferWakeSkips audits the expired defers the owner scope declined:
// ONE stderr line per sweep (never stdout — bd ready --json owns it), the
// per-row detail behind bd -v / BD_DEBUG, nothing under --quiet. It repeats
// on every ready read while the rows stay deferred, by design: the line is
// the only trace of a defer this node will never wake.
func reportDeferWakeSkips(skipped []DeferWakeSkip, localNode string) {
	if len(skipped) == 0 || debug.IsQuiet() {
		return
	}
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
			fmt.Fprintf(&b, "no owner label and no %s<prefix> (%d)", PrefixHomeConfigKey, counts[owner])
			continue
		}
		fmt.Fprintf(&b, "%q (%d)", owner, counts[owner])
	}
	return fmt.Sprintf("defer-wake: skipped %d expired dated %s not owned by this node (%q): %s. "+
		"Only the owning node wakes them; label the row %s<node> or set %s<prefix> on every clone. (bd -v lists up to %d.)\n",
		len(skipped), pluralWord(len(skipped), "defer", "defers"), localNode, b.String(),
		OwnerLabelPrefix, PrefixHomeConfigKey, deferWakeSkipDetailRows)
}
