package schema

import (
	"context"
	"database/sql"
	"fmt"
	"sort"
	"strings"

	"github.com/steveyegge/beads/internal/storage/rowid"
)

// auxRowRekeyMarkerVersion is the ignored (clone-local) migration that records
// completion of the one-time aux-row-id re-key. The backfill runs only while
// this version is still pending; ignoredSource.migrate then records it later
// in the same MigrateUp pass. The cursor table is dolt-ignored, so every clone
// performs its own convergence pass exactly once.
const auxRowRekeyMarkerVersion = 9

// auxRekeyTable describes one table covered by the re-key. columns is the
// frozen SELECT list of every non-id column, in creation order, with datetime
// columns CAST to CHAR server-side so the scanned text is identical across
// drivers and connection settings.
//
// These lists are part of the id derivation and are FROZEN: they must keep
// naming exactly the columns the tables had when this backfill shipped, even
// if later migrations add columns. Clones upgrade at different binary
// versions; only a version-independent column set makes two clones derive the
// same id for the same ancestral row.
type auxRekeyTable struct {
	name    string
	columns string
}

// auxRekeyTables covers the four synced tables whose 0037 backfill randomized
// primary keys across clones. The wisp_ twins (wisp_events, wisp_comments) are
// deliberately excluded: they are dolt-ignored, never merged, and promotion
// copies their rows without ids, so their random keys never escape the clone.
var auxRekeyTables = []auxRekeyTable{
	{
		name:    "events",
		columns: "issue_id, event_type, actor, old_value, new_value, comment, CAST(created_at AS CHAR)",
	},
	{
		name:    "comments",
		columns: "issue_id, author, text, CAST(created_at AS CHAR)",
	},
	{
		name:    "issue_snapshots",
		columns: "issue_id, CAST(snapshot_time AS CHAR), compaction_level, original_size, compressed_size, original_content, archived_events",
	},
	{
		name:    "compaction_snapshots",
		columns: "issue_id, compaction_level, snapshot_json, CAST(created_at AS CHAR)",
	},
}

// rekeyAuxRowIDs converges the primary keys that migration 0037 randomized
// (bd-6dnrw.2). 0037 backfilled the CHAR(36) ids of events, comments,
// issue_snapshots and compaction_snapshots with per-clone-random UUID()s, so
// legacy clones that migrated independently hold the same logical rows under
// different keys and their merges duplicate or refuse. This rewrites every
// row's id to the deterministic content-derived value (internal/storage/rowid)
// so independently-upgraded clones converge to byte-identical tables.
//
// It runs from MigrateUp after the schema migrations, gated on the clone-local
// marker (see auxRowRekeyMarkerVersion): once per clone, not on every later
// migration pass — rows inserted after the pass carry ids that are random but
// already consistent across clones (minted once, then merged), so re-keying
// them would churn synced tables for no convergence benefit. Changes are
// staged and committed by MigrateUp like any other backfill.
func rekeyAuxRowIDs(ctx context.Context, db DBConn) (bool, error) {
	pending, err := ignoredSource.pendingVersions(ctx, db)
	if err != nil {
		return false, fmt.Errorf("reading pending ignored migrations: %w", err)
	}
	markerPending := false
	for _, v := range pending {
		if v == auxRowRekeyMarkerVersion {
			markerPending = true
			break
		}
	}
	if !markerPending {
		return false, nil
	}

	wrote := false
	for _, t := range auxRekeyTables {
		w, err := rekeyAuxRowTable(ctx, db, t)
		wrote = wrote || w
		if err != nil {
			return wrote, fmt.Errorf("%s: %w", t.name, err)
		}
	}
	return wrote, nil
}

// rekeyAuxRowTable re-derives the ids of one table. The whole table is grouped
// by content digest; each digest's rows take the deterministic ids for
// ordinals 0..n-1. A row already holding one of its group's target ids keeps
// it (idempotence: re-running never swaps ids within a group), and the
// remaining rows take the remaining targets in sorted-current-id order. Across
// clones that assignment may permute within a group of exact-duplicate rows,
// but duplicates are interchangeable and the id set is identical, so the
// merged result still converges.
func rekeyAuxRowTable(ctx context.Context, db DBConn, t auxRekeyTable) (bool, error) {
	// Skip cleanly if the table or its id column isn't present (older or partial
	// schema): nothing to re-key. After MigrateUp's main pass the id column is
	// CHAR(36) on any schema this runs against (0037 precedes the marker).
	hasID, err := columnExists(ctx, db, t.name, "id")
	if err != nil {
		return false, err
	}
	if !hasID {
		return false, nil
	}

	//nolint:gosec // G201: name/columns come from the hardcoded auxRekeyTables, never user input.
	rows, err := db.QueryContext(ctx, fmt.Sprintf(`SELECT id, %s FROM %s`, t.columns, t.name))
	if err != nil {
		return false, err
	}
	nFields := strings.Count(t.columns, ",") + 1
	groups := make(map[string][]string)
	for rows.Next() {
		var id string
		fields := make([]sql.NullString, nFields)
		dests := make([]any, 0, nFields+1)
		dests = append(dests, &id)
		for i := range fields {
			dests = append(dests, &fields[i])
		}
		if err := rows.Scan(dests...); err != nil {
			_ = rows.Close()
			return false, err
		}
		digest := rowid.Digest(fields)
		groups[digest] = append(groups[digest], id)
	}
	_ = rows.Close()
	if err := rows.Err(); err != nil {
		return false, err
	}

	type rekey struct{ oldID, newID string }
	var todo []rekey
	for digest, ids := range groups {
		targets := make([]string, len(ids))
		targetSet := make(map[string]bool, len(ids))
		for i := range ids {
			targets[i] = rowid.New(t.name, i, digest)
			targetSet[targets[i]] = true
		}
		held := make(map[string]bool, len(ids))
		var free []string
		for _, id := range ids {
			if targetSet[id] {
				held[id] = true
			} else {
				free = append(free, id)
			}
		}
		if len(free) == 0 {
			continue
		}
		sort.Strings(free)
		i := 0
		for _, target := range targets {
			if held[target] {
				continue
			}
			todo = append(todo, rekey{oldID: free[i], newID: target})
			i++
		}
	}
	// Deterministic UPDATE order (groups is a map) so runs are reproducible.
	sort.Slice(todo, func(i, j int) bool { return todo[i].oldID < todo[j].oldID })

	for _, r := range todo {
		//nolint:gosec // G201: table name is a hardcoded constant, never user input.
		if _, err := db.ExecContext(ctx, fmt.Sprintf(`UPDATE %s SET id = ? WHERE id = ?`, t.name),
			r.newID, r.oldID); err != nil {
			return true, fmt.Errorf("re-key id %s -> %s: %w", r.oldID, r.newID, err)
		}
	}
	return len(todo) > 0, nil
}
