package linear

import "strings"

// LinearLabel pairs a Linear label name with its server-assigned ID.
// IDs are required so renames (same ID, different name) can be detected.
type LinearLabel struct {
	Name string
	ID   string
}

// SnapshotEntry is the persisted form of a single label in the
// last-synced state for a bead.
type SnapshotEntry struct {
	Name string
	ID   string
}

// LabelReconcileInput captures the three label sets the reconciler compares,
// plus the exclusion filter applied before reconciliation.
type LabelReconcileInput struct {
	Beads    []string        // current beads label names (post-exclusion-filterable)
	Linear   []LinearLabel   // current Linear labels with IDs
	Snapshot []SnapshotEntry // last-synced state from linear_label_snapshots
	Exclude  map[string]bool // keys are lowercase label names; nil means no exclusion
}

// LabelReconcileResult is the reconciler's verdict.
// AddToBeads/RemoveFromBeads are by name; Linear sides separate adds (by name,
// to be resolved/created) from removes (by ID, since the IDs are known).
type LabelReconcileResult struct {
	AddToBeads       []string
	RemoveFromBeads  []string
	AddToLinear      []string
	RemoveFromLinear []string
	NewSnapshot      []SnapshotEntry
	RenamesApplied   []LabelRename
}

// LabelRename captures a Linear-side rename that was applied to the bead.
type LabelRename struct {
	OldName string
	NewName string
	ID      string
}

type renameClass struct {
	applied            []LabelRename
	dropped            []LabelRename // OldName + ID only matter; NewName captured for diagnostics
	consumedSnapshotID map[string]bool
	consumedLinearID   map[string]bool
	consumedBeadsName  map[string]bool
}

// classifyRenames is pass 2 of the reconciler. It detects Linear-side renames
// (snapshot ID matches Linear ID, names differ) and decides which to apply
// vs. which to drop based on whether the user has deleted the old name in beads.
//
// "Consume" means: the row should be removed from pass-3's input. The boolean
// maps record what to skip.
//
// Case-insensitive `beadsSet` lookup: rename detection compares snapshot.Name
// against beadsSet using case-folded keys. Without this, a casing mismatch
// (snapshot has "Bug" from a prior Linear sync, bead has "bug") combined with
// a Linear rename would falsely classify as DROPPED rename and emit
// RemoveFromLinear — destroying the Linear label even though the user just
// has a casing inconsistency. The truth table (pass 3) still matches by exact
// name; case-insensitive matching for that broader case is deferred to v2.
func classifyRenames(beads []string, linear []LinearLabel, snap []SnapshotEntry) renameClass {
	r := renameClass{
		consumedSnapshotID: map[string]bool{},
		consumedLinearID:   map[string]bool{},
		consumedBeadsName:  map[string]bool{},
	}
	// beadsSetExact preserves original case for consumption marking.
	// beadsSetFold provides case-insensitive lookup for rename classification.
	beadsSetExact := make(map[string]bool, len(beads))
	beadsSetFold := make(map[string]string, len(beads)) // lower → original
	for _, b := range beads {
		beadsSetExact[b] = true
		beadsSetFold[strings.ToLower(b)] = b
	}
	snapByID := make(map[string]SnapshotEntry, len(snap))
	for _, s := range snap {
		snapByID[s.ID] = s
	}

	for _, l := range linear {
		s, ok := snapByID[l.ID]
		if !ok {
			continue
		}
		if s.Name == l.Name {
			// Names match — pass-3 will see them as in-agreement; no consumption needed.
			continue
		}
		// Case-insensitive: does beads still have the old (pre-rename) name?
		if beadOriginal, exists := beadsSetFold[strings.ToLower(s.Name)]; exists {
			r.applied = append(r.applied, LabelRename{OldName: beadOriginal, NewName: l.Name, ID: l.ID})
			r.consumedSnapshotID[l.ID] = true
			r.consumedLinearID[l.ID] = true
			r.consumedBeadsName[beadOriginal] = true // mark the bead's actual spelling
		} else {
			r.dropped = append(r.dropped, LabelRename{OldName: s.Name, NewName: l.Name, ID: l.ID})
			r.consumedSnapshotID[l.ID] = true
			r.consumedLinearID[l.ID] = true
			// Also consume the new-name beads row if the user happens to have
			// independently re-added the new name (case-insensitive too) — prevents spurious add.
			if beadOriginal, exists := beadsSetFold[strings.ToLower(l.Name)]; exists {
				r.consumedBeadsName[beadOriginal] = true
			}
		}
	}
	return r
}

// applyTruthTable is pass 3 of the reconciler. It takes the post-exclusion
// inputs and the consumption decisions from pass 2, then computes adds/removes
// per the 7-row truth table in the design doc.
//
// It does not handle the rename results themselves — those are emitted
// separately by the orchestrator using the LabelRename entries from pass 2.
//
// Matching is case-insensitive on label names — Linear treats labels case-
// insensitively, and bead casing may diverge from Linear's display casing.
// AddToBeads emits Linear's display case (so beads adopts Linear's spelling
// for new labels). AddToLinear and RemoveFromBeads emit the bead's actual
// spelling so existing local references stay correct.
func applyTruthTable(beads []string, linear []LinearLabel, snap []SnapshotEntry, rc renameClass) LabelReconcileResult {
	// Build presence maps keyed by lowercase name; preserve original casing in values.
	beadsByLower := map[string]string{} // lower → bead's actual spelling
	for _, b := range beads {
		if rc.consumedBeadsName[b] {
			continue
		}
		beadsByLower[strings.ToLower(b)] = b
	}
	linearByLower := map[string]LinearLabel{} // lower → LinearLabel (with Linear's display case)
	for _, l := range linear {
		if rc.consumedLinearID[l.ID] {
			continue
		}
		linearByLower[strings.ToLower(l.Name)] = l
	}
	snapByLower := map[string]SnapshotEntry{} // lower → SnapshotEntry
	for _, s := range snap {
		if rc.consumedSnapshotID[s.ID] {
			continue
		}
		snapByLower[strings.ToLower(s.Name)] = s
	}

	// Union of all lowercase keys across the three sets.
	all := map[string]bool{}
	for k := range beadsByLower {
		all[k] = true
	}
	for k := range linearByLower {
		all[k] = true
	}
	for k := range snapByLower {
		all[k] = true
	}

	var res LabelReconcileResult
	for k := range all {
		beadName, inBeads := beadsByLower[k]
		linearLabel, inLinear := linearByLower[k]
		snapEntry, inSnap := snapByLower[k]

		switch {
		case inSnap && inBeads && inLinear:
			// unchanged (bead and Linear may have different casing — leave as-is)
		case !inSnap && inBeads && !inLinear:
			res.AddToLinear = append(res.AddToLinear, beadName)
		case !inSnap && !inBeads && inLinear:
			res.AddToBeads = append(res.AddToBeads, linearLabel.Name)
		case !inSnap && inBeads && inLinear:
			// agreement (both sides have it, casing may differ — no action)
		case inSnap && !inBeads && inLinear:
			res.RemoveFromLinear = append(res.RemoveFromLinear, snapEntry.ID)
		case inSnap && inBeads && !inLinear:
			res.RemoveFromBeads = append(res.RemoveFromBeads, beadName)
		case inSnap && !inBeads && !inLinear:
			// agreement — nothing
		}
	}
	return res
}

// synthesizeFirstSyncSnapshot returns the intersection of beads and Linear
// label names, with IDs taken from the Linear side. Used as the synthetic
// snapshot input on the first sync for a bead, so the truth table behaves
// as if both sides were already in agreement on shared labels — preventing
// removals while still allowing both-side adds to flow.
//
// Matching is case-insensitive (Linear treats labels case-insensitively); the
// snapshot entry preserves Linear's display case for the canonical name.
func synthesizeFirstSyncSnapshot(beads []string, linear []LinearLabel) []SnapshotEntry {
	beadsByLower := make(map[string]bool, len(beads))
	for _, b := range beads {
		beadsByLower[strings.ToLower(b)] = true
	}
	var out []SnapshotEntry
	for _, l := range linear {
		if beadsByLower[strings.ToLower(l.Name)] {
			out = append(out, SnapshotEntry{Name: l.Name, ID: l.ID})
		}
	}
	return out
}

// ReconcileLabels runs the three-pass reconciler.
//
// Pass 1: apply exclusion filter to all input sets.
// Pass 2: classify Linear-side renames (rename map + per-row consumption flags).
// Pass 3: run the per-label decision table on the unconsumed rows.
//
// Returns adds/removes for each side, the rename events to surface, and the
// next snapshot. Callers apply the mutations and persist NewSnapshot inside a
// transaction (see internal/tracker/engine.go for the integration point).
//
// First-sync rule: if Snapshot is empty and either side has labels, synthesize
// a snapshot equal to the intersection of beads and Linear, then run normally.
// This guarantees no removals on first sync (rows in only one side become adds,
// rows in both become in-agreement).
func ReconcileLabels(in LabelReconcileInput) LabelReconcileResult {
	beads, linear, snap := applyExclusionFilter(in)

	if len(snap) == 0 && (len(beads) > 0 || len(linear) > 0) {
		snap = synthesizeFirstSyncSnapshot(beads, linear)
	}

	rc := classifyRenames(beads, linear, snap)
	res := applyTruthTable(beads, linear, snap, rc)

	// Apply rename effects to the user-visible result.
	for _, r := range rc.applied {
		res.RemoveFromBeads = append(res.RemoveFromBeads, r.OldName)
		res.AddToBeads = append(res.AddToBeads, r.NewName)
		res.RenamesApplied = append(res.RenamesApplied, r)
	}
	// For dropped renames, the user deleted the OLD name locally so the rename
	// is not propagated. Normally we mirror that delete to Linear by removing
	// the renamed label. BUT if the user has independently re-added the NEW
	// name in beads (case-insensitive), the local state already matches Linear
	// and we should leave the label alone — pass-2 marks this with
	// consumedBeadsName on the new-name row.
	beadsLower := make(map[string]bool, len(beads))
	for _, b := range beads {
		beadsLower[strings.ToLower(b)] = true
	}
	for _, r := range rc.dropped {
		if beadsLower[strings.ToLower(r.NewName)] {
			// Local re-add absorbs the rename: don't touch Linear, but DO record
			// it in RenamesApplied so the snapshot updates to the new name.
			res.RenamesApplied = append(res.RenamesApplied, r)
			continue
		}
		res.RemoveFromLinear = append(res.RemoveFromLinear, r.ID)
	}

	res.NewSnapshot = computeNewSnapshot(snap, linear, res)
	return res
}

// computeNewSnapshot builds the snapshot to persist after the pull-side
// reconciler runs. It represents "last-known agreement between beads and
// Linear", preserved until push completes its own snapshot write.
//
// Algorithm: start from the input snapshot, then:
//   - Apply renames (RenamesApplied): change OldName to NewName, ID stays.
//   - Apply RemoveFromBeads: drop from snapshot.
//   - Apply AddToBeads: add with Linear's ID/Name.
//   - LEAVE entries for pending push-side mutations (RemoveFromLinear,
//     AddToLinear) untouched. Push computes its own snapshot after the
//     Linear API call succeeds; if push fails or is skipped, the snapshot
//     correctly reflects what we last agreed on, so the next sync's
//     reconciler sees the same delta and retries.
func computeNewSnapshot(snapshot []SnapshotEntry, linear []LinearLabel, res LabelReconcileResult) []SnapshotEntry {
	// Use lowercase keys so case mismatches between snapshot, beads, and Linear
	// don't produce duplicate entries or missed lookups. Values preserve the
	// canonical display case (Linear's, when known).
	out := make(map[string]SnapshotEntry, len(snapshot))
	for _, s := range snapshot {
		out[strings.ToLower(s.Name)] = s
	}

	// Apply renames: match the snapshot entry by ID (case-insensitive on names),
	// then replace with the new name from the rename event.
	for _, r := range res.RenamesApplied {
		var matchKey string
		for k, v := range out {
			if v.ID == r.ID {
				matchKey = k
				break
			}
		}
		if matchKey != "" {
			delete(out, matchKey)
			out[strings.ToLower(r.NewName)] = SnapshotEntry{Name: r.NewName, ID: r.ID}
		}
	}

	// Apply RemoveFromBeads: drop from snapshot (case-insensitive).
	for _, n := range res.RemoveFromBeads {
		delete(out, strings.ToLower(n))
	}

	// Apply AddToBeads: add with Linear's display case (case-insensitive lookup).
	linearByLower := make(map[string]LinearLabel, len(linear))
	for _, l := range linear {
		linearByLower[strings.ToLower(l.Name)] = l
	}
	for _, n := range res.AddToBeads {
		if l, ok := linearByLower[strings.ToLower(n)]; ok {
			out[strings.ToLower(l.Name)] = SnapshotEntry{Name: l.Name, ID: l.ID}
		}
	}

	// RemoveFromLinear / AddToLinear are push-side concerns — NOT applied here.
	// Push computes its own snapshot after the Linear API call. If push fails or
	// is skipped, snapshot entries for those pending actions stay, so next
	// sync's reconciler sees the same delta and retries.

	result := make([]SnapshotEntry, 0, len(out))
	for _, v := range out {
		result = append(result, v)
	}
	return result
}

// applyExclusionFilter returns the three input sets with excluded labels removed.
// Matching is case-insensitive on the label name.
func applyExclusionFilter(in LabelReconcileInput) (beads []string, linear []LinearLabel, snap []SnapshotEntry) {
	excluded := func(name string) bool {
		if in.Exclude == nil {
			return false
		}
		return in.Exclude[strings.ToLower(name)]
	}
	for _, n := range in.Beads {
		if !excluded(n) {
			beads = append(beads, n)
		}
	}
	for _, l := range in.Linear {
		if !excluded(l.Name) {
			linear = append(linear, l)
		}
	}
	for _, s := range in.Snapshot {
		if !excluded(s.Name) {
			snap = append(snap, s)
		}
	}
	return beads, linear, snap
}
