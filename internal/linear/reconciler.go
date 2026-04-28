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
func applyTruthTable(beads []string, linear []LinearLabel, snap []SnapshotEntry, rc renameClass) LabelReconcileResult {
	// Build presence sets, skipping consumed rows.
	beadsSet := map[string]bool{}
	for _, b := range beads {
		if rc.consumedBeadsName[b] {
			continue
		}
		beadsSet[b] = true
	}
	linearByName := map[string]LinearLabel{}
	for _, l := range linear {
		if rc.consumedLinearID[l.ID] {
			continue
		}
		linearByName[l.Name] = l
	}
	snapByName := map[string]SnapshotEntry{}
	for _, s := range snap {
		if rc.consumedSnapshotID[s.ID] {
			continue
		}
		snapByName[s.Name] = s
	}

	// Union of all names across the three sets.
	all := map[string]bool{}
	for n := range beadsSet {
		all[n] = true
	}
	for n := range linearByName {
		all[n] = true
	}
	for n := range snapByName {
		all[n] = true
	}

	var res LabelReconcileResult
	for n := range all {
		inBeads := beadsSet[n]
		_, inLinear := linearByName[n]
		snapEntry, inSnap := snapByName[n]

		switch {
		case inSnap && inBeads && inLinear:
			// unchanged
		case !inSnap && inBeads && !inLinear:
			res.AddToLinear = append(res.AddToLinear, n)
		case !inSnap && !inBeads && inLinear:
			res.AddToBeads = append(res.AddToBeads, n)
		case !inSnap && inBeads && inLinear:
			// agreement — nothing
		case inSnap && !inBeads && inLinear:
			res.RemoveFromLinear = append(res.RemoveFromLinear, snapEntry.ID)
		case inSnap && inBeads && !inLinear:
			res.RemoveFromBeads = append(res.RemoveFromBeads, n)
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
func synthesizeFirstSyncSnapshot(beads []string, linear []LinearLabel) []SnapshotEntry {
	beadsSet := make(map[string]bool, len(beads))
	for _, b := range beads {
		beadsSet[b] = true
	}
	var out []SnapshotEntry
	for _, l := range linear {
		if beadsSet[l.Name] {
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
			continue
		}
		res.RemoveFromLinear = append(res.RemoveFromLinear, r.ID)
	}

	res.NewSnapshot = computeNewSnapshot(beads, linear, res)
	return res
}

// computeNewSnapshot builds the post-sync snapshot. It contains an entry for
// every label that exists on BOTH sides after the reconciler's mutations would
// be applied. Labels added to Linear via auto-create are NOT included here —
// the caller adds them after resolving/creating IDs.
func computeNewSnapshot(beads []string, linear []LinearLabel, res LabelReconcileResult) []SnapshotEntry {
	// Project end-state on each side.
	beadsEnd := make(map[string]bool, len(beads))
	for _, b := range beads {
		beadsEnd[b] = true
	}
	for _, n := range res.RemoveFromBeads {
		delete(beadsEnd, n)
	}
	for _, n := range res.AddToBeads {
		beadsEnd[n] = true
	}

	linearEnd := make(map[string]LinearLabel, len(linear))
	for _, l := range linear {
		linearEnd[l.Name] = l
	}
	for _, id := range res.RemoveFromLinear {
		for name, l := range linearEnd {
			if l.ID == id {
				delete(linearEnd, name)
			}
		}
	}
	// AddToLinear has no IDs yet; caller resolves and re-snapshot writes after push.

	out := make([]SnapshotEntry, 0)
	for name, l := range linearEnd {
		if beadsEnd[name] {
			out = append(out, SnapshotEntry{Name: name, ID: l.ID})
		}
	}
	return out
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
