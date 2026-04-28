package linear

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
