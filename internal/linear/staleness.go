package linear

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/steveyegge/beads/internal/storage"
	"github.com/steveyegge/beads/internal/tracker"
)

const (
	lastPullFileName      = "last_pull"
	lastPullMetaKey       = "linear.last_pull_at"
	DefaultStaleThreshold = 20 * time.Minute
	debounceThreshold     = 5 * time.Minute
)

// WriteLastPullTimestamp writes the current time as ISO 8601 to .beads/last_pull.
func WriteLastPullTimestamp(beadsDir string) error {
	if beadsDir == "" {
		return fmt.Errorf("beadsDir must not be empty")
	}
	path := filepath.Join(beadsDir, lastPullFileName)
	ts := time.Now().UTC().Format(time.RFC3339)
	return os.WriteFile(path, []byte(ts+"\n"), 0600)
}

// ReadLastPullTimestamp reads the last pull timestamp from .beads/last_pull.
// Returns the zero time if the file doesn't exist or is unreadable.
func ReadLastPullTimestamp(beadsDir string) (time.Time, error) {
	if beadsDir == "" {
		return time.Time{}, fmt.Errorf("beadsDir must not be empty")
	}
	path := filepath.Join(beadsDir, lastPullFileName)
	data, err := os.ReadFile(path) // #nosec G304 -- path is constrained to the beads directory.
	if err != nil {
		if os.IsNotExist(err) {
			return time.Time{}, nil
		}
		return time.Time{}, fmt.Errorf("reading last_pull: %w", err)
	}
	ts := strings.TrimSpace(string(data))
	if ts == "" {
		return time.Time{}, nil
	}
	t, err := time.Parse(time.RFC3339, ts)
	if err != nil {
		return time.Time{}, fmt.Errorf("parsing last_pull timestamp %q: %w", ts, err)
	}
	return t, nil
}

// IsPullStale returns true if the last pull is older than the given threshold,
// or if no pull has ever been recorded.
func IsPullStale(beadsDir string, threshold time.Duration) bool {
	lastPull, err := ReadLastPullTimestamp(beadsDir)
	if err != nil || lastPull.IsZero() {
		return true
	}
	return time.Since(lastPull) > threshold
}

// StalenessInfo holds computed staleness details for display purposes.
type StalenessInfo struct {
	LastPull    time.Time
	Age         time.Duration
	IsFresh     bool
	IsStale     bool
	NeverPulled bool
	// WithinDebounce reports whether the last pull completed within the
	// debounce window (see IsWithinDebounce); always false when NeverPulled.
	WithinDebounce bool
}

// LastPullFallback supplies a last-pull timestamp from an alternate source
// when .beads/last_pull yields none — typically the tracker's recorded
// last-sync metadata in the store. It is consulted only when the file is
// absent or unreadable, so the common path stays free of DB access.
type LastPullFallback func() time.Time

// GetStalenessInfo returns detailed staleness information for display and logic.
func GetStalenessInfo(beadsDir string, threshold time.Duration) StalenessInfo {
	return GetStalenessInfoWithFallback(beadsDir, threshold, nil)
}

// GetStalenessInfoWithFallback is GetStalenessInfo with a fallback source
// for the last-pull timestamp. Databases synced by bd versions that predate
// the last_pull file — and fresh clones, since the file is per-machine and
// gitignored — have tracker sync metadata but no file; without a fallback
// they would report NeverPulled forever (bd-stc).
func GetStalenessInfoWithFallback(beadsDir string, threshold time.Duration, fallback LastPullFallback) StalenessInfo {
	lastPull, err := ReadLastPullTimestamp(beadsDir)
	if (err != nil || lastPull.IsZero()) && fallback != nil {
		lastPull = fallback()
		err = nil
	}
	if err != nil || lastPull.IsZero() {
		return StalenessInfo{NeverPulled: true, IsStale: true}
	}
	age := time.Since(lastPull)
	return StalenessInfo{
		LastPull:       lastPull,
		Age:            age,
		IsFresh:        age <= threshold,
		IsStale:        age > threshold,
		WithinDebounce: age <= debounceThreshold,
	}
}

// RecordLastPullMetadata mirrors WriteLastPullTimestamp into store-local
// metadata. The .beads/last_pull file is per-machine (and gitignored), so
// it vanishes on fresh clones; the metadata stamp travels with the database
// and lets staleness fallbacks distinguish "pulled recently elsewhere" from
// "never pulled" (bd-stc).
func RecordLastPullMetadata(ctx context.Context, store storage.Storage) error {
	ts := time.Now().UTC().Format(time.RFC3339)
	return store.SetLocalMetadata(ctx, lastPullMetaKey, ts)
}

// StoreLastPullFallback returns a LastPullFallback backed by store evidence
// of past pulls. It prefers the exact last_pull_at stamp (dual-written with
// the last_pull file by the pull paths) and falls back to the tracker's
// last-sync timestamp for databases synced before the stamp existed.
// last_sync moves on any successful sync — including push-only — so on
// those legacy databases a push-only sync can defer the next pull by up to
// the staleness threshold; the alternative is the false NeverPulled that
// bd-stc exists to fix, and the window closes for good on the database's
// first pull with a stamping bd.
func StoreLastPullFallback(ctx context.Context, store storage.Storage) LastPullFallback {
	return func() time.Time {
		if raw, err := store.GetLocalMetadata(ctx, lastPullMetaKey); err == nil {
			if ts := strings.TrimSpace(raw); ts != "" {
				if t, perr := time.Parse(time.RFC3339, ts); perr == nil {
					return t
				}
			}
		}
		return tracker.LastSyncTime(ctx, store, "linear")
	}
}

// IsWithinDebounce returns true if the last pull completed within the
// debounce window (5 minutes), preventing agent loops.
func IsWithinDebounce(beadsDir string) bool {
	lastPull, err := ReadLastPullTimestamp(beadsDir)
	if err != nil || lastPull.IsZero() {
		return false
	}
	return time.Since(lastPull) <= debounceThreshold
}

// FormatAge formats a duration as a human-friendly string like "5m" or "2h30m".
func FormatAge(d time.Duration) string {
	if d < time.Minute {
		return fmt.Sprintf("%ds", int(d.Seconds()))
	}
	if d < time.Hour {
		return fmt.Sprintf("%dm", int(d.Minutes()))
	}
	hours := int(d.Hours())
	mins := int(d.Minutes()) % 60
	if mins == 0 {
		return fmt.Sprintf("%dh", hours)
	}
	return fmt.Sprintf("%dh%dm", hours, mins)
}
