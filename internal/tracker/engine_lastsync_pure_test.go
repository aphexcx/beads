// Pure-Go tests for the LastSync/LastSyncTime helpers. Runs without cgo so
// it gates changes even where the shared Dolt test server is unavailable.

package tracker

import (
	"context"
	"testing"
	"time"
)

func TestLastSyncTime(t *testing.T) {
	ctx := context.Background()

	t.Run("never synced returns zero", func(t *testing.T) {
		store := newPureTestStore()
		if got := LastSyncTime(ctx, store, "linear"); !got.IsZero() {
			t.Errorf("LastSyncTime() on fresh store = %v, want zero", got)
		}
	})

	t.Run("parses local metadata timestamp", func(t *testing.T) {
		store := newPureTestStore()
		want := time.Date(2026, 7, 1, 12, 30, 15, 0, time.UTC)
		if err := store.SetLocalMetadata(ctx, "linear.last_sync", want.Format(time.RFC3339Nano)); err != nil {
			t.Fatal(err)
		}
		if got := LastSyncTime(ctx, store, "linear"); !got.Equal(want) {
			t.Errorf("LastSyncTime() = %v, want %v", got, want)
		}
	})

	t.Run("falls back to config for pre-refactor databases", func(t *testing.T) {
		store := newPureTestStore()
		if err := store.SetConfig(ctx, "linear.last_sync", "2026-06-09T20:11:39Z"); err != nil {
			t.Fatal(err)
		}
		want := time.Date(2026, 6, 9, 20, 11, 39, 0, time.UTC)
		if got := LastSyncTime(ctx, store, "linear"); !got.Equal(want) {
			t.Errorf("LastSyncTime() = %v, want %v", got, want)
		}
	})

	t.Run("unparseable value returns zero", func(t *testing.T) {
		store := newPureTestStore()
		if err := store.SetLocalMetadata(ctx, "linear.last_sync", "not-a-timestamp"); err != nil {
			t.Fatal(err)
		}
		if got := LastSyncTime(ctx, store, "linear"); !got.IsZero() {
			t.Errorf("LastSyncTime() with invalid value = %v, want zero", got)
		}
	})
}
