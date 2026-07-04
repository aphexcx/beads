package linear

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/steveyegge/beads/internal/storage"
)

func TestWriteAndReadLastPullTimestamp(t *testing.T) {
	dir := t.TempDir()

	if err := WriteLastPullTimestamp(dir); err != nil {
		t.Fatalf("WriteLastPullTimestamp: %v", err)
	}

	got, err := ReadLastPullTimestamp(dir)
	if err != nil {
		t.Fatalf("ReadLastPullTimestamp: %v", err)
	}
	if got.IsZero() {
		t.Fatal("expected non-zero timestamp after write")
	}
	if time.Since(got) > 5*time.Second {
		t.Fatalf("timestamp too old: %v (expected within 5s of now)", got)
	}
}

func TestReadLastPullTimestamp_MissingFile(t *testing.T) {
	dir := t.TempDir()

	got, err := ReadLastPullTimestamp(dir)
	if err != nil {
		t.Fatalf("expected nil error for missing file, got: %v", err)
	}
	if !got.IsZero() {
		t.Fatalf("expected zero time for missing file, got: %v", got)
	}
}

func TestReadLastPullTimestamp_EmptyFile(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, lastPullFileName), []byte(""), 0644); err != nil {
		t.Fatal(err)
	}

	got, err := ReadLastPullTimestamp(dir)
	if err != nil {
		t.Fatalf("expected nil error for empty file, got: %v", err)
	}
	if !got.IsZero() {
		t.Fatalf("expected zero time for empty file, got: %v", got)
	}
}

func TestReadLastPullTimestamp_MalformedFile(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, lastPullFileName), []byte("not-a-timestamp\n"), 0644); err != nil {
		t.Fatal(err)
	}

	_, err := ReadLastPullTimestamp(dir)
	if err == nil {
		t.Fatal("expected error for malformed timestamp")
	}
}

func TestIsPullStale(t *testing.T) {
	t.Run("missing file is always stale", func(t *testing.T) {
		dir := t.TempDir()
		if !IsPullStale(dir, 20*time.Minute) {
			t.Fatal("expected stale when last_pull file is missing")
		}
	})

	t.Run("just written is fresh", func(t *testing.T) {
		dir := t.TempDir()
		if err := WriteLastPullTimestamp(dir); err != nil {
			t.Fatal(err)
		}
		if IsPullStale(dir, 20*time.Minute) {
			t.Fatal("expected fresh immediately after write")
		}
	})

	t.Run("old timestamp is stale", func(t *testing.T) {
		dir := t.TempDir()
		oldTime := time.Now().UTC().Add(-30 * time.Minute).Format(time.RFC3339)
		if err := os.WriteFile(filepath.Join(dir, lastPullFileName), []byte(oldTime+"\n"), 0644); err != nil {
			t.Fatal(err)
		}
		if !IsPullStale(dir, 20*time.Minute) {
			t.Fatal("expected stale for 30m-old timestamp with 20m threshold")
		}
	})
}

func TestIsWithinDebounce(t *testing.T) {
	t.Run("no file returns false", func(t *testing.T) {
		dir := t.TempDir()
		if IsWithinDebounce(dir) {
			t.Fatal("expected false when file doesn't exist")
		}
	})

	t.Run("just written returns true", func(t *testing.T) {
		dir := t.TempDir()
		if err := WriteLastPullTimestamp(dir); err != nil {
			t.Fatal(err)
		}
		if !IsWithinDebounce(dir) {
			t.Fatal("expected within debounce immediately after write")
		}
	})

	t.Run("old timestamp returns false", func(t *testing.T) {
		dir := t.TempDir()
		oldTime := time.Now().UTC().Add(-10 * time.Minute).Format(time.RFC3339)
		if err := os.WriteFile(filepath.Join(dir, lastPullFileName), []byte(oldTime+"\n"), 0644); err != nil {
			t.Fatal(err)
		}
		if IsWithinDebounce(dir) {
			t.Fatal("expected not within debounce for 10m-old timestamp")
		}
	})
}

func TestGetStalenessInfo(t *testing.T) {
	t.Run("never pulled", func(t *testing.T) {
		dir := t.TempDir()
		info := GetStalenessInfo(dir, 20*time.Minute)
		if !info.NeverPulled {
			t.Fatal("expected NeverPulled=true")
		}
		if !info.IsStale {
			t.Fatal("expected IsStale=true when never pulled")
		}
	})

	t.Run("fresh pull", func(t *testing.T) {
		dir := t.TempDir()
		if err := WriteLastPullTimestamp(dir); err != nil {
			t.Fatal(err)
		}
		info := GetStalenessInfo(dir, 20*time.Minute)
		if info.NeverPulled {
			t.Fatal("expected NeverPulled=false")
		}
		if !info.IsFresh {
			t.Fatal("expected IsFresh=true")
		}
		if info.IsStale {
			t.Fatal("expected IsStale=false")
		}
	})
}

func TestGetStalenessInfoWithFallback(t *testing.T) {
	threshold := 20 * time.Minute

	t.Run("file present ignores fallback", func(t *testing.T) {
		dir := t.TempDir()
		if err := WriteLastPullTimestamp(dir); err != nil {
			t.Fatal(err)
		}
		called := false
		info := GetStalenessInfoWithFallback(dir, threshold, func() time.Time {
			called = true
			return time.Time{}
		})
		if called {
			t.Fatal("fallback must not be consulted when last_pull exists")
		}
		if info.NeverPulled || !info.IsFresh {
			t.Fatalf("expected fresh from file, got %+v", info)
		}
	})

	t.Run("missing file uses fresh fallback", func(t *testing.T) {
		dir := t.TempDir()
		lastSync := time.Now().UTC().Add(-2 * time.Minute)
		info := GetStalenessInfoWithFallback(dir, threshold, func() time.Time { return lastSync })
		if info.NeverPulled {
			t.Fatal("expected NeverPulled=false when fallback supplies a timestamp")
		}
		if !info.IsFresh || info.IsStale {
			t.Fatalf("expected fresh for 2m-old fallback with 20m threshold, got %+v", info)
		}
		if !info.WithinDebounce {
			t.Fatal("expected WithinDebounce=true for 2m-old fallback")
		}
		if !info.LastPull.Equal(lastSync) {
			t.Fatalf("expected LastPull=%v, got %v", lastSync, info.LastPull)
		}
	})

	t.Run("missing file uses stale fallback", func(t *testing.T) {
		dir := t.TempDir()
		info := GetStalenessInfoWithFallback(dir, threshold, func() time.Time {
			return time.Now().UTC().Add(-30 * time.Minute)
		})
		if info.NeverPulled {
			t.Fatal("expected NeverPulled=false when fallback supplies a timestamp")
		}
		if !info.IsStale || info.IsFresh {
			t.Fatalf("expected stale for 30m-old fallback with 20m threshold, got %+v", info)
		}
		if info.WithinDebounce {
			t.Fatal("expected WithinDebounce=false for 30m-old fallback")
		}
	})

	t.Run("missing file with zero fallback reports never pulled", func(t *testing.T) {
		dir := t.TempDir()
		info := GetStalenessInfoWithFallback(dir, threshold, func() time.Time { return time.Time{} })
		if !info.NeverPulled || !info.IsStale {
			t.Fatalf("expected NeverPulled+IsStale for zero fallback, got %+v", info)
		}
	})

	t.Run("missing file with nil fallback reports never pulled", func(t *testing.T) {
		dir := t.TempDir()
		info := GetStalenessInfoWithFallback(dir, threshold, nil)
		if !info.NeverPulled || !info.IsStale {
			t.Fatalf("expected NeverPulled+IsStale for nil fallback, got %+v", info)
		}
	})

	t.Run("malformed file consults fallback", func(t *testing.T) {
		dir := t.TempDir()
		if err := os.WriteFile(filepath.Join(dir, lastPullFileName), []byte("not-a-timestamp\n"), 0644); err != nil {
			t.Fatal(err)
		}
		info := GetStalenessInfoWithFallback(dir, threshold, func() time.Time {
			return time.Now().UTC().Add(-2 * time.Minute)
		})
		if info.NeverPulled || !info.IsFresh {
			t.Fatalf("expected fallback to cover malformed file, got %+v", info)
		}
	})

	t.Run("stale file is not shadowed by fresh fallback", func(t *testing.T) {
		dir := t.TempDir()
		oldTime := time.Now().UTC().Add(-30 * time.Minute).Format(time.RFC3339)
		if err := os.WriteFile(filepath.Join(dir, lastPullFileName), []byte(oldTime+"\n"), 0644); err != nil {
			t.Fatal(err)
		}
		called := false
		info := GetStalenessInfoWithFallback(dir, threshold, func() time.Time {
			called = true
			return time.Now().UTC()
		})
		if called {
			t.Fatal("fallback must not be consulted when last_pull has a valid timestamp")
		}
		if info.NeverPulled || !info.IsStale || info.IsFresh {
			t.Fatalf("expected stale from 30m-old file with 20m threshold, got %+v", info)
		}
	})
}

// stalenessStubStore is a minimal storage.Storage for the store-backed
// staleness helpers; unimplemented methods panic via the nil embedded
// interface.
type stalenessStubStore struct {
	storage.Storage
	localMetadata map[string]string
	config        map[string]string
}

func newStalenessStubStore() *stalenessStubStore {
	return &stalenessStubStore{
		localMetadata: make(map[string]string),
		config:        make(map[string]string),
	}
}

func (s *stalenessStubStore) GetLocalMetadata(_ context.Context, key string) (string, error) {
	return s.localMetadata[key], nil
}

func (s *stalenessStubStore) SetLocalMetadata(_ context.Context, key, value string) error {
	s.localMetadata[key] = value
	return nil
}

func (s *stalenessStubStore) GetConfig(_ context.Context, key string) (string, error) {
	return s.config[key], nil
}

func TestStoreLastPullFallback(t *testing.T) {
	ctx := context.Background()

	t.Run("empty store returns zero", func(t *testing.T) {
		store := newStalenessStubStore()
		if got := StoreLastPullFallback(ctx, store)(); !got.IsZero() {
			t.Errorf("expected zero time from empty store, got %v", got)
		}
	})

	t.Run("prefers last_pull_at stamp over last_sync", func(t *testing.T) {
		store := newStalenessStubStore()
		pullAt := time.Date(2026, 7, 4, 10, 0, 0, 0, time.UTC)
		store.localMetadata[lastPullMetaKey] = pullAt.Format(time.RFC3339)
		store.localMetadata["linear.last_sync"] = time.Date(2026, 7, 4, 12, 0, 0, 0, time.UTC).Format(time.RFC3339)
		if got := StoreLastPullFallback(ctx, store)(); !got.Equal(pullAt) {
			t.Errorf("expected last_pull_at %v to win, got %v", pullAt, got)
		}
	})

	t.Run("legacy database falls back to last_sync metadata", func(t *testing.T) {
		store := newStalenessStubStore()
		lastSync := time.Date(2026, 7, 4, 12, 30, 0, 0, time.UTC)
		store.localMetadata["linear.last_sync"] = lastSync.Format(time.RFC3339)
		if got := StoreLastPullFallback(ctx, store)(); !got.Equal(lastSync) {
			t.Errorf("expected last_sync fallback %v, got %v", lastSync, got)
		}
	})

	t.Run("oldest databases fall back to config last_sync", func(t *testing.T) {
		store := newStalenessStubStore()
		lastSync := time.Date(2026, 6, 9, 20, 11, 39, 0, time.UTC)
		store.config["linear.last_sync"] = lastSync.Format(time.RFC3339)
		if got := StoreLastPullFallback(ctx, store)(); !got.Equal(lastSync) {
			t.Errorf("expected config last_sync fallback %v, got %v", lastSync, got)
		}
	})

	t.Run("malformed last_pull_at falls back to last_sync", func(t *testing.T) {
		store := newStalenessStubStore()
		store.localMetadata[lastPullMetaKey] = "not-a-timestamp"
		lastSync := time.Date(2026, 7, 4, 12, 30, 0, 0, time.UTC)
		store.localMetadata["linear.last_sync"] = lastSync.Format(time.RFC3339)
		if got := StoreLastPullFallback(ctx, store)(); !got.Equal(lastSync) {
			t.Errorf("expected last_sync fallback %v for malformed stamp, got %v", lastSync, got)
		}
	})
}

func TestRecordLastPullMetadata(t *testing.T) {
	ctx := context.Background()
	store := newStalenessStubStore()

	if err := RecordLastPullMetadata(ctx, store); err != nil {
		t.Fatalf("RecordLastPullMetadata: %v", err)
	}
	raw := store.localMetadata[lastPullMetaKey]
	if raw == "" {
		t.Fatal("expected last_pull_at metadata to be written")
	}
	stamped, err := time.Parse(time.RFC3339, raw)
	if err != nil {
		t.Fatalf("stamp %q not RFC3339: %v", raw, err)
	}
	if time.Since(stamped) > 5*time.Second {
		t.Fatalf("stamp too old: %v", stamped)
	}

	// The stamp must satisfy the read side.
	if got := StoreLastPullFallback(ctx, store)(); !got.Equal(stamped) {
		t.Errorf("StoreLastPullFallback = %v, want stamped %v", got, stamped)
	}
}

func TestFormatAge(t *testing.T) {
	tests := []struct {
		d    time.Duration
		want string
	}{
		{30 * time.Second, "30s"},
		{5 * time.Minute, "5m"},
		{45 * time.Minute, "45m"},
		{1 * time.Hour, "1h"},
		{90 * time.Minute, "1h30m"},
		{2*time.Hour + 15*time.Minute, "2h15m"},
	}
	for _, tt := range tests {
		got := FormatAge(tt.d)
		if got != tt.want {
			t.Errorf("FormatAge(%v) = %q, want %q", tt.d, got, tt.want)
		}
	}
}

func TestPullIfStaleSkipsWhenFresh(t *testing.T) {
	dir := t.TempDir()

	// Write a recent timestamp — data is fresh
	if err := WriteLastPullTimestamp(dir); err != nil {
		t.Fatal(err)
	}

	// Verify that IsPullStale returns false (would skip pull)
	if IsPullStale(dir, DefaultStaleThreshold) {
		t.Fatal("expected IsPullStale=false immediately after writing timestamp")
	}

	// Verify StalenessInfo agrees
	info := GetStalenessInfo(dir, DefaultStaleThreshold)
	if !info.IsFresh {
		t.Fatal("expected IsFresh=true")
	}
	if info.IsStale {
		t.Fatal("expected IsStale=false for fresh data")
	}
}

func TestPullIfStaleDebounce(t *testing.T) {
	dir := t.TempDir()

	// Write a timestamp that's past the threshold (stale) but within debounce
	// e.g., 3 minutes ago: past a 1-minute threshold but within the 5-minute debounce
	recentTime := time.Now().UTC().Add(-3 * time.Minute).Format(time.RFC3339)
	if err := os.WriteFile(filepath.Join(dir, lastPullFileName), []byte(recentTime+"\n"), 0644); err != nil {
		t.Fatal(err)
	}

	// With a 1-minute threshold, data is "stale" by threshold
	if !IsPullStale(dir, 1*time.Minute) {
		t.Fatal("expected stale with 1m threshold and 3m-old timestamp")
	}

	// But the debounce window (5 min) should prevent a pull
	if !IsWithinDebounce(dir) {
		t.Fatal("expected within debounce for 3m-old timestamp (debounce is 5m)")
	}

	// Write a timestamp that's outside the debounce window
	oldTime := time.Now().UTC().Add(-10 * time.Minute).Format(time.RFC3339)
	if err := os.WriteFile(filepath.Join(dir, lastPullFileName), []byte(oldTime+"\n"), 0644); err != nil {
		t.Fatal(err)
	}

	// Both stale and outside debounce
	if !IsPullStale(dir, 1*time.Minute) {
		t.Fatal("expected stale for 10m-old timestamp")
	}
	if IsWithinDebounce(dir) {
		t.Fatal("expected not within debounce for 10m-old timestamp")
	}
}

func TestWriteLastPullTimestamp_EmptyDir(t *testing.T) {
	err := WriteLastPullTimestamp("")
	if err == nil {
		t.Fatal("expected error for empty beadsDir")
	}
}

func TestReadLastPullTimestamp_EmptyDir(t *testing.T) {
	_, err := ReadLastPullTimestamp("")
	if err == nil {
		t.Fatal("expected error for empty beadsDir")
	}
}
