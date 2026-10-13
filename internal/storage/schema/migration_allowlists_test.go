package schema

import (
	"os"
	"path"
	"strconv"
	"strings"
	"testing"
)

// The two allowlists scripts/check-migration-hygiene.sh consults beside
// frozen-edit-allowlist.txt (gf-xid finding 2g) are each pinned here to the
// Go fact they stand on, so the script can never sanction more than the
// binary carries: a rename row must be one the lineage reconciler
// fingerprints (upstreamIgnoredBaseFiles), and a prepared-DML row must name
// the file's current bytes and a file the fresh bundle overrides with direct
// SQL (cliCompatibleMigrationSQL).

// allowlistRows returns the non-comment, non-blank rows of a migrations/
// allowlist as whitespace-split fields, with their 1-based line numbers.
func allowlistRows(t *testing.T, name string) map[int][]string {
	t.Helper()
	data, err := os.ReadFile(path.Join("migrations", name))
	if err != nil {
		t.Fatalf("read %s: %v", name, err)
	}
	rows := map[int][]string{}
	for i, line := range strings.Split(string(data), "\n") {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		rows[i+1] = strings.Fields(line)
	}
	return rows
}

func isLowerHexSHA256(s string) bool {
	return len(s) == 64 && strings.Trim(s, "0123456789abcdef") == ""
}

// TestFrozenRenameAllowlistMatchesTheReconcilersRows pins
// migrations/frozen-rename-allowlist.txt to upstreamIgnoredBaseFiles: every
// row renames upstream's ignored file NNNN (the upstream cursor row) to the
// file this lineage ships, the hash is the current bytes of that file, the
// two names differ only by their number, and every reconciler row has
// exactly one allowlist row. A rename the reconciler does not fingerprint is
// refused here before the script could ever sanction it.
func TestFrozenRenameAllowlistMatchesTheReconcilersRows(t *testing.T) {
	rows := allowlistRows(t, "frozen-rename-allowlist.txt")
	seen := map[int]string{}
	for lineNo, fields := range rows {
		if len(fields) < 4 {
			t.Fatalf("frozen-rename-allowlist.txt:%d: want <old> <new> <sha256> <justification>, got %q", lineNo, strings.Join(fields, " "))
		}
		oldPath, newPath, hash := fields[0], fields[1], fields[2]
		if !isLowerHexSHA256(hash) {
			t.Fatalf("frozen-rename-allowlist.txt:%d: %q is not a lowercase hex sha256", lineNo, hash)
		}
		oldName, okOld := strings.CutPrefix(oldPath, "ignored/")
		newName, okNew := strings.CutPrefix(newPath, "ignored/")
		if !okOld || !okNew {
			t.Fatalf("frozen-rename-allowlist.txt:%d: the reconciler fingerprints ignored-chain renames only, got %s -> %s", lineNo, oldPath, newPath)
		}
		version, err := strconv.Atoi(strings.SplitN(oldName, "_", 2)[0])
		if err != nil {
			t.Fatalf("frozen-rename-allowlist.txt:%d: old name %q does not start with a version number", lineNo, oldName)
		}
		want, ok := upstreamIgnoredBaseFiles[version]
		if !ok {
			t.Errorf("frozen-rename-allowlist.txt:%d: upstream ignored row %d is not one upstreamIgnoredBaseFiles reconciles", lineNo, version)
			continue
		}
		if newName != want {
			t.Errorf("frozen-rename-allowlist.txt:%d: row %d renames to %s, the reconciler maps it to %s", lineNo, version, newName, want)
		}
		if oldSuffix, newSuffix := strings.SplitN(oldName, "_", 2)[1], strings.SplitN(newName, "_", 2)[1]; oldSuffix != newSuffix {
			t.Errorf("frozen-rename-allowlist.txt:%d: %s and %s differ beyond their number", lineNo, oldName, newName)
		}
		if current := forkFileHash(t, ignoredSource, newName); current != hash {
			t.Errorf("frozen-rename-allowlist.txt:%d: hash %s is not the current bytes of ignored/%s (%s); a rename that also edits is never sanctioned", lineNo, hash, newName, current)
		}
		if prev, dup := seen[version]; dup {
			t.Errorf("frozen-rename-allowlist.txt:%d: row %d is listed twice (%s and %s)", lineNo, version, prev, newName)
		}
		seen[version] = newName
	}
	for version, file := range upstreamIgnoredBaseFiles {
		if _, ok := seen[version]; !ok {
			t.Errorf("upstreamIgnoredBaseFiles reconciles row %d -> %s, which frozen-rename-allowlist.txt does not list", version, file)
		}
	}
}

// TestPreparedDMLAllowlistPinsAnOverriddenMigration pins
// migrations/prepared-dml-allowlist.txt to the two facts an entry asserts:
// the hash is the file's CURRENT bytes (so an edit re-trips check E until the
// entry is re-justified), and the fresh bundle substitutes direct SQL for the
// file (cliCompatibleMigrationSQL returns something other than the source
// text), which is why its prepared writes never reach `dolt sql`.
func TestPreparedDMLAllowlistPinsAnOverriddenMigration(t *testing.T) {
	rows := allowlistRows(t, "prepared-dml-allowlist.txt")
	if len(rows) == 0 {
		t.Fatal("prepared-dml-allowlist.txt lists nothing; 0072 is expected")
	}
	for lineNo, fields := range rows {
		if len(fields) < 3 {
			t.Fatalf("prepared-dml-allowlist.txt:%d: want <file> <sha256> <justification>, got %q", lineNo, strings.Join(fields, " "))
		}
		file, hash := fields[0], fields[1]
		if !isLowerHexSHA256(hash) {
			t.Fatalf("prepared-dml-allowlist.txt:%d: %q is not a lowercase hex sha256", lineNo, hash)
		}
		if strings.Contains(file, "/") {
			t.Fatalf("prepared-dml-allowlist.txt:%d: check E scans main-plane files only, got %s", lineNo, file)
		}
		data, err := mainSource.files.ReadFile(mainSource.dir + "/" + file)
		if err != nil {
			t.Fatalf("prepared-dml-allowlist.txt:%d: %s is not an embedded main migration: %v", lineNo, file, err)
		}
		if current := forkFileHash(t, mainSource, file); current != hash {
			t.Errorf("prepared-dml-allowlist.txt:%d: hash %s is not the current bytes of %s (%s); the exemption was written for other bytes", lineNo, hash, file, current)
		}
		if substituted := cliCompatibleMigrationSQL(file, string(data)); substituted == string(data) {
			t.Errorf("prepared-dml-allowlist.txt:%d: the fresh bundle does not override %s, so its prepared DML would reach dolt sql; the exemption does not hold", lineNo, file)
		}
	}
}
