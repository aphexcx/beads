package schema

import (
	"crypto/sha256"
	"encoding/hex"
	"sort"
	"strings"
)

// ContentHashSkew returns the migration versions that have a recorded content
// hash on BOTH sides but whose hashes DIFFER — i.e. two clones applied divergent
// content for the same migration version. That is the silent schema fork from
// gastownhall/beads#4259: two clones report the same MAX(version) yet ran
// different migration content, which only surfaces (cryptically) when a merge
// fails.
//
// Versions missing from either side, or carrying an empty/unknown (NULL) hash on
// either side, are ignored — only a definite hash-vs-hash mismatch counts. The
// result is sorted ascending. It is a pure comparison so any caller (a doctor
// check, a future pre-merge gate, either storage mode) can reuse it.
//
// Two hashes that are both sanctioned contents of the same migration file
// (its current bytes and a prior content registered in
// migrationPriorContentHashes, gf-4c9 row 1) are NOT skew: a clone that
// recorded the original bytes and a fresh init that applied the edited file
// ran the same migration on the same lineage.
func ContentHashSkew(local, remote map[int]string) []int {
	var skewed []int
	sanctioned := sanctionedMainContentHashes()
	for version, localHash := range local {
		remoteHash, ok := remote[version]
		if !ok || localHash == "" || remoteHash == "" {
			continue
		}
		if localHash != remoteHash && !(sanctioned[version][localHash] && sanctioned[version][remoteHash]) {
			skewed = append(skewed, version)
		}
	}
	sort.Ints(skewed)
	return skewed
}

// sanctionedMainContentHashes maps each main-chain version that has a
// registered prior content to the set of hashes this lineage accepts for it:
// the embedded file's current sha256 plus every migrationPriorContentHashes
// entry. Versions without a prior content are absent (their only sanctioned
// hash is the current one, and ContentHashSkew's plain comparison covers it).
func sanctionedMainContentHashes() map[int]map[string]bool {
	out := map[int]map[string]bool{}
	for file, priors := range migrationPriorContentHashes {
		if strings.HasPrefix(file, "ignored/") {
			continue
		}
		version, err := parseVersion(file)
		if err != nil {
			continue
		}
		data, err := mainSource.files.ReadFile(mainSource.dir + "/" + file)
		if err != nil {
			continue
		}
		sum := sha256.Sum256(data)
		set := map[string]bool{hex.EncodeToString(sum[:]): true}
		for _, prior := range priors {
			set[prior] = true
		}
		out[version] = set
	}
	return out
}
