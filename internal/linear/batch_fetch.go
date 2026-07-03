package linear

import (
	"context"
)

// batchFetchIssuesAcrossTeams resolves many Linear identifiers to their
// current remote state and host client in batched number-in queries —
// ceil(N/MaxPageSize) requests per team instead of N (bd-kqt). Team routing
// mirrors fetchIssueAcrossTeams: the primary team is tried first and only
// identifiers it doesn't resolve are retried against later teams. The client
// map records which team's client resolved each identifier so mutations can
// be sent to the owning team.
//
// Identifiers not found in any team are absent from both maps. A non-nil
// error reports a per-team batch failure; both maps still carry everything
// resolved before (and, for non-primary failures, after) it. Rate-limit
// exhaustion stops the team walk immediately — later teams share the same
// exhausted budget — and takes precedence over earlier transient errors in
// the returned error, so callers can reliably distinguish "abort" from
// "degrade to per-issue fetches".
func (t *Tracker) batchFetchIssuesAcrossTeams(ctx context.Context, identifiers []string) (map[string]*Issue, map[string]*Client, error) {
	issues := make(map[string]*Issue, len(identifiers))
	clients := make(map[string]*Client, len(identifiers))

	pending := make([]string, 0, len(identifiers))
	seen := make(map[string]bool, len(identifiers))
	for _, identifier := range identifiers {
		if identifier == "" || seen[identifier] {
			continue
		}
		seen[identifier] = true
		pending = append(pending, identifier)
	}

	var walkErr error
	for _, teamID := range t.teamIDs {
		if len(pending) == 0 {
			break
		}
		client := t.clients[teamID]
		if client == nil {
			continue
		}
		fetched, err := client.FetchIssuesByIdentifiers(ctx, pending)
		if err != nil {
			// Rate-limit exhaustion must win over any earlier transient
			// error: callers decide between "abort the phase" and "degrade
			// to per-issue fetches" from the returned error, and degrading
			// against a tripped circuit breaker would grind out doomed
			// requests (codex bd-kqt round-3).
			if walkErr == nil || isRateLimitExhausted(err) {
				walkErr = err
			}
		}
		remaining := pending[:0]
		for _, identifier := range pending {
			if li, ok := fetched[identifier]; ok {
				issues[identifier] = li
				clients[identifier] = client
				continue
			}
			remaining = append(remaining, identifier)
		}
		pending = remaining
		if err != nil && isRateLimitExhausted(err) {
			break // no budget for further teams either
		}
	}

	return issues, clients, walkErr
}
