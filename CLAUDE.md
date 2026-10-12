# Claude Code Entry Point for Beads

This file is intentionally short. Do not copy workflow, build, storage, or UI
rules here; those details drift quickly when repeated across agent entrypoints.

## Read First

- **Workflow and safety**: [AGENTS.md](AGENTS.md)
- **Detailed agent operations**: [AGENT_INSTRUCTIONS.md](AGENT_INSTRUCTIONS.md)
- **Architecture orientation**: [engdocs/CLAUDE.md](engdocs/CLAUDE.md)
- **PR maintenance policy**: [PR_MAINTAINER_GUIDELINES.md](PR_MAINTAINER_GUIDELINES.md)

## Current Ground Rules

- Run `bd prime` before doing tracked work.
- Follow `go.mod` and [AGENT_INSTRUCTIONS.md](AGENT_INSTRUCTIONS.md) for build
  and test commands; do not hard-code toolchain versions here.
- Beads uses Dolt as the issue database. Use `bd dolt push` / `bd dolt pull`
  for issue data sync; do not use export/import as a routine git workflow.
- The CLI Visual Design System lives in
  [AGENT_INSTRUCTIONS.md](AGENT_INSTRUCTIONS.md#visual-design-system).
- If this file conflicts with a linked source, trust the linked source and fix
  this file by removing the duplicate.
## Codex Review Gate (any agent shipping code)

If you're about to ship code — opening a PR, pushing a branch you intend to
merge, running `gt done`, or otherwise putting a change on a path to
production — you MUST run a local codex review on the diff first.

**This applies to crew, mayor, polecats, and any other agent producing
code.** Polecats already see this gate explicitly in `mol-polecat-work`'s
`submit-and-exit` step (via formula overlay); crew and others read it
here.

### Gate steps

1. On **Citadel**, run your own independent Codex review round through
   **`codex exec -p city -m gpt-6-astra`**, using the city's
   `/Users/tailor512/city/assets/ops/mayor-tools/codex-gate.sh` helper.
   This is the fork's reviewer route of record; no `/codex:rescue` skill
   is required. Use the bead's **QUICK / STANDARD / DEEP** class, including
   for docs when the bead explicitly requires review. Commit the changes
   first, keep the worktree clean, and review the full branch delta against
   the actual PR target (`origin/integrated-20260831` for this fork).

   For **QUICK**, run from the assigned beads worktree, replacing
   `<bead-id>` with the claimed bead:
   ```bash
   gate=/Users/tailor512/city/assets/ops/mayor-tools/codex-gate.sh
   evidence="/Users/tailor512/city/assets/pr-evidence/<bead-id>"
   mkdir -p "$evidence"
   "$gate" review --base origin/integrated-20260831 -C "$PWD" \
     --model gpt-6-astra --output "$evidence/worker-review.txt"
   ```
   The helper's `review` mode is QUICK only. For **STANDARD / DEEP**, define
   `gate` and `evidence` and create the directory as above, then write a
   prompt file there naming the required
   class, the immutable merge-base and HEAD SHAs (`git merge-base
   origin/integrated-20260831 HEAD` and `git rev-parse HEAD`), the bead's
   requirements, and the full diff to inspect. Request actionable
   CRITICAL/MAJOR/MINOR findings with file:line and a final standalone
   `VERDICT: CLEAN` or `VERDICT: BLOCK`. Then run:
   ```bash
   "$gate" exec "$evidence/worker-review-prompt.txt" -C "$PWD" \
     --model gpt-6-astra --output "$evidence/worker-review.txt"
   ```
   Both modes invoke a separate `codex exec` process with the city profile
   and a read-only review contract. Save the final answer and `.log`
   transcript; record the reviewed base/head and class with the evidence.
   A nonzero exit, missing verdict, or failed invocation does not clear the gate.
2. Address EVERY CRITICAL and MAJOR finding. MINORs may be deferred to
   followup beads (`bd create`) only if genuinely out of scope; explain
   why in the commit body.
3. Commit fixes and re-run the same class of review until clean (no
   CRITICAL/MAJOR/MINOR findings outstanding, or only deferred MINORs
   tracked by followups). Read the findings: the helper's `VERDICT: CLEAN`
   checks CRITICAL/MAJOR only, so it does not waive this MINOR requirement.
4. Add a review trailer to one of your commits and the PR body. For a
   clean QUICK round, use:
   ```
   Codex-Reviewed-By: gpt-6-astra QUICK via codex-gate.sh review -> codex exec -p city (clean: no CRITICAL/MAJOR/MINOR findings)
   ```
   For STANDARD / DEEP, name the actual class and `codex-gate.sh exec`
   route. If you deferred MINORs, for example:
   ```
   Codex-Reviewed-By: gpt-6-astra STANDARD via codex-gate.sh exec -> codex exec -p city (clean: CRITICAL/MAJOR addressed; MINORs deferred to bd-xxx)
   Followups: bd-xxx, bd-yyy
   ```
   Link the evidence in the PR prose. Keep the trailers in the final
   contiguous block, one `Key: value` per line, with a blank line before
   the block and no `---` separator or non-trailer lines inside it. Include
   `Bead:`, `Codex-Reviewed-By:`, `Fable-Reviewed-By:`, and `Co-Authored-By:`
   as the bead requires; use `Fable-Reviewed-By: pending (mayor read follows
   READY)` until that review happens. Agent-prepared commits also require
   `Agent-Signature:` per [engdocs/AGENT_SIGNING.md](engdocs/AGENT_SIGNING.md);
   use reliable runtime metadata or its `unknown-model` / `unknown-reasoning`
   placeholders, not the reviewer's identity. Amend the commit message to
   add trailers after review; a message-only amend preserves the reviewed
   tree, while any file change requires another review. The worker round
   precedes READY; the mayor's gate, Fable read, and Afik's merge approval
   remain separate requirements of the assigned bead.

### Why

The GitHub-side codex bot also reviews PRs but has been empirically less
rigorous than the independent local Codex review (it missed CRITICAL bugs
that local codex caught). The local gate is your one chance to catch
real bugs before they merge. Skipping it has shipped CRITICAL
regressions to production.

### When the gate does NOT apply

- Exploration / read-only investigation (no code change)
- Reverts and emergency rollbacks (urgency overrides; document why)
- Pure docs/config changes you've already eyeballed, unless the bead requires review
- Trivial typo fixes < ~5 lines
