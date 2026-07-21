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

1. Invoke the **`/codex:rescue`** skill on your branch's diff vs the
   target branch (usually `origin/main`). If you haven't used the skill
   before, read it first — it explains how to invoke and what to ask
   for.
2. Address EVERY CRITICAL and MAJOR finding. MINORs may be deferred to
   followup beads (`bd create`) only if genuinely out of scope; explain
   why in the commit body.
3. Re-run `/codex:rescue` after fixes until it returns clean (no
   CRITICAL/MAJOR/MINOR findings outstanding, or only deferred MINORs
   tracked by followups).
4. Add a trailer to one of your commits:
   ```
   Codex-Reviewed-By: codex-rescue (clean: no CRITICAL/MAJOR/MINOR findings)
   ```
   Or if you deferred MINORs:
   ```
   Codex-Reviewed-By: codex-rescue (clean: CRITICAL/MAJOR addressed; MINORs deferred to bd-xxx)
   Followups: bd-xxx, bd-yyy
   ```
   Use `git commit --amend` if you've already committed.

### Why

The GitHub-side codex bot also reviews PRs but has been empirically less
rigorous than the local `codex:rescue` agent (it missed CRITICAL bugs
that local codex caught). The local gate is your one chance to catch
real bugs before they merge. Skipping it has shipped CRITICAL
regressions to production.

### When the gate does NOT apply

- Exploration / read-only investigation (no code change)
- Reverts and emergency rollbacks (urgency overrides; document why)
- Pure docs/config changes you've already eyeballed
- Trivial typo fixes < ~5 lines
