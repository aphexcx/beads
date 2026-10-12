# Agent Instructions

<!-- bd-doctor-divergence: ok -->

See [AGENT_INSTRUCTIONS.md](AGENT_INSTRUCTIONS.md) for full instructions.

This file exists for compatibility with tools that look for AGENTS.md.

The marker above tells `bd doctor` that the intentional divergence between
this file and `CLAUDE.md` (different audiences, different reading orders) is
expected and should not be flagged.

## Key Sections

- **Issue Tracking** - How to use bd for work management
- **Development Guidelines** - Code standards and testing
- **Project Scope** - Read [engdocs/PROJECT_CHARTER.md](engdocs/PROJECT_CHARTER.md) before adding new feature surface area
- **Visual Design System** - Status icons, colors, and semantic styling for CLI output
- **Contributor Protection** - Read [CONTRIBUTING.md](CONTRIBUTING.md) before handling external PRs
- **Maintainer PR Guidelines** - Read [PR_MAINTAINER_GUIDELINES.md](PR_MAINTAINER_GUIDELINES.md) before triaging, landing, or closing PRs

## Project Scope

Before adding new feature surface area, read
[engdocs/PROJECT_CHARTER.md](engdocs/PROJECT_CHARTER.md). Beads owns issue tracking
primitives and should not encode orchestration-layer policy, become a storage
engine, or casually expand the database schema when metadata would work.

## PR Safety for Agents

Before triaging, reviewing, landing, closing, or otherwise maintaining PRs, read
[PR_MAINTAINER_GUIDELINES.md](PR_MAINTAINER_GUIDELINES.md). The maintainer
policy is to maximize community throughput: find useful contributor value,
absorb or transform it locally when practical, preserve attribution, and use
request-changes only as a last resort.

Before implementing work, opening a PR, or merging/closing a PR, run the PR
preflight:
```bash
scripts/pr-preflight.sh --search "<topic keywords>" --repo gastownhall/beads
scripts/pr-preflight.sh <pr-number> --repo gastownhall/beads
```

External contributor PRs have priority. Review and build on their branch when
possible, preserve their tests and attribution, and never close or supersede
their PR silently. If a rewrite is unavoidable, explain why on the original PR
and credit their design/tests.

## Visual Design Anti-Patterns

**NEVER use emoji-style icons** (🔴🟠🟡🔵⚪) in CLI output. They cause cognitive overload.

**ALWAYS use small Unicode symbols** with semantic colors (status uses symbols; priority uses labels):
- Status: `○ ◐ ● ✓ ❄`
- Priority: `P0`–`P4` label with color (no status glyph)

See [AGENT_INSTRUCTIONS.md](AGENT_INSTRUCTIONS.md) for full development guidelines.

## Storage Boundary

The canonical storage boundary is in
[engdocs/PROJECT_CHARTER.md](engdocs/PROJECT_CHARTER.md#storage-boundary). In short:
Beads talks to storage through a driver interface (`dolthub/driver` for Dolt).
Do not add beads-side flocks, engine introspection, storage-specific retry or
crash-recovery logic, or public SDK return types that leak driver internals.
If the boundary is too narrow, widen the interface or route the issue to the
driver instead of patching around it in beads.

A live application of this rule: `bd doctor` support for embedded mode is
enabled one subcommand at a time, each human-vetted (GH#3794). Do not lift the
embedded-mode gate in `cmd/bd/doctor.go` wholesale, and keep database-layer
checks and fixes server-gated until the driver interface covers them.

## Agent Warning: Interactive Commands

**DO NOT use `bd edit`** - it opens an interactive editor ($EDITOR) which AI agents cannot use.

Use `bd update` with flags instead:
```bash
bd update <id> --description "new description"
bd update <id> --title "new title"
bd update <id> --design "design notes"
bd update <id> --notes "additional notes"
bd update <id> --acceptance "acceptance criteria"

# Use stdin for descriptions with special characters (backticks, !, nested quotes)
echo 'Description with `backticks` and "quotes"' | bd create "Title" --description=-
echo 'Updated text' | bd update <id> --description=-
```

## Testing

Use [engdocs/TESTING.md](engdocs/TESTING.md) for the canonical commands,
test-design guidance, and PR-readiness gates.

## Citadel Codex lane environment recipe

Last checked: 2026-09-14 for `aphexcx/beads`, base
`integrated-20260831` at `ca61b9bbcccb346a5c2a39d1593eec26c7926318`.
Citadel dispatches should link to this section. These are host and fork facts
from the five 2026-09-12 rounds, not upstream requirements. The 14 facts below
name the papercuts they retire; records live in the city's `.papercuts.jsonl`.
Use [engdocs/TESTING.md](engdocs/TESTING.md) for test selection.

### 1. Use the assigned fork workspace

When a beads assignment explicitly calls for a cross-repository checkout,
fetch `origin integrated-20260831` from `/Users/tailor512/code/beads` and create
the assigned branch under `/Users/tailor512/code/beads-wt/<bead-id>`. Record the
fetched base SHA and compare the branch with `gc.work_branch`, updating that
metadata through GC if needed. The gascity-packs lane is a different repository;
leave that lane and the human clone checkouts untouched. This is the explicit
bead-specific workspace exception, not a general instruction to create worktrees.
Retires: `pc_dee0377ecf38`.

### 2. Source the Go build flags

Use `gms_pure_go` for both vet and test: the default CGO regex path needs
`go-icu-regex`'s `unicode/regex.h`, absent from this host's default include paths.
Source [.buildflags](.buildflags) in the command's shell; it exports the tag in
`GOFLAGS`. For explicit tags, compose them as `-tags=gms_pure_go,integration`.
Retires: `pc_9290edaca46c`.

```bash
(
  source .buildflags
  go vet ./...
  env -u BEADS_DOLT_SERVER_PORT -u BEADS_DOLT_PORT \
    go test -tags=gms_pure_go ./test/docsync
)
```

### 3. Own the SQL test server

Run SQL-server tests on a disposable, test-owned `dolt sql-server`, bound to
`127.0.0.1` on a free port, with temporary data and home directories, a readiness
check, and cleanup of that exact child process. The OrbStack Docker socket may
be unavailable. The existing `startScratchDoltServer` in
`internal/storage/schema/fork_reconcile_sqlserver_test.go` implements this
lifecycle; its tests need the local `dolt` binary and must actually run rather
than skip when supplying integration evidence. Retires: `pc_a7caea63ea55`.

Set `BEADS_TEST_SERVER=1` explicitly in the test/client process, alongside
`BEADS_TEST_MODE=1`, only after establishing ownership of the disposable server:
test-prefixed databases can otherwise be refused even on the intended fixture.
These flags do not redirect a connection or make a live server disposable.
Retires: `pc_340a5f16101b`.

### 4. Clear the live port before test subprocesses

Unset both ambient `BEADS_DOLT_SERVER_PORT` and legacy `BEADS_DOLT_PORT` for
test subprocesses: this lane can inherit the live server's port `10232`, and
direct package tests do not all sanitize it. Let a test-owned helper choose
its port; if a fixture client requires port variables, set them only to the
verified disposable server's port after clearing the inherited values.
Retires: `pc_687cae568aaa`.

```bash
env -u BEADS_DOLT_SERVER_PORT -u BEADS_DOLT_PORT \
  BEADS_TEST_MODE=1 BEADS_TEST_SERVER=1 \
  go test -tags=gms_pure_go -count=1 -v \
    ./internal/storage/schema -run '^TestForkReconcile_.*OnSQLServer$'
```

Check the verbose output for actual runs and named skips. The ordinary
`scripts/test.sh` runner deliberately defaults to skipping Dolt; consult its
documented opt-ins when choosing that runner.

### 5. Keep source-only Git hooks process-local

For an authorized source-only commit or push, use `git -c
core.hooksPath=/dev/null` after running the relevant checks directly:
`.githooks/pre-commit`, `prepare-commit-msg`, and `pre-push` invoke installed
`bd` against repository state, including for non-Go commits. Do not persist a
hooksPath change or edit the hooks. Retires: `pc_3b8598004f18`, `pc_80035df1c222`.

```bash
git -c core.hooksPath=/dev/null commit -F "$commit_message_file"
```

### 6. Use the stored aphexcx identity for the fork

The lane's session credential resolved to `citadel-mayor`, which lacks fork
write access; the SSH alternative had no authorized key. Use the stored
`aphexcx` login process-locally for both HTTPS Git and `gh`, and verify the login
name without printing credentials. Clear ambient token overrides when loading
the profile. Never write the loaded token to a file, remote URL, or evidence,
and do not switch shared authentication state. Retires: `pc_d0bf719fcdc1`,
`pc_f12bf25c0676`, `pc_cd45d4de133c`.

After validation and review, this subshell pushes the current assigned branch;
run it only in that branch's beads worktree. Prepare `pr_body_file` beforehand
and keep the PR draft for the mayor's gate, Fable read, and Afik's merge word.

```bash
(
  set +x
  set -e
  unset GH_TOKEN GITHUB_TOKEN
  export GH_CONFIG_DIR=/Users/tailor512/.config/gh-aphexcx
  GH_TOKEN="$(gh auth token --hostname github.com --user aphexcx)"
  test -n "$GH_TOKEN"
  export GH_TOKEN
  test "$(gh api user --jq .login)" = aphexcx
  lane_branch="$(git branch --show-current)"
  test -n "$lane_branch"
  git -c core.hooksPath=/dev/null -c credential.helper= \
    -c 'credential.helper=!gh auth git-credential' \
    push -u origin "HEAD:refs/heads/$lane_branch"
  gh pr create --repo aphexcx/beads --base integrated-20260831 \
    --head "$lane_branch" --draft --title "$pr_title" --body-file "$pr_body_file"
)
```

### 7. Select Bash 5 for scripts using mapfile

`/usr/bin/env bash` resolves to macOS Bash 3.2 in this lane; it lacks `mapfile`,
which `scripts/check-build-tags.sh` needs. The installed Bash 5.3.3 was verified
at the path below on the date above. Recheck its version if the Nix store
changes; do not assume `/opt/homebrew/bin/bash` exists. Put the Bash 5 directory
first on the command's `PATH` so nested env-bash scripts also use it.
Retires: `pc_f05016715a76`.

```bash
lane_bash=/nix/store/i2yfb9hqscbws8nqw7hkdq811x5fyc1n-bash-5.3p3/bin/bash
"$lane_bash" --version
env PATH="$(dirname "$lane_bash"):$PATH" make ci-pr-policy
```

### 8. Extract regular files for base comparisons

Filter a `git archive` extraction to regular files and directories: tracked
absolute `.claude/skills` symlinks point outside the tree and safe tar extraction
rejects them. A source comparison does not need those external skill targets.
Retires: `pc_487e31dda5de`, `pc_fb324f51c6c5`.

```bash
python3 - <<'PY'
import io
import subprocess
import tarfile
import tempfile

base_ref = "ca61b9bbcccb346a5c2a39d1593eec26c7926318"  # Use the recorded base.
archive = subprocess.check_output(["git", "archive", base_ref])
base_dir = tempfile.mkdtemp(prefix="beads-source-base-")
with tarfile.open(fileobj=io.BytesIO(archive)) as source:
    members = [entry for entry in source if entry.isfile() or entry.isdir()]
    source.extractall(base_dir, members=members, filter="data")
print(base_dir)
PY
```

### 9. Serialize Go lint and isolate its cache

Wait for each `golangci-lint` invocation to finish before starting another,
including `make ci-pr-lint`: its linter refuses a parallel run. Shared cache
diagnostics can name a sibling worktree even when `go list` resolves the current
checkout. Use one task-local `GOLANGCI_LINT_CACHE` for sequential invocations,
and `BD_LINT_NEW_FROM_MERGE_BASE=origin/integrated-20260831` for the PR wrapper's
intended scope. Retires: `pc_4fc96e5c0d1d`, `pc_b7a4a128d24b`.

### 10. Discover files without unmatched zsh globs

Use `rg --files scripts` and pass discovered, quoted paths to subsequent reads:
zsh aborts an unmatched glob such as `scripts/ci-pr-lint*` before the command
runs. Retires: `pc_ff6a923455eb`.

### 11. Distinguish recorded base failures from regressions

At `ca61b9bbc`, `make test` reproduced two `internal/storage/uow` failures:
`TestInitSchemaAcquiresMigrationLockBeforeBootstrapDDL` and
`TestInitSchemaConvergenceProbeRunsWithNoSessionDatabase`. Both report
`uow pin connection: expected a connection to be available` instead of the
migration sentinel. Compare against the same recorded base and keep the logs;
these observations are not permission to dismiss a new failure.
Retires: `pc_71dd7b139964`, `pc_b191ee6ca43e`.

That base also fails `scripts/check-testing-short.sh` and its
`TestCheckTestingShort*` regression. The fix is
[aphexcx/beads#5](https://github.com/aphexcx/beads/pull/5), still OPEN/DRAFT on
2026-09-14 and awaiting Afik's merge word. Treat this as a known base failure
until that fix lands; recheck the new base afterward. Retires: `pc_9302145f10b2`.

### 12. Provision the Linear fixtures and report body failures

The local fixture sites are `setupLabelSyncTest` in
`cmd/bd/linear_roundtrip_test.go` (the `integration` tag), `newPullProjectsEnv`
in `internal/linear/project_pull_integration_test.go`, and the four snapshot
CRUD/empty cases in `internal/storage/embeddeddolt/linear_{issue,project}_snapshots_test.go`.
They need initialized disposable Dolt fixtures, not live Linear credentials.
PR #5 introduces the named `BEADS_TEST_SKIP=linear-fixture` opt-out; do not
assume that helper exists on `ca61b9bbc` or count skipped cases as exercised.

With initialized fixtures, the eight label roundtrip cases reached their bodies
but failed because existing issue literals omit `IssueType`; the unchanged base
reproduced this. Retires: `pc_f1c0f7dcbe75`.
The unchanged `TestLinearSyncRequestProfile`, with `BEADS_PROFILE_N=10`, imported
all ten issues then failed when its HTTP mock rejected `TeamLabels` with HTTP
400. Its normal 300-issue workload retains a large-fixture short-mode skip.
Provisioning the fixture does not repair either body failure; report them
separately from prerequisite skips. Retires: `pc_1478ee93ef58`.

### 13. Resolve each skill's own catalog root

Expand the root alias attached to the specific skill using the current session's
skill-roots table, then read that skill's `SKILL.md`: skills can live under
different roots. For example, an `r1/openai-docs/SKILL.md` entry must use `r1`,
not another skill's `.agents` root. Alias numbers are session-specific.
Retires: `pc_3b87f44693c1`.

### 14. Retain optional evidence when cleanup is refused

Keep the task-created archive or lint cache and record its path if a command
guard refuses optional cleanup: deleting generated evidence is not required
for a valid comparison or handoff. Do not work around the guard.
Retires: `pc_3b1d58f34fed`, `pc_487e31dda5de`.

## Non-Interactive Shell Commands

**ALWAYS use non-interactive flags** with file operations to avoid hanging on confirmation prompts.

Shell commands like `cp`, `mv`, and `rm` may be aliased to include `-i` (interactive) mode on some systems, causing the agent to hang indefinitely waiting for y/n input.

**Use these forms instead:**
```bash
# Force overwrite without prompting
cp -f source dest           # NOT: cp source dest
mv -f source dest           # NOT: mv source dest
rm -f file                  # NOT: rm file

# For recursive operations
rm -rf directory            # NOT: rm -r directory
cp -rf source dest          # NOT: cp -r source dest
```

**Other commands that may prompt:**
- `scp` - use `-o BatchMode=yes` for non-interactive
- `ssh` - use `-o BatchMode=yes` to fail instead of prompting
- `apt-get` - use `-y` flag
- `brew` - use `HOMEBREW_NO_AUTO_UPDATE=1` env var

## Landing the Plane (Session Completion)

**When ending a work session** (or when the user says "let's land the
plane"), you MUST complete ALL steps below. Work is NOT complete until
`git push` succeeds.

**MANDATORY WORKFLOW:**

1. **File issues for remaining work** - Create issues for anything that needs follow-up
2. **Run quality gates** (if code changed):
   - `make ci-pr-lint` (required zero-finding formatting and lint wrapper)
   - `make test` (and `make test-icu-path` only if you intentionally need the ICU regex path)
   - File a P0 issue if quality gates are broken
3. **Update issue status** - Close finished work, update in-progress items
4. **PUSH TO REMOTE** - This is MANDATORY:
   ```bash
   git pull --rebase
   git push
   git status  # MUST show "up to date with origin"
   ```
5. **Clean up**:
   ```bash
   git stash clear                    # Remove old stashes
   git remote prune origin            # Clean up deleted remote branches
   ```
6. **Verify** - All changes committed AND pushed, no untracked files remain
7. **Hand off** - Choose a follow-up issue and give the user a prompt for
   the next session, e.g. "Continue work on bd-X: [issue title]. [Brief
   context about what's been done and what's next]"

**CRITICAL RULES:**
- Work is NOT complete until `git push` succeeds
- NEVER stop before pushing - that leaves work stranded locally
- NEVER say "ready to push when you are" - YOU must push
- If push fails, resolve and retry until it succeeds

Close with a summary for the user: what was completed this session, issues
filed for follow-up, quality-gate status, confirmation everything is pushed,
and the recommended prompt for the next session.

<!-- BEGIN BEADS INTEGRATION v:1 profile:full hash:bacef91e -->
## Issue Tracking with bd (beads)

**IMPORTANT**: This project uses **bd (beads)** for ALL issue tracking. Do NOT use markdown TODOs, task lists, or other tracking methods.

### Why bd?

- Dependency-aware: Track blockers and relationships between issues
- Git-friendly: Dolt-powered version control with native sync
- Agent-optimized: JSON output, ready work detection, discovered-from links
- Prevents duplicate tracking systems and confusion

### Quick Start

**Check for ready work:**

```bash
bd ready --json
```

**Create new issues:**

```bash
bd create "Issue title" --description="Detailed context" -t bug|feature|task -p 0-4 --json
bd create "Issue title" --description="What this issue is about" -p 1 --deps discovered-from:bd-123 --json
```

**Claim and update:**

```bash
bd update <id> --claim --json
bd update bd-42 --priority 1 --json
```

**Complete work:**

```bash
bd close bd-42 --reason "Completed" --json
```

### Issue Types

- `bug` - Something broken
- `feature` - New functionality
- `task` - Work item (tests, docs, refactoring)
- `epic` - Large feature with subtasks
- `chore` - Maintenance (dependencies, tooling)

### Priorities

- `0` - Critical (security, data loss, broken builds)
- `1` - High (major features, important bugs)
- `2` - Medium (default, nice-to-have)
- `3` - Low (polish, optimization)
- `4` - Backlog (future ideas)

### Workflow for AI Agents

1. **Check ready work**: `bd ready` shows unblocked issues
2. **Claim your task atomically**: `bd update <id> --claim`
3. **Work on it**: Implement, test, document
4. **Discover new work?** Create linked issue:
   - `bd create "Found bug" --description="Details about what was found" -p 1 --deps discovered-from:<parent-id>`
5. **Complete**: `bd close <id> --reason "Done"`

### Quality
- Use `--acceptance` and `--design` fields when creating issues
- Use `--validate` to check description completeness

### Lifecycle
- `bd defer <id>` / `bd supersede <id>` for issue management
- `bd stale` / `bd orphans` / `bd lint` for hygiene
- `bd human <id>` to flag for human decisions
- `bd formula list` / `bd mol pour <name>` for structured workflows

### Sync

bd stores issue history in Dolt:

- Each write auto-commits to Dolt history
- Use `bd dolt push`/`bd dolt pull` for remote sync
- Do not treat `.beads/issues.jsonl` as the sync protocol

**Architecture in one line:** issues live in a local Dolt DB; sync uses `refs/dolt/data` on your git remote; `.beads/issues.jsonl` is a passive export. See https://github.com/gastownhall/beads/blob/main/docs/core-concepts/sync-concepts.md for details and anti-patterns.

### Important Rules

- ✅ Use bd for ALL task tracking
- ✅ Always use `--json` flag for programmatic use
- ✅ Link discovered work with `discovered-from` dependencies
- ✅ Check `bd ready` before asking "what should I work on?"
- ❌ Do NOT create markdown TODO lists
- ❌ Do NOT use external issue trackers
- ❌ Do NOT duplicate tracking systems

For more details, see README.md and https://github.com/gastownhall/beads/blob/main/docs/getting-started/quickstart.md.

## Agent Context Profiles

The managed Beads block is task-tracking guidance, not permission to override repository, user, or orchestrator instructions.

- **Conservative (default)**: Use `bd` for task tracking. Do not run git commits, git pushes, or Dolt remote sync unless explicitly asked. At handoff, report changed files, validation, and suggested next commands.
- **Minimal**: Keep tool instruction files as pointers to `bd prime`; use the same conservative git policy unless active instructions say otherwise.
- **Team-maintainer**: Only when the repository explicitly opts in, agents may close beads, run quality gates, commit, and push as part of session close. A current "do not commit" or "do not push" instruction still wins.

## Session Completion

This protocol applies when ending a Beads implementation workflow. It is subordinate to explicit user, repository, and orchestrator instructions.

1. **File issues for remaining work** - Create beads for anything that needs follow-up
2. **Run quality gates** (if code changed) - Tests, linters, builds
3. **Update issue status** - Close finished work, update in-progress items
4. **Handle git/sync by active profile**:
   ```bash
   # Conservative/minimal/default: report status and proposed commands; wait for approval.
   git status

   # Team-maintainer opt-in only, unless current instructions forbid it:
   git pull --rebase
   bd dolt push
   git push
   git status
   ```
5. **Hand off** - Summarize changes, validation, issue status, and any blocked sync/commit/push step

**Critical rules:**
- Explicit user or orchestrator instructions override this Beads block.
- Do not commit or push without clear authority from the active profile or the current user request.
- If a required sync or push is blocked, stop and report the exact command and error.

<!-- END BEADS INTEGRATION -->
