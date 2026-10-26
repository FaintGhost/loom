# loom operations

Go is the primary implementation. Python source, tests, onboarding code and legacy service templates have
been removed from the development checkout. See `README.md` for the native CLI, configuration, migration
and recovery contracts.

## Current state

The whole stack is **stopped**. All bot, worker and retrospective units are inactive and disabled; no
`loom` process runs and no timer is armed. Restart with `systemctl enable --now` only after the instance
configuration and secrets are in place.

- Source: this repository (`github.com/FaintGhost/loom`), a single `main` branch.
- Executable: `/root/.hermes/scripts/loom/loom` (module root).
- Homes: four instances under one home root on the deployment host, two of them tooling destinations.
- `instances/example.json` is a complete sanitized template. Copy it into a home as `config.json` and fill
  in real repository, channel and credential *paths*; secrets themselves stay outside the repository.
- The former Python service units and runtime directories are removed after archival; no Python consumers remain.
- The production cutover imported three and two reports respectively from two legacy instances, with their
  message links, events and issue/PR identities unchanged. Imported result receipts suppress reannouncements.
- Each new home retains `migration.json`, `legacy/database.json` and `legacy/files/` for audit. Legacy
  worktree paths remain inert metadata; they are not adopted by Go cleanup.
- Production publication is enabled. Both retrospective timers and the independent tooling workers are enabled.

## Self-improvement deployment

Two tooling destinations use independent clones so self-improvement edits never touch the running source.
Their worker services are enabled. They have no Discord credentials and publication is disabled: automatic
policy-approved tooling jobs produce verified local worktrees/commits for operator review, never live upgrades.
The automation gate checks its race suite against the reviewed effectgo binary. The effectgo gate builds
a candidate in sandbox scratch and runs that candidate against the automation module and mutation contracts.

Both business-instance retrospectives route into these explicit homes. The classifier model is unchanged.
Structured retrospective responses use `group/auto-glm-5-3-flash`: the prior Deepseek Flash group returned
empty content during actual probing. Services explicitly set `GOPATH=/root/.local/share/go` so offline
checks see the installed module cache. Model HTTP requests honor the configured caller deadline instead
of being truncated by the ordinary 60-second transport default.

The first complete deployed retrospective exited successfully on 2026-10-13 at 22:14 UTC, including
the isolated compatibility run and real gateway classification. It found no actionable findings and created
no tooling tasks. Its retained result and classification remain in that instance's `jobs/` directory.
Every unit is now stopped and disabled.

## Verified behavior

The full Go race suite passed for all packages. Native CLI smoke exercised a passing sandbox gate,
a failing gate, durable failure state, explicit maintainer approval and an unchanged source clone.
Snapshot import of both real databases preserved report IDs/statuses/issue/PR associations; a second
import was idempotent and kept publication disabled until explicitly configured.

A real question smoke against a private repository returned the installed Go version with an exact
commit-and-line citation. The report stayed `question/held`, with no issue/PR and zero coding attempts.
This smoke performed no Discord/GitHub writes. A live channel-to-PR scenario was not manufactured.

Effectgo whole-module vet passed. `check-contracts.sh` accepted the Go 1.26 AsType narrowing reproducer
and detected deliberately introduced impurity, undeclared errors, missing enum cases and an unmatched
AsType error. Native `loom compat` also passed inside the actual network-disabled read-only sandbox.

Reviewed effectgo identity:

- Binary: a reviewed local build of the effectgo analyzer (host path recorded in the instance config)
- SHA256: `a17d3cb2b9feceba96e28e0399b7a40756439b54a807e00f4f9733cd4dea8f19`
- Revision: `81e442d57bdac85887976c0112a84d13c2f7214f`
- Original dogfood baseline: `3e5543d266b19747ab64f1e03ccfb5fd6f4c7160`

Independent reviews covered intake/reconnect/delivery, questions, store/migration, execution/publication
and retrospective isolation. Fixes include atomic message-specific reopen receipts, durable duplicate
routing, fresh-session REST catch-up, explicit maintainer authority, PR-aware interrupted recovery,
per-round retry budgets without reused attempt paths, retained failure context, confirmed triage,
credential-aware retrospective evidence and post-review verification logs. Full regression coverage
includes these state transitions and crash boundaries.

## Boundaries and operational traps

1. Maintain the automation and effectgo integration; do not fix target-repository business bugs as part
   of this handoff. Known baseline failures in a target repository are its behavior, not analyzer failures.
   They must remain red and be reported; they do not justify stopping services.
2. Keep bot credentials separate per instance, in files outside the repository and outside every sandbox.
   Never print a token.
3. Git helpers clear untrusted global configuration. Trusted remote reads/publication explicitly use
   `gh auth git-credential`; private question snapshots need this too.
4. A scratch HOME makes mise shims unsuitable for the compatibility sandbox. Its PATH starts with the
   concrete validated Go toolchain. Do not weaken filesystem isolation or trust checks to use a shim.
5. Runtime `GOROOT` must match the installed Go toolchain; commands on this host use `env -u GOROOT`.
6. Never reset the lifetime attempt counter. `RetryBase` grants a fresh bounded budget while preserving
   independent evidence directories. Existing PRs use `StartReview`, not a fresh-issue retry.
7. Uncertain remote writes stay blocked until their markers reconcile. Do not delete reservations to
   force a resend without checking the external result.
8. The importer refuses any open source database handle, including a diagnostic reader. Close readers
   before cutover; it never stops processes on the operator's behalf.
9. Tooling tasks may produce validated local commits/worktrees, but do not automatically replace running
   binaries, merge PRs or deploy. Upgrade approval remains an operator decision.
10. The repository lives at `/root/workspace/auto` on branch `main`. `/root/.git` is an unrelated invalid
    ancestor, so builds on this host use `-buildvcs=false`.
11. The configured `retro.effectgo_binary` no longer exists on the deployment host: its build directory was
    removed after the reviewed digest was recorded. Retrospectives therefore fail their compatibility step
    and remain evidence-producing, not silent. Rebuild the analyzer at the recorded revision, re-review it,
    and update `effectgo_binary`/`effectgo_sha256` before re-enabling the timers.
12. Eight automation reports are persisted as `needs_human`: their triage harness returned empty output, so
    no change was attempted. They are retained, not retried, while the stack is stopped.
