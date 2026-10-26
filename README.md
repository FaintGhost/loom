# loom

Go automation for one repository and one independently authenticated Discord bot per instance.
Reports flow through intent classification, durable intake, repository triage, an isolated coding attempt,
verification, review, pull-request publication and a reply to the reporter. Implementation questions remain
local conversations: a fresh read-only checkout supplies a cited answer without creating an issue.

Python is retired after the Go cutover. The Go importer reads the old TOML and SQLite formats directly;
Python is not a runtime dependency. Deployment state, validation evidence and operational traps are in
[`docs/OPERATIONS.md`](docs/OPERATIONS.md).

```
cmd/loom/       CLI: init, import, inject, work/worker, bot, retro, compat, ops
internal/       intake, questions, worker, pipeline, runner, store, migrate, remote, improvement, model
instances/      sanitized example instance configuration (credential paths only)
systemd/        templates for bot, worker and daily retrospective
```

## Build and check

Requirements: Linux, Go 1.26+, Git, GitHub CLI authenticated for configured private repositories,
bubblewrap, the configured coding harness, and project-specific build tools. Herdr is required when
`harness_mode` is `herdr`. Discord bots need access to their configured channels and message content.

```bash
env -u GOROOT go test -race ./...
env -u GOROOT go build -o bin/loom ./cmd/loom
```

Where a parent directory is an invalid Git repository, add `-buildvcs=false`. Do not carry an unrelated
`GOROOT` into the service. The compatibility sandbox selects the concrete Go installation before PATH shims.
The supplied systemd units set `GOPATH` to the installed module cache location; adjust it for the host.

Effectgo is a development/compatibility check, not a runtime dependency. Pin the reviewed executable:

```bash
export EFFECTGO_BIN=/opt/effectgo/bin/effectgo
export EFFECTGO_SHA256=<sha256 of that reviewed build>
env -u GOROOT -u EFFECTGO_STUBS go vet -vettool="$EFFECTGO_BIN" ./...
env -u GOROOT bash check-contracts.sh
```

`check-contracts.sh` checks the real status model, Go 1.26 `errors.AsType` narrowing, and deliberate
purity/error/exhaustiveness regressions in disposable source. It verifies the executable digest first.

## Instance configuration

An instance has an absolute, canonical home containing `config.json`, `go-store.sqlite`, jobs, workspaces
and service locks. Homes cannot be symlinked or contain a Python `config.toml`/`store.sqlite`. Each instance
uses a dedicated ordinary Git clone, not somebody else's linked worktree.

```bash
mkdir -p /srv/loom
loom --home /srv/loom/example init \
  --repo /absolute/path/to/dedicated-clone --base HEAD \
  --agent '["codex","exec","--sandbox","workspace-write","-o","{last}","{prompt}"]' \
  --verify 'go vet ./... && go build ./... && go test ./...' --timeout 720
```

For a complete configuration, use `init --config /absolute/config.json`; `instances/example.json` is a
fully populated, sanitized template. Unknown fields are rejected.
`internal/worker/config.go` defines the supported schema. Essential groups:

| Fields | Purpose |
| --- | --- |
| `repo`, `base`, `setup`, `verify`, timeouts | Dedicated clone, starting revision and trusted project commands |
| `agent`, `commands`, `harness`, `gui_harness`, `harness_mode` | Named harness slots and headless/Herdr execution |
| `question_agent`, `commands.*.read_only_args` | Question harness; must support explicit `--sandbox read-only` |
| `github_repo`, `publish`, readiness/category labels | Remote repository and opt-in publication |
| `discord_token_file`, `discord_channel`, `discord_guild` | Separate bot credentials and channel identity |
| `magpie_url`, `magpie_key_file`, `decider_model`, `title_model` | Intent/model gateway; credentials stay in files |
| `allowed_users`, intake limits, `auto_kinds`, `max_auto_work` | Admission, queue and execution policy |
| `red_verify`, test globs, `guard_paths`, `secret_files`, `secret_patterns` | Regression proof and publication guards |
| `hidden`, `sandbox_env`, `sandbox_tmpfs` | Offline verification filesystem/environment isolation |
| `triage_enabled`, `review_enabled`, reviewer settings | Brief validation and fresh-context review |
| `max_attempts`, `max_review_rounds`, `max_reopens` | Retry, maintainer review and reporter reopen budgets |
| `retro` | Evidence review, destination homes and pinned effectgo compatibility |

Command arguments substitute `{task}`, `{prompt}`, `{last}` and named-slot `{model}` directly; report text
is not evaluated as a shell command. Setup and verify strings are operator-controlled shell commands.
Setup can access the network before implementation. Verification runs with a read-only host filesystem,
hidden credentials, writable owned checkout/caches and no network; missing isolation fails the attempt.
The agent uses its configured harness sandbox. Review and questions require read-only harness support.

## Operator commands

Every instance command accepts `--home PATH` before the command, or `FB_HOME`.

```bash
loom --home "$INSTANCE" inject --key stable-id 'Describe the observed problem'
loom --home "$INSTANCE" list
loom --home "$INSTANCE" show 1
loom --home "$INSTANCE" approve 1
loom --home "$INSTANCE" promote 1
loom --home "$INSTANCE" retry 1
loom --home "$INSTANCE" resume 1
loom --home "$INSTANCE" notify 1
loom --home "$INSTANCE" replay intake:discord:MESSAGE_ID
loom --home "$INSTANCE" work --once
loom --home "$INSTANCE" stats
loom --home "$INSTANCE" gc --dry-run --keep-days 14
loom instances
```

`inject` uses real classification when gateway/repository settings exist. The minimal local configuration
queues directly, which is useful for an isolated executor smoke. `approve` records explicit maintainer
authority; `promote` reclassifies held intake. A question cannot become work through `retry`.

The lifetime attempt number never resets: every attempt retains its own evidence. A trusted reopen starts
a new retry budget without reusing directories. Existing PR rounds retain their PR identity and feedback.
An interrupted usable checkout resumes; interrupted preparation can requeue within its budget. Exhaustion
requires human action. Inspect the report and its evidence before manual recovery.

`bot` owns Discord gateway intake, reconnect catch-up and pending delivery. `worker` owns issue/PR watches,
execution and result delivery. Their instance locks prevent duplicate daemons. REST catch-up has a durable
cursor independent of live message IDs. Issue/comment/PR operations reserve intent before remote writes;
uncertain writes reconcile by stable markers. An unresolved remote outcome blocks instead of blindly
creating a duplicate. Discord delivery persists each chunk and links bot replies to the original report.

Readiness labels and trusted review feedback trigger another round on the existing PR. The repository's
`docs/agents/triage-labels.md` supplies role labels when present. No code path automatically merges or deploys.
Target-project baseline test failures remain reportable gate failures, not a reason to stop the bot or worker.

## Migration and services

Stop the source bot, worker and retrospective timer before importing. The importer checks running Python
consumers and database handles, and rejects active jobs or changing source data. Never run both versions
against the same channel. Import into a fresh sibling home:

```bash
loom --home /srv/loom/example import --source /absolute/stopped-python-home
loom --home /srv/loom/example list
```

Import is atomic and repeatable for the same snapshot. It retains report IDs, issue/PR associations,
messages, events, failed intake and evidence. `legacy/database.json` retains complete SQLite tables and
storage types; `legacy/files/` retains historical evidence/configuration. Old worktrees are recorded as
`legacy_worktree`, never adopted for automatic deletion. Imported result receipts prevent reannouncements.
Publication and retrospective execution start disabled: validate the converted configuration, then enable
them for the intended instance. Keep the stopped source backup until rollback is no longer needed.

Templates in `systemd/` expect the binary at `/root/.hermes/scripts/loom/loom` and homes under
`/root/.hermes/fb-go/%i`; edit both if your layout differs. Install templates only after binary and
instance validation:

```bash
systemctl daemon-reload
systemctl enable --now loom-bot@example.service loom-worker@example.service loom-retro@example.timer
systemctl status loom-bot@example.service loom-worker@example.service
```

A release upgrade must drain/stop consumers, preserve the current binary and state backup, replace the
binary, then restart and inspect logs. Do not run an older binary against new state without checking schema
compatibility. Do not restore a stale state snapshot after remote writes without reconciling them first.

## Self-improvement and effectgo

A retrospective reads persisted reports, events, attempt logs and compatibility failures. It requires exact
evidence citations, filters configured credentials, and distinguishes business failures from automation and
analyzer defects. Business findings remain evidence; they do not become tooling tasks. Tooling findings go
to `retro.automation_home` or `retro.effectgo_home`, using that destination's `auto_kinds`/`max_auto_work`
policy and a stable deduplication key. A local repository path is a valid destination identity when no
GitHub remote exists. Reviewable changes stay in owned worktrees; deployment remains an operator action.

Configure `retro.enabled`, `retro.hours`, destination homes, `retro.compatibility_module`,
`retro.effectgo_binary`, `retro.effectgo_sha256`, `retro.effectgo_revision` and optional
`retro.regression_commands`. Both destination homes must already contain valid independent configurations.

```bash
loom --home "$INSTANCE" compat
loom --home "$INSTANCE" retro
```

Compatibility runs the pinned analyzer, whole-module checks, mutation contracts and configured regressions
inside a network-disabled read-only sandbox. Source, tools and module cache stay read-only; only disposable
scratch is writable. Logs and a result manifest remain in `compatibility/`, including failures. Additional
operator-approved regression argv arrays can be retained in `compatibility-regressions.json` in the home.
A newly built effectgo binary requires a new reviewed digest; never silently follow a mutable executable.
