# Harness research matrix (card 002)

Inputs: `contracts/drivers/engine-adapter.md` v1.0.0 @ 4ac51e1; spec "Harness capabilities", "Details for implementation"; decisions 03, 07, 23; `role-skills/README.md` §2.
I accessed every source on **2026-11-18**. Source IDs (C*, X*, K*) are listed in §5 with their link-check status.
This is a documentation review only. No authenticated runs were made (Kimi qualification: `not run: deferred by owner`). Nothing here counts as `qualified` under dec. 23. Every value below is a *documented* capability, which becomes `untested` in a capability declaration until a qualification run passes.
*unverified* = not confirmed from a primary source.

## 1. Locally installed versions (AC3)

Host: Windows 11 Pro 10.0.26200, checked 2026-11-18.

| harness | command | result |
|---|---|---|
| Codex CLI | `codex --version` | `codex-cli 0.169.0` |
| Claude Code | `~/.local/bin/claude.exe --version` (not on bash PATH) | `2.1.325 (Claude Code)` |
| Kimi Code CLI | `kimi --version` | `not installed` |

### Local argument validation (card 002-F2, 2026-11-18)

I checked argument parsing only: `--bare`, `ANTHROPIC_API_KEY` unset, empty stdin, so no authenticated request was possible.

| harness | check | result |
|---|---|---|
| Claude Code 2.1.325 | `claude -p --bare --input-format stream-json --output-format stream-json` | `Error: When using --print, --output-format=stream-json requires --verbose`, exit 1 |
| Claude Code 2.1.325 | the same command plus `--verbose` (the §3 `send` command) | no output, exit 0 (passes validation) |
| Claude Code 2.1.325 | `claude -p --bare --append-system-prompt-file sp.txt` | `Error: Input must be provided either through stdin or as a prompt argument when using --print`: the flag is accepted (control: an unknown flag gives `error: unknown option '--bogus-flag'`) |
| Claude Code 2.1.325 | `claude --help` | lists `--bare --append-system-prompt --settings --mcp-config --input-format --output-format --verbose --include-partial-messages --replay-user-messages --resume --continue --add-dir --permission-prompt-tool` |
| Codex 0.169.0 | `codex exec --help` | lists `--json --sandbox --ephemeral --output-last-message` |
| Codex 0.169.0 | `codex exec resume --help` | `Usage: codex exec resume [OPTIONS] [SESSION_ID] [PROMPT]`, with `--last` |
| Codex 0.169.0 | `codex app-server --help` | `[experimental] Run the app server or related tooling` (local help labels app-server experimental) |
| Kimi Code CLI | all §3 `kimi` commands | not run: not installed, so every Kimi command line is *unverified* locally (documented in K2/K3 only) |

## 2. Supported versions and OSes

| harness | OS (documented) | versions | source |
|---|---|---|---|
| Claude Code | macOS 13.0+; Windows 10 1809+ / Server 2019+; Ubuntu 20.04+; Debian 10+; Alpine 3.19+; x64/ARM64. Sandboxing is not supported on native Windows (WSL 2 only). | Release channels `latest`/`stable`. `minimumVersion`/`requiredMinimumVersion`/`requiredMaximumVersion` can pin a range. Several headless features carry version floors (e.g. `--permission-prompts` ≥ 2.1.259; Manual-mode default for `-p` ≥ 2.1.302). | C3, C1 |
| Codex CLI | macOS arm64/x86_64, Linux x86_64/arm64 (X6). Native Windows: Windows 10 1809+ required, Windows 11 recommended. MXC sandbox needs Win 11 24H2 build 26100.9278+. WSL1 is unsupported from 0.115 (WSL1 claim from a search snippet only, *unverified*). | Latest release number *unverified* (GitHub Releases did not render). Local: 0.169.0. | X6, X5 |
| Kimi Code CLI | macOS, Linux, Windows (Windows needs Git for Windows / Git Bash). No OS version floors are published (*unverified*). npm install needs Node ≥ 22.19.0. | Latest release *unverified*. Note: `MoonshotAI/kimi-cli` (Python) is **archived**. The successor is `MoonshotAI/kimi-code`. | K1, K5, K8 |

## 3. Capability matrix: harness × adapter operation (AC1)

| op | Claude Code | Codex CLI | Kimi Code CLI |
|---|---|---|---|
| `start` | `claude -p "<prompt>"`. `--bare` is recommended for scripted calls and skips host hooks/skills/MCP/CLAUDE.md. Context comes in via `--append-system-prompt(-file)`, `--settings`, `--mcp-config`. **C1** | `codex exec "<task>"` (stdout = final message), or app-server `thread/start` + `turn/start` over stdio JSONL (X2 recommends app-server for "deep integration inside your own product"). **X1, X2** | `kimi -p/--prompt "<prompt>"` (no TUI), or `kimi acp` → `initialize`, `session/new`, `session/prompt` over JSON-RPC stdio. **K2, K3** |
| `send` | Live session: `claude -p --input-format stream-json --output-format stream-json --verbose` reads user messages from stdin. C6 documents that "a message still queued when the limit ends a turn stays queued and starts a new turn", which implies repeated input to one running process. `--replay-user-messages` echoes them. Message schema and keep-alive between turns: *unverified*. (Fallback, not live: a new `--resume` invocation; see `native_resume`.) **C6, C1** | Live session: app-server `turn/start` on a loaded thread; `turn/steer` adds input to the in-flight turn. **X2**. (Fallback, not live: `codex exec resume`; see `native_resume`. **X1**) | Live session: ACP `session/prompt` on the open session. **K3**. Print mode has no live send (*unverified*); fallback is a new `--session`/`--continue` run, see `native_resume`. |
| `stream` | `--output-format stream-json --verbose [--include-partial-messages]`: NDJSON with `system/init`, `assistant`/`user` messages carrying tool_use/tool_result blocks, `stream_event` deltas, `system/api_retry`, and a final `result`. Exit code 0 / non-zero. **C1** | `exec --json`: JSONL `thread.started`, `turn.started/completed/failed`, `item.*` (agent messages, reasoning, command executions, file changes, MCP tool calls), `error`. App-server: `item/started`, `item/completed`, `item/agentMessage/delta`, `turn/completed`. **X1, X2** | Print: `--output-format stream-json` (one JSON object per line; thinking is not emitted, tool progress goes to stderr). ACP: `session/update` with `agent_message_chunk`, `tool_call*`, `plan`. **K2, K3** |
| `interrupt` (opt.) | SIGINT ends the turn, or Agent SDK `interrupt()`. `system/init.capabilities` advertises `interrupt_receipt_v1`. A raw stream-json control message for the CLI is *unverified*. Windows signal behaviour is *unverified*. **C1** | app-server `turn/interrupt` → `{}`, turn ends `interrupted`. `exec` interruption: *unverified*. **X2** | ACP `session/cancel`. Print mode: *unverified*. **K3** |
| `stop` | SIGTERM → exit 143, kills the process tree of running Bash commands, runs `SessionEnd` hooks, leaves the turn unfinished. **C1** | App-server `thread/unsubscribe` detaches this connection. When the last subscriber leaves, the thread unloads after an idle grace period (default 60 s, `thread_unload_delay_secs`), then emits `thread/status/changed` `notLoaded` and `thread/closed`. **X2**. Immediate supervisor-initiated stop (no grace wait), app-server shutdown, and `exec` stop/signal handling: *unverified*. | ACP `session/close`. Process-level stop: *unverified*. **K3** |
| `native_resume` (opt.) | `--resume <session_id>` (or a transcript `.jsonl` path) or `--continue`. `session_id` is in json/stream-json output. **C1** | `codex exec resume <SESSION_ID>` / `--last`. App-server `thread/resume` (and `thread/fork`). **X1, X2** | `-S/--session <id>`, `-c/--continue`. ACP `session/load` (with replay) and `session/resume` (without replay). **K2, K3** |
| `usage` (opt.) | `result` carries usage and `total_cost_usd` plus a per-model breakdown (client-side estimates; on resume the figures are cumulative). **C1** | `turn.completed.usage`: `input_tokens`, `cached_input_tokens`, `output_tokens`, `reasoning_output_tokens`. App-server `thread/tokenUsage/updated` (payload shape *unverified*). **X1, X2** | *unverified*. Not documented on K2/K3. |
| `tool_interception` (opt.) | `PreToolUse` hook can `deny` (JSON `permissionDecision` or exit 2) and covers MCP tools. A timed-out hook does not block unless `onFailure: "block"`. `--permission-prompt-tool` routes approvals to an MCP tool. **Hooks don't run under `--bare`.** **C5, C1** | `PreToolUse`/`PermissionRequest` hooks can deny shell, `apply_patch`, MCP, and local function tools. Hosted tools (WebSearch) are **not** covered. Docs: "a useful guardrail, not a complete enforcement boundary". App-server `item/commandExecution/requestApproval` and `item/fileChange/requestApproval` (accept/decline/cancel). **X3, X2** | `PreToolUse` hook blocks (exit 2 or `permissionDecision: deny`). **Hooks fail open** on error or timeout. ACP `session/request_permission`. **Print mode auto-approves** (`auto` policy, static deny rules only). **K4, K3, K2** |
| `skill_delivery` | `native`: `.claude/skills` discovery; in `--bare`, `--add-dir <dir>` loads skills from `<dir>/.claude/skills/` only, which allows a role-scoped dir. `/skill-name` expands in `-p`. `injection`: `--append-system-prompt-file`. **C1** | `native`: discovery from `$CWD/.agents/skills`, parent, repo root, `$HOME/.agents/skills`, `/etc/codex/skills`, plus bundled system skills. Per-skill disable via `[[skills.config]]`. No per-run skill-dir flag found (*unverified*), so role-scoping needs an isolated CWD/HOME. App-server `skills/list`. `injection`: prompt text. **X4, X2** | `native`: `--skills-dir <dir>` **replaces** discovered user/project dirs for that launch (role-scoped). `injection`: prompt text. **K2** |

## 4. Terms relevant to driving each harness from a third-party daemon (AC2)

**Stop-condition status: open, pending owner decision** (§6 Q0, Q1, Q3). No clause reviewed explicitly forbids a local daemon driving the operator's own unmodified harness, but the Claude Code C4 conditions below, the Codex X2 clause and the Kimi K7 clause may each apply to Synch.

**Claude Code**: [Legal and compliance (C4)](https://code.claude.com/docs/en/legal-and-compliance):
> "Anthropic does not permit third-party developers to offer Claude.ai login into their own applications, or to route requests through Free, Pro, Max, or Ultra plan credentials on behalf of their users. Moreover, developers may not collect, store, or intermediate Claude.ai credentials or session tokens"

> "Nor does it prevent an end user from signing in to the unmodified Claude Code binary with their own Claude subscription"

C4 also says: "Unless we've mutually agreed otherwise, preinstalling or running Claude Code in your products or services (e.g. in hosted sandboxes or other agent infrastructure) requires agreeing to our [Commercial Terms of Service](https://www.anthropic.com/legal/commercial-terms) and complying with the conditions below", including that customers "may not remove, disable, or restrict any authentication method built into it (including methods that permit signing in with a Claude account or the user's own API key)." Also: "The Claude Code binary must not be modified" and "Each end user must authenticate with their own Anthropic API key, Claude subscription plan credentials, or 3P inference provider credential". Also: "Advertised usage limits for Pro, Max, and Ultra plans assume ordinary, individual usage of Claude Code and the Agent SDK." [Agent SDK overview (C2)](https://code.claude.com/docs/en/agent-sdk/overview): "Unless previously approved, Anthropic does not allow third party developers to offer claude.ai login or rate limits for their products". Its branding rules say not to name the product "Claude Code".

**Codex CLI**: [App Server (X2)](https://learn.chatgpt.com/docs/app-server), "Auth endpoints" section:
> "App-server authentication has never been permitted for commercial or hosted services."

X2 says existing local/open-source apps may continue, and points commercial/hosted use to Sign in with ChatGPT (partner waitlist). [Non-interactive mode (X1)](https://learn.chatgpt.com/docs/non-interactive-mode): "API keys are the right default for automation because they are simpler to provision and rotate." ChatGPT-managed auth is the advanced path, with "enterprise teams using ChatGPT-managed Codex access on trusted runners" given as an example. The repo is Apache-2.0 ([X6](https://github.com/openai/codex)). OpenAI Terms of Use ([X7](https://openai.com/policies/terms-of-use/)): link check **failed: 403**. Alternatives tried (services-agreement, row-terms-of-use: 403; cdn PDF: 404) found no reachable official copy. Clauses *unverified*; X7 left for owner disposition.

**Kimi Code CLI**: The repo is MIT ([K1](https://github.com/MoonshotAI/kimi-code)). The [membership page (K6)](https://www.kimi.com/code/docs/en/kimi-code/membership.html) says: "Requests from the CLI, VS Code, Desktop, and third-party tools all count toward that quota." The general [Kimi Terms of Service (K7)](https://www.kimi.com/user/agreement/modelUse?lang=en-US), effective 2024-01-05, say:
> "you may only use Kimi in a non-commercial, personal research, and study manner"

> "you may not use our Services to develop products or services that compete with us"

It is unclear whether K7 governs Kimi Code CLI / Kimi Platform API keys (*unverified*). I also saw claims that changing the User-Agent is a violation, and that membership keys may not be used for non-interactive automation. Those came from search snippets and an unofficial site, so they are *unverified*.

## 5. Sources and link check

Every source was accessed 2026-11-18. Link check (`curl -sL`, 2026-11-18): all 200 except **X7 failed: 403** (owner disposition; not passed).

| id | URL |
|---|---|
| C1 | https://code.claude.com/docs/en/headless |
| C2 | https://code.claude.com/docs/en/agent-sdk/overview |
| C3 | https://code.claude.com/docs/en/setup |
| C4 | https://code.claude.com/docs/en/legal-and-compliance |
| C5 | https://code.claude.com/docs/en/hooks |
| C6 | https://code.claude.com/docs/en/cli-reference |
| X1 | https://learn.chatgpt.com/docs/non-interactive-mode (redirect target of developers.openai.com/codex/noninteractive) |
| X2 | https://learn.chatgpt.com/docs/app-server |
| X3 | https://learn.chatgpt.com/docs/hooks |
| X4 | https://learn.chatgpt.com/docs/build-skills.md |
| X5 | https://learn.chatgpt.com/docs/windows/windows-sandbox.md |
| X6 | https://github.com/openai/codex |
| X7 | https://openai.com/policies/terms-of-use/ |
| K1 | https://github.com/MoonshotAI/kimi-code |
| K2 | https://moonshotai.github.io/kimi-code/en/reference/kimi-command |
| K3 | https://www.kimi.com/code/docs/en/kimi-code-cli/reference/kimi-acp |
| K4 | https://moonshotai.github.io/kimi-code/en/customization/hooks |
| K5 | https://moonshotai.github.io/kimi-code/en/guides/getting-started |
| K6 | https://www.kimi.com/code/docs/en/kimi-code/membership.html |
| K7 | https://www.kimi.com/user/agreement/modelUse?lang=en-US |
| K8 | https://github.com/MoonshotAI/kimi-cli (archived) |

## 6. Risks and blockers as owner questions (AC4)

0. **Claude Code product conditions (C4).** Does C4's "running Claude Code in your products or services" clause apply to Synch, so that Commercial Terms acceptance is required? If so, would requiring an API key, or running `--bare` (which never reads OAuth credentials, C1), "restrict any authentication method built into it"? Until decided, the §4 stop condition stays open, and Q2/Q6 remedies that force API-key-only auth are on hold.
1. **Codex auth path.** X2 bars app-server auth for "commercial or hosted services". Is Synch (a local daemon) commercial or hosted? If it might be, should the Codex adapter use `codex exec` with the operator's own `CODEX_API_KEY` (losing `turn/interrupt`), or should we join the Sign in with ChatGPT partner track?
2. **Claude Code subscription use.** C4 allows an end user's own subscription in an unmodified binary, but plan limits "assume ordinary, individual usage". Is daemon-driven multi-thread use on a Pro/Max login acceptable, or does V1 require an API key / 3P provider for Claude Code? `--bare` skips Claude subscription login: the Anthropic API needs an API key in this mode, while supported cloud providers (Amazon Bedrock, Google Cloud's Agent Platform, Microsoft Foundry) can use their own credentials (C1). Whether that restricts a built-in method is Q0.
3. **Kimi terms scope.** K7 says "non-commercial, personal research, and study". Does it govern Kimi Code CLI with membership or Platform keys? This needs an owner or Moonshot confirmation before Kimi is advertised.
4. **Kimi product identity.** The spec says "Kimi Code CLI", and `kimi-cli` is archived. Do we target `MoonshotAI/kimi-code` only?
5. **Interception gaps.** Kimi hooks fail open, Kimi print mode auto-approves, Codex hooks skip hosted tools (and Codex calls itself "not a complete enforcement boundary"), and Claude hooks are off in `--bare`. Do we accept `tool_interception` as advisory and still require the journal-coverage banner? Or do we mandate ACP/app-server permission requests plus fail-closed hook settings (Claude `onFailure: "block"`)?
6. **Claude `--bare` vs hooks.** Bare mode (recommended for scripts; isolates host config, gives role-scoped skills via `--add-dir`) disables hooks. Interception would then rely on `--permission-prompt-tool` (MCP) only. Which mode does the adapter use, given that `--bare` may conflict with C4 (see Q0)?
7. **Codex role-scoped skills.** No per-run skill-dir flag was found. Is CWD/HOME isolation acceptable as `native`, or do we use `injection` only for Codex?
8. **Windows interrupt/stop.** Claude interrupt is signal-based (SIGINT/SIGTERM), and its Windows semantics are undocumented. Should qualification cover Windows first, as in the contract example?
9. **Kimi usage reporting** is undocumented. Do we accept `usage` as `unavailable` for Kimi in V1?
10. **Version pinning.** Claude and Codex auto-update. Should qualification pin versions (`requiredMinimum/MaximumVersion`, `DISABLE_UPDATES`) so declarations match the `harness_versions` ranges?
