# Synch V1 release evidence

## Verdict

**partial** - The fixture-backed core demonstrates event history, child lifecycle, policy, mode switching, local gates, provider mocks and redaction paths. The desktop has been built and exercised against seeded daemon data in earlier coordinator checks; this is not an authenticated desktop-to-harness run. The 602 scenarios below report partial even where their fixtures pass. Release for real workloads is not established.

Real use needs card 601 authenticated qualification (D-57), real hypervisor/CI provider and sandbox certification, and production sandbox wiring. Recommended next: (1) owner decide whether to resume 601 in a separate account or VM; (2) qualify real hosts and providers; (3) close production wiring and API gaps below; (4) rerun end-to-end release gates and obtain owner acceptance.

Audited tree: `7ee4d3ca2e735af9b46e810e2c2790be6339487a` (main plus the coordinator card); published app snapshot `9580c8038b54abd51fb78590234110984566cda7`. Status vocabulary: passed, failed, error, partial, not run. A passing fixture is not a production qualification.

## Release checks

| Spec requirement | Status | Evidence and reason |
|---|---|---|
| Core tests, race, vet, lint; fresh review; retain findings | partial | Raw checks below; fresh-session review not run: deferred D-37; findings retained here. |
| Three threads edit one file on separate branches without silent loss | partial | internal/gitops tests and 602 fixtures; no live authenticated harness demonstration. |
| Kill desktop, reconnect, verify history | partial | TestCloseDesktopAndRecoverThreadInCLI, internal/scenarios/recovery_test.go; API/CLI replay fixture, desktop kill not exercised. |
| Force rollover and confirm task continues | partial | TestGateRolloverContinues; fixture, live harness not run D-57. |
| Real task desktop to headless core and live harness | not run | D-57 authenticated qualification deferred; desktop seeded daemon screenshots are not this test. |
| Close desktop and recover thread in CLI | partial | TestCloseDesktopAndRecoverThreadInCLI; no live harness or actual desktop closure. |
| Child cancellation, limits, result delivery, handoff to another harness | partial | TestChildCancellationLimitsResultAndHandoff; separate lifecycle fixtures and private handoff state. |
| Separate adapter fixtures from authenticated end-to-end evidence | passed | 602 table and capability section explicitly distinguish them; authenticated evidence not run D-57. |
| Plant missing requirement and show it | partial | TestPlantedMissingRequirementIsVisible; local gate fixture, daemon presentation not exercised. |
| Change revision or target; stale approval refused | partial | TestStaleApprovalRefusedAfterRevisionOrTargetChange; policy fixture, daemon path not exercised. |
| Switch mode with active child and running process | partial | TestModeSwitchWithActiveChildAndRunningProcess; daemon fixture uses private helpers. |
| Revoke inherited Host grants before downgrade complete | partial | TestModeSwitchSettlesAndRevokes; fake sandbox, production configuration not qualified. |
| Known test secrets absent from supported prompts, journals, exports | partial | TestKnownSecretsStayOutOfPromptsJournalsAndExports; 602 covers output and export, not prompt/journal in that scenario; daemon credentials tests separately. |
| Lose snapshot response and reconcile on every claiming driver | partial | TestLostSnapshotResponseOnEveryDriver; fake providers; real hosts not run: deferred by owner. |
| Close operator laptop while owned runner works; replay missing events | partial | TestOwnedRunnerContinuesAndReplaysAfterDisconnect; fixture; engine launchers require Direct. |
| Failed static CI gate through fix, rerun, audit and human acceptance | partial | TestFailedStaticCIGateThroughFixRerunAuditAndAcceptance; local rerun only; provider, audit and human acceptance not exercised. |

R2 counts: passed 1, failed 0, error 0, partial 14, not run 1.

## 602 derived table

The exact output of `go test ./internal/scenarios -run TestReleaseEvidence -v -count=1` is in [scenarios.txt](release-evidence/scenarios.txt).


| Scenario | Status | Reason |
|---|---|---|
| S1 | partial | API to CLI history replay verified; needs 601 authenticated harness |
| S2 | partial | existing lifecycle fixtures are separate; handoff pin change uses private state |
| S3 | partial | local gate verified; daemon requirement presentation not exercised |
| S4 | partial | policy fixture verified; daemon path not exercised |
| S5 | partial | existing daemon fixture uses private setup and inspection helpers |
| S6 | partial | broker redaction verified for retained output, excerpts and export; prompt and journal paths not exercised by this scenario |
| S7 | partial | provider fixtures passed; real host deferred by owner |
| S8 | partial | engine launchers require the Direct driver |
| S9 | partial | local gate rerun verified; CI mocks, audit and human acceptance not exercised |

## Capability and contract matrix

| Surface | Status | Evidence / limit |
|---|---|---|
| Codex, Claude Code, Kimi | not run | Every claimed operation untested pending authenticated card 601, D-57; adapter fixtures are separate. Kimi authenticated qualification is also deferred. |
| Proxmox, ESXi, Workstation, VirtualBox, Hyper-V | not run | Each `internal/vmdriver/*/capabilities.json` has nine real-host operations deferred by owner, empty advertised and certified lists; driver fakes do not establish host support. |
| GitHub Actions, GitLab CI, Jenkins | not run | Real provider rows deferred by owner; `contracts/drivers/ci-driver.md`, `internal/ci/*` use mocks. |
| Sandbox certification | not run | Deferred by owner; `internal/sandbox` fake-provider boundary tests are not certification. |

Contract versions: OpenAPI 3.2.1 (OpenAPI format 3.1.0), event catalogue 4.2.0, runner schema ID 2.0.3, CI driver 0.2.0 (D-51 accepted card 504 despite stale pending header), VM driver 3.1.0. Runner schema title says 2.0.2 while its ID and catalogue say 2.0.3: low severity documentation inconsistency, `contracts/events/runner-event.schema.json:4`; contract maintenance follow-up card needed.

## Deferrals and gaps

| Item | Status / impact | Closing card |
|---|---|---|
| Authenticated harnesses incl. Kimi | not run: owner deferred D-57; no advertised operation qualified | 601 |
| Real hypervisor hosts, real CI providers and sandbox certification | not run: owner deferred (STATUS 402-406, 503-504; no separate decision ID recorded) | real-host/provider qualification follow-up; 406 certification |
| age-encrypted credential fallback | not run: not approved, stub fails closed (`internal/credentials/store.go:171`, D-54) | 401 follow-up, owner approval |
| Fresh-session reviews | not run: owner deferred D-37 (D-48) | owner-directed review |
| Snapshot verification | partial: only Proxmox/ESXi implement verifier; Workstation, VirtualBox and Hyper-V cannot verify checkpoint references (`internal/vmdriver/verify.go:13`) | 114b follow-up |
| VM ownership hook | partial: production wiring absent; checkpoint ownership verification unavailable (`internal/daemon/daemon.go:86`) | 115 / API wiring follow-up |
| Remote engine launch | partial: launchers require Direct; remote runner replay fixtures do not prove remote engine tasks (`internal/engine/claudecode/adapter.go:46`) | 405/engine integration follow-up |
| Finding triage | partial: UI has no write endpoint, operator cannot persist triage (`desktop/src/Findings.tsx:157` and STATUS 202c) | API contract follow-up |
| Thread listing | partial: GET /v1/threads lists roots only; children require thread view (`internal/api/threads.go:139`, STATUS 202c) | API contract follow-up |
| Sandbox production | partial: VM address, durable journal, policy authorizer, daemon configuration, worktree copy and real guest inspector missing (`internal/daemon/daemon.go:62`); no production isolation claim | 115 |
| Checkpoint verification | partial: existence/metadata checks do not prove restore works; real-host tests deferred (D-55, 114b) | 114b follow-up + real-host qualification |
| Per-task runtime budget | not run: API has no per-task override; project/environment controls only (`internal/daemon/dispatch.go:30`, D-42) | API contract follow-up |
| Real scanner binaries | not run: synthetic Trivy/Gitleaks fixtures only (STATUS 505) | 505 qualification follow-up |

## Audit findings

- **medium** - Missing package documentation in `internal/sandbox/sandboxtest` and `internal/scenarios` (test-only directory), contrary to CONVENTIONS.md. Closing card: documentation cleanup follow-up.
- **medium** - Exported function comment spot-check identifies 314 candidates, including `internal/api/server.go:91`; regex includes tests and may overcount valid comment blocks. Review and fix against the full convention, including unexported functions, in a documentation cleanup card.
- **low** - Runner schema title version mismatch noted above; contract maintenance follow-up.

## Self-audit

Full scoped results and limitations: [self-audit.txt](release-evidence/self-audit.txt).

- passed: 0 `nolint` directives in tracked Go.
- passed: 0 insecure TLS/SSH bypass references in non-test Go.
- partial: secret-pattern scan found 5 candidate files; all five candidates are test or fake fixture files; heuristic cannot establish absence of secrets. Paths only in raw output; review candidates before release.
- failed: 2 Go source directories lack `doc.go` (including test-only directories).
- partial: exported function comment regex spot-check found 314 candidates; parser limitations and unexported functions not covered.
- passed: publish branch contains 0 excluded PLAN/STATUS/tasks/reviews/glm paths. Coordinator main intentionally contains them.

## Decisions D-01-D-61

Source: coordinator STATUS.md; summaries below retain the recorded first line, with later superseding decisions noted by their IDs. Missing IDs are explicitly identified.

| ID | Summary |
|---|---|
| D-01 | Use installed Claude stdio, Codex app-server, and Kimi ACP; terms clearance remains open. |
| D-02 | (2026-11-18) Go toolchain 1.27 (go.mod go 1.27.0; CI reads go.mod). Card 001-F1. |
| D-03 | Use 17 Nov spec; baseline targets remain, other missing details require proposed defaults. |
| D-04 | Add Git remote when needed; later fulfilled by D-36. |
| D-05 | (2026-11-18) "get into coding, we recheck this later": contracts provisionally frozen at 885f49a for Phase 1. |
| D-06 | (2026-11-18) Owner "accepted" 000, 001, 002 → merged to main @ 395a659 (build, vet, test -race, contracts check passed). |
| D-07 | Truncated output records the retained byte range and digest. |
| D-08 | Guard-blocked transition stays in its state and re-evaluates when facts change. |
| D-09 | Lost work already paused for unavailable harness stays paused; T21 or T36 exits. |
| D-10 | Package, function, section and test comments required; see CONVENTIONS.md. |
| D-11 | Codex defaults to Sol medium, low for mechanical work; later superseded by D-29 and D-52. |
| D-12 | Claude defaults to Sonnet medium; Opus for security/design and Haiku for mechanical reviews. |
| D-13 | Empty match.args matches nothing; multiple entries AND; absent dimension matches anything. |
| D-14 | Resolve symlinks and test path containment fail-closed, case-insensitive on Windows. |
| D-15 | Open decisions derive from triggering event and resolve with matching stored accepted receipt. |
| D-16 | Harness and model selection persists in accepted receipt and is reconstructed after restart. |
| D-17 | Supervisor owns lifetime; protocol I/O provides lossless stdout and raw stdin to adapters. |
| D-18 | Add daemon integration card 108 and pull git-per-thread card 501 forward. |
| D-19 | (2026-11-19, owner) Card 104 accepted at 8f3f133 with hardening card 104-H1 (R104-5's 4 findings) to land before 108 wires policy. |
| D-20 | Owner accepted core cards 101, 102, 103, 104, 106 and 501 into main. |
| D-21 | GLM-5.3 Flash via separate ZCode mailbox and worktrees, excluded from git. |
| D-22 | Authority effects must commit with receipts; transaction-hook proposal superseded by D-23. |
| D-23 | Use prepare/commit/abort for authority effects, preserving policy lock order. |
| D-24 | STATUS references 108 HEAD evidence check but does not record a standalone decision; confirm details with coordinator. |
| D-25 | Owner accepted 105, 105-H1, 107 and 104-H1 into main. |
| D-26 | Claude Code stdio protocol source: Anthropic public claude-agent-sdk-python. |
| D-27 | Add catalogue event types for child assignment, result and audit setting. |
| D-28 | (2026-11-19, owner) SSH for remote execution (405): use golang.org/x/crypto/ssh (new dependency approved). |
| D-29 | Codex Sol low; one agent at a time; later model superseded by D-52. |
| D-30 | (2026-11-19, owner: "mvp is the normal project we won't change a thing") No separate MVP track and no MVP card 109. Follow PLAN.md phases as written; the app becomes testable as the planned cards land. |
| D-31 | (2026-11-19, owner: "108 and 304 needs glm work so we wait for it and run the reviews") 108 and 304 stay unmerged until GLM cards 009-011 land and are verified. Meanwhile run the review queue: Claude one at a time (R405, R305, R302, R505, R402), Codex one at a time (R000-C1 confirm, R-GLM-007). |
| D-32 | (2026-11-19, owner: "stop using codex let the current agent finish and stop using it till i say it again") No new Codex dispatches or messages until the owner lifts this. The running 305 fix round (thr_hcfrdk2gxm) finishes. Builds go to GLM; reviews of Codex- and GLM-built work go to Claude (fresh, opposite vendor). Claude-built cards that need a Codex review wait. |
| D-33 | (2026-11-19, owner: "this is what i liked") Owner design reference: docs/design-reference/ (threads, approvals, findings screens). Replaces the parked 200 design pack as the basis for card 202 (desktop). Views not shown (Live output, Diffs, Machines, Settings) follow the same style. |
| D-34 | (2026-11-19, owner: "check 010 fix it he couldn't do it you do it for him write up all his cards in the folder so he continues automatically") The coordinator finished GLM card 010 itself at 28855db (glm/work-003, feat/glm-003-107-H1): backpressure kept, quit signal on shutdown, exit slot reserved, K3 in Stop; all checks passed (report appended to glm/outbox/010-report.md). Review needs a non-Claude, non-GLM reviewer: Codex after D-32 is lifted. glm/inbox/000-README-run-order.md now tells GLM to run 011 (resume) -> 012 -> 013 -> 014 -> 015 on its own. |
| D-35 | (2026-11-19, owner: "accepted and the 108 and the other one") Accepted 000-C1 (b836e32), 405 (fd4bb43), 305 (20b0a7b), 108 (0f8a9cd) and 304 (b8934c4); D-31 hold on 108/304 lifted by the owner. Merged to main @ 9dfd623 (merge commits 6bbd02b, 8f8a157, 1d45049, 4bc081d, 9dfd623). Coordinator rerun on the merged tree: go mod verify, build, vet windows/linux/darwin, golangci-lint 0 issues, go test ./... -race all 15 test packages ok, node contracts/check.mjs 187 PASS / ALL CHECKS PASSED. Note: main carries GLM gitops 6d3ab7c via 108; GLM card 011 (round 3) lands on top later. |
| D-36 | (2026-11-20, owner: "just Synch the app" to https://github.com/perseverance-syn/Synch) Remote origin added. Only the app is published, as snapshot commits on local branch publish pushed to origin/main (first: 3b474a0 = local main 9dfd623). Published paths: .gitattributes .github .gitignore .golangci.yml CONVENTIONS.md Makefile cmd contracts desktop docs go.mod go.sum internal role-skills. Never published: PLAN.md STATUS.md tasks/ reviews/ glm/. Never push local main. To publish later: build a tree of those paths from main (temp GIT_INDEX_FILE, read-tree --empty, ls-tree -r main -- PATHS / update-index --index-info, write-tree), commit-tree with -p publish, move publish, secret-scan, build/vet the archive, git pull --rebase not applicable (publish only), git push origin publish:main. |
| D-37 | (2026-11-20, owner: "just drop review for now till further notice ... we wasting tokens on reviews") Fresh-agent reviews suspended until the owner says otherwise. Each build is verified only by the coordinator's rerun (scope, build, vet x3, race tests, lint, regression proof). Nothing merges without the owner's "accepted". GLM: no builds, no reviews for now (owner: reviewing with a lower model is pointless). GLM fix cards 011-016 reassigned to Claude Sonnet builders, one at a time (Codex still paused, D-32); 011 started as thr_k6br7bnib5. |
| D-38 | (2026-11-20, owner: "accepted and proceed") Accepted 501-O1 fix 3 (adfb3ba) and 201 CLI fix 2 (02b05f7). Merged to main @ d0c6e7a (3ffcb45, d0c6e7a). Coordinator rerun on the merged tree: go mod verify, vet x3, lint 0, go test ./... -race all 16 ok, contracts ALL CHECKS PASSED. Published app snapshot 29e3695 to origin/main (secret scan clean, standalone build/vet ok). Main now carries GLM gitops through fix 3 and the CLI inside the synch binary. |
| D-39 | (2026-11-20, owner: "whatever you delegate to codex do it now on claude") Every card PLAN.md assigns to CX is built by CC (claude-sonnet-5-5 @ medium; Opus only for the security core per D-12) while Codex stays paused (D-32). One Claude agent at a time; no reviews (D-37); coordinator rerun + owner acceptance. Queue after 302/505/402 fixes: 107-H1 merge (engine hardening, GLM 003 + coordinator 010 @ 28855db, awaiting owner acceptance), daemon wiring of routing/children/remote/gates, 303 context/rollover, 403 Workstation+VirtualBox, 404 Hyper-V, 407 mode switching, 503 GHA+GitLab, 504 Jenkins, 602 cross-component scenarios; CC-native: 202 desktop, 203 MCP, 301 roles+skills, 506 proposals/evidence export; Opus: 401 credentials, 406 sandbox. |
| D-40 | (2026-11-20, owner: "accepted") Accepted 502 gate runner fix 1 (6c21009) and 107-H1 engine hardening (GLM 003 + coordinator 010, 28855db). Merged to main @ 70b904a. Coordinator rerun on merged tree: vet x3, lint 0, go test ./... -race all 18 ok. Published app snapshot a23779d to origin/main. |
| D-41 | (2026-11-20, owner answers) Follow decision 05 exactly: top-level threads are Coordinators and never run tools; work runs in managed specialist children (card 112 undoes the 111 role shortcut and wires child work into the daemon). Full Control grants stay manual per task (synch grants create); the daemon never issues grants. |
| D-42 | (2026-11-20, owner, from ARTEX heartbeat idea "if it's cheap") Child watchdog: supervisor StallTimeout (10 min, no output) already exists; add in the daemon a per-child runtime budget (default 2h) and a loop detector (5 identical consecutive tool calls), both stopping the child into T15 paused(harness_unavailable) with the reason. Existing transitions only, no contract change. Queued on the 112 builder (thr_bs5h6v5td7) as a follow-up commit. Owner: budget must be modifiable -> SYNCH_CHILD_MAX_RUNTIME (default 2h, 0/off disables) + per-project max_runtime override; same for loop repeats. Per-task override would need an API contract change (owner OK required). |
| D-43 | (2026-11-20, coordinator, under D-41 and the spec) 112 open questions: (1) a coordinator thread is not delegated until the operator has issued a Full Control grant on it (stays queued, shown as waiting for a grant), so the Builder child inherits the grant (Q-21); (2) a child with not-run/error checks waits for operator acceptance (spec: show failed/not-run, human accepts); (3) crash window between parent T05 and T08 and (4) API creating top-level threads via children.CreateRoot go to card 113. |
| D-44 | (2026-11-20, owner: "accepeted proceed") Accepted the 110-113 chain (81f3b59 incl. 302 fix 1, watchdog, waiting_for API field) + 505 fix 1 (6d7485a) + 402 fix 1 (7be0f77). Merged to main @ f4e5f34; rerun all 22 packages -race ok, vet x3, lint 0, contracts ok. Published. Follow-up: list waiting_for in contracts/api/openapi.yaml. |
| D-45 | (2026-11-20, coordinator) 503 stopped on a missing CI contract (correct). Unblocked: the builder writes contracts/drivers/ci-driver.md (proposed, accepted by the owner together with card 503), reuses external_request.* events (no catalogue change), runs the repository gate file (internal/gates) in CI for parity, defines a minimal TokenBroker in internal/ci until card 401. |
| D-46 | (2026-11-20, owner: "accepted") Accepted 303 (846daa2), 403 (037b326), 503 (479c499) incl. contract contracts/drivers/ci-driver.md v0.1.0 (now accepted). Merged to main @ 47a5144; rerun 28 packages -race ok, vet x3, lint 0, contracts ok. Published. |
| D-47 | (2026-11-20, owner: "codex unlocked but pay attention to usage and proceed with everything") D-32 lifted. Codex gpt-6.2-sol @ low, at most one Codex agent at a time (D-29); CX cards go back to Codex (504, 404, 407, 602), CC cards to Claude Sonnet (301, 203, 506, 202; Opus for 401, 406). One CX + one CC in parallel. Reviews stay off (D-37). |
| D-48 | (2026-11-20, owner: "lower on the reviews you're wasting time and tokens") Coordinator verification slimmed: per card only scope check + go build + go vet (host OS) + golangci-lint + tests of the changed packages (-race, once). No cross-OS vet, no stress repeats, no full-suite run per card. The full suite (all packages -race, vet x3, contracts) runs once at merge integration only. No fresh-agent reviews (D-37). |
| D-49 | (2026-11-20, owner: "you do it you built the api side") Claude builds the desktop (202), split into 202a (shell, API client, Threads view + live output, Opus for design fidelity) and 202b (Approvals, Findings, Diffs, Machines, Settings). 202a starts when 301 finishes (no CC+CC). |
| D-50 | (2026-11-20, coordinator) 407 scope widened to two additive internal Go API changes: api.Config ChangeMode hook (pre-acceptance refusal + actor) and execdriver Direct new admission generation after confirmed settle. After an upgrade back to Host access, existing children do not re-inherit (Q-21); they show waiting_for and the operator grants each child directly. |
| D-51 | (2026-11-20, owner: "accepted") Accepted 202a (ece3e81), 301 (8b53984), 504 (b0eb5d5, CI contract v0.2.0), 404 (6d75926). Merged to main @ 45a53f6; full rerun 31 packages -race ok, vet x3, lint 0, contracts ok. Published. Incident: the 407 Codex builder had written stray copies of its edits into the shared main checkout (12 modified + 4 new files); all byte-identical to its commit 515fada; coordinator restored the checkout before merging. |
| D-52 | (2026-11-20, owner: "6.2 lower to an optimal model") Codex model for new cards: gpt-6.1-terra @ low (medium only for security-sensitive cards); one Codex agent at a time. Basis: Terra $1/$5 per M tokens vs Sol $2/$10, coding index 51.0 vs 54.5, about 0.45-0.56x Sol cost per task; Luna rejected (agentic coding regression). 407 finishes on Sol. |
| D-53 | (2026-11-20, coordinator) 602 needs to inject several engine adapters into a real daemon: approved one additive public daemon option (Config.Adapters factories taking the event sink) replacing the private test hook; capability gate, routing, policy and grants still apply to injected adapters. |
| D-54 | (2026-11-20, owner: "accepted proceed") Accepted 202b (b8e2c2c), 407 (231d644 via 602), 602 (4401016, partial release-evidence harness), 401 (c02cd91). filippo.io/age not approved (owner silent) -> fallback stays a failing stub. Merged to main @ 9a37cf0; full rerun 33 packages -race ok, vet x3, lint 0, contracts ok. Published. Follow-up: S6 still reports failed because the 602 test does not use the 401 broker -> rewire S6 to 401 (small card). |
| D-55 | (2026-11-20, coordinator) Operational proposals: the approval rollback field holds a rollback command or a verified checkpoint reference; there is no mechanism yet that verifies a checkpoint reference (gap for 603 / API contract card). 506 continues without changing internal/policy. |
| D-56 | (2026-11-20, owner: "accepted / go api card") Accepted 506 (11d8395, partial). Merged to main @ 0d1cf32; full rerun 34 packages -race ok, vet x3, lint 0. Published. API contract card 114 approved (contract change). |
| D-57 | (2026-11-20, owner: "i don't wanna try harnesses for now, i can't disconnect my sessions") Card 601 (authenticated harness qualification) deferred by owner. Every capability stays untested; release evidence marks harness-dependent checks not run: deferred by owner. Continue all other work. Note for later: 601 can run in a separate Windows user or a VM so the owner's own Codex/Claude sessions stay connected. |
| D-58 | MCP retains read-only thread_status and search_output; approval and result tools dropped. |
| D-59 | (2026-11-20, owner: "accepted") Accepted 406 (04934be) and 114 part 1 (488451f; openapi 3.1.0). Merged to main @ 95b4593; full rerun 35 packages -race ok, vet x3, lint 0, contracts ok. Published. 114b (on 488451f) will merge as an increment. |
| D-60 | (2026-11-20, owner: "accepted") Accepted 114b (61621da; openapi 3.2.1, catalogue 4.2.0) and 203 (36ea6e0). Merged to main @ 96fd87a; full rerun 36 packages -race ok, vet x3, lint 0, contracts ok. Published. Still paused for new work (202c finishing). |
| D-61 | (2026-11-21, owner: "accepted and proceed") Accepted 202c (28af756), merged to main @ 6c115b1, published. Pause lifted: 603 next. Note: two untracked files appeared in the shared checkout (docs/improvements-from-artex.md, docs/pentest-core-review-brief.md), not created by the coordinator; left untouched and not published. |

## Check outputs

Raw logs in [release-evidence/](release-evidence/). Exit codes are recorded in each log; check summary below is generated after execution.


| Check | Status | Raw output |
|---|---|---|
| go-build | passed | [go-build.txt](release-evidence/go-build.txt) |
| go-vet-windows | passed | [go-vet-windows.txt](release-evidence/go-vet-windows.txt) |
| go-vet-linux | passed | [go-vet-linux.txt](release-evidence/go-vet-linux.txt) |
| go-vet-darwin | passed | [go-vet-darwin.txt](release-evidence/go-vet-darwin.txt) |
| golangci-lint | passed | [golangci-lint.txt](release-evidence/golangci-lint.txt) |
| go-test-race | passed | [go-test-race.txt](release-evidence/go-test-race.txt) |
| scenarios | passed | [scenarios.txt](release-evidence/scenarios.txt) |
| contracts | passed | [contracts.txt](release-evidence/contracts.txt); pinned npx tools used offline cache. |
| npm-ci | passed | [npm-ci.txt](release-evidence/npm-ci.txt) |
| npm-typecheck | passed | [npm-typecheck.txt](release-evidence/npm-typecheck.txt) |
| npm-build | passed | [npm-build.txt](release-evidence/npm-build.txt) |

[Package table](release-evidence/package-table.md).
