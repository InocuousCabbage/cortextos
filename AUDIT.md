# cortextOS Full System Audit

Owner: boris | Started: 2026-03-31 | Purpose: Map every system function, validate current implementation, identify gaps and fixes needed. Bottom-up: fundamentals first, complexity last.

---

## HOW TO USE THIS DOC

Each section has STATUS: ✅ validated | ⚠️ partial | ❌ broken/missing | 🔲 not audited yet

Work top-to-bottom. Don't fix higher layers before lower layers are validated.
After each audit step, update status + add notes.
James reviews and approves before we make changes.

---

## CRITICAL CROSS-CUTTING ISSUE — Cron loss on restart (reported by Donna 2026-04-01)

**Severity:** HIGH — affects all agents
**Status:** ⚠️ partially addressed — AGENTS.md updated, pending-reminders.json spec not yet built

Session-only crons (CronCreate) are in-memory and die on any restart. On session continuation (--continue), all crons are silently gone. Current restart protocol does NOT include a CronList verify step. Agents can falsely tell users crons are still running without checking. Donna lost 4 user reminders this way and sent a false confirmation.

Required fixes (applies to all agent templates and AGENTS.md):
1. Restart checklist MUST include: run CronList immediately, compare against config.json recurring crons AND pending-reminders.json one-shots, recreate any missing BEFORE sending online notification
2. Online notification MUST only claim crons are set AFTER CronList confirms them
3. pending-reminders.json pattern must be standardized across all agents — any one-shot cron created must be written to this file; on restart it must be read and recreated
4. False state assertion = critical failure. Agents must NEVER tell users something is set/done/active without verifying actual system state first

**Done:** All 3 template AGENTS.md session start protocols now include CronList-first recovery step
**Remaining:** pending-reminders.json spec and implementation (filed to build with James)

---

## LAYER 0 — SYSTEM FOUNDATION

### 0.1 File-system layout — ✅ validated — PR #25 merged

What it is: `~/.cortextos/{instance}/` is the runtime state store. Separate from the framework repo.
Files involved: `src/cli/install.ts`, `bus/_ctx-env.sh`
Changes made: Removed 3 dead root-level dirs; added `state/oauth/`, `state/usage/`, `outbox/`; consolidated heartbeat path; cleaned duplicate instance-level dirs
Remaining questions: Env resolution chain correctness; bus script consistency (no hardcoded paths)

### 0.2 Agent identity and spawn — ✅ validated — PR #28 merged

Becky bug fix, agent_name fallback, SYSTEM.md generation, list-agents.sh wrapper, session start protocol update, outbox dir creation

### 0.3 Environment variables and secrets — ✅ validated — PR #28 merged

4-layer env hierarchy; shared secrets in `orgs/{org}/.env`; agent-specific secrets in `agents/{agent}/.env`; ANTHROPIC_API_KEY inherited from shell by design

### 0.4 Bus script completeness — ✅ validated — PR #28 merged

48 CLI commands, 34 shell wrappers existed (12 missing); all 12 created; now 46 of 48 have wrappers

Full list of 46 wrappers: create-task, update-task, complete-task, list-tasks, archive-tasks, check-stale-tasks, check-human-tasks, send-message, check-inbox, ack-inbox, send-telegram, edit-message, answer-callback, post-activity, create-approval, update-approval, list-approvals, update-heartbeat, read-all-heartbeats, check-goal-staleness, log-event, collect-metrics, enable-agent, disable-agent, self-restart, hard-restart, soft-restart, list-agents, list-skills, auto-commit, check-upstream, hook-ask-telegram, hook-permission-telegram, hook-planmode-telegram, create-experiment, run-experiment, evaluate-experiment, list-experiments, gather-context, manage-cycle, kb-query, kb-ingest, kb-collections, kb-setup, browse-catalog, install-community-item, prepare-submission, submit-community-item

Outstanding: E2E testing; full TOOLS.md documentation audit

### 0.4b Bus script end-to-end testing — 🔲 not audited

Requires test infrastructure (future sprint)

### 0.5 Core skills — mandatory, every agent — ✅ validated — PRs #34/#36 merged

env-management, tool-registration, secrets-rotation skills created in all 3 templates

---

## LAYER 1 — AGENT BOOTSTRAP

### 1.0 AGENTS.md (was CLAUDE.md) — session start protocol — ✅ fully rewritten — 6 iterations with James

Design decisions: CLAUDE.md → AGENTS.md rename; progressive disclosure principle; three-layer memory system designed; checkpoint model for memory writes; first action = "Booting up… one moment" Telegram message BEFORE reading files; cron recovery = CronList FIRST before online notification; Telegram response rule = reply to pending messages BEFORE resuming other work

### 1.0a AGENTS.md propagation to orchestrator and analyst templates — ⚠️ partial — legacy content preserved, merge checklist created

Orchestrator unique sections to preserve:
- ORCHESTRATOR-SPECIFIC ROLE
- Activity Channel
- Coordination event logging

Orchestrator missing vs agent template:
- Full 3-layer memory
- Blocked/Human/Approval 3-state section
- Telegram CRITICAL rule
- Skills section
- Cron verification rule
- Detailed event logging table
- KB heartbeat auto-ingest

Analyst unique sections to preserve:
- Analyst Responsibilities (nightly metrics, health monitoring, system status, event log analysis)
- Local Version Control
- Upstream Sync
- Community Catalog Browsing
- Community Publishing
- Extended agent spawn

Analyst missing vs agent template:
- Full 3-layer memory
- Blocked/Human/Approval 3-state section
- Telegram CRITICAL rule
- Detailed event logging table
- KB heartbeat auto-ingest pattern
- First action "Booting up"
- Cron verification rule

**Action plan for template merge:**
1. Start with canonical `templates/agent/AGENTS.md` as base
2. Add orchestrator-specific sections after "Agent-to-Agent Messages"
3. Add analyst-specific sections after "Restart"
4. Verify both templates still pass `tests/sprint1-templates.test.ts`
5. Do NOT delete existing AGENTS.md files without confirming all unique content is captured

### 1.1 Bootstrap file read order — 🔲 not audited

Files: IDENTITY.md → SOUL.md → GOALS.md → HEARTBEAT.md → MEMORY.md → memory/YYYY-MM-DD.md → USER.md → TOOLS.md → SYSTEM.md → config.json

### 1.2 IDENTITY.md — 🔲 not audited

Contains: Name, role, emoji, vibe, work style, who they report to

### 1.3 SOUL.md — ⚠️ partial

Contains: Day/night mode behavior, autonomy rules, philosophical guidelines

James's day/night definitions:
- **Day** = normal heartbeats + workflows, otherwise idle waiting to work WITH user
- **Night** = treat idle as failure mode, iteratively work through task list, find new tasks, deliverables, system improvements

### 1.4 GOALS.md — 🔲 not audited

### 1.5 USER.md — 🔲 not audited

### 1.6 TOOLS.md — ⚠️ partial

Current state: 488-line monolith; all 3 templates have Secrets section; Issue #32 filed for progressive disclosure conversion

### 1.7 SYSTEM.md — ✅ partially resolved — PR #28

Now generated correctly from context.json; static only; dynamic roster via `cortextos list-agents`

### 1.8 MEMORY.md — three-layer memory system — ⚠️ designed, not yet validated

- Layer 1: `memory/YYYY-MM-DD.md` (working/daily)
- Layer 2: `MEMORY.md` (consolidated long-term learnings)
- Layer 3: KB/ChromaDB (associative indexed memory)

KB auto-index at heartbeat:
```bash
cortextos bus kb-ingest ./MEMORY.md ./memory/$(date -u +%Y-%m-%d).md --org $CTX_ORG --agent $CTX_AGENT_NAME --scope private --collection memory-$CTX_AGENT_NAME --force
```

---

## LAYER 2 — CORE OPERATIONS

### 2.1 Task system — full CRUD — ⚠️ partial

Operations: create-task, list-tasks, update-task (statuses: pending/in_progress/blocked/completed), complete-task, archive-tasks
Task types: Agent tasks, Human tasks, Blocked tasks

### 2.2 Agent-to-agent messaging — ⚠️ partial

Send, check inbox, ACK; un-ACK'd messages redeliver

### 2.3 Telegram messaging — ⚠️ partial

Bug filed: "Your last message" context field always shows stale/old message (grandamenium/claude-remote-manager#35, grandamenium/cortextos#31)

### 2.4 Event logging — 🔲 not audited

`log-event.sh <source> <action> <level> '<json_data>'`
Storage: `CTX_ROOT/orgs/{org}/analytics/events/*.jsonl`

### 2.5 Heartbeat — ⚠️ partial

`update-heartbeat.sh`; Storage: `CTX_ROOT/state/{agent}/heartbeat.json`; Contains: mode, status, timestamp, current task

### 2.6 Knowledge Base (RAG) — end-to-end validation — 🔲 not validated — MUST ship before launch

ChromaDB vector store with Gemini Embedding 2 (3072 dims). Gemini Flash describes non-text media before embedding. Supports 30+ file types. No auto-indexing — fully manual/on-demand.

Three-collection design:
- `memory-{agent}` (private, episodic/semantic)
- `private-{agent}` (private, agent outputs)
- `shared-{org}` (org-wide shared research)

Required to ship:
- E2E test (setup → ingest text/image/PDF → query → verify)
- `--collection` flag verification
- GEMINI_API_KEY in org .env template
- kb-setup creates all 3 standard collections
- knowledge-base SKILL.md update

---

## LAYER 3 — ORCHESTRATION

### 3.1 Goals propagation — 🔲 not audited

Intended flow: User → orchestrator → GOALS.md → propagate to analysts/specialists → break into tasks

### 3.2 Orchestrator daily task monitoring — 🔲 not audited

### 3.3 Approval workflow — ⚠️ partial

Flow: create-approval → block task → notify user → user decides → dashboard calls update-approval → inbox notification → agent unblocks/cancels
Categories: external-comms | financial | deployment | data-deletion | other

### 3.4 Human task workflow — 🔲 not audited

Difference from approval: Human task = agent CANNOT do it. Approval = agent CAN but needs permission.

### 3.5 Night mode autonomous operation — 🔲 not audited

Intended: idle = failure mode; work through task list; proactively find new tasks; no external comms/purchases/deletes

---

## LAYER 4 — SKILLS AND WORKFLOWS

### 4.0 Core vs domain skills split — ❌ not implemented

- **Core skills:** always present in every agent from day 1, framework-maintained, never in community catalog
- **Domain skills:** agent-maintained, can be installed/updated/removed, can be in community catalog

Required: inventory all existing skills, classify each, ensure core skills in all templates, document in CONTRIBUTING.md

### 4.1 Skill architecture — ⚠️ partial

Skill = directory with SKILL.md (YAML frontmatter + markdown body); discovery: framework → template → agent (later overrides earlier)

### 4.2 Workflow creation — 🔲 not audited

Types: raw skill, chained skills, skill+tool pair, skill+cron

### 4.2a Autoresearch (experiments) cron — ✅ wired up in onboarding

The `autoresearch/SKILL.md` skill exists in every agent template and is intentionally NOT in the default cron list. It must be configured per-agent — either during onboarding or any time the user requests it later.

**Implemented:**
- `experiments/config.json` ships in both `templates/agent/` and `templates/orchestrator-template/` with `{approval_required: true, cycles: []}`
- `cortextos bus manage-cycle create` now includes all fields the dashboard expects: `metric_type`, `measurement`, `loop_interval`, `enabled`, `created_by`, `created_at`
- `autoresearch/SKILL.md` updated in both templates with correct setup commands
- Agent ONBOARDING.md Part 5 (steps 21-24) updated to use `manage-cycle create` + set up cron immediately
- Orchestrator ONBOARDING.md Step 18 updated with orch-specific metrics + same flow
- SKILL.md rule fixed: "cannot autonomously modify" (user-directed changes are allowed)

**Cycle creation flow (applies whenever a cycle is created):**
1. Collect: metric, metric_type, surface, direction, window (measurement), loop_interval (cron frequency), measurement method, approval_required
2. `cortextos bus manage-cycle create $CTX_AGENT_NAME --cycle <name> --metric <m> --metric-type <t> --surface <s> --direction <d> --window <w> --measurement <how> --loop-interval <freq>`
3. Set up cron immediately: `/loop <loop_interval> Read .claude/skills/autoresearch/SKILL.md and execute the experiment loop.` + add to config.json crons

This applies to ALL agents (specialist, analyst, orchestrator). The metric and surface are role-specific.
Analyst ONBOARDING.md still needs the same update when Layer 5 redesign is done.

### 4.3 Analyst metrics collection — 🔲 not audited

---

## LAYER 5 — ONBOARDING CHAIN

> Note (2026-04-01): Full onboarding chain redesign — root/system-level, orchestrator, analyst, and generic agent — is a dedicated audit task for when we reach Layer 5. All 4 levels must be audited and redesigned together.

### 5.1 Root onboarding — ⚠️ audited, issues found

Issues:
- Phase 5b asks analyst name (wrong)
- Phase 8e writes theta wave config before analyst exists
- KB should say "upcoming feature, skip"
- Migration step not yet added

### 5.2 Orchestrator ONBOARDING.md — ⚠️ audited, issues found

Issues:
- Part 6 theta wave awareness redundant
- Part 7 step 24 re-asks analyst name
- No explicit "switch to analyst Telegram chat" instruction
- Doesn't read system config files

### 5.3 Analyst ONBOARDING.md — ⚠️ audited, issues found

Issues:
- Parts 1/1b ask for org info instead of reading context.json
- Re-asks working hours
- No final handoff instruction

### 5.4 Specialist ONBOARDING.md — ⚠️ audited, issues found

Issues:
- Step 2 re-asks agent name (already seeded)
- Step 6 re-asks working hours (third time in chain)
- No "you're online, tell your orchestrator" close

### 5.5 Bootstrap file isolation — ❌ not implemented

Principle: No agent writes another agent's bootstrap files. Ever.

Current violations:
- Root onboarding writes analyst theta wave config
- Orchestrator implies it might write analyst identity

### 5.6 System-config vs agent-config split — ❌ not implemented

- **System config** (org-level, never re-asked): org name/description/timezone, day/night mode, global approval policy, dashboard URL, KB (skip for now), global tools, migration data
- **Agent config** (per-agent Telegram chat): IDENTITY.md, GOALS.md, SOUL.md, USER.md, working hours override, theta wave posture, specialist list, crons/workflows

### 5.7 Migration step — ❌ not implemented

Optional Phase 0: "Are you migrating from OpenClaw or another agent setup?" — extract/adapt existing IDENTITY/GOALS/SOUL/tool configs

---

## LAYER 6 — DASHBOARD SYNC

### 6.1 Data flow — ⚠️ partial

Flow: File changes in CTX_ROOT → Chokidar watcher → SQLite → SSE → browser
Dashboard reads: tasks, approvals, analytics events, heartbeats, inbox messages

---

## OPEN SYSTEM DESIGN QUESTIONS

- Shared secrets/keys — ✅ RESOLVED: `orgs/{org}/.env` is the shared secrets file
- Global tools — Proposal: `orgs/{org}/TOOLS.md` for org-wide tools
- KB — Remove KB questions from onboarding for now; say "upcoming feature"
- Dashboard URL — Default localhost:3000; only prompt for iOS deploy
- Working hours — Proposal: stored once in `orgs/{org}/config.json` as system default
- Theta wave — Proposal: each agent decides own experimental posture; remove from root
- Skill chaining — No documented pattern; proposal: skills include "## Chains Into:" section
- New workflow creation — No documented process; need a "skill creation" meta-skill
- Agent system awareness — Are there gaps in what agents know about the system?
- Orchestrator conflict prevention — No locking if orchestrator and specialist update same task
- Multi-machine orchestration — ✅ FILED: grandamenium/cortextos#29; single-machine only for now

---

## NEXT STEPS (in order)

1. - [x] Fix cross-cutting cron loss — update all 3 template AGENTS.md restart sections ✅ DONE — pending-reminders.json spec still needed
2. - [x] Implement 0.5 core skills — env-management, tool-registration, secrets-rotation in all templates ✅ DONE (PR #34/#36)
3. - [ ] Implement 4.0 core vs domain split — classify all skills, document principle
4. - [ ] Validate Layer 1 bootstrap files — audit SOUL.md, GOALS.md, TOOLS.md, MEMORY.md, IDENTITY.md, USER.md consistency across all 3 templates
5. - [ ] Define pending-reminders.json spec — standardize one-shot cron persistence for restart recovery
6. - [ ] Validate Layer 1.8 memory system — verify memory/ dir ships, daily file path consistent, MEMORY.md format in all 3 templates
7. - [ ] Validate 2.6 KB (three-collection design) — must ship before launch, needs E2E test with James
8. - [ ] Answer open design questions 2-10 with James before touching onboarding
9. - [ ] Design system-config schema — what goes in `orgs/{org}/config.json`, full spec
10. - [ ] Redesign onboarding chain — root → orch → analyst → specialist with clean isolation (Layer 5 dedicated sprint)
11. - [ ] Validate Layer 2 — trace task system, messaging, approvals end-to-end
12. - [ ] Validate Layer 3 — trace goals propagation and approval workflow
13. - [ ] Document skill architecture — create skill chaining conventions (Layer 4)
14. - [ ] Dashboard gap analysis — what's missing from the UI (Layer 6)
15. - [ ] 0.4b bus testing sprint — build integration test infrastructure, test all 46 wrappers
