# Node cortextOS Audit — April 3, 2026

Issues found during James's onboarding session. To be fixed in grandamenium/cortextos.

---

## 1. Onboarding CLI outputs explanatory text with literal quotes

**What happens:** The CLI wraps intro/context text in literal quote marks when printing to the user. The quotes were meant as script markers indicating what the CLI should *say*, not to be rendered verbatim.

**Example:**
```
"cortextOS organizes your agents into Organizations. An Organization is a group of agents..."
```
Should render without the surrounding quotes.

**Fix needed:** Strip the quote wrappers from all explanatory onboarding text blocks in the CLI output.

---

## 2. Instance ID bleed-over — new system shares state with live bash system

**What happens:** New node v2 install defaults to `CTX_INSTANCE_ID=default`, same as the live bash cortextos-mac system. Both write to `~/.cortextos/default/`. The `enabled-agents.json` ends up with agents from both systems, and the dashboard shows everything mixed together.

**Fix needed:** The install flow (or first-run config) should assign a unique `CTX_INSTANCE_ID` so the new system's state lives at its own path (e.g., `~/.cortextos/v2/`). PM2 ecosystem.config.js and the dashboard `.env.local` must both use the correct `CTX_ROOT`.

---

## 3. Add Playwright config to all agent templates

**What James wants:** Every agent template (agent, orchestrator, analyst) should ship with the Playwright-recommended `playwright.config.ts` so agents automatically have Playwright access upon install — no manual setup required.

**Fix needed:** Add the official Playwright recommended config to each template directory. Ensure `@playwright/test` is included as a dependency in the template or installed as part of agent setup.

---

## 4. Analyst onboarding never prompts for remote GitHub repo setup

**What happens:** The analyst agent (Sentinel2) completed onboarding without asking James to set up a remote GitHub repo for local version control. The analyst template includes local-version-control and upstream-sync skills, but the onboarding flow doesn't walk the user through creating/linking a remote repo.

**Fix needed:** Analyst ONBOARDING.md should include a step that prompts the user to create a GitHub repo (or provide an existing one) and configures the remote so `git push` and upstream sync work from day one.

---

## 5. New agents not added to enabled-agents.json after onboarding

**What happens:** When a new agent completes onboarding, it does not automatically register itself in `enabled-agents.json`. Sentinel2 flagged this herself: "analyst2 isn't in enabled-agents.json yet (the list is empty). If I go offline, run: cortextos start analyst2."

This means agents that crash or restart may not come back up automatically — they are not tracked by the daemon as enabled.

**Evidence:** Screenshot from Sentinel2's onboarding message, 12:41 today. Same issue visible in dashboard bleed-over (enabled-agents.json not being updated when agents are created via the onboarding flow).

**Fix needed:** The agent onboarding completion step (or the CLI's `add-agent`/`enable` flow) must write the agent's entry to `enabled-agents.json` at the correct `CTX_ROOT`. This is likely the root cause of multiple associated issues: agents not appearing in the dashboard, not restarting after crashes, and manual intervention required to bring them back online.

---

## 6. Install script does not detect existing claude-remote-manager state (upgrade path collision)

**Who is affected:** Anyone in the Agent Architects community upgrading from the old bash claude-remote-manager to node v2. Not a problem for fresh installs.

**What happens:** The bash system writes heartbeats, state files, and logs to `~/.cortextos/default/` via its bus scripts. When node v2 installs with `CTX_INSTANCE_ID=default` (the default), the node dashboard scans the same directory and picks up the bash agents' state — causing bleed-over identical to what James experienced.

**Fix needed:** The install script should check whether `~/.cortextos/default/` already contains agent state (e.g., presence of a `state/` subdirectory with files). If detected, prompt the user to either choose a different `CTX_INSTANCE_ID` for the new system, or acknowledge the existing state and handle it explicitly. First-time installers are never affected.

---

## 7. Analyst template missing "notify user on boot/restart" step

**What happens:** After a restart, paul2 (orchestrator) sent James a message confirming it was back online. analyst2 (Sentinel2) did not — it came back in continue mode and jumped straight into work silently.

**Root cause:** The orchestrator CLAUDE.md includes an explicit step to notify the user on session start. The analyst CLAUDE.md does not.

**Fix needed:** Add a "notify James on boot/restart via Telegram" step to the analyst template's CLAUDE.md, matching the orchestrator pattern. Every agent should announce itself when it comes online so the user knows the system is live.

---

---

## 8. complete-task CLI rejects --result argument despite AGENTS.md documenting it

**What happens:** Agents are instructed to complete tasks with a result summary:
```
cortextos bus complete-task <id> --result "[summary]"
```
Running this produces: `error: too many arguments for 'complete-task'. Expected 1 argument but got 2.`

The command works without the result argument, but result summaries cannot be attached via CLI. All task completions are bare — no outcome recorded.

**Fix needed:** Add `--result` option to the `complete-task` command so completion summaries can be stored on the task record.

---

## 9. send-message reply_to argument not supported despite being documented

**What happens:** AGENTS.md instructs agents to reply to messages using a 4th argument:
```
cortextos bus send-message <agent> normal '<msg>' <msg_id>
```
Running with 4 arguments produces: `error: too many arguments for 'send-message'. Expected 3 arguments but got 4.`

This means agents cannot auto-ACK a message by including the original msg_id. They must call `ack-inbox <msg_id>` as a separate step, and even then, the reply is not linked to the original message.

**Fix needed:** Add an optional 4th argument (or `--reply-to` flag) to `send-message` that accepts a message ID, writes it as `reply_to` on the outbound message, and auto-ACKs the original.

---

## 10. Telegram messages containing underscores cause Markdown parse errors

**What happens:** Sending a Telegram message with underscores (e.g., variable names like `CTX_ROOT`, `CTX_INSTANCE_ID`) causes the Telegram API to return:
```
Bad Request: can't parse entities: Can't find end of the entity starting at byte offset X
```
Telegram's regular Markdown treats `_` as italic markers. A single underscore without a closing `_` is invalid.

**Fix needed:** Either (a) escape underscores automatically in `send-telegram` before sending, or (b) switch all Telegram sending to `parse_mode=None` (plain text) or `MarkdownV2` with proper escaping. AGENTS.md should warn agents not to use underscores in Telegram messages until this is fixed.

---

## 11. send-message to cross-instance agents shows "agent not found" warning and may silently drop

**What happens:** When analyst2 (lifeos2 instance) attempts to send a message to boris (lifeos/default instance), the CLI warns:
```
Warning: agent 'boris' not found in project. Message will be queued but may never be read.
```
The message is queued at `~/.cortextos/lifeos2/inbox/boris/` — not the path boris actually reads (`~/.cortextos/default/inbox/boris/`). Cross-instance agent messaging does not work without manually overriding env vars.

**Fix needed:** Either (a) add a cross-instance routing layer so messages to known agents on other instances are delivered correctly, or (b) document that agents must set `CTX_INSTANCE_ID` to the target agent's instance before calling send-message. Workaround: `CTX_INSTANCE_ID=default CTX_AGENT_NAME=<sender> cortextos bus send-message <agent> ...`

---

## 12. Dashboard Workflows page crashes on crons with raw crontab expressions

**Status: FIXED** (committed today)

**What happens:** The Workflows page (/workflows) rendered a full-page error boundary crash:
```
Something went wrong
Cannot read properties of undefined (reading 'match')
```

**Root cause:** The `Cron` interface in `workflows/page.tsx` only declared `interval: string` (required), but the config schema also supports crons with `type: "recurring"` + a raw `cron: "0 9 * * *"` field instead of `interval`. Two analyst2 crons (daily-digest, theta-wave) use this format. The `intervalToHuman(cron.interval)` call received `undefined` → crash.

**Fix applied:** Updated `Cron` interface to include optional `interval`, `cron`, and `fire_at` fields. Updated `intervalToHuman` to handle `undefined`. Updated badge display to show `cron: 0 9 * * *` for raw-cron entries and `once at <datetime>` for fire_at entries.

---

## 13. Dashboard Activity page shows "Reconnecting..." SSE indicator

**What happens:** The Activity page's real-time event stream shows a yellow "Reconnecting..." badge persistently. Events still load, but the SSE connection appears to be cycling.

**Likely cause:** The SSE connection is re-establishing after the Next.js production server drops the connection (common with PM2 + production build). May be a keep-alive or timeout issue.

**Fix needed:** Investigate SSE keep-alive configuration in the activity event stream API route. Consider increasing server-side timeout or sending periodic heartbeat pings to keep the connection alive.

---

## 14. Analytics page — "Fleet Health" shows bash-era instructions

**What happens:** The Fleet Health section on the Analytics page shows:
```
No health data available. Run collect-analytics.sh to generate reports.
```
This references a shell script that doesn't exist in the node v2 system.

**Fix needed:** Update the empty state message to reflect the node v2 way to generate health data, or wire up a real health data collection API.

---

## 15. Experiments page — "Get started" block references bash CLI

**What happens:** The Experiments page empty state shows:
```
Use manage-cycle.sh create to assign a research cycle to an agent
```
This is a bash-era command that doesn't exist in node v2.

**Fix needed:** Update the empty state instructions to reflect the node v2 API/CLI or dashboard workflow for creating experiments.

---

## 17. First Telegram message to newly spawned agent sometimes requires sending twice

**What happens:** When orchestrator spawns a new agent and sends the first Telegram message to trigger onboarding, the agent may not pick it up on the first send. James observed:
- Orchestrator (paul2): picked up first "hi" message immediately on first check cycle
- Analyst (analyst2, spawned by orchestrator): required sending "hi" twice before the agent processed it

**Suspected cause:** The orchestrator spawns the sub-agent, then immediately sends a message before the sub-agent's fast-checker daemon has fully initialized and begun polling its inbox. The first message arrives in the inbox before the daemon is listening; the second send hits while it's now active.

**Expected behavior:** Every agent should reliably receive the first message regardless of spawn timing.

**Fix needed:** Either (a) add a startup delay before the orchestrator sends the initial message to newly spawned agents, giving the daemon time to initialize, or (b) have the sub-agent send its own "I'm online" message first, and only then have the orchestrator reply. The orchestrator's spawn workflow should account for daemon startup latency.

---

## 18. Mac-native dashboard Analytics page crashes (used_pct)

**Found during:** visual audit of mac-native dashboard at `~/.cortextos/default`

**What happens:** Analytics page shows: "Failed to load analytics — Cannot read properties of undefined (reading 'used_pct')"

**Context:** The v2 dashboard Analytics page works fine. The mac-native dashboard (bash system) crashes here, suggesting the bash analytics data format differs from what the shared dashboard component expects, or that a required analytics data field is missing from the bash system's output.

**Fix needed:** Guard against `undefined` before accessing `used_pct` in the analytics component, with a graceful empty state.

---

## 19. Mac-native dashboard Strategy page shows no goals (data not shared across instances)

**Found during:** visual audit comparison

**What happens:** Mac-native Strategy shows "No goals yet — Add your first goal." V2 Strategy shows James's goals (100k/month from Skool, cortextOS launch, etc.). Goals are stored per-org config in the v2 system and not shared with the bash system at `~/.cortextos/default`.

**Not strictly a bug** — expected behavior of separate instances. But worth noting that when users migrate from bash to v2, they need to manually re-enter goals/strategy data. Should document in the migration guide.

---

## 20. Mac-native Settings shows "Organization not configured" despite org existing

**Found during:** visual audit of mac-native Settings

**What happens:** Settings > Organization tab shows: "Organization not configured. Run /cortextos-setup to set up your Organization." This is because the bash system doesn't use the same org config file format that the dashboard Settings page expects.

**Fix needed:** Either (a) the Settings page should read from the bash-compatible org config path, or (b) the migration path should copy/convert existing org config.

---

## 21. Tasks page missing filters and "+ New Task" button in v2

**Found during:** visual audit comparison

**Mac-native Tasks:** Has 5 filter dropdowns (all/all/all/all/all), Board/List view toggle, "+ New Task" button. Shows 47 pending / 2 in progress / 9 blocked / 4 completed.

**v2 Tasks:** No filters, no list view toggle, no new task button. Only shows Kanban columns. The rich filtering and task creation UI from the mac-native version is absent in v2.

**Fix needed:** Port the filter dropdowns, List view, and "+ New Task" button to the v2 Tasks page.

---

## 22. Experiments page shows real data in mac-native, empty in v2

**Found during:** visual audit comparison

**Mac-native Experiments:** Full page with stats (1 active cycle, 11 total, 50% keep rate), tabs (By Agent, Timeline, Learnings), and all experiments listed with hypothesis/metric data.

**v2 Experiments:** Empty state only — no stats, no experiment data, no tabs.

**Root cause:** V2 experiments page reads from `~/.cortextos/lifeos2/` which has no theta wave / experiment history yet. This will self-resolve as agents run experiments, but the empty state text referencing `manage-cycle.sh` is still a bash-era reference (item #15).

---

## 23. Skills page shows "Installed (0)" in v2 vs "Installed (4)" in mac-native

**Found during:** visual audit comparison

**Mac-native Skills:** "Installed (4)" — comms, cron-management, Task System, web-research shown as installed with agent tags.

**v2 Skills:** "Installed (0)" — all 8 skills show as Available. No skills registered as installed.

**Root cause:** V2 instance (lifeos2) has not had skills installed yet. Expected for a fresh system, but the install status is per-instance. This will resolve as agents install skills via the dashboard.

---

## 24. Agent templates have no standard workspace folder structure

**What James wants:** Every agent template should ship with a defined folder structure so agents have consistent, clean workspaces out of the box. Currently agent directories contain only bootstrap files (CLAUDE.md, IDENTITY.md, GOALS.md, etc.) with no standard subdirectory organization.

**Proposed structure (to be designed):**
- `docs/` — agent-generated documentation, research notes, reference material
- Potentially: `scratch/`, `exports/`, or other standard working dirs

**Fix needed:** Define the canonical workspace folder structure for agent templates (agent, orchestrator, analyst). Add the folders to each template directory. Consider whether all templates should share the same structure or if analysts/orchestrators get additional folders.

---

## 25. Bootstrap files have no workspace hygiene instructions

**What James wants:** Agents need explicit guidance somewhere in their bootstrap files on how to keep a clean workspace — what to create, where to put things, what to clean up, and when.

**What happens now:** Agents create files wherever they want with no standard conventions. Over time workspaces accumulate scattered files with no obvious structure.

**Fix needed:** Add a "Workspace Hygiene" section to either CLAUDE.md, SYSTEM.md, or a dedicated WORKSPACE.md bootstrap file in each template. Should cover: where to put working files, how to name them, when to archive vs delete, and how to keep the root agent directory clean.

---

## 26. Agent/system creation flow needs a "migrate existing config" step

**Status:** In development — James and paul2 working on this now (April 3, 2026)

**What's needed:** Early in the agent creation and system setup flow, there should be a step that lets users migrate existing agent configs into cortextOS. Exact scope TBD — more detail coming from James.

**Placeholder for now.** Will update when James provides full spec.

---

## 28. Agents get stuck when PermissionRequest hook fires for non-claude-dir operations

**Status:** Needs fix in ALL agent templates (specialist, orchestrator, analyst, worker)

**What happened:** donna2 froze for ~43 minutes. stdout showed spinning cursor (interactive prompt waiting for input). The daemon restarted her after detecting the stall.

**Root cause:** Even with `--dangerously-skip-permissions` passed to Claude Code, `PermissionRequest` hooks still fire. The `hook-permission-telegram` hook auto-approves operations in the `.claude/` directory, but for any other tool use that triggers a permission request (e.g., editing agent config files like SOUL.md, USER.md, or external files), it sends a Telegram Approve/Deny button and waits up to 30 minutes. If James doesn't respond, donna2 is stuck.

**Evidence:** donna2 JSONL transcript 46135d4a, April 3 2026 ~14:33. Paul2 independently diagnosed: "that permission prompt is the 'allow Claude to edit files' dialog. It can be pre-approved via settings so agents never get stuck on it."

**Fix needed:** Add a `permissions.allow` block to the Claude Code `settings.json` in ALL agent templates to pre-approve common operations (Edit, Write, Bash, Read). When permissions are pre-approved at the settings level, Claude Code does not fire the PermissionRequest hook for those tools, preventing the freeze entirely. Apply to: templates/agent, templates/orchestrator, templates/analyst, and any worker templates.

**Current settings.json structure:**
```json
{
  "hooks": { ... }  // no "permissions" key
}
```

**Fix:**
```json
{
  "permissions": {
    "allow": ["Edit", "Write", "Bash", "Read", "WebFetch", "WebSearch"]
  },
  "hooks": { ... }
}
```

---

## 27. Agents receive blank messages when James sends a photo via Telegram

**What happens:** When James sends a photo to an agent via Telegram, the agent receives a blank message instead of the photo with its caption. paul2 reported: "I don't see any screenshot on my end — I only received a blank message."

**Observed:** paul2 Telegram conversation, April 3, 2026 at 14:33.

**Expected behavior:** Photo messages should arrive with a local file path and caption, matching the format documented in CLAUDE.md:
```
=== TELEGRAM from <name> (chat_id:<id>) ===
<caption>
local_file: ./telegram-images/<filename>
```

**Suspected cause:** The fast-checker or Telegram polling script for node v2 agents may not be downloading photo attachments or injecting the `local_file:` path into the message before delivering it to the Claude session. May also affect other media types (documents, voice messages).

**Fix needed:** Verify that the node v2 fast-checker/Telegram handler downloads photo attachments and delivers them with the `local_file:` path format. Compare with the bash system's `check-telegram.sh` photo handling.

---

## 16. Settings page — Brand Voice renders raw markdown with HTML comments

**What happens:** The Brand Voice section in Settings renders the raw markdown source including HTML comment placeholders:
```
# Brand Voice
## Tone
<!-- e.g., casual, professional, technical -->
## Style
<!-- e.g., concise, detailed, conversational -->
```

**Fix needed:** Either render it as a proper editable textarea (matching the Goals section style), or render the markdown properly. HTML comments should not be visible to the user.

---

## 12. No detection mechanism for permission-blocked agents

**What happens:** When a Claude Code agent hits a permission prompt it cannot auto-accept, it goes silent — no Telegram response, no heartbeat update, no error. From the outside it looks identical to a crashed or context-compacted agent. James had to manually diagnose Donna today (2026-04-03 ~18:30 UTC) when she stopped responding due to a permissions block.

**Root cause:** The PTY sees a permission prompt but the fast-checker has no way to detect this state. The agent's stdout log fills with the permission UI, but nothing surfaces it to the user.

**Fix needed:** Two options — (a) fast-checker detects >N minutes of silence during day mode and sends a proactive Telegram alert ("agent may be blocked — check terminal"), or (b) the PTY wrapper detects permission prompt patterns in stdout and auto-surfaces them via Telegram. Option (a) is simpler. Threshold: >30 min silence during day mode = alert.


---

## 29. `cortextos bus hook-*` commands crash with MODULE_NOT_FOUND — all permission hooks broken

**Severity:** Critical — the entire Telegram permission notification system has never worked.

**What happens:** When any `PermissionRequest` hook fires and calls `cortextos bus hook-permission-telegram`, the process immediately crashes with:
```
Error: Cannot find module '/Users/cortextos/cortextos-v2/hooks/hook-permission-telegram.js'
```
This means no Telegram message is sent, no decision is returned to Claude Code, and the agent hangs until the hook `timeout: 1860` is reached (~31 minutes).

**Root cause:** In `src/cli/bus.ts:800`, the hook path is computed as:
```typescript
const hookPath = join(__dirname, `../hooks/${hookName}.js`);
```
The CLI is compiled as a **single bundle** to `dist/cli.js`, so `__dirname` = `dist/`. Then `../hooks/` = `cortextos-v2/hooks/` — a directory that does not exist. The hooks are actually at `dist/hooks/`.

**Fix needed:** Change line 800 to:
```typescript
const hookPath = join(__dirname, `hooks/${hookName}.js`);
```
(Remove the `../` prefix.) Same fix applies to the `crash-alert` hook path at the bottom of `bus.ts`. After the fix, rebuild and reinstall with `npm run build && npm install -g .`.

**Evidence:**
- `cortextos bus hook-permission-telegram` with test input → instant MODULE_NOT_FOUND crash
- `ls dist/hooks/` confirms hooks ARE compiled there
- donna2 freeze (~43 min) was NOT caused by the hook sending Telegram and waiting — the hook crashed before sending anything
- James confirmed he received no Telegram notification for donna2's permission request

**Impact:** The `permissions.allow` fix (audit item 28) is the correct immediate mitigation — it prevents PermissionRequest hooks from firing for pre-approved operations. But non-approved operations (unexpected tool calls, external operations without permissions.allow) will still cause agent freezes with no Telegram alert.

---

## 13. No staggered multi-agent restart command

**What James expected:** A single CLI command to restart all agents in an org with staggered delays (e.g., `cortextos bus restart-all --stagger 10s`).

**What exists:** `cortextos bus soft-restart <agent>` restarts one agent at a time via IPC. `cortextos bus self-restart` only writes a marker file — it does NOT trigger an immediate restart. Agents calling self-restart may sit indefinitely waiting for the daemon to poll the marker.

**Fix needed:**
1. Add `cortextos bus restart-all [--org <org>] [--stagger <seconds>]` that iterates all enabled agents and calls soft-restart on each with a configurable delay between restarts.
2. Fix self-restart to immediately send an IPC signal rather than passively writing a marker. Agents should be able to trigger their own restart on demand.


---

## 30. Daemon leaves stale socket on SIGKILL — new instance fails to bind

**What happens:** When the daemon is killed with SIGKILL (or crashes uncleanly), the `daemon.sock` Unix socket file is not cleaned up. On next startup, `net.createServer().listen(socketPath)` throws `EADDRINUSE` and the daemon fails to bind, leaving agents in a "daemon not running" state even though PM2 shows the process as online.

**Root cause:** `IpcServer` only removes the socket on graceful shutdown. SIGKILL bypasses all cleanup handlers.

**Fix needed:** In `IpcServer.listen()` (or daemon startup), call `fs.unlinkSync(socketPath)` (wrapped in try/catch) before `server.listen()`. This is standard practice for Unix socket servers. Same fix should handle stale `daemon.pid` file.

**Evidence:** April 3, 2026 ~19:22 — daemon restarted by PM2 after SIGKILL, socket file remained at `~/.cortextos/lifeos2/daemon.sock`, new daemon couldn't connect, all 3 agents appeared running via heartbeats but Telegram pollers were dead. Fixed by manually deleting socket + pid and `pm2 restart cortextos-daemon`.

---

## 31. AgentProcess.stop() crashes daemon with TypeError when PTY exits during graceful shutdown

**What happens:** `restartAgent()` crashes the daemon with:
```
TypeError: Cannot read properties of null (reading 'kill')
at AgentProcess.stop (agent-process.ts:117)
```

**Root cause:** Race condition in `AgentProcess.stop()`. The method checks `if (this.pty)` then does `await sleep(3000)` while sending `/exit`. If the PTY process actually exits during those 3 seconds, the `onExit` handler fires and `handleExit()` may set `this.pty = null`. When execution resumes after the sleep, `this.pty.kill()` crashes because `this.pty` is now null.

**Fix needed:** Capture `this.pty` in a local variable before the await:
```typescript
const pty = this.pty;
if (pty) {
    try {
        pty.write('\x03');
        await sleep(1000);
        pty.write('/exit\r');
        await sleep(3000);
    } catch { }
    pty.kill();
    this.pty = null;
}
```

**Impact:** Every soft-restart attempt (via `cortextos bus soft-restart`) has a high chance of crashing the daemon if the agent exits cleanly within 3 seconds of receiving `/exit`. This was the root cause of the April 3 fleet outage — sentinel2's soft-restarts of paul2 and analyst2 triggered the crash loop.

---

## 32. SSE stream — 401 Unauthorized, Live Activity feed dead

**What happens:** `GET /api/events/stream` returns 401 on both the Overview and Activity pages. The feed shows "Reconnecting..." permanently and never displays live events.

**Severity:** High — the core real-time feed is completely broken.

**Fix needed:** Check auth middleware on the SSE route. The `/api/events/stream` endpoint is likely not passing the session token correctly for EventSource connections (browsers don't send cookies by default with EventSource on some configurations).

---

## 33. Agent Stop action returns 400 Bad Request — silent failure

**What happens:** Agent card actions dropdown > Stop → `POST /api/agents/<name>/lifecycle` with `action: "stop"` returns 400. No error toast or feedback is shown to the user — the UI silently fails.

**Severity:** High — agent lifecycle management (stop) is broken with no user feedback.

**Fix needed:** (a) Fix the API endpoint to handle the stop action correctly. (b) Add error handling in the UI to surface the 400 response as a toast/error message.

---

## 34. Memory tab — today's memory file returns 403, raw JSON error rendered

**What happens:** Agent detail page > Memory tab > today's date file (e.g. `2026-04-03.md`) → `GET /api/agents/<name>/memory?path=...` returns `{"error":"Forbidden"}`. The raw JSON string is rendered in the accordion body instead of a user-friendly message.

**Severity:** Medium — daily memory is inaccessible from the dashboard; raw API error shown to user.

**Fix needed:** (a) Fix the path traversal/permission check so today's memory file is accessible. (b) Add graceful error state in the Memory tab UI.

---

## 35. Task creation silently fails — task never appears after form submit

**What happens:** Tasks page > New Task button > fill form > Submit → dialog closes with no error, but the task never appears in any column (Pending/In Progress) on either Board or List view, even after full page reload. No console errors.

**Severity:** High — task creation (core workflow) is completely non-functional from the dashboard.

**Fix needed:** Debug `POST /api/tasks` — likely the API is returning success but writing to the wrong path, wrong instance, or wrong org. Check that the task write path uses the correct `CTX_ROOT`/instance ID.

---

## 36. Analytics — Recharts renders with negative container dimensions

**What happens:** 5 instances of `<rect> attribute width: A negative value is not valid` in console on the Analytics page. Charts render before container width is calculated.

**Severity:** Low — charts do display but flicker briefly on first load.

**Fix needed:** Wrap Recharts components in a `ResponsiveContainer` with a minimum width, or use `useEffect` + `useState` to defer render until container dimensions are known.

---

## 37. Skill slash command display — port from bash system to Node v2

**What happens:** When a user types `/` in a Telegram chat with an agent bot, no skill commands appear. The Node v2 daemon already calls `registerTelegramCommands` at startup via `agent-manager.ts`, but the scan only covers `agentDir` and `frameworkRoot` — it misses template-level skills (`templates/<role>/.claude/skills/`) and does not re-register if new skills are added after boot.

**Severity:** Medium — UX gap: users can't discover available agent commands from Telegram chat UI.

**Fix needed:**
- In `agent-manager.ts`, also include the agent's template skills dir in `scanDirs` (read template name from `config.json`, derive `templates/<role>` path)
- Optionally: re-register commands on agent restart, not just first start
- Reference: `cortextos/bus/list-skills.sh` has the correct multi-tier scan logic (framework → template → agent) to port

---

## 38. Pre-compact hook missing from agent templates — user gets no warning during context compaction

**What happens:** When Claude Code auto-compacts an agent's context window, the agent goes silent for 30–90 seconds with no notification. Users see no response and don't know if the agent crashed or is compacting.

**Severity:** Medium — confusing UX; users may think the agent is down.

**Fix needed:**
- Create hook handler `src/cli/hooks/hook-pre-compact.ts` (or shell equivalent) that reads `BOT_TOKEN`/`CHAT_ID` from the agent's `.env` and sends "⏳ Auto-compacting context — back in a moment" to Telegram
- Add `PreCompact` hook entry to all agent template `settings.json` files:
  ```json
  "PreCompact": [
    {
      "hooks": [
        {
          "type": "command",
          "command": "cortextos bus hook-pre-compact",
          "timeout": 10
        }
      ]
    }
  ]
  ```
- Apply to: `templates/orchestrator/.claude/settings.json`, `templates/analyst/.claude/settings.json`, `templates/agent/.claude/settings.json`
- Also apply retroactively to all active lifeos2 agent `settings.json` files

---

## 39. Cron restoration gap — agents restart with fewer crons than config.json specifies

**What happens:** On session start, agents are instructed to restore crons from `config.json`. In practice, agents inconsistently restore only some crons (e.g., analyst2 booted with 2 of 6 crons active — only `digest` and `theta-wave` — skipping `heartbeat`, `nightly-metrics`, `auto-commit`, and `upstream-check`). No error is surfaced; the missing crons silently don't run.

**Severity:** Medium — agents run in a degraded state without knowing it. Heartbeat crons especially critical (skipping = agent shows stale on dashboard).

**Root cause:** The session startup prompt instructs agents to run `CronList` first to avoid duplicates, then recreate from config. In practice agents may misparse the config format (interval vs cron field), skip entries that look already active, or stop early on context pressure.

**Fix needed:**
- Add a post-startup verification step: after restoring crons, compare active cron count to config.json entry count and warn if mismatch
- Or: add a `cortextos bus verify-crons` CLI command that diffs config.json against CronList output and reports missing entries

---

## 40. Autoresearch cycle setup skipped by most agents during onboarding

**What happens:** Of all lifeos2 agents that went through onboarding, only one (analyst2) set up an autoresearch cycle. paul2, donna2, and data2 did not. No errors — agents simply didn't initiate it.

**Severity:** Medium — autoresearch is a core autonomous capability. Agents silently missing it means degraded long-term knowledge accumulation without any visibility.

**Root cause (likely):** Two contributing factors:
1. The onboarding process was run during agent migration (not a clean first boot), so agents may have treated it as a partial re-run and skipped steps they considered already done.
2. The onboarding protocol may not make autoresearch setup mandatory or explicitly checkpoint it — it is likely framed as optional or role-specific, making it easy to skip under time/context pressure.

**Fix needed:**
- Audit each agent's onboarding steps against the ONBOARDING.md checklist — identify exactly which steps were skipped and why
- Add an explicit autoresearch setup checkpoint to ONBOARDING.md with a pass/fail verification (e.g., confirm at least 1 research cron is active before marking onboarding complete)
- Consider making autoresearch setup role-aware: required for analyst/orchestrator, optional but prompted for agent template

---

## 41. Agents not asked what model to use during onboarding (except orchestrator)

**What happens:** During onboarding, only the orchestrator template asks the user which Claude model to use. Analyst, agent (specialist), and other templates silently default to whatever model the daemon was started with — no prompt, no configuration step.

**Severity:** Medium — users may not realize agents are running on a different/more expensive model than intended, or agents may be running on a weaker model that limits capability for their role.

**Root cause:** Model selection question is only in the orchestrator onboarding script. Other templates assume the default is fine without asking.

**Fix needed:**
- Add model selection step to ONBOARDING.md for analyst and agent templates
- Write the selected model to `config.json` (e.g., `"model": "claude-sonnet-4-6"`) during onboarding
- Daemon should pass `--model` flag when spawning agent sessions if `config.json` specifies one
- Default: suggest role-appropriate model (orchestrator → opus, analyst → sonnet, specialist agent → sonnet or haiku depending on task complexity)

---

## 42. Dashboard CLI command doesn't pass CTX_ROOT/CTX_INSTANCE_ID to the process — causes cross-instance data bleed

**What happens:** Running `cortextos dashboard` (or `pm2 start "npm start"` without explicit env) inherits the shell's environment. If the shell has `CTX_ROOT=/Users/.../.cortextos/default` from a previous session or launchd, the dashboard reads the wrong SQLite DB. Data from the old system (wrong agents, tasks, heartbeats, analytics) bleeds into the UI even when `CTX_INSTANCE_ID` in `.env.local` says otherwise — because `process.env` vars set in the shell override `.env.local`.

**Severity:** High — causes complete cross-instance data bleed, makes the dashboard untrustworthy.

**Root cause:** `dashboard.ts` CLI command writes `CTX_ROOT` into `.env.local` (for Next.js to read at build time), but the running process inherits shell env which takes precedence over `.env.local` at runtime. `db.ts` and `config.ts` read `process.env.CTX_ROOT` directly — so the shell value wins.

**Fix needed:**
- In `dashboard.ts` action, explicitly set `CTX_ROOT`, `CTX_INSTANCE_ID`, and `CTX_FRAMEWORK_ROOT` in the `dashEnv` object passed to `spawn()` — the process already constructs `dashEnv`, just ensure these three vars are included (they currently are, but verify the CLI path is used rather than raw `pm2 start` commands)
- Add a startup log line: `[dashboard] CTX_ROOT=... CTX_INSTANCE_ID=...` so misconfigurations are immediately visible in logs
- Document: never start the dashboard via raw `pm2 start "npm start"` without passing env vars explicitly
