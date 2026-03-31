---
name: nighttime-mode
description: "Autonomous overnight orchestration mode (11 PM - 8:30 AM). Dispatch and monitor deep work across agents while James sleeps. Internal building only, no external actions."
---

# Nighttime Mode

> 11:00 PM - 8:30 AM ET -- Orchestrate deep work across agents while James sleeps.
> Dispatch tasks, monitor progress, prepare morning briefing.

---

## Quick Start Loop

```
1. CHECK: cortextos bus list-tasks --status in_progress
   -> Any overnight tasks dispatched?

2. IF tasks are running:
   a. Monitor agent heartbeats: cortextos bus read-all-heartbeats
   b. Check inbox for agent reports: cortextos bus check-inbox
   c. Process completions, dispatch next tasks
   d. GOTO step 1

3. IF no tasks pending:
   a. Prepare morning briefing
   b. Update heartbeat: cortextos bus update-heartbeat "morning briefing ready"
   c. Reply HEARTBEAT_OK
```

---

## Hard Guardrails -- NEVER Cross

1. **No external communications** -- No emails, tweets, posts, DMs sent
2. **No purchases/transactions** -- No buying, no transfers
3. **No permanent deletes** -- Always reversible
4. **No production deploys** -- Prepare PRs, don't merge
5. **No commitments** -- No promises, deadlines, agreements on James's behalf
6. **No approval creation** -- Do not create approvals at night, queue for morning

**When in doubt:** Document it, present in morning.

---

## What TO Do

| Tier | Examples | Agent |
|------|----------|-------|
| **High-Value** | Research, software building, content drafts, analysis | boris, alex, data |
| **Maintenance** | System health checks, task grooming, metrics | sentinel, paul |
| **Self-Improvement** | Skill development, workflow optimization | paul, sentinel |

---

## Overnight Orchestration Protocol

### Step 1: Check Approved Queue

```bash
# List tasks approved for overnight work
cortextos bus list-tasks --status in_progress

# Check which agents have overnight tasks
cortextos bus read-all-heartbeats
```

### Step 2: Monitor Agent Progress

```bash
# Regular heartbeat checks (every 30 min)
cortextos bus read-all-heartbeats

# Check inbox for completion reports
cortextos bus check-inbox
```

### Step 3: Process Completions

When an agent reports task completion:

```bash
# 1. Complete the task in cortextOS
cortextos bus complete-task "$TASK_ID" "<what was produced>"

# 2. Log the event
cortextos bus log-event task task_completed info --meta '{"task_id":"'$TASK_ID'","agent":"'$CTX_AGENT_NAME'"}'

# 3. Write to memory
TODAY=$(date -u +%Y-%m-%d)
echo "COMPLETED: $TASK_ID - <description> (by <agent>)" >> "memory/$TODAY.md"

# 4. Dispatch next task if queue has more
cortextos bus list-tasks --status pending
```

### Step 4: Handle Blockers

When an agent reports a blocker:

```bash
# 1. Log the blocker
TODAY=$(date -u +%Y-%m-%d)
echo "BLOCKED: $TASK_ID - <reason> (agent: <name>)" >> "memory/$TODAY.md"

# 2. Try to unblock if possible (reassign, provide info)
cortextos bus send-message <agent> normal '<unblocking info>'

# 3. If cannot unblock, queue for morning review
echo "MORNING REVIEW: Blocker needs James - $TASK_ID" >> "memory/$TODAY.md"
```

---

## Heartbeat During Nighttime

Update heartbeat regularly to show overnight activity:

```bash
# Every 30 minutes or after significant events
cortextos bus update-heartbeat "nighttime mode - X/Y tasks complete, monitoring agents"
```

---

## Before 8:30 AM: Prepare Morning Briefing

Create comprehensive morning briefing data:

1. **What was completed** (by which agent)
2. **Files created/modified** (with paths)
3. **What needs James's review**
4. **Blockers discovered**
5. **Recommended priorities for today**

```bash
# Write overnight summary to memory
TODAY=$(date -u +%Y-%m-%d)
cat >> "memory/$TODAY.md" << MEMEOF

## Overnight Summary - $(date -u +%H:%M:%S)

### Completed
- [task] by [agent] -- [deliverable]
- [task] by [agent] -- [deliverable]

### Blocked
- [task] -- [reason]

### Needs Review
- [item needing James's decision]

### Agent Status at Morning
- sentinel: [status]
- donna: [status]
- boris: [status]
- alex: [status]
- data: [status]
MEMEOF

# Update heartbeat
cortextos bus update-heartbeat "morning briefing ready - overnight complete"
```

---

## Required Reading

For full evening-to-morning lifecycle:
- `skills/evening-review/SKILL.md` -- How tasks get approved
- `skills/morning-review/SKILL.md` -- Morning handoff format

---

## Related Skills

- `skills/morning-review/` -- Morning handoff
- `skills/evening-review/` -- Evening planning

---

## Event Logging

Log overnight milestones:

```bash
# Session transition
cortextos bus log-event action nighttime_mode_start info --meta '{"agent":"paul"}'

# Task completions
cortextos bus log-event task task_completed info --meta '{"task_id":"<id>","agent":"<completing_agent>"}'

# Morning ready
cortextos bus log-event action morning_briefing_ready info --meta '{"tasks_completed":"X","tasks_blocked":"Y"}'
```

---

## Philosophy

> "Lower risk, higher autonomy -- No external actions, internal building only."

The night is for making James's day easier. Dispatch, monitor, coordinate -- but never act externally without him. The orchestrator's job overnight is to keep agents productive and prepare a clear morning briefing.

---

*This is the single source of truth for nighttime mode. All instructions are here.*
