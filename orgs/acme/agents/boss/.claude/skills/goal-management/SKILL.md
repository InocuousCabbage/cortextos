---
name: goal-management
description: "Daily goal lifecycle management. Use when: morning briefing, setting daily focus, refreshing agent goals, reviewing goal progress. Triggered daily as part of morning routine."
triggers: ["morning", "goals", "focus", "priorities", "what should we work on", "daily plan"]
---

# Goal Management

The orchestrator owns the daily goal lifecycle. Goals flow from the human's daily focus down to agent-specific objectives and tasks.

## Hierarchy

```
North Star (org-level, rarely changes, set by human)
  -> Daily Focus (what the human wants done TODAY)
    -> Agent Goals (orchestrator translates focus into role-specific goals)
      -> Tasks (agents create from their goals, confirm with orchestrator)
```

## Morning Goal Cascade

Run this every morning as part of your briefing:

### 1. Consult the human

Ask via Telegram:
> "Good morning. Your north star is: [read from goals.json]. What's the focus for today?"

Wait for their response. They may give specific directives or say "continue yesterday's work."

### 2. Set your own goals

From the conversation, determine YOUR goals for the day:
- Coordination tasks (who needs to be unblocked?)
- Delegation (which agents get which work?)
- Reviews (what needs your attention?)
- Briefings (morning, evening)

Write your GOALS.md with today's goals.

### 3. Set each agent's goals

For each agent, based on their role and the daily focus:
1. Determine 2-5 goals appropriate for their role
2. Write their GOALS.md:
   ```
   # Goals

   ## Bottleneck
   [what's blocking them, or "none"]

   ## Goals
   1. [goal derived from daily focus + their role]
   2. [goal derived from daily focus + their role]
   ...

   ## Updated
   [current ISO timestamp]
   ```
3. Message the agent: "New goals for today: [summary]. Create tasks and confirm."

### 4. Confirm task plans

Each agent will create tasks from their goals and send you their task list.
- Review for overlap (two agents doing the same thing)
- Review for missing coverage (daily focus items nobody picked up)
- Approve or adjust

### 5. Update org goals.json

```bash
# Update daily focus
jq --arg focus "<today's focus>" --arg ts "$(date -u +%Y-%m-%dT%H:%M:%SZ)" \
    '.daily_focus = $focus | .daily_focus_set_at = $ts' \
    orgs/<org>/goals.json > /tmp/goals.tmp && mv /tmp/goals.tmp orgs/<org>/goals.json
```

## Evening Review

At end of day:
1. Check each agent's task completion against their goals
2. Note what was achieved vs planned
3. Carry forward unfinished goals to tomorrow's conversation with human
4. Update bottlenecks

## North Star

The north star lives in `orgs/<org>/goals.json`. It is set by the human, rarely changes. The orchestrator references it when setting daily focus to ensure alignment.

If the daily focus drifts from the north star, flag it:
> "Today's focus on [X] is different from our north star of [Y]. Is this intentional?"
