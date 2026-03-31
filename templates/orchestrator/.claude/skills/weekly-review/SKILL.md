---
name: weekly-review
description: "Weekly comprehensive synthesis. Triggered Sunday 8 PM cron. Reviews week's accomplishments across all agents, metrics, goals progress, business health. Plans next week."
---

# Weekly Review Skill

> Comprehensive Sunday check-in covering all agents' output, business metrics, goals progress, personal accountability, and orchestrator self-evaluation with creative problem-solving.

**Trigger:** Cron job `weekly-review` - Sunday 8 PM
**Duration:** ~15-30 minutes interactive review with James
**Chat ID:** 7940429114
**Output:** Memory log, actionable insights, next week plan

---

## When This Runs

- **Cron:** Sunday 8 PM EST
- **Manual:** James says "weekly review" or "weekly check-in"

---

## Data Sources

| Domain | Source | Method |
|--------|--------|--------|
| Agent Performance | cortextOS bus | `cortextos bus read-all-heartbeats` |
| Tasks | cortextOS task system | `cortextos bus list-tasks` |
| Calendar | Google Calendar MCP / gogcli | `gcal_list_events` or gogcli fallback |
| Email | Gmail MCP / gogcli | `gmail_search_messages` or gogcli fallback |
| Goals | GOALS.md | Direct file read |
| Memory | memory/*.md (last 7 days) | Direct file reads |
| Skool | Browser screenshot of MRR | Manual (read-only) |

---

## Review Flow

### Phase 1: Data Aggregation (Automated)

Gather the week's data from all sources:

```bash
# 1. All agent heartbeats
cortextos bus read-all-heartbeats

# 2. All tasks (completed, pending, blocked)
cortextos bus list-tasks
cortextos bus list-tasks --status completed

# 3. This week's memory files
for i in 0 1 2 3 4 5 6; do
  DATE=$(date -v-${i}d +%Y-%m-%d 2>/dev/null || date -d "$i days ago" +%Y-%m-%d)
  echo "=== $DATE ==="
  cat memory/${DATE}.md 2>/dev/null || echo "(no entry)"
done

# 4. Calendar events for this week
# Preferred: gcal_list_events MCP tool
# Fallback:
export GOG_ACCOUNT=grandamenium@gmail.com
WEEK_START=$(date -v-7d +%Y-%m-%d 2>/dev/null || date -d '7 days ago' +%Y-%m-%d)
TODAY=$(date +%Y-%m-%d)
gog calendar events "6034843a6c1d917609322f14f361c66e11fb5ef6de0b30b9d07b06cb666bf3e7@group.calendar.google.com" \
  --from "$WEEK_START" --to "$TODAY"

# 5. Goals
cat GOALS.md

# 6. Check inbox for any pending messages
cortextos bus check-inbox
```

### Phase 2: Skool MRR Check (Browser)

1. Open Skool in browser
2. Navigate to community settings
3. Screenshot the MRR display
4. Extract MRR number
5. **READ ONLY - Never edit anything in Skool**

### Phase 3: Present Review to James

Format the data into a comprehensive review (see template below).

Send via chunked Telegram messages:
```bash
cortextos bus send-telegram 7940429114 "<message chunk>"
```

### Phase 4: Interactive Discussion

Ask James:
1. What went well this week?
2. What was challenging?
3. Any adjustments for next week?

### Phase 5: Creative Problem-Solving

Based on the weekly data, Paul should:
1. Identify patterns/problems across all agent domains
2. Propose **creative, actionable solutions** to long-term problems
3. Suggest system improvements and new agent setups
4. Note experiments to try next week
5. Identify which agents are underutilized or need capability expansion

### Phase 6: Update State

1. Log to `memory/YYYY-MM-DD.md`
2. Update MEMORY.md with persistent learnings
3. Log event

```bash
cortextos bus log-event action briefing_sent info --meta '{"type":"weekly_review"}'
cortextos bus update-heartbeat "weekly review complete"
```

---

## Review Template

```markdown
# Weekly Review - Week of [DATE]

---

## AGENT PERFORMANCE

### Agent Summary

| Agent | Status | Tasks Completed | Key Wins | Issues |
|-------|--------|----------------|----------|--------|
| paul | [heartbeat] | X | [coordination wins] | [gaps] |
| sentinel | [heartbeat] | X | [optimizations] | [alerts] |
| donna | [heartbeat/planned] | X | [email/calendar] | [gaps] |
| boris | [heartbeat/planned] | X | [code/PRs] | [gaps] |
| alex | [heartbeat/planned] | X | [content] | [gaps] |
| data | [heartbeat/planned] | X | [research] | [gaps] |

### Agent Health
- Agents online: X/6
- Agents needing setup: [list]
- Coordination events logged: X
- Messages exchanged: X

### System Health (from sentinel)
- Uptime: X%
- Anomalies detected: X
- Optimizations applied: X

---

## PRODUCTIVITY

### Tasks Completed by Agent

| Agent | Completed | In Progress | Blocked |
|-------|-----------|-------------|---------|
| paul | X | Y | Z |
| sentinel | X | Y | Z |
| donna | X | Y | Z |
| boris | X | Y | Z |
| alex | X | Y | Z |
| data | X | Y | Z |
| **TOTAL** | **X** | **Y** | **Z** |

### Overnight Work
- Tasks dispatched: X
- Tasks completed: X
- Notable deliverables: [list]

### Coordination Quality
- Tasks dispatched to right agent first time: X%
- Avg time from request to dispatch: Xm
- Briefings sent on time: X/X

---

## BUSINESS

### Agent Architects (Skool)
- **MRR:** $X (+/-$Y from last week)
- **Members:** X (+/-Y)
- Content published: X pieces
- Engagement posts: X
- Key wins: [list]

### CoinTally
- Milestones: [list]
- Customers: X
- Key progress: [summary]

### cortextOS
- Commits: X
- Features shipped: [list]
- Open issues: X

---

## GOALS PROGRESS

| Domain | Goal | Progress | Status |
|--------|------|----------|--------|
| Fitness | [goal] | [progress] | [status] |
| Skool | [revenue target] | [current] | [status] |
| CoinTally | [milestone] | [current] | [status] |
| Personal | [goal] | [qualitative] | [status] |
| cortextOS | [goal] | [progress] | [status] |

---

## PAUL SELF-EVALUATION (as orchestrator)

**Tangible Metrics:**
- Tasks dispatched: X
- Briefings sent on time: X/X
- Agent coordination messages: X
- Approvals routed: X
- Overnight work managed: X tasks
- Errors/re-dos: X

**Scores (1-10):**
| Category | Score | Notes |
|----------|-------|-------|
| Usefulness | X | [why] |
| Proactivity | X | [why] |
| Coordination | X | [why] |
| Communication | X | [why] |
| Learning | X | [why] |
| **TOTAL** | X/50 | |

**What Went Well:**
- [bullet list]

**What Could Improve:**
- [bullet list]

**Key Learnings:**
- [bullet list]

---

## CREATIVE SOLUTIONS

Based on this week's data, here are actionable solutions to observed problems:

### Problem 1: [Observed Pattern]
**Solution:** [Creative, specific, actionable solution]
**Implementation:** [How to do it, which agent]
**Expected Impact:** [What changes]

### Problem 2: [Observed Pattern]
**Solution:** [Creative, specific, actionable solution]
**Implementation:** [How to do it, which agent]
**Expected Impact:** [What changes]

### Agent System Gaps
**Missing capability:** [What's needed]
**Proposed agent/skill:** [Solution]
**Priority:** [High/Medium/Low]

---

## NEXT WEEK

**Top 3 Priorities:**
1.
2.
3.

**Goals:**
- Skool: [specific]
- CoinTally: [specific]
- cortextOS: [specific]
- Personal: [specific]

**Agent Focus:**
- sentinel: [priority work]
- donna: [priority work or setup plan]
- boris: [priority work or setup plan]
- alex: [priority work or setup plan]
- data: [priority work or setup plan]

**Paul Focus:**
- [What I'll improve/experiment with as orchestrator]

**System Improvements Queued:**
- [New scripts/skills to build]
- [New agents to set up]
- [Integrations to add]
```

---

## Delivery

Send the review as chunked Telegram messages:

```bash
# Chunk 1: Agent Performance + Productivity
cortextos bus send-telegram 7940429114 "<chunk 1>"
sleep 2

# Chunk 2: Business + Goals
cortextos bus send-telegram 7940429114 "<chunk 2>"
sleep 2

# Chunk 3: Self-Eval + Creative Solutions
cortextos bus send-telegram 7940429114 "<chunk 3>"
sleep 2

# Chunk 4: Next Week Plan + Action Prompts
cortextos bus send-telegram 7940429114 "<chunk 4>"
```

---

## State Updates

After review:

```bash
# 1. Log event
cortextos bus log-event action briefing_sent info --meta '{"type":"weekly_review"}'

# 2. Update heartbeat
cortextos bus update-heartbeat "weekly review complete - next week planned"

# 3. Write to daily memory
TODAY=$(date -u +%Y-%m-%d)
cat >> "memory/$TODAY.md" << MEMEOF

## Weekly Review - $(date -u +%H:%M:%S)

### Summary
- Total tasks completed this week: X
- Agents active: X/6
- Self-eval score: X/50
- Top priorities next week: [list]

### Key Insights
- [insight 1]
- [insight 2]

### System Improvements Proposed
- [improvement 1]
- [improvement 2]
MEMEOF

# 4. Update MEMORY.md with persistent learnings
# Add any new patterns, preferences, or system learnings
```

---

## Cron Configuration

```json
{
  "name": "weekly-review",
  "schedule": "0 20 * * 0",
  "payload": {
    "kind": "systemEvent",
    "text": "Weekly Review Time!\n\nRead skills/weekly-review/SKILL.md and run the full weekly review protocol."
  }
}
```

---

## Important Notes

1. **Skool is READ-ONLY** - Never edit anything, just screenshot MRR
2. **Be creative** - The solutions section should have genuinely novel ideas
3. **Be honest** - Self-eval should reflect actual performance as orchestrator
4. **Keep it actionable** - Every insight needs a concrete next step and an assigned agent
5. **Track experiments** - Note what we try and whether it works
6. **Identify agent gaps** - Which agents need to be set up, which need new capabilities
7. **Cross-agent patterns** - Look for coordination improvements across the whole system

---

*This is the single source of truth for weekly review. All instructions are here.*
