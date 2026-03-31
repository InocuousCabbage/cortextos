---
name: m2c1-worker
description: "Autonomous software development via worker agents. Spin up a dedicated Claude Code session, act as the 'human' in the M2C1 lifecycle, manage it through all 12 phases to completion. Use when: building new software, major features, or any project that benefits from structured autonomous development."
triggers: ["build", "m2c1", "worker agent", "autonomous build", "spin up worker", "new project", "build from scratch"]
---

# M2C1 Worker Agent Skill

> Any cortextOS agent can autonomously build complete software by acting as the "human" in the M2C1 framework, managing a dedicated worker Claude Code session through the full 12-phase lifecycle.

---

## Overview

This skill enables 3-layer agentception:
1. **You** (the cortextOS agent) act as the human/supervisor
2. **Worker** (a fresh Claude Code session) acts as the M2C1 orchestrator
3. **Subagents** (spawned by the worker) execute parallel research and tasks

You provide the brain dump, answer discovery questions, help with tool setup, monitor progress, and validate the final output. The worker does all the building.

---

## Prerequisites

- M2C1 skill files available (copy from grandamenium/paul-workspace if not local)
- A clear project idea or brain dump
- An isolated directory for the build
- tmux access for send-keys communication

---

## Phase 0: Setup

### 1. Create the project directory

```bash
PROJECT_DIR="$HOME/<project-name>"
mkdir -p "$PROJECT_DIR"
cd "$PROJECT_DIR"
git init
echo "node_modules/" > .gitignore
echo ".claude/" >> .gitignore
git add .gitignore && git commit -m "init: $PROJECT_DIR"
```

### 2. Copy M2C1 skill files

```bash
mkdir -p .claude/skills/m2c1/artifact-templates

# From GitHub (if not local)
for file in SKILL.md orchestration-workflow.md; do
  gh api "repos/grandamenium/paul-workspace/contents/skills/m2c1/$file" --jq '.content' | base64 -d > ".claude/skills/m2c1/$file"
done

for file in $(gh api repos/grandamenium/paul-workspace/contents/skills/m2c1/artifact-templates --jq '.[].name'); do
  gh api "repos/grandamenium/paul-workspace/contents/skills/m2c1/artifact-templates/$file" --jq '.content' | base64 -d > ".claude/skills/m2c1/artifact-templates/$file"
done
```

### 3. Create the worker's inbox

```bash
mkdir -p "$CTX_ROOT/inbox/<worker-name>"
mkdir -p "$CTX_ROOT/state/<worker-name>"
```

### 4. Write BRAINDUMP.md

Write a comprehensive brain dump in `$PROJECT_DIR/BRAINDUMP.md`. Include:
- What you are building and why
- Technical requirements and constraints
- Reference implementations or existing code to study
- Any research already done
- Success criteria

### 5. Copy comms skill to worker project

The worker needs the bus messaging skill to communicate with you:

```bash
mkdir -p "$PROJECT_DIR/.claude/skills/comms"
cp "$CTX_FRAMEWORK_ROOT/templates/agent/skills/comms/SKILL.md" "$PROJECT_DIR/.claude/skills/comms/SKILL.md" 2>/dev/null

# Also set up the worker's inbox
mkdir -p "$CTX_ROOT/inbox/<worker-name>"
mkdir -p "$CTX_ROOT/state/<worker-name>"
```

### 6. Set up .claude/ permission bypass (CRITICAL - do this BEFORE spawning)

`--dangerously-skip-permissions` does NOT bypass `.claude/` directory protections. Workers WILL get stuck on permission prompts without this step. Field-tested and verified: the auto-approve hook approach is the ONLY reliable method.

```bash
# Create auto-approve hook that approves ALL permissions
mkdir -p "$PROJECT_DIR/.claude/hooks"
cat > "$PROJECT_DIR/.claude/hooks/auto-approve.sh" << 'HOOKEOF'
#!/usr/bin/env bash
# Auto-approve all permissions for worker agents (no Telegram, fully autonomous)
echo '{"hookSpecificOutput":{"hookEventName":"PermissionRequest","decision":{"behavior":"allow"}}}'
HOOKEOF
chmod +x "$PROJECT_DIR/.claude/hooks/auto-approve.sh"

# Create or update settings.json with the hook + permission allowances
cat > "$PROJECT_DIR/.claude/settings.json" << 'SETTINGSEOF'
{
  "permissions": {
    "allow": ["Edit", "Write", "Bash", "Read", "Glob", "Grep"],
    "allowedPaths": [".claude/"]
  },
  "hooks": {
    "PermissionRequest": [
      {
        "hooks": [
          {
            "type": "command",
            "command": "bash .claude/hooks/auto-approve.sh",
            "timeout": 5
          }
        ]
      }
    ]
  }
}
SETTINGSEOF
```

If .claude/settings.json already exists (e.g., with MCP config), merge the hooks and permissions keys into it rather than overwriting.

**Why the hook approach?** Path patterns (Edit:.claude/**) are unreliable - they miss subdirectories and new file types. The PermissionRequest hook catches EVERYTHING and auto-approves. Verified working: Claude shows "Allowed by PermissionRequest hook" with zero prompts.

### 7. Write CLAUDE.md

Write `$PROJECT_DIR/CLAUDE.md` with instructions for the worker:

```markdown
# <Project Name> - M2C1 Autonomous Build

You are building <description>.

## Your Role
You are the M2C1 orchestrator. Follow the 12-phase workflow in .claude/skills/m2c1/orchestration-workflow.md.

## Communication
Send messages to <your-agent-name>:
```
cortextos bus send-message <your-agent-name> normal '<message>'
```
Check inbox:
```
cortextos bus check-inbox
```

Set environment:
```
export CTX_AGENT_NAME="<worker-name>"
export CTX_ORG="<org>"
export CTX_FRAMEWORK_ROOT="<path>"
export CTX_ROOT="$HOME/.cortextos/default"
```

When you have questions during Phase 3 (Discovery), send them via send-message.sh. Do NOT use AskUserQuestion.

## Start
1. Read BRAINDUMP.md
2. Read .claude/skills/m2c1/orchestration-workflow.md
3. Begin Phase 0, then Phase 1
4. Message <your-agent-name> when PRD is ready
5. Continue autonomously through all phases
```

---

## Phase 1: Spawn the Worker

### tmux Session (REQUIRED - full Claude Code session)

```bash
WORKER_SESSION="m2c1-<project-slug>"
tmux new-session -d -s "$WORKER_SESSION" bash
tmux send-keys -t "$WORKER_SESSION" "cd $PROJECT_DIR" Enter
tmux send-keys -t "$WORKER_SESSION" "claude --dangerously-skip-permissions" Enter

# Wait for Claude to boot
sleep 10

# Inject the initial prompt
PROMPT="Read CLAUDE.md for your instructions, then read BRAINDUMP.md for the project spec. Begin the M2C1 workflow starting with Phase 0."
tmux send-keys -t "$WORKER_SESSION" "$PROMPT" Enter
```

tmux sessions allow you to:
- Monitor the worker: `tmux capture-pane -t $WORKER_SESSION -p | tail -20`
- Inject follow-up context: `tmux send-keys -t $WORKER_SESSION "<text>" Enter`
- Handle stuck states: `tmux send-keys -t $WORKER_SESSION Escape Enter`

---

## Permission Bypass Reference

The auto-approve hook in Step 6 above is the ONLY reliable method. Do NOT use path patterns (Edit:.claude/**) - they are unreliable and miss subdirectories.

If a worker still hits a permission prompt despite the hook (e.g., hook not picked up yet):
```bash
# Select "always allow" option via tmux
tmux send-keys -t "$WORKER_SESSION" "2" Enter

# Or restart with --continue to pick up the hook
tmux send-keys -t "$WORKER_SESSION" "/exit" Enter
sleep 5
tmux send-keys -t "$WORKER_SESSION" "claude --continue --dangerously-skip-permissions" Enter
```

---

## Phase 2: Monitor and Communicate

### Communication Priority
1. **Bus messages** (primary) - send-message.sh / check-inbox.sh. Worker sends you updates, you reply via bus.
2. **tmux send-keys** (fallback) - only when the worker is stuck, unresponsive to bus, or needs CLI-level intervention.
3. **tmux capture-pane** (monitoring) - check what the worker is doing without interrupting it.

### Checking Progress

```bash
# Via bus messages (worker sends updates)
cortextos bus check-inbox

# Via tmux (see what worker is doing)
tmux capture-pane -t "$WORKER_SESSION" -p | tail -30

# Via git (see what was built)
cd $PROJECT_DIR && git log --oneline | head -10

# Via file system (check orchestration artifacts)
ls $PROJECT_DIR/.claude/orchestration-*/
```

### Answering Discovery Questions (Phase 3)

The worker will send you questions via send-message.sh. Answer them:

```bash
cortextos bus send-message <worker-name> normal '<your answers>'
```

Base your answers on:
- The original brain dump requirements
- Any research you have done
- Your domain knowledge as a cortextOS agent
- The org's goals and constraints (GOALS.md, knowledge.md)

If you do not know the answer, make a reasonable decision and note it. Do not block the worker with "ask James" unless it is truly a human-only decision.

### Handling Stuck States

If the worker stops making progress:

```bash
# Check what it is doing
tmux capture-pane -t "$WORKER_SESSION" -p | tail -20

# If stuck at a prompt, send Enter or Escape
tmux send-keys -t "$WORKER_SESSION" Enter

# If stuck on permissions
tmux send-keys -t "$WORKER_SESSION" Escape Enter

# If stuck on AskUserQuestion (TUI)
tmux send-keys -t "$WORKER_SESSION" Down Enter  # Select an option

# If completely frozen, nudge it
tmux send-keys -t "$WORKER_SESSION" "Continue with the M2C1 workflow. What phase are you on?" Enter
```

---

## Phase 3: Tool Setup Support (CRITICAL - Act Like a Human)

This is one of the most important phases. You act exactly like a human developer setting up a project: installing tools, configuring MCPs, setting env variables, logging into services, testing that everything works. The worker cannot do this itself - it needs you to configure its environment.

### Think Holistically About Tools

Before the worker starts building, ask yourself:
- What MCPs would help? (Playwright for browser testing, etc.)
- What accounts/services does the project need? (APIs, hosting, etc.)
- What CLI tools should be installed? (expo, vercel, railway, etc.)
- What env variables does the worker need? (API keys, tokens, etc.)
- What skills could help the worker? (existing cortextOS skills)
- What testing tools are needed? (iOS Simulator, Playwright, etc.)

### MCP Configuration

```bash
# 1. Create or update the worker's MCP config
mkdir -p "$PROJECT_DIR/.claude"
cat > "$PROJECT_DIR/.claude/settings.json" << 'EOF'
{
  "mcpServers": {
    "playwright": {
      "command": "npx",
      "args": ["@anthropic-ai/mcp-playwright"]
    }
  }
}
EOF

# 2. Restart worker to pick up MCP config
tmux send-keys -t "$WORKER_SESSION" "/exit" Enter
sleep 5
tmux send-keys -t "$WORKER_SESSION" "claude --continue --dangerously-skip-permissions" Enter
sleep 10

# 3. Verify MCP works by asking worker to test it
tmux send-keys -t "$WORKER_SESSION" "Test that Playwright MCP is working by taking a screenshot of google.com" Enter

# 4. If it does not work, debug and retry
# Check capture-pane for errors, fix config, restart again
```

### Iterative Tool Verification

Do NOT assume tools work after installation. Test each one:

```
FOR EACH TOOL:
  1. Install/configure it
  2. Restart worker if needed (--continue to preserve context)
  3. Ask worker to USE the tool
  4. Check capture-pane for success/failure
  5. If failed: fix config, repeat from step 2
  6. If succeeded: move to next tool
```

### Environment Variables and Credentials

```bash
# Check what is available
cat $CTX_FRAMEWORK_ROOT/orgs/$CTX_ORG/secrets.env | grep -v "^#" | cut -d= -f1

# Set env vars for the worker session
tmux send-keys -t "$WORKER_SESSION" "export API_KEY=<value>" Enter

# Or write a .env file the worker can source
cat > "$PROJECT_DIR/.env" << 'EOF'
ANTHROPIC_API_KEY=<from secrets.env>
GEMINI_API_KEY=<from secrets.env>
EOF
tmux send-keys -t "$WORKER_SESSION" "source .env" Enter
```

### Account Logins (via Playwright or CLI)

If the worker needs authenticated access to services:

```bash
# Option 1: Use Playwright MCP to log in via browser
# Tell the worker to navigate and log in, or do it yourself via send-keys

# Option 2: Use CLI tools
tmux send-keys -t "$WORKER_SESSION" "expo login" Enter
# Then inject credentials via send-keys

# Option 3: Reuse existing auth
# Copy auth tokens from your own environment
```

### System Dependencies

```bash
# npm packages (install in project dir)
cd $PROJECT_DIR && npm install <package>

# System tools
brew install <tool>  # or apt-get on Linux

# Python packages
pip3 install <package>
```

### Integrate Tools into the Plan

After all tools are set up and verified:

```bash
# Tell the worker to update its PHASES.md and task files
# to USE the tools for testing and validation
tmux send-keys -t "$WORKER_SESSION" \
  "Update your PHASES.md and task files to integrate the tools we just set up. Use Playwright for E2E testing. Use the API keys for integration tests. Every task should have a testing step that uses real tools, not just assertions." Enter
```

### Skills for the Worker

Copy relevant cortextOS skills to the worker's project:
```bash
# If the worker needs browser automation knowledge
cp -r $CTX_FRAMEWORK_ROOT/templates/agent/skills/peekaboo-automation "$PROJECT_DIR/.claude/skills/"

# If it needs Google Workspace access
cp -r $CTX_FRAMEWORK_ROOT/templates/agent/skills/google-workspace "$PROJECT_DIR/.claude/skills/"
```

---

## Phase 4: Autonomous Execution

Once the worker is past discovery and tool setup, it should run autonomously:

### Set Up Auto-Iteration

Tell the worker to create a /loop:
```bash
cortextos bus send-message <worker-name> normal \
  'Set up a /loop every 10 minutes to check START.md for pending tasks. If not working on a task, pick the next one.'
```

### Periodic Check-ins

Check in every 30-60 minutes:
```bash
# Quick health check
tmux capture-pane -t "$WORKER_SESSION" -p | tail -10

# Check git progress
cd $PROJECT_DIR && git log --oneline | head -5

# Check orchestration progress
cat $PROJECT_DIR/.claude/orchestration-*/PROGRESS.md 2>/dev/null | tail -20
```

### When NOT to Intervene
- Worker is actively coding (stdout flowing)
- Worker is running tests
- Worker is in a research subagent phase
- Worker sent you a message and is waiting (check inbox first)

### When to Intervene
- Worker has been idle > 15 minutes
- Worker is looping on the same error
- Worker is asking AskUserQuestion (stuck at TUI)
- Worker went off-scope (building wrong thing)

---

## Phase 5: Validate the Output

### Synergy Review (before execution)
Verify the worker's task files are coherent:
```bash
ls $PROJECT_DIR/.claude/orchestration-*/tasks/
# Read a few task files - do they reference each other correctly?
# Are there gaps? Overlaps?
```

### Per-Task Testing
After each phase of execution, verify:
```bash
# Do tests pass?
cd $PROJECT_DIR && npm test 2>/dev/null

# Does it build?
cd $PROJECT_DIR && npm run build 2>/dev/null

# Check git for clean commits
git log --oneline | head -10
```

### Final E2E Testing
The worker's last phase should be comprehensive testing. Verify:
1. The software actually runs
2. It connects to real systems (if applicable)
3. Core user flows work end-to-end
4. Edge cases are handled

If tests fail, tell the worker:
```bash
cortextos bus send-message <worker-name> normal \
  'E2E test failed: <specific failure>. Fix it and re-test.'
```

---

## Phase 6: Cleanup

### On Success
```bash
# Log the milestone
cortextos bus log-event milestone m2c1_complete info \
  '{"project":"<name>","location":"<path>","tasks":<count>,"tests":<count>}'

# Notify orchestrator
cortextos bus send-message paul normal \
  'M2C1 build complete: <project>. Location: <path>. <summary>'

# Clean up worker inbox
rm -rf "$CTX_ROOT/inbox/<worker-name>"
rm -rf "$CTX_ROOT/state/<worker-name>"

# Kill worker session (if tmux)
tmux kill-session -t "$WORKER_SESSION" 2>/dev/null
```

### On Failure
```bash
# Log what happened
cortextos bus log-event action m2c1_failed info \
  '{"project":"<name>","phase":"<where it failed>","reason":"<why>"}'

# Keep the directory for debugging
# Report to orchestrator
cortextos bus send-message paul normal \
  'M2C1 build FAILED: <project>. Failed at phase <N>. Reason: <why>. Directory preserved at <path>.'
```

---

## Key Principles

1. **You are the human.** The worker treats you as the decision-maker. Answer questions decisively.
2. **Do not micro-manage.** Let the worker run. Check in periodically, not constantly.
3. **Intervene on stuck states.** If the worker is blocked > 15 minutes, help it.
4. **Validate at phase gates.** Check PRD, discovery, task plans, and final output.
5. **The worker spawns its own subagents.** You do not manage them directly.
6. **Keep the scope tight.** If the worker goes off-scope, redirect it immediately.
7. **Testing is non-negotiable.** Do not accept "it should work" - verify it works.
8. **Log everything.** Tasks, events, milestones. Invisible work does not exist.

---

## Anti-Patterns

- **Doing the work yourself** instead of letting the worker do it
- **Answering "ask James"** for decisions you can make (only escalate truly human-only decisions)
- **Not checking the worker** for hours (it may be stuck)
- **Skipping the synergy review** (tasks will conflict)
- **Accepting untested output** (always verify E2E)
- **Running multiple workers in the same directory** (git conflicts)
