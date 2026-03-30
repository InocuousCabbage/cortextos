# cortextOS

Persistent 24/7 Claude Code agents with multi-agent orchestration, task management, Telegram control, and a web dashboard.

---

## What It Is

cortextOS runs Claude Code as a persistent agent that never sleeps. You control it via Telegram. Multiple agents can orchestrate each other, assign tasks, run experiments, and report results — all while you're away.

- **Agents** run in PTY sessions managed by a Node.js daemon
- **Telegram** is the primary interface (send messages, approve actions, get reports)
- **Dashboard** provides a web UI for tasks, approvals, analytics, and experiments
- **Knowledge base** (optional) gives agents semantic memory via ChromaDB + Gemini embeddings

---

## Prerequisites

| Requirement | Notes |
|-------------|-------|
| Node.js 20+ | [nodejs.org](https://nodejs.org) — checked by installer |
| Claude Code | `npm install -g @anthropic-ai/claude-code` — checked by installer |
| Claude Code login | Run `claude login` once to authenticate — agents use your stored session |
| Telegram bot token | Created during `/onboarding` — @BotFather walks you through it |

**Platform support:** Mac, Linux, and Windows natively supported.

---

## Quick Start

### 1. Install

Run this single command:

**Mac / Linux:**
```bash
curl -fsSL https://get.cortextos.dev/install.mjs | node
```

**Windows (PowerShell):**
```powershell
node -e "$(irm https://get.cortextos.dev/install.mjs)"
```

This clones the repo, installs dependencies, builds, and links the `cortextos` CLI. PM2 is installed automatically if missing. Works natively on Mac, Linux, and Windows — no WSL2 required. At the end it prints exactly where the project was cloned.

### 2. Run the guided onboarding

Open the cloned project directory in Claude Code:

```bash
claude ~/cortextos
```

Then run:

```
/onboarding
```

The `/onboarding` slash command walks you through the complete setup end to end:

1. **Dependency check** — verifies Node.js, Claude Code, PM2, jq; auto-installs anything missing
2. **Install** — runs `cortextos install`, sets up state directories
3. **Organization setup** — name, description, north star goals, timezone, communication style
4. **Knowledge base** — org context (business, team, tech stack, key decisions)
5. **Agent planning** — choose names for your Orchestrator and Analyst
6. **Orchestrator setup** — walks through BotFather bot creation, auto-detects your chat ID, creates the agent directory and pre-populates bootstrap files
7. **Daemon start** — generates PM2 config, starts the daemon, sets up auto-restart on reboot
8. **Dashboard setup** — installs dependencies, writes `.env.local`, builds and starts the dashboard

After that, the Orchestrator comes online in Telegram and continues onboarding there — gathering its identity, persona, goals, and cron schedule through your conversation. It then creates your Analyst agent, which does its own Telegram onboarding. By the end, the full system is configured.

---

## Manual Setup (Advanced)

If you prefer to configure everything by hand, the individual CLI commands are:

```bash
cortextos install                          # Set up state directories
cortextos init myorg                       # Create an organization
cortextos add-agent boss --template orchestrator --org myorg
cortextos add-agent analyst --template analyst --org myorg
```

For each agent, create `.env` with Telegram credentials:

```bash
cat > orgs/myorg/agents/boss/.env << EOF
BOT_TOKEN=<your-bot-token>
CHAT_ID=<your-chat-id>
ALLOWED_USER=<your-telegram-user-id>
EOF
```

Then generate the PM2 config and start:

```bash
cortextos ecosystem
pm2 start ecosystem.config.js && pm2 save && pm2 startup
```

Dashboard:

```bash
cd dashboard && npm install
# Write dashboard/.env.local with CTX_ROOT, CTX_FRAMEWORK_ROOT, AUTH_SECRET, ADMIN_PASSWORD
npm run build && npm start
```

Default dashboard login: `admin` / `cortextos`. Set `ADMIN_PASSWORD` env var to change it.

---

## Agent Templates

| Template | Purpose | Default Crons | Skills |
|----------|---------|---------------|--------|
| `orchestrator` | Coordinates agents, manages goals, approves actions | Morning review, evening review, weekly review | 11 |
| `agent` | General-purpose worker | Heartbeat | 5 |
| `analyst` | System health, metrics, theta wave deep analysis | Theta wave, heartbeat, analytics | 10 |

---

## CLI Reference

### System Commands

```bash
cortextos install                          # Set up state directories
cortextos init <org>                       # Create an organization
cortextos add-agent <name>                 # Add agent (--template, --org)
cortextos list-agents                      # List agents with status
cortextos status                           # Show agent health table
cortextos doctor                           # Check prerequisites
cortextos ecosystem                        # Generate PM2 config
cortextos dashboard [--port 3000]          # Start web dashboard
cortextos enable <agent>                   # Enable agent in daemon
cortextos disable <agent>                  # Disable agent
cortextos start                            # Start daemon
cortextos stop                             # Stop daemon
cortextos onboarding                       # Interactive setup wizard
```

### Bus Commands (Agent Operations)

All bus commands are also available as shell scripts in `bus/` for use inside agent prompts.

```bash
# Messaging
cortextos bus send-message <to> <priority> <text>    # Send agent-to-agent message
cortextos bus send-telegram <chat-id> <message>      # Send Telegram message
cortextos bus check-inbox                            # List unread messages
cortextos bus ack-inbox <id>                         # Acknowledge message

# Tasks
cortextos bus create-task <title> [--desc <d>] [--assignee <a>] [--priority <p>]
cortextos bus update-task <id> <status> [note]       # Status: pending|in_progress|blocked|completed|cancelled
cortextos bus complete-task <id> [--result <text>]
cortextos bus list-tasks [--agent <a>] [--status <s>] [--format json|text]
cortextos bus archive-tasks                          # Archive completed tasks

# Heartbeat
cortextos bus update-heartbeat <status>              # Update agent heartbeat

# Events & Analytics
cortextos bus log-event <category> <event> <severity> [--meta <json>]

# Approvals
cortextos bus create-approval <title> <category> [context]   # Categories: external-comms|financial|deployment|data-deletion|other
cortextos bus update-approval <id> <approved|rejected> [note]

# Experiments (Autoresearch)
cortextos bus create-experiment <metric> <hypothesis> [--surface <path>] [--direction higher|lower] [--window <dur>]
cortextos bus run-experiment <id> [description]
cortextos bus evaluate-experiment <id> <value> [--score <n>] [--justification <text>]
cortextos bus list-experiments [--agent <a>] [--status <s>] [--metric <m>]
cortextos bus gather-context [--agent <a>] [--format json|markdown]
cortextos bus manage-cycle <create|modify|remove|list> <agent> [--metric <m>] [--surface <path>]

# Knowledge Base (requires kb-setup first)
cortextos bus kb-query <question> [--org <o>] [--scope shared|private|all] [--top-k <n>]
cortextos bus kb-ingest <path> [--org <o>] [--scope shared|private] [--agent <a>]
cortextos bus kb-collections [--org <o>]
```

---

## Knowledge Base (RAG)

cortextOS ships with an optional semantic memory layer using ChromaDB + Gemini Embedding.

### Setup

```bash
# 1. Add your Gemini API key
echo "GEMINI_API_KEY=your-key" >> orgs/<org>/secrets.env

# 2. Initialize the knowledge base
bash bus/kb-setup.sh --org <org>

# 3. Ingest documents
bash bus/kb-ingest.sh /path/to/docs --org <org> --scope shared

# 4. Query
bash bus/kb-query.sh "your question" --org <org> --scope all
```

### Collections

| Scope | Collection Name | Use |
|-------|----------------|-----|
| `shared` | `shared-{org}` | Org-wide knowledge (accessible by all agents) |
| `private` | `agent-{name}` | Per-agent private knowledge |
| `all` | both | Queries both and merges results |

### Dashboard API

```
GET /api/kb/search?q=<query>&org=<org>&scope=<shared|private|all>&limit=10&threshold=0.5
GET /api/kb/collections?org=<org>
```

---

## Agent Configuration (`config.json`)

Each agent has a `config.json` in its directory. All fields are optional.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `max_session_seconds` | number | `255600` (71h) | Auto-restart session after this many seconds |
| `max_crashes_per_day` | number | `10` | Halt the agent after this many crashes in one day |
| `startup_delay` | number | `0` | Wait N seconds before starting the PTY session |
| `model` | string | system default | Claude model to use (e.g. `claude-opus-4-6`) |
| `working_directory` | string | agent dir | Working directory for the Claude Code session |
| `enabled` | boolean | `true` | Whether this agent is active |
| `crons` | array | `[]` | Scheduled prompts — see Cron Format below |

### Cron Format

```json
{
  "crons": [
    {
      "name": "heartbeat",
      "interval": "4h",
      "prompt": "Read HEARTBEAT.md and follow its instructions."
    }
  ]
}
```

Intervals: `30s`, `5m`, `1h`, `4h`, `1d`. Crons are registered as `/loop` commands inside the Claude Code session and persist across soft restarts (`--continue`). They are re-registered on each hard restart from `config.json`.

---

## Environment Variables

| Variable | Description | Default |
|----------|-------------|---------|
| `CTX_INSTANCE_ID` | State directory name | `default` |
| `CTX_ROOT` | State directory path | `~/.cortextos/{instance}` |
| `CTX_FRAMEWORK_ROOT` | Repo root path | auto-detected |
| `CTX_ORG` | Active organization | auto from agent dir |
| `CTX_AGENT_NAME` | Active agent name | auto from agent dir |

Each agent directory can have a `.cortextos-env` file that auto-loads when running commands from that directory.

---

## Architecture

```
You (Telegram)
      │
      ▼
 Daemon (Node.js)
      │
      ├── Orchestrator agent ─── PTY session (Claude Code)
      │         │
      │    Agent-to-agent messages (file bus)
      │         │
      ├── Analyst agent ──────── PTY session (Claude Code)
      │
      └── Worker agents ───────── ephemeral (tmux sessions)

State: ~/.cortextos/{instance}/
  orgs/{org}/
    tasks/          ← task JSON files
    approvals/      ← pending + resolved approvals
    experiments/    ← experiment history + config
    analytics/      ← event logs
    knowledge-base/ ← ChromaDB (if KB enabled)

Framework: <repo-root>/
  orgs/{org}/
    agents/{name}/  ← IDENTITY.md, GOALS.md, config.json, .env
  templates/        ← orchestrator, agent, analyst starter configs
  bus/              ← shell script wrappers for all bus commands
  dashboard/        ← Next.js web UI
```

---

## Dashboard API

The dashboard exposes a REST API for mobile and third-party integrations.

### Authentication

```bash
# Get a JWT token
POST /api/auth/mobile
Body: { "username": "admin", "password": "cortextos" }
Response: { "token": "<jwt>" }

# Use the token
GET /api/agents
Authorization: Bearer <jwt>
```

### Key Endpoints

| Method | Path | Description |
|--------|------|-------------|
| `GET` | `/api/agents` | List agents with health status |
| `GET` | `/api/tasks` | List tasks |
| `POST` | `/api/tasks` | Create task |
| `GET` | `/api/approvals` | List approvals |
| `POST` | `/api/approvals/:id/decide` | Approve or reject |
| `GET` | `/api/analytics/overview` | Metrics overview |
| `GET` | `/api/kb/search` | Semantic search |
| `GET` | `/api/kb/collections` | List KB collections |
| `GET` | `/api/events/stream` | Server-sent events stream |

---

## Directory Structure

```
cortextos/
├── src/
│   ├── cli/           # CLI commands (cortextos <cmd>)
│   ├── bus/           # Core business logic
│   ├── daemon/        # Agent lifecycle manager
│   ├── telegram/      # Telegram API client
│   ├── hooks/         # Claude Code hook scripts
│   └── types/         # TypeScript types
├── bus/               # Shell script wrappers (for agent prompts)
├── dashboard/         # Next.js web dashboard
├── templates/
│   ├── orchestrator/  # Orchestrator CLAUDE.md, IDENTITY.md, skills/
│   ├── agent/         # Agent CLAUDE.md, IDENTITY.md, skills/
│   └── analyst/       # Analyst CLAUDE.md, IDENTITY.md, skills/
├── knowledge-base/
│   └── scripts/       # mmrag.py (ChromaDB + Gemini RAG)
└── tests/             # Vitest unit tests + Playwright E2E
```

---

## Development

```bash
npm install          # Install dependencies
npm run build        # Build CLI + daemon
npm run dev          # Watch mode
npm run typecheck    # Type check without building
npm test             # Run unit tests
npm run test:playwright  # Run Playwright E2E tests
```

---

## License

UNLICENSED — Private software. Agent Architects community members only.
