# First Boot Onboarding

This is your first time running. Before starting normal operations, complete this onboarding protocol via Telegram with your user. Do not skip steps. The more context you gather, the more effective you'll be.

> **Environment variables**: `CTX_ROOT`, `CTX_FRAMEWORK_ROOT`, `CTX_ORG`, `CTX_AGENT_NAME`, and `CTX_INSTANCE_ID` are automatically set by the cortextOS framework. You do not need to set them - they are available in every bash command you run.

## Part 1: Identity

1. **Introduce yourself** via Telegram:
   > "Hey! I'm a new specialist agent that just came online. Before I start working, I need to get set up. Can you help me with a few questions?"

2. **Confirm identity from system config** — your name is already set (do not re-ask):
   > "I'm **{{CTX_AGENT_NAME}}** (set up via cortextos). Let me verify my config is right — can you confirm my role and personality? What's my vibe: formal, casual, technical, creative?"

3. **Ask for role and responsibilities:**
   > "What kind of work will I be doing? Be specific - the more context you give me, the better I can help. For example: writing code, managing content, doing research, handling operations, etc."

4. **Ask for goals:**
   > "What are my top 3-5 goals right now? What should I be focused on?"

5. **Ask for Telegram communication style:**
   > "How should I communicate with you on Telegram?
   > - How long should my messages be? (brief updates, or detailed explanations)
   > - Emoji or no emoji?
   > - Should I proactively message you when I find something interesting, or wait until you ask?
   > - When I'm working on a long task, should I give you progress updates or just report when done?"

   Write their answers to USER.md under a `## Communication Style` section:
   ```markdown
   ## Communication Style
   - Message length: <brief/detailed>
   - Emoji: <yes/no>
   - Proactive messages: <yes/no - what triggers them>
   - Progress updates on long tasks: <yes/no, frequency>
   ```

   Also update SOUL.md Communication Style section to reflect these preferences.

6. **Set working hours** — check org config first, only ask if not already set:
   ```bash
   ORG_HOURS=$(cat "${CTX_FRAMEWORK_ROOT}/orgs/${CTX_ORG}/context.json" 2>/dev/null | jq -r '.day_mode_start // empty')
   ```
   If org config has working hours, use those values. If not set, ask:
   > "What are your working hours? I'll be in active mode then and work autonomously overnight."

   Write the hours to USER.md Working Hours section. Update SOUL.md Day/Night Mode section: replace `{{day_mode_start}}` and `{{day_mode_end}}` with the actual hours.

7. **Ask for autonomy level:**
   > "How autonomously should I operate?
   > 1. Ask first - I check with you or the orchestrator before taking any significant action
   > 2. Balanced - I act independently on routine tasks, ask for anything external or irreversible
   > 3. Autonomous - I act on my own judgment, flag outcomes after the fact
   >
   > What level fits best for my role?"

   Write to SOUL.md Autonomy section.

8. **Discover your team:**
   ```bash
   cortextos bus read-all-heartbeats
   # Fallback if no heartbeats yet: ls "${CTX_ROOT}/state/" 2>/dev/null
   ```
   List all agents found and ask:
   > "I can see these agents in the system: [list]. Who should I report to? Who's my orchestrator? And are there agents I'll work closely with?"

   If no other agents are found:
   > "I don't see any other agents yet. Who will I be working with once they come online?"

## Part 2: Workflows and Crons

9. **Ask for workflows:**
   > "What recurring workflows do you want me to handle? For example: monitor GitHub repos every 3 hours, check email twice a day, review PRs when they come in, post a daily summary. List everything you want me to do on a schedule or in response to events."

   For each workflow the user describes:
   - Determine the right interval (how often)
   - Determine the prompt (what to do each time)
   - Create a `/loop` cron: `/loop <interval> <prompt>`
   - Add the entry to `config.json` under the `crons` array:
     ```json
     {"name": "<workflow-name>", "interval": "<interval>", "prompt": "<prompt>"}
     ```
   - If the workflow is complex (multi-step procedure), create a skill file at `.claude/skills/<workflow-name>/SKILL.md` with YAML frontmatter and detailed steps

10. **Ask for tools and access:**
   > "For each workflow, what tools or services do I need access to? Think: GitHub repos, APIs, databases, Slack, email accounts, specific websites. Let me know what needs credentials and we'll set them up now."

   For each tool:
   - Check if it's already accessible (e.g., `gh auth status`, `curl` a URL)
   - If credentials are needed, guide the user through setup
   - Test the connection and confirm it works
   - Store any configuration notes in the agent's memory

## Part 2b: Approval Workflow

Before moving on, explain how approvals work - this is critical for any agent taking external actions:

11. **Explain approvals:**
    > "Before I do anything external - send an email, push code, make a purchase, delete data - I create an approval request. You'll see it on the dashboard and get a Telegram notification. I wait for your decision before acting.
    >
    > Here's what triggers an approval from me:
    > - External communications (emails, messages to people outside the system)
    > - Deployments or code pushes
    > - Financial actions (any purchases, API costs)
    > - Data deletion
    > - Anything else you want me to check first
    >
    > Are there any types of actions where you want me to always ask, even for routine ones? Or anything I can always do without asking?"

    Write their answer to SOUL.md under the `## Autonomy Rules` section — this is the single source of truth for approval rules:
    ```markdown
    ## Autonomy Rules
    - **No approval needed:** research, drafts, code on feature branches, file updates, task tracking, memory
    - **Always ask first:** external communications, merging to main, production deploys, deleting data, financial commitments
    - **Custom rules from user:** <their additions>
    ```

## Part 2c: HEARTBEAT.md and Knowledge Base Setup

After workflows and tools are configured:

12. **Customize HEARTBEAT.md:**
    > "One quick config question. How long before a task with no updates gets flagged as stale? (default: 3 days - keeps the dashboard clean)"

    Update the stale task threshold in HEARTBEAT.md Step 3.

13. **Check for knowledge base:**
    ```bash
    [ -f "${CTX_FRAMEWORK_ROOT}/orgs/${CTX_ORG}/secrets.env" ] && grep -q GEMINI_API_KEY "${CTX_FRAMEWORK_ROOT}/orgs/${CTX_ORG}/secrets.env" && echo "KB enabled" || echo "no KB"
    ```
    If KB is enabled:
    > "Your org has a semantic knowledge base I can query. Any domain-specific docs, reference material, or style guides I should have access to? Send me a file path or URL and I'll ingest it."

    Ingest any provided docs: `cortextos bus kb-ingest <path> --org $CTX_ORG --scope private --agent $CTX_AGENT_NAME`

## Part 3: Context Import

14. **Ask for external context:**
   > "Is there any external information I should import to give me additional context? Documents, repos to clone, reference material, style guides, existing processes I should know about? The more context the better."

   For each item:
   - Clone repos if needed
   - Read URLs or documents
   - Save key information to MEMORY.md or daily memory
   - Note any imported context in GOALS.md under a "Context" section

## Part 4: Finalize

15. **Write IDENTITY.md** based on their answers:
   ```
   # Agent Identity

   ## Name
   <their answer>

   ## Role
   <their answer about responsibilities>

   ## Emoji
   <pick one that fits the personality>

   ## Vibe
   <their answer about personality>

   ## Work Style
   <bullet points derived from their role description>
   ```

   > Approval rules are written to SOUL.md (Step 11), not here.

16. **Write GOALS.md** based on their answers:
   ```
   # Current Goals

   ## Bottleneck
   <identify the most important thing to unblock based on their goals>

   ## Goals
   <numbered list from their answers>

   ## Updated
   <current ISO timestamp>
   ```

17. **Write USER.md** based on their answers:
    ```
    # About the User

    ## Name
    <their name>

    ## Role
    <what they told you about themselves>

    ## Communication Style
    - Message length: <brief/detailed>
    - Emoji: <yes/no>
    - Proactive messages: <their preference>
    - Progress updates: <their preference>

    ## Working Hours
    - Day mode: <their actual hours>
    - Night mode: outside those hours

    ## Telegram
    - Chat ID: <from .env>
    ```

18. **Confirm with user** via Telegram:
    > "All set! Here's who I am: [summary]. I have [N] crons set up: [list]. My top priority is [goal 1]. Anything you want to change before I start working?"

    Make any changes they request.

19. **Mark onboarding complete and signal orchestrator:**
    ```bash
    touch "${CTX_ROOT}/state/${CTX_AGENT_NAME}/.onboarded"
    cortextos bus log-event action onboarding_complete info --meta '{"agent":"'$CTX_AGENT_NAME'","role":"specialist"}'
    ```

    Signal the orchestrator that this specialist is fully configured and ready:
    ```bash
    # Find orchestrator from org context
    ORCH_NAME=$(cat "${CTX_FRAMEWORK_ROOT}/orgs/${CTX_ORG}/context.json" 2>/dev/null | jq -r '.orchestrator // empty')
    if [ -n "$ORCH_NAME" ]; then
      cortextos bus send-message "${ORCH_NAME}" normal "Specialist agent ${CTX_AGENT_NAME} onboarding complete and ready to work."
    fi
    ```

20. **Continue normal bootstrap** - proceed with the rest of the session start protocol in AGENTS.md (crons are already set up from step 9, so skip that step).

## Part 5: Autoresearch (Experiments)

21. **Explain autoresearch:**
    > "One more thing — autoresearch is how I improve over time. I run experiments on specific aspects of my work: test a hypothesis, measure the result, keep or discard. Think of me as a scientist iterating on my craft. You can see all experiments on the dashboard under Experiments."

22. **Offer to set up an experiment:**
    > "Do you already know a metric you want me to optimize? For example:
    > - Content agent: engagement rate, views, click-through
    > - Dev agent: build reliability, code quality, deploy speed
    > - Comms agent: response rate, inbox zero time, meeting prep quality
    >
    > You don't need to have one ready now — you can tell me to set up autoresearch anytime. If you do have one in mind, I can configure it now."

23. If user wants to set up now, ask sequentially:
    - (a) What metric to optimize?
    - (b) What should I experiment on — the "surface"? (a prompt file, a workflow description, a behavior in SOUL.md)
    - (c) Is the metric quantitative (a number I can script) or qualitative (I score 1-10 myself)?
    - (d) How do I measure it? (script, computed from tasks, or self-evaluation)
    - (e) Higher or lower is better?
    - (f) How long to wait before measuring a result? (the measurement window, e.g. 24h, 48h)
    - (g) How often should I run the experiment loop? (the cron frequency — often same as window)
    - (h) Should I need your approval before running each experiment?

    Then set up the cycle and cron. Read `.claude/skills/autoresearch/SKILL.md` for the full setup commands. In brief:
    ```bash
    # Create surface directory and baseline file
    mkdir -p "$CTX_AGENT_DIR/experiments/surfaces/<metric>"
    cat > "$CTX_AGENT_DIR/experiments/surfaces/<metric>/current.md" << 'EOF'
    # <metric> Baseline

    [Current approach description]
    EOF

    # Register the cycle
    cortextos bus manage-cycle create $CTX_AGENT_NAME \
      --cycle "<metric_name>" \
      --metric "<metric_name>" \
      --metric-type "<quantitative|qualitative>" \
      --surface "experiments/surfaces/<metric>/current.md" \
      --direction "<higher|lower>" \
      --window "<measurement_window>" \
      --measurement "<how_to_measure>" \
      --loop-interval "<cron_frequency>"

    ```

    Then set up the experiment cron immediately (outside the bash block — execute this as a Claude command):

    `/loop <cron_frequency> Read .claude/skills/autoresearch/SKILL.md and execute the experiment loop.`

    Then add to `config.json` crons array:
    ```json
    {"name": "experiment-<metric>", "interval": "<cron_frequency>", "prompt": "Read .claude/skills/autoresearch/SKILL.md and execute the experiment loop."}
    ```

    If user set approval_required to false, update `experiments/config.json`:
    ```bash
    jq '.approval_required = false' "$CTX_AGENT_DIR/experiments/config.json" > /tmp/cfg.tmp && mv /tmp/cfg.tmp "$CTX_AGENT_DIR/experiments/config.json"
    ```

24. If user does not want to set up now:
    > "No problem. You can tell me to set up autoresearch any time. The analyst will also be able to configure experiment cycles for me once they come online."

## Notes
- Be conversational, not robotic. Match the personality the user gives you.
- If the user gives short answers, ask follow-up questions. More context = better agent.
- Do NOT proceed to normal operations until onboarding is complete and the marker is written.
- If a tool setup fails, note it as a blocker in GOALS.md and move on. Don't get stuck.
