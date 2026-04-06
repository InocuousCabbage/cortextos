---
name: community-skill-review
description: "Review an inbound community skill or agent template PR against the safety and quality checklist, summarize findings, and recommend approve or flag for James."
triggers: ["review skill", "review PR", "community PR", "skill submission", "check contribution", "review contribution", "community review", "inbound skill", "skill review checklist"]
external_calls: []
---

# Community Skill Review

Review inbound community contributions (skills, agent templates, org templates) against the cortextOS quality and safety checklist. Produce a structured findings summary for James.

## When to Use

- James forwards a GitHub PR URL for a community skill or template
- An automated check flags a pending catalog entry for review
- paul2 or another agent surfaces a new community submission

---

## Workflow

### Step 1: Fetch the PR

```bash
# Get PR details and files changed
gh pr view <PR_NUMBER> --json title,author,body,files
gh pr diff <PR_NUMBER>
```

Note the PR author, title, and which files were added or modified.

### Step 2: Locate the skill files

Community contributions should add files under:
- `community/skills/<name>/SKILL.md` — for skills
- `community/agents/<name>/` — for agent templates
- `community/catalog.json` — catalog registration entry

If files are in a different location, flag it immediately — contributions must follow the standard structure.

### Step 3: Run the review checklist

Work through each item. Mark pass, fail, or warn.

#### Structure checks
- [ ] Directory name matches `name` field in SKILL.md frontmatter
- [ ] `SKILL.md` is present and has valid frontmatter (`name`, `description`, `triggers`)
- [ ] `external_calls` field is present in frontmatter (required — `[]` if no external calls)
- [ ] `description` is one sentence and specific (not generic filler like "a useful skill")
- [ ] `triggers` array has at least 3 meaningful phrases
- [ ] Catalog entry added to `community/catalog.json` with `review_status: "pending"`
- [ ] For agent templates: IDENTITY.md, SOUL.md, GUARDRAILS.md, and config.json all present

#### Content quality checks
- [ ] Instructions are written for the agent (actionable, concrete commands)
- [ ] All bash uses `cortextos bus` CLI — no direct file path manipulation
- [ ] No hardcoded absolute paths (e.g. `/Users/someone/...`)
- [ ] Steps are sequential and unambiguous

#### External calls checks
Grep the diff for: `curl`, `fetch(`, `http://`, `https://`, `wget`, any domain names.
- [ ] Every external network call in the skill body is declared in `external_calls` frontmatter
- [ ] `external_calls` is not suspiciously empty for a skill that clearly makes HTTP calls
- [ ] Declared external hosts are recognizable (public APIs) — flag any unknown or suspicious domains
- [ ] No calls to data collection services, analytics endpoints, or telemetry hosts
- [ ] If `external_calls` lists a domain, verify the actual URL in the skill body matches it

> A skill that makes undeclared external calls is an automatic FLAG, not a request-for-changes.

#### Safety checks
- [ ] No secrets, tokens, API keys, or credentials anywhere in the diff
- [ ] No PII: no real names, email addresses, phone numbers, chat IDs
- [ ] No destructive shell patterns: `rm -rf`, `curl | sh`, `eval`, `> /dev/sda`
- [ ] No obfuscated commands (base64-decoded payloads, hex strings run as code)
- [ ] No attempts to exfiltrate data outside the cortextOS bus (unexpected network calls)
- [ ] GUARDRAILS.md (if present) does not weaken safety constraints

### Step 4: Determine recommendation

**Approve** if:
- All structure checks pass
- All safety checks pass
- Content quality is reasonable (minor wording issues are fine)

**Flag** if:
- Any safety check fails — do not approve regardless of other checks
- Structure is missing required files for agent templates
- Description/triggers are so vague the skill would never activate correctly

**Request changes** (not a hard flag) if:
- Minor structure issues (wrong directory, catalog entry missing) that are easy to fix
- Quality issues: instructions are vague but not unsafe

### Step 5: Write findings summary

Format your summary as:

```
## Community Skill Review: <skill-name> (PR #<number>)

**Author:** <github_username>
**Type:** skill | agent | org
**Recommendation:** APPROVE | FLAG | REQUEST CHANGES

### Checklist Results
- Structure: PASS / FAIL (<details if fail>)
- Content quality: PASS / WARN (<details if warn>)
- Safety: PASS / FAIL (<details if fail>)

### Notes
<Any specific observations, edge cases, or context>

### Action
<What James should do: merge, close, or ask author to fix X>
```

### Step 6: Report to James

Send via Telegram:

```bash
cortextos bus send-telegram $CTX_TELEGRAM_CHAT_ID "<summary>"
```

Keep the Telegram message brief — headline recommendation + one sentence on the most important finding. Attach the full checklist as a follow-up message if needed.

If it is a FLAG, also log an event:

```bash
cortextos bus log-event action community_review_flagged warning \
  --meta '{"pr":"<number>","skill":"<name>","reason":"<short reason>"}'
```

---

## Safety Escalation

If you find evidence of malicious intent (credential harvesting, data exfiltration, backdoor patterns), do not just flag — immediately notify James with URGENT priority:

```bash
cortextos bus send-telegram $CTX_TELEGRAM_CHAT_ID "URGENT: PR #<number> contains suspicious code — <one line description>. Do not merge. Full details in activity log."
cortextos bus log-event error community_review_security_alert critical \
  --meta '{"pr":"<number>","skill":"<name>","detail":"<description>"}'
```

---

## Notes

- When in doubt, flag. It is easier to un-flag a safe contribution than to undo a merged backdoor.
- Skills that only use `cortextos bus` commands are much safer than skills with raw bash — weight this heavily.
- A poorly written but safe skill can be fixed. An unsafe skill cannot be approved at any quality level.
