---
name: knowledge-base
description: Query and ingest documents into the RAG knowledge base. Use when doing research, storing findings, or querying org knowledge.
triggers:
  - kb-query
  - kb-ingest
  - kb-collections
  - kb-setup
  - knowledge base
  - search knowledge
  - ingest
  - RAG
---

# Knowledge Base (RAG)

The knowledge base lets you search indexed documents using natural language - your memory, research, notes, and org knowledge.

## Query (before starting research)
```bash
cortextos bus kb-query "your question" --org $CTX_ORG --agent $CTX_AGENT_NAME
```

## Ingest (after completing research or updating memory)
```bash
# Ingest to shared org collection (visible to all agents)
cortextos bus kb-ingest /path/to/docs --org $CTX_ORG --scope shared

# Ingest to your private collection (only visible to you)
cortextos bus kb-ingest /path/to/docs --org $CTX_ORG --agent $CTX_AGENT_NAME --scope private
```

## When to query
- Before starting a research task - check if knowledge already exists
- When referencing named entities (people, projects, tools) - check for existing context
- When answering factual questions about the org - query before searching externally

## When to ingest
- After completing substantive research (always ingest your findings)
- After writing or updating MEMORY.md (knowledge persists across sessions)
- After learning important facts about the org, users, or systems

## List collections
```bash
cortextos bus kb-collections --org $CTX_ORG
```

## First-time setup
```bash
cortextos bus kb-setup --org $CTX_ORG
```
