---
name: google-workspace
description: "Google Workspace operations via gogcli (gog) - Gmail, Calendar, Drive, Contacts, Tasks, Sheets, Docs. Use when: managing email, scheduling, file operations, or any Google service interaction."
homepage: https://github.com/steipete/gogcli
---

# Google Workspace via gogcli

Full Google Workspace CLI for Gmail, Calendar, Drive, Contacts, Tasks, Sheets, Docs, and more.

## Accounts

Accounts are configured during onboarding. Always specify which account with `-a`:

| Account | Use For |
|---------|---------|
| Primary email | Gmail (email triage, send, drafts). Main communication email. |
| Secondary email (if configured) | Drive, Docs, Sheets, service-linked accounts. |

Check your agent's TOOLS.md or knowledge.md for the specific account emails configured for your org.

## Common Flags

| Flag | Purpose |
|------|---------|
| `-a EMAIL` | Account to use (required for multi-account) |
| `-j` | JSON output (for parsing) |
| `-p` | Plain TSV output (for scripting) |
| `--max N` | Limit results |
| `-n` | Dry run (no changes) |
| `-y` | Skip confirmations |

## Gmail

```bash
# List unread
gog gmail ls -a YOUR_EMAIL "is:unread" --max 20

# Search
gog gmail ls -a YOUR_EMAIL "from:example.com subject:notification" --max 10

# Read a message (by ID from ls output)
gog gmail read -a YOUR_EMAIL MESSAGE_ID

# Read a thread
gog gmail thread -a YOUR_EMAIL THREAD_ID

# Send email
gog gmail send -a YOUR_EMAIL --to "user@example.com" --subject "Subject" --body "Body text"

# Send with attachment
gog gmail send -a YOUR_EMAIL --to "user@example.com" --subject "Subject" --body "Body" --attach /path/to/file.pdf

# Send with CC/BCC
gog gmail send -a YOUR_EMAIL --to "user@example.com" --cc "cc@example.com" --subject "Subject" --body "Body"

# Create draft
gog gmail draft -a YOUR_EMAIL --to "user@example.com" --subject "Subject" --body "Body"

# List labels
gog gmail labels -a YOUR_EMAIL

# Archive (remove INBOX label)
gog gmail modify -a YOUR_EMAIL MESSAGE_ID --remove-labels INBOX

# Mark as read
gog gmail modify -a YOUR_EMAIL MESSAGE_ID --remove-labels UNREAD
```

## Calendar

```bash
# List upcoming events
gog calendar ls -a YOUR_EMAIL --max 10

# List events in date range
gog calendar ls -a YOUR_EMAIL --from "2026-03-28" --to "2026-03-30"

# Create event
gog calendar create -a YOUR_EMAIL --summary "Meeting" --start "2026-03-28T14:00:00" --end "2026-03-28T15:00:00"

# Create event with location
gog calendar create -a YOUR_EMAIL --summary "Lunch" --start "2026-03-28T12:00:00" --end "2026-03-28T13:00:00" --location "Restaurant Name"

# Delete event
gog calendar delete -a YOUR_EMAIL EVENT_ID
```

## Drive

```bash
# List files
gog drive ls -a YOUR_EMAIL --max 20

# Search files
gog drive ls -a YOUR_EMAIL --query "name contains 'report'"

# Download file
gog drive download -a YOUR_EMAIL FILE_ID

# Upload file
gog drive upload -a YOUR_EMAIL /path/to/local/file.pdf
```

## Contacts

```bash
# List contacts
gog contacts ls -a YOUR_EMAIL --max 20

# Search contacts
gog contacts ls -a YOUR_EMAIL --query "John"
```

## Tasks

```bash
# List task lists
gog tasks ls -a YOUR_EMAIL

# List tasks in a list
gog tasks ls -a YOUR_EMAIL --list "My Tasks"
```

## Sheets

```bash
# Read spreadsheet
gog sheets read -a YOUR_EMAIL SPREADSHEET_ID

# Read specific range
gog sheets read -a YOUR_EMAIL SPREADSHEET_ID --range "Sheet1!A1:D10"
```

## Important Notes

- **gog replaces Gmail/Calendar MCP tools.** Do NOT use gmail_search_messages, gmail_create_draft, gcal_list_events, etc. Use gog commands instead.
- **Always specify account** with `-a` flag. Without it, gog may use the wrong account.
- **Use `-j` for parsing** when you need to process results programmatically.
- **Sending email requires approval.** Create an approval via create-approval.sh before sending any external email.
- **Check your org's knowledge.md** for specific calendar names, scheduling preferences, and account details.
