---
name: superset
description: Automate Apache Superset work by reading connection info from a project-root `superset.json`, logging into the Superset site, and then using browser automation to write or run SQL, create or update charts, and create or update dashboards. Use this skill whenever the user asks to log into Superset, open SQL Lab, run a query in Superset, make a chart, edit a chart, build a dashboard, adjust dashboard layout, or automate BI work in Superset through the browser, even if they do not explicitly mention browser automation.
allowed-tools: Bash(python3:*), Bash(agent-browser:*), Read, Glob
---

# Superset browser automation

Use this skill for browser-driven Apache Superset tasks. It is designed for projects that keep Superset access info in a repository-root `superset.json` file. If the user refers to `@superset.json`, treat that as the project file `superset.json`.

This skill should **reuse `agent-browser`** for all web interaction instead of inventing a custom browser workflow.

## What this skill expects

Project root `superset.json` should contain:

```json
{
  "url": "https://your-superset.example.com/superset/",
  "username": "your-user",
  "password": "your-password"
}
```

Extra keys are allowed and can be ignored unless the user asks for them.

## Core rules

1. Read `superset.json` from the current project root before doing anything else.
2. Treat credentials as sensitive:
   - do not paste them into final user-facing summaries
   - read them from the bundled helper or directly from `superset.json`
   - do not write copied credentials into logs, notes, or extra files
3. Use `agent-browser` for navigation, snapshotting, filling, clicking, waiting, screenshots, and validation.
4. Re-snapshot after each page transition or large DOM change.
5. Confirm success from page state, not from assumptions. For example, check that SQL Lab opened, a chart save dialog appeared, or a dashboard title is visible.
6. If the user asks to modify an existing chart or dashboard, first inspect the current page state and preserve the existing object unless the user asked to replace it.

## Recommended workflow

### 1. Load Superset config

Use the bundled helper to safely read the project-root config:

```bash
python3 "$CLAUDE_PROJECT_DIR/.agents/skills/superset/scripts/read_superset_config.py"
```

If `CLAUDE_PROJECT_DIR` is not set, use the current working directory as the project root.

The helper returns JSON with:
- `url`
- `username`
- `password`
- `base_url`
- `login_candidates`
- `sql_lab_candidates`

If `superset.json` is missing or required fields are empty, stop and ask the user to fix the config.

### 2. Start an isolated browser session

Use a named session so the Superset flow does not collide with other browser automations:

```bash
agent-browser --session superset open <url>
agent-browser --session superset wait --load networkidle
agent-browser --session superset snapshot -i
```

If a stale Superset session exists, close it first:

```bash
agent-browser --session superset close
```

### 3. Login flow

Preferred approach:
- open the configured `url`
- if already authenticated, continue
- otherwise navigate to the first working login page from `login_candidates`
- snapshot the page
- find username, password, and submit controls
- fill credentials and submit
- wait for redirect or a post-login page
- re-snapshot and verify authentication by checking for navigation items like SQL Lab, Dashboards, Charts, or the user menu

Typical flow:

```bash
agent-browser --session superset open <login-url>
agent-browser --session superset wait --load networkidle
agent-browser --session superset snapshot -i
agent-browser --session superset fill @e1 "<username>"
agent-browser --session superset fill @e2 "<password>"
agent-browser --session superset click @e3
agent-browser --session superset wait --load networkidle
agent-browser --session superset snapshot -i
```

Use the actual refs from the snapshot; do not assume the control order.

If the login page uses unusual markup, prefer semantic locators such as:

```bash
agent-browser --session superset find label "username" fill "..."
agent-browser --session superset find label "password" fill "..."
agent-browser --session superset find role button click --name "Sign In"
```

### 4. SQL Lab workflow

Use this when the user asks to write SQL, run SQL, inspect results, or save a dataset/query.

Recommended sequence:
1. Navigate to the first valid page in `sql_lab_candidates`.
2. Wait for the editor and result area.
3. Snapshot the page.
4. Select database, catalog, or schema only if required by the page and known from the user request.
5. Fill or insert the SQL text carefully.
6. Run the query.
7. Wait for completion.
8. Verify success from results, status text, or visible table output.
9. If the user asked to save as dataset, chart, or query, continue from the result page instead of starting over.

Tips:
- Large SQL strings are safer with `agent-browser keyboard inserttext` after focusing the editor.
- Some editors are Monaco/Ace and may not work with plain `fill`; if so, click into the editor and use keyboard insertion.
- If a previous query is present, clear only the editor area the user asked to replace.

### 5. Chart workflow

Use this when the user asks to create or edit a chart.

Recommended sequence:
1. Start from a saved dataset or SQL result that can produce a chart.
2. Open chart creation or chart edit.
3. Snapshot and inspect controls.
4. Set chart name only if the user supplied one or asked to rename.
5. Choose visualization type that matches the user request.
6. Set dimensions, metrics, filters, time grain, sorting, or limit according to the request.
7. Run or update the chart.
8. Verify the visualization rendered.
9. Save, then confirm the saved title or success toast.

Be conservative:
- do not guess business metrics if the user did not specify them
- if a chart choice is ambiguous, ask the user before continuing
- when editing an existing chart, preserve unchanged controls

### 6. Dashboard workflow

Use this when the user asks to create or update a dashboard.

Recommended sequence:
1. Open the target dashboard or create a new one.
2. Snapshot the page and confirm edit mode if needed.
3. Set or update the dashboard title.
4. Add existing charts or save a new chart first if required.
5. Adjust layout only as far as the user requested.
6. Save the dashboard.
7. Verify the dashboard title and chart tiles are visible after save.

When adding charts, prefer existing saved charts if the user references one by name. Do not duplicate charts unless the user asked for a separate copy.

## How to decide what to do

### If the user asks only to log in
- read `superset.json`
- log in
- report whether login succeeded and what landing page is open

### If the user asks for SQL only
- log in if needed
- open SQL Lab
- run the SQL
- summarize whether it succeeded and what result area appeared

### If the user asks for a chart
- make sure the SQL or dataset source is ready
- create or edit the chart
- save only if the user asked for a persisted chart or if saving is required for dashboard work

### If the user asks for a dashboard
- ensure required charts exist first
- create or edit the dashboard
- save and verify final layout/title state

## Failure handling

If automation fails:
1. capture a fresh snapshot
2. capture a screenshot if the page looks visually inconsistent
3. report the exact blocker briefly: login failure, missing permissions, missing dataset, SQL error, unexpected modal, or UI mismatch
4. do not keep retrying the same broken step blindly

## Minimal command patterns

### Open and inspect

```bash
agent-browser --session superset open <url> && \
agent-browser --session superset wait --load networkidle && \
agent-browser --session superset snapshot -i
```

### Take a screenshot for debugging

```bash
agent-browser --session superset screenshot --annotate
```

### Insert SQL into rich editor

```bash
agent-browser --session superset click @e1
agent-browser --session superset keyboard inserttext "SELECT 1"
```

## Bundled helper files

- `scripts/read_superset_config.py`: reads and validates `superset.json`

Read the helper when you need the exact JSON contract or want to adjust path resolution.
