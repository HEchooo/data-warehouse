---
name: bigquery-safe-table-change
description: Safely apply BigQuery table schema changes and data updates with a staging-plus-swap workflow. Use when tasks involve table field changes, backfills, partition replacements, deduplicated upserts, or avoiding DELETE/UPDATE on tables that may have streaming-buffer constraints. Trigger for requests like "add columns", "update table data", "replace partition data", "avoid delete+insert", "drop and rename table", or "rebuild table from temp table".
---

# BigQuery Safe Table Change

Use this skill to execute BigQuery table changes with low-risk table swap semantics:
1) build changed temp table from source table
2) drop source table
3) rename changed temp table back to source table name

This skill is for operational ETL jobs and SQL migrations where direct `DELETE` or `UPDATE` can fail or cause risk.

## Required Workflow

### 1. Prepare names and scope

- Define:
  - `target_table`: fully qualified target table id
  - `staging_table`: table for incoming changed rows
  - `changed_table`: rebuilt full table that will replace target
- Keep `staging_table` and `changed_table` in the same dataset as `target_table`.
- Add a run suffix (timestamp or UUID) to prevent name collisions.

### 2. Build staging table

Create empty staging table from target schema:

```sql
CREATE TABLE `project.dataset.target_tmp_{{run_id}}` AS
SELECT * FROM `project.dataset.target`
WHERE 1 = 0;
```

Load changed rows into staging table (API insert, load job, or SQL insert).

### 3. Build changed table from target + staging

Create changed table by preserving unaffected rows from target and appending replacement rows from staging.

Partition replacement template:

```sql
CREATE TABLE `project.dataset.target_changed_{{run_id}}`
PARTITION BY dt AS
SELECT *
FROM `project.dataset.target`
WHERE dt < DATE '{{start_date}}' OR dt > DATE '{{end_date}}'
UNION ALL
SELECT *
FROM `project.dataset.target_tmp_{{run_id}}`;
```

Key-based replacement template:

```sql
CREATE TABLE `project.dataset.target_changed_{{run_id}}` AS
SELECT *
FROM `project.dataset.target`
WHERE business_key NOT IN (
  SELECT business_key FROM `project.dataset.target_tmp_{{run_id}}`
)
UNION ALL
SELECT *
FROM `project.dataset.target_tmp_{{run_id}}`;
```

### 4. Validate before swap

Run pre-swap checks:

- row count check (expected range)
- partition/key coverage check
- nullability/type checks for new fields
- optional checksum/sample comparison

Do not swap if checks fail.

### 5. Swap table (required sequence)

Use this exact order:

```sql
DROP TABLE `project.dataset.target`;
ALTER TABLE `project.dataset.target_changed_{{run_id}}`
RENAME TO target;
```

Notes:
- `RENAME TO` takes table name only (no dataset qualifier).
- Keep this sequence explicit to match operational runbooks.

### 6. Cleanup

After successful rename:

```sql
DROP TABLE IF EXISTS `project.dataset.target_tmp_{{run_id}}`;
```

If swap fails after target drop, keep `changed_table` and recover immediately by renaming it to `target`.

## Playbook: Table Field Changes

### Add new fields in DDL repos

- Do not modify existing `CREATE TABLE` statements.
- Add new field changes using `ALTER TABLE` statements below original DDL.

Example:

```sql
ALTER TABLE `project.dataset.target`
ADD COLUMN new_col STRING;
```

### Backfill strategy for new fields

- If historical rows need defaults, implement defaults while building `changed_table`.
- Use explicit projection when defaults are needed:

```sql
CREATE TABLE `project.dataset.target_changed_{{run_id}}` AS
SELECT
  t.*,
  IFNULL(t.new_col, 'unknown') AS new_col
FROM `project.dataset.target` t;
```

Prefer explicit field lists when renaming or transforming columns.

## Playbook: Data Updates

### Partition-scoped update

- Recommended for daily/hourly ETL.
- Replace only affected partition range via `target without range + staging`.

### Key-scoped update

- Recommended for dedup/upsert use cases.
- Preserve non-matching keys from target and union changed rows from staging.

### Streaming-buffer safety rule

- Avoid direct `DELETE`/`UPDATE` on tables likely to receive streaming inserts.
- Use staging and swap workflow instead of `delete + insert`.

## Operational Guardrails

- Use unique temp table names with run suffix.
- Always wrap cleanup in `finally`-style logic in application code.
- Log all generated table ids and date/key scopes.
- If failure occurs, do not drop `changed_table` until recovery decision is made.

