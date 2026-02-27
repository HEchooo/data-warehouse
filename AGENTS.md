# AGENTS.md

## Rules

- Do not modify existing `CREATE TABLE` statements in the `ddl` directory. Add new fields using `ALTER TABLE` statements below the original definition.
- Do not change any file paths in the codebase. They may reference production environments.
- After completing a task, remove temporary or generated folders such as `__pycache__`.
