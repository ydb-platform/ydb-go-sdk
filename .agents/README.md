# Agent workspace — ydb-go-sdk

Canonical home for AI coding agent assets. Human and agent entry point remains [`AGENTS.md`](../AGENTS.md) in the repo root.

## Layout

| Directory | Purpose | Load when |
|-----------|---------|-----------|
| [`context/`](context/) | Project knowledge — architecture, tooling, progress | Stable files on demand; `activeContext.md` is branch-only (see its header) |
| [`rules/`](rules/) | Coding standards and workflow | On demand via `AGENTS.md` router |

`skills/` and `prompts/` may be added later when project-specific agent workflows are needed (see [ydb-rs-sdk `.agents/`](https://github.com/ydb-platform/ydb-rs-sdk/tree/feat/memory-bank-for-agents/.agents) and [ydb-pg-extension `.agents/`](https://github.com/ydb-platform/ydb-pg-extension/pull/257)).
