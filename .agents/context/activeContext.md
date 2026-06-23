# Active Context

> **Branch-only scratch pad** — not merge material for `main`/`master`.

## Policy

- **Green master:** `main`/`master` holds only completed, merged work. Half-done tasks do not land in the default branch.
- **This file must have no diff at PR merge time.** While iterating on a feature branch you may edit it freely; **before merge, revert this file** so the PR does not change it relative to the target branch.
- On `main`/`master` this file stays exactly this placeholder — no session notes, no current-PR focus, no “recent changes”.

## Current focus

_(empty on master — use only on a local/feature branch; do not commit branch notes here when opening a merge-ready PR)_

## Where to put durable knowledge

| What | Where |
|------|-------|
| Architecture, tooling, scope | stable files: `systemPatterns.md`, `techContext.md`, `projectBrief.md`, … |
| Completed milestones | `progress.md` — update in the same PR that delivers the work |
| Coding rules | `AGENTS.md` → `.agents/rules/` |
