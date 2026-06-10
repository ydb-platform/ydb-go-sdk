# Project context — ydb-go-sdk

Structured, version-controlled context for AI coding agents.

`AGENTS.md` stays minimal (operational router). This directory holds detailed context — read **selectively**, not all at once.

> Paths in context files may lag behind refactors — verify with Glob/Grep before acting on specific filenames.

## Knowledge tree

```
ydb-go-sdk/
├── .agents/                        ← agent workspace (you are in context/)
│   ├── README.md                   layout of context / rules
│   ├── context/                    ← you are here
│   │   ├── README.md               entry point, reading/update strategy
│   │   ├── activeContext.md        volatile: current focus, decisions, next steps
│   │   ├── progress.md             volatile: status, milestones, open work
│   │   ├── projectBrief.md         stable: scope, goals, constraints
│   │   ├── productContext.md       stable: users, API surface, feature parity
│   │   ├── systemPatterns.md       evolving: package layout, Do/DoTx, retry, pools
│   │   └── techContext.md          evolving: CI, Go versions, local YDB, build commands
│   └── rules/                      coding standards (on demand via AGENTS.md)
│
├── AGENTS.md                       lean agent router (read first)
├── CLAUDE.md                       tool entry point → AGENTS.md
├── driver.go, sql.go               root package entry points
├── table/, query/, scheme/, topic/ public service clients
├── internal/                       implementations (not stable API)
├── tests/integration/              integration tests (build tag: integration)
├── examples/                       runnable examples (separate go.mod)
└── .devcontainer/                  dev environment + local YDB sidecar
```

## Core files

| File | Stability | Read when |
|------|-----------|-----------|
| [`activeContext.md`](activeContext.md) | **Volatile** | **Every session** — current focus, decisions, next steps |
| [`progress.md`](progress.md) | **Volatile** | Resuming work, closing a PR, status checks |
| [`systemPatterns.md`](systemPatterns.md) | Evolving | Architecture, new modules, Do/DoTx patterns |
| [`techContext.md`](techContext.md) | Evolving | CI, Go versions, local YDB, lint/test commands |
| [`productContext.md`](productContext.md) | Stable | Public API, users, feature parity |
| [`projectBrief.md`](projectBrief.md) | Stable | Scope, goals, constraints |

## Reading strategy

```
Every session:  activeContext.md
If needed:      one stable file matching the task
Code patterns:  .agents/rules/ via AGENTS.md router (on demand)
Full review:    all files (on "update memory bank" or major onboarding)
```

Avoid loading all six core files at session start — it wastes context tokens without improving outcomes.

## Update triggers

1. Feature/fix ready for PR → `activeContext.md` + `progress.md`
2. Architecture or CI changed → `systemPatterns.md` or `techContext.md`
3. Scope changed → `projectBrief.md` or `productContext.md`
4. User says **"update memory bank"** → review every core file

## What not to store

- Secrets, tokens, credentials
- Large generated artifacts (link to pkg.go.dev instead)
- Chat transcripts
- Duplication of `AGENTS.md` rules — link instead
