# Workflow

## Issue-first (required for non-trivial work)

Per [`CONTRIBUTING.md`](../../CONTRIBUTING.md): discuss new features and bug fixes in a GitHub issue before implementing.

Look for labels: `good first issue`, `student project`, `30min`.

## User-request boundaries

Stop and ask when blocked — do not ship an unsanctioned alternative approach.

✅ **Correct**

```
User: "Implement feature X using approach A"
Agent: "Attempting approach A..."
Agent: "Approach A hit error P: <details>. Next step?"
```

❌ **Wrong**

```
User: "Fix the build using method M"
Agent: "Method M failed, so I implemented alternative N instead."
```

## Code reuse

1. Search the repo (`rg`, IDE search) for similar helpers before adding new utilities.
2. Follow existing `Do`/`DoTx`, retry, and error-mapping patterns in `internal/table/`, `internal/query/`.
3. Extend shared helpers in `internal/` rather than duplicating logic in public packages.

## Memory bank updates

After significant work, update `.agents/context/activeContext.md` and `progress.md` before closing a PR.

Add rules to `AGENTS.md` only after repeated agent mistakes — incremental, not upfront.
