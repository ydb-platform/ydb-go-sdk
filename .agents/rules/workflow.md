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

## SLO label (developer only)

- Agents must not add the `SLO` label automatically.
- Only a developer adds `SLO` manually, after all other review feedback and failing tests have been addressed.
- A failing `check-slo-label` check is not authorization to add the label or bypass the gate. Finish the other fixes and checks, and leave SLO activation to the developer.

## Code reuse

1. Search the repo (`rg`, IDE search) for similar helpers before adding new utilities.
2. Follow existing `Do`/`DoTx`, retry, and error-mapping patterns in `internal/table/`, `internal/query/`.
3. Extend shared helpers in `internal/` rather than duplicating logic in public packages.

## Context updates

- **`activeContext.md`** — branch-only scratch pad. Revert to the placeholder before merge; never land session notes on `master`.
- **`progress.md`** — update in the PR that delivers completed work.
- Stable files (`systemPatterns.md`, …) — only when architecture, tooling, or scope actually changed.

Add rules to `AGENTS.md` only after repeated agent mistakes — incremental, not upfront.
