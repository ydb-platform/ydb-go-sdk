# Feature Request: Minimal Metrics Option for Query Client Session Pool

## Problem

Currently, the query client metrics lack a minimal option that records only session pool size and active session count metrics. For the table client, such an option exists — `TableSessionLifeCycleEvents` — which provides a lightweight way to track essential session pool metrics.

The existing `QueryPoolEvents` option is too broad and includes significantly more metrics than needed:
- `pool.with.*` metrics (errors, attempts, latency)
- `pool.node_hint_miss` metrics
- All metrics that are written when `details&QueryEvent != 0` (as per [trace/details.go:105](trace/details.go#L105))

This results in much more verbose metrics than what `TableSessionLifeCycleEvents` provides for the table client.

## Current Workaround

While `QueryPoolEvents` can be used as a temporary solution, it enables a much larger set of metrics than necessary for simply tracking pool size and active session count.

## Proposed Solution

Add a new minimal metrics option (e.g., `QueryPoolLifeCycleEvents` or similar) that enables only:
1. Session pool size metrics (limit, idle, index, wait, create_in_progress, in_use)
2. Active session count metrics

This would provide granular control over which metrics are enabled, allowing users to independently enable smaller sets of metrics rather than being forced to use the broader `QueryPoolEvents` option.

## Expected Behavior

The new option should:
- Enable only pool size and active session count metrics
- Be independent from other query event flags
- Follow the same pattern as `TableSessionLifeCycleEvents` for consistency
- Allow users to combine it with other specific event flags if needed

## Related Code

- [trace/details.go](trace/details.go) - Event flags definitions
- [metrics/query.go](metrics/query.go) - Query metrics implementation
- [metrics/table.go](metrics/table.go) - Table metrics implementation (reference for minimal option pattern)
- [spans/query.go](spans/query.go) - Query spans implementation

## Additional Context

This feature would improve the observability story for query client users who need basic pool monitoring without the overhead of more detailed metrics.




