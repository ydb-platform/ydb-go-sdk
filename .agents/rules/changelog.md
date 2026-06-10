# Changelog

**Every pull request with user-facing changes must include a `CHANGELOG.md` entry.**

## When to add an entry

- API changes: additions, renames, deletions, deprecations.
- Observable behavior changes.

Internal refactoring, agent docs, or non-observable changes: add PR label **`no changelog`** to skip `changelog.yml`.

## Format

1. **Past tense verbs**: Added, Fixed, Changed, Removed, Deprecated.
2. **Insert at the very top** of `CHANGELOG.md`, before existing entries (including before `## v3.x.x` headers).
3. **No version number** — versions are assigned at release (`.github/workflows/publish.yml`).

Example:

```markdown
* Added `query.WithLazyTx(bool)` option for `query.Client.DoTx` calls to enable/disable lazy transactions per operation
```

## Release process (maintainer)

`publish.yml` reads unreleased bullets, bumps `internal/version/version.go`, prepends `## vX.Y.Z`, tags, and creates GitHub release.
