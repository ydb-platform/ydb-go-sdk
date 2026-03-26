# Agent Guidelines

This document contains guidelines for AI agents working on this codebase.

## Code Comments

**All code comments must be written in English.**

- Inline comments, function documentation, and package documentation should use English
- Variable names and function names should follow Go naming conventions
- Error messages and log messages should be in English
- This ensures consistency and maintainability across the codebase

## Linting

**The linter is run with `golangci-lint run` from the project root.**

If `golangci-lint` is not installed, it can be installed using:
```bash
curl -sSfL https://raw.githubusercontent.com/golangci/golangci-lint/HEAD/install.sh | sh -s -- -b $(go env GOPATH)/bin v2.4.0
```
(The version `v2.4.0` is specified in [`.github/workflows/lint.yml`](.github/workflows/lint.yml#L10). This installation command is taken from the official documentation)

## Changelog

**Every pull request must include a changelog entry in [`CHANGELOG.md`](CHANGELOG.md).**

Rules for adding a changelog entry:

1. **Only include user-facing changes**: API changes (additions, renames, deletions, deprecations) or behavior changes. Internal refactoring or non-observable changes do not require an entry.
2. **Use past tense verbs** (e.g., "Added", "Fixed", "Changed", "Removed", "Deprecated").
3. **Insert the new line(s) at the very beginning of `CHANGELOG.md`**, before any existing entries.
4. **Do not include a version number** — version numbers are assigned by the maintainer at release time (see [`.github/workflows/publish.yml`](.github/workflows/publish.yml)).

Example entry format:
```markdown
* Added `query.WithLazyTx(bool)` option for `query.Client.DoTx` calls to enable/disable lazy transactions per operation
```

If the pull request contains no user-facing changes, add the label `no changelog` to the PR to skip the changelog check.

