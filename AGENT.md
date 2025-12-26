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

