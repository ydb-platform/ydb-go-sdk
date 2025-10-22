# How to contribute

ydb-go-sdk (and YDB also) is an open project, and you can contribute to it in many ways. You can help with ideas, code, or documentation. We appreciate any efforts that help us to make the project better.

Thank you!

## Table of contents
- [How to contribute](#how-to-contribute)
  - [Table of contents](#table-of-contents)
  - [How You Can Help](#how-you-can-help)
    - [🔧 Work on Issues](#-work-on-issues)
    - [👀 Review Pull Requests](#-review-pull-requests)
    - [📚 Add Examples](#-add-examples)
    - [📝 Improve Documentation](#-improve-documentation)
    - [🧪 Test the SDK](#-test-the-sdk)
  - [Using Dev Containers and GitHub Codespaces](#using-dev-containers-and-github-codespaces)
    - [Quick Start with Dev Containers](#quick-start-with-dev-containers)
    - [Using GitHub Codespaces](#using-github-codespaces)
    - [Instructions for checks code changes locally](#instructions-for-checks-code-changes-locally)
      - [Prerequisites](#prerequisites)
      - [Run linter checks](#run-linter-checks)
      - [Run tests](#run-tests)
        - [Only unit tests](#only-unit-tests)
        - [All tests](#all-tests)

## How You Can Help

There are many ways to contribute to the YDB Go SDK project, regardless of your experience level:

### 🔧 Work on Issues
- Browse our [open issues](https://github.com/ydb-platform/ydb-go-sdk/issues) and pick one that interests you
- Look for issues labeled `good first issue` or `student project` or even `30min` if you're new to the project
- Comment on the issue to let others know you're working on it
- Submit a Pull Request with your solution

### 👀 Review Pull Requests
- Help review [open pull requests](https://github.com/ydb-platform/ydb-go-sdk/pulls)
- Test proposed changes locally
- Provide constructive feedback on code quality, performance, and design
- Share your expertise to help improve submissions

### 📚 Add Examples
- Create practical examples demonstrating SDK features
- Add code samples to the `/examples` directory
- Show real-world use cases and best practices
- Help others understand how to use different SDK capabilities

### 📝 Improve Documentation
- Document undocumented functions and methods
- Improve existing documentation clarity
- Add missing godoc comments
- Create tutorials or guides for common tasks
- Fix typos and grammatical errors

### 🧪 Test the SDK
- Test the SDK in different environments and scenarios
- Report bugs with detailed reproduction steps
- Verify fixes for reported issues
- Help improve test coverage by writing unit or integration tests
- Test compatibility with different YDB versions

Every contribution, no matter how small, helps make the YDB Go SDK better for everyone. Thank you for your support!

## Using Dev Containers and GitHub Codespaces

### Quick Start with Dev Containers

For quick development setup, you can use [Dev Containers](https://containers.dev/):

1. Make sure you have a container runtime (Docker, Podman, or any compatible container engine) and [Visual Studio Code](https://code.visualstudio.com/) installed.
2. Install the [Dev Containers](https://marketplace.visualstudio.com/items?itemName=ms-vscode-remote.remote-containers) extension for VS Code.
3. Open the repository in VS Code and select the command `Dev Containers: Reopen in Container`.
4. The environment will be automatically built and configured based on files in `.devcontainer/`.
5. After the container starts, you can immediately run tests, linters, and develop the SDK.

**What's included in the development environment:**
- Go 1.21+ with all project dependencies pre-downloaded
- golangci-lint for code quality checks
- Local YDB database instance running in a separate container
- VS Code extensions for Go development, YAML, Protocol Buffers, and GitHub Actions
- Pre-configured YDB CLI with connection profile for the local database
- All necessary environment variables and certificates for testing

### Using GitHub Codespaces

You can also start working with the SDK directly in your browser using [GitHub Codespaces](https://github.com/features/codespaces):

1. On the repository page, click the `Code` → `Codespaces` → `Create codespace on master` button (or select the desired branch).
2. Codespaces will automatically use settings from `.devcontainer/` and prepare a fully working environment.
3. All commands for testing and linting work the same as locally.

**Benefits:**
- No manual environment setup required.
- Quick start for new contributors.
- Consistent environment for all participants.

Learn more about Dev Containers: https://containers.dev/
Learn more about GitHub Codespaces: https://docs.github.com/en/codespaces

### Instructions for checks code changes locally

#### Prerequisites

- Docker. See [official documentations](https://docs.docker.com/engine/install/) for install `docker` to your operating system
- go >= 1.18. See [official instructions](https://go.dev/doc/install) for install `golang` to your operating system
- golangci-lint >= 1.48.0. See [official instructions](https://golangci-lint.run/usage/install/) for install `golangci-lint` to your operating system

#### Run linter checks

All commands must be called from project directory.

```sh
golangci-lint run ./...
```

#### Run tests

All commands must be called from project directory.

##### Only unit tests

```sh
go test -race -tags fast ./... 
```

##### All tests

```sh
docker run -itd --name ydb -dp 2135:2135 -dp 2136:2136 -dp 8765:8765 -v `pwd`/ydb_certs:/ydb_certs -e YDB_LOCAL_SURVIVE_RESTART=true -e YDB_USE_IN_MEMORY_PDISKS=true -h localhost ydbplatform/local-ydb:latest
export YDB_CONNECTION_STRING="grpcs://localhost:2135/local"
export YDB_SSL_ROOT_CERTIFICATES_FILE="`pwd`/ydb_certs/ca.pem"
export YDB_SESSIONS_SHUTDOWN_URLS="http://localhost:8765/actors/kqp_proxy?force_shutdown=all"
go test -race ./... 
docker stop ydb
```
