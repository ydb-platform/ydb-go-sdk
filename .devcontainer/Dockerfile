FROM mcr.microsoft.com/devcontainers/go:1-1.21-bookworm

# [Optional] Uncomment this section to install additional OS packages.
# RUN apt-get update && export DEBIAN_FRONTEND=noninteractive \
#     && apt-get -y install --no-install-recommends <your-pkg>

# Install golangci from GitHub. Binary will be $(go env GOPATH)/bin/golangci-lint
RUN curl -sSfL https://raw.githubusercontent.com/golangci/golangci-lint/master/install.sh | sh -s -- -b $(go env GOPATH)/bin v1.62.2

# Install ydb cli from GitHub.
# COPY ./ydb /usr/local/bin/ydb
# RUN chmod +x /usr/local/bin/ydb

# [Optional] Uncomment the next lines to use go get to install anything else you need
# USER vscode
# RUN go get ...
# USER root
