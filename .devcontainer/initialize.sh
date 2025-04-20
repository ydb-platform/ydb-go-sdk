#!/bin/bash
set -e

git config --local user.email "$(git config user.email)"
git config --local user.name "$(git config user.name)"

if [ -n "${GITHUB_TOKEN}" ]; then
    echo "GITHUB_TOKEN is set and gh is authenticated."
else
    export GITHUB_TOKEN="$(gh auth token)"
    if [ -z "${GITHUB_TOKEN}" ]; then
        echo "GITHUB_TOKEN is not set and gh is not authenticated. Please authenticate gh."
        exit 0
    fi
fi
