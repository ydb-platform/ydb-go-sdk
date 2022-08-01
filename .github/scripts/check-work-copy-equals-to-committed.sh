#!/usr/bin/env bash

MESSAGE="$1"

CODE_DIFF="$(git diff)"
if [ -n "$CODE_DIFF"  ]; then
  echo "$MESSAGE"
  echo
  echo "$CODE_DIFF"
  exit 1;
fi
