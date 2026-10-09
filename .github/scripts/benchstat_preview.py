#!/usr/bin/env python3
"""Make a benchstat report readable in GitHub's artifact preview."""

import sys
from pathlib import Path


REPLACEMENTS = str.maketrans({
    "│": "|",
    "±": "+/-",
    "µ": "u",
    "¹": "[1]",
    "²": "[2]",
})


def render(text: str) -> str:
    preview = text.translate(REPLACEMENTS)
    if not preview.isascii():
        raise ValueError("benchstat preview contains unmapped non-ASCII characters")
    return preview


def main() -> None:
    source, destination = map(Path, sys.argv[1:])
    destination.parent.mkdir(parents=True, exist_ok=True)
    destination.write_text(render(source.read_text(encoding="utf-8")), encoding="ascii")


if __name__ == "__main__":
    main()
