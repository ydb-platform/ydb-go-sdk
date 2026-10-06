#!/usr/bin/env python3
"""Run alternating SDK revisions against identical, separate-process mock nodes."""

import argparse
import hashlib
import json
import os
from pathlib import Path
import platform
import shutil
import subprocess
import tempfile


def execute(command, cwd=None):
    return subprocess.check_output(command, cwd=cwd, text=True)


def prepare(root, revision, directory):
    archive = subprocess.check_output(["git", "archive", revision], cwd=root)
    subprocess.run(["tar", "-x", "-C", str(directory)], input=archive, check=True)
    destination = directory / "tests/benchmarks/p2c"
    destination.mkdir(parents=True, exist_ok=True)
    for name in ("main.go", "server.go", "main_test.go"):
        shutil.copyfile(root / "tests/benchmarks/p2c" / name, destination / name)
    for name in ("internal/conn/conn_test.go", "internal/balancer/elector_test.go"):
        shutil.copyfile(root / name, directory / name)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--base", default="origin/master")
    parser.add_argument("--repeats", type=int, default=5)
    parser.add_argument("--duration", default="6s")
    parser.add_argument("--gomaxprocs", type=int, default=4)
    parser.add_argument("--control-only", action="store_true")
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    root = Path(__file__).resolve().parents[3]
    environment = dict(os.environ, GOMAXPROCS=str(args.gomaxprocs))
    args.output.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix="ydb-p2c-") as temporary:
        directory = Path(temporary)
        baseline = directory / "baseline"
        baseline.mkdir()
        prepare(root, args.base, baseline)
        binaries = {"random": directory / "random", "p2c": directory / "p2c"}
        for label, source in (("random", baseline), ("p2c", root)):
            subprocess.run(
                ["go", "build", "-o", str(binaries[label]), "./tests/benchmarks/p2c"],
                cwd=source, env=environment, check=True,
            )
        metadata = {
            "base": execute(["git", "rev-parse", args.base], root).strip(),
            "head": execute(["git", "rev-parse", "HEAD"], root).strip(),
            "patchSha256": hashlib.sha256(
                subprocess.check_output(["git", "diff", "HEAD"], cwd=root)
            ).hexdigest(),
            "binariesSha256": {
                label: hashlib.sha256(binary.read_bytes()).hexdigest()
                for label, binary in binaries.items()
            },
            "platform": platform.platform(),
            "go": execute(["go", "version"]).strip(),
            "gomaxprocs": args.gomaxprocs, "workersPerNode": 4, "fastServiceMs": 8,
            "slowServiceMs": 40, "repeats": args.repeats, "duration": args.duration,
            "controlOnly": args.control_only,
        }
        (args.output / "metadata.json").write_text(json.dumps(metadata, indent=2) + "\n")
        cases = [(1, "equal", 250)]
        for nodes in (3, 9):
            cases.extend((nodes, "equal", nodes * rate) for rate in (80, 250, 400))
            cases.extend((nodes, "slow", nodes * rate) for rate in (80, 250))
            cases.extend(
                [(nodes, "temporary", nodes * 250), (nodes, "streams", nodes * 250),
                 (nodes, "pinned", nodes * 60)]
            )
        if args.control_only:
            cases = [(1, "equal", 250), (3, "equal", 750),
                     (9, "equal", 3600), (3, "pinned", 180)]
        with (args.output / "runs.jsonl").open("w") as output:
            for repeat in range(args.repeats):
                for nodes, scenario, rate in cases:
                    order = ("random", "p2c") if repeat % 2 == 0 else ("p2c", "random")
                    for label in order:
                        raw = subprocess.check_output(
                            [str(binaries[label]), "-nodes", str(nodes), "-scenario", scenario,
                             "-rate", str(rate), "-duration", args.duration],
                            env=environment, text=True, timeout=90,
                        )
                        record = json.loads(raw)
                        record.update(algorithm=label, repeat=repeat)
                        output.write(json.dumps(record) + "\n")
                        output.flush()
                        print(
                            f'{label:6s} n={nodes:2d} {scenario:9s} rate={rate:4d} '
                            f'p95={record["p95Ms"]:7.2f}ms errors={record["errors"]:4d} '
                            f'cpu={record["clientCpuSeconds"]:.3f}s',
                            flush=True,
                        )
    print(args.output, flush=True)


if __name__ == "__main__":
    main()
