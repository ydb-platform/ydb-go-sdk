"""Require client recovery and data integrity, while retaining transient errors."""

import json
import math
import os
import subprocess
import urllib.parse
import urllib.request
from pathlib import Path

from continuity import check_continuity

SCENARIOS = [
    "01-switchover",
    "02-takedown-synchronized",
    "03-takedown-primary",
    "04-failover-synchronized",
    "05-failover-primary",
]


def command(*args, check=True):
    return subprocess.run(
        args, text=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, check=check
    ).stdout


def json_lines(path):
    rows = []
    for line in path.read_text().splitlines():
        try:
            row = json.loads(line)
            if isinstance(row, dict):
                rows.append(row)
        except json.JSONDecodeError:
            continue
    return rows


def main():
    artifacts = Path("bridge-artifacts")
    artifacts.mkdir(exist_ok=True)
    result = {"errors": [], "action_outcome": os.environ.get("ACTION_OUTCOME")}
    for name in ("ydb-workload-current", "ydb-chaos-monkey"):
        (artifacts / f"{name}.log").write_text(
            command("docker", "logs", name, check=False)
        )
    try:
        if result["action_outcome"] != "success":
            result["errors"].append("Action did not complete successfully")
        chaos_info = json.loads(command("docker", "inspect", "ydb-chaos-monkey"))[0]
        chaos = chaos_info["State"]
        result["chaos"] = chaos
        log = (artifacts / "ydb-chaos-monkey.log").read_text()
        if (
            chaos["Status"] != "exited"
            or chaos["ExitCode"] != 0
            or chaos_info["RestartCount"]
            or "Chaos Monkey finished" not in log
        ):
            result["errors"].append(
                "Chaos sequence did not finish successfully during the workload"
            )
        for scenario in SCENARIOS:
            if f"Running scenario: {scenario}" not in log:
                result["errors"].append(f"Scenario not executed: {scenario}")
        ip = command(
            "docker",
            "inspect",
            "--format",
            "{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}",
            "ydb-prometheus",
        ).strip()

        def metric(expression):
            url = f"http://{ip}:9090/api/v1/query?" + urllib.parse.urlencode(
                {"query": expression}
            )
            with urllib.request.urlopen(url, timeout=15) as response:
                data = json.load(response)
            if data["status"] != "success":
                raise RuntimeError(data)
            return sum(float(item["value"][1]) for item in data["data"]["result"])

        observations = {}
        for operation in ("read", "write"):
            selector = f'sdk_operations_total{{ref="current",operation_type="{operation}",operation_status="success"}}'
            count = metric(f"sum({selector})")
            recent = metric(f"sum(increase({selector}[120s]))")
            observations[operation] = count
            observations[f"recent_{operation}"] = recent
            if (
                not math.isfinite(count)
                or not math.isfinite(recent)
                or count <= 0
                or recent <= 0
            ):
                result["errors"].append(
                    f"No recent successful native {operation} observations"
                )
        observations["errors"] = metric(
            'sum(sdk_operations_total{ref="current",operation_status="failure"})'
        )
        result["native_observations"] = observations
        probe_state = json.loads(command("docker", "inspect", "ydb-bridge-probe"))[0][
            "State"
        ]
        if probe_state["Running"]:
            command("docker", "kill", "--signal=USR1", "ydb-bridge-probe")
        command("docker", "wait", "ydb-bridge-probe")
        (artifacts / "ydb-bridge-probe.log").write_text(
            command("docker", "logs", "ydb-bridge-probe", check=False)
        )
        command(
            "docker",
            "cp",
            "ydb-bridge-probe:/tmp/bridge-probe.json",
            str(artifacts / "probe.json"),
        )
        probe = json.loads((artifacts / "probe.json").read_text())
        result["probe"] = probe
        if not probe.get("passed"):
            result["errors"].append("Independent data audit failed")
        continuity, errors = check_continuity(
            log,
            json_lines(artifacts / "ydb-bridge-probe.log"),
            json_lines(artifacts / "docker-events.jsonl"),
            probe["auditStartedAt"],
        )
        result["continuity"] = continuity
        result["errors"].extend(errors)
        nodes = json.loads(
            command(
                "docker",
                "inspect",
                "ydb-storage-1",
                "ydb-storage-2",
                "ydb-database-1",
                "ydb-database-2",
            )
        )
        result["node_states"] = {node["Name"]: node["State"] for node in nodes}
        result["node_restarts"] = {node["Name"]: node["RestartCount"] for node in nodes}
        if any(node["State"]["OOMKilled"] or node["RestartCount"] for node in nodes):
            result["errors"].append("YDB node was OOM killed or unexpectedly restarted")
    except Exception as error:
        result["errors"].append(repr(error))
    result["passed"] = not result["errors"]
    (artifacts / "result.json").write_text(json.dumps(result, indent=2))
    print(json.dumps(result, indent=2))
    return 0 if result["passed"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
