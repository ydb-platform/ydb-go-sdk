"""Verify progress through unavailable-pile intervals and after every recovery."""

import datetime
import re


def timestamp(value):
    return datetime.datetime.fromisoformat(value.replace("Z", "+00:00")).timestamp()


def check_continuity(chaos_log, operations, events, audit_started):
    successes = [
        timestamp(row["ts"])
        for row in operations
        if row.get("event") == "operation" and row.get("ok")
    ]
    recovered = re.findall(r"\[([^\]]+)\] Recovered: ([^\n]+)", chaos_log)
    details = {"recovery_windows": [], "unavailable_intervals": []}
    errors = []
    if len(recovered) != 5:
        errors.append(f"Expected five completed recoveries, found {len(recovered)}")
    for value, name in recovered:
        start = timestamp(value)
        count = sum(start <= value <= start + 60 for value in successes)
        details["recovery_windows"].append({"name": name, "successes": count})
        if count < 3:
            errors.append(f"No sustained progress after recovery: {name}")
    stopped = {}
    for event in events:
        name = event.get("Actor", {}).get("Attributes", {}).get("name", "")
        if name not in ("ydb-database-1", "ydb-database-2"):
            continue
        time = event["timeNano"] / 1e9 if event.get("timeNano") else event["time"]
        action = event.get("Action") or event.get("status")
        if action == "stop":
            stopped[name] = time
        elif action == "start" and name in stopped:
            start = stopped.pop(name)
            if time - start < 10:
                continue
            # Exclude responses already in flight when the node was stopped.
            count = sum(start + 2 <= value < time for value in successes)
            details["unavailable_intervals"].append(
                {"node": name, "seconds": time - start, "successes": count}
            )
            if count < 3:
                errors.append(f"No sustained progress while {name} was unavailable")
    if len(details["unavailable_intervals"]) < 4:
        errors.append(
            "Missing unavailable-pile intervals for the four takedown/failover scenarios"
        )
    end = timestamp(audit_started)
    details["final_successes"] = sum(end - 60 <= value <= end for value in successes)
    if details["final_successes"] < 3:
        errors.append("Client did not keep making progress at the end of the run")
    return details, errors
