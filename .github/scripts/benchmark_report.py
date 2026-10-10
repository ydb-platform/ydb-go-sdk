"""Build a review-oriented PR comment from benchstat CSV output.

The privileged workflow runs this file from the trusted default branch.
Benchmark artifacts are read only as data.
"""

import argparse
import csv
import html
import math
import re
from collections import defaultdict
from dataclasses import dataclass
from io import StringIO
from pathlib import Path


MODULE = "github.com/ydb-platform/ydb-go-sdk/v3"
MARKER = "<!-- benchmark-report -->"
CHANGE = re.compile(r"^[+-]\d+(?:\.\d+)?%$")
P_VALUE = re.compile(r"^p=(\d+(?:\.\d+)?) n=(\d+)$")
METRIC_ORDER = {"sec/op": 0, "sec/op(p50)": 1, "sec/op(p99)": 2, "B/op": 3, "allocs/op": 4}


@dataclass(frozen=True)
class Measurement:
    package: str
    name: str
    metric: str
    master: float | None
    master_ci: str
    pr: float | None
    pr_ci: str
    change: str
    p: str
    n: int | None

    @property
    def direction(self):
        if self.master is None or self.pr is None or self.change in ("", "?"):
            return "incomplete"
        if self.change == "~":
            return "unchanged"
        if self.metric not in METRIC_ORDER:
            return "unknown"
        return "regression" if self.change.startswith("+") else "improvement"


def parse_benchstat_csv(source):
    """Parse the CSV table columns emitted by the pinned benchstat CLI."""
    package = None
    metric = None
    seen_tables = 0
    measurements = []
    keys = set()

    for line_number, row in enumerate(csv.reader(StringIO(source)), start=1):
        if not row or not any(field.strip() for field in row):
            continue
        if len(row) == 1:
            if row[0].startswith("pkg: "):
                package = row[0][5:].strip()
                metric = None
            continue
        if row[0] == "" and len(row) >= 7 and row[1] == "master" and row[3] == "pr":
            metric = None
            continue
        if row[0] == "" and len(row) >= 7 and row[2] == "CI" and row[4] == "CI":
            if row[1] != row[3] or row[5:7] != ["vs base", "P"] or not package:
                raise ValueError(f"Invalid benchstat header on line {line_number}")
            metric = row[1]
            seen_tables += 1
            continue
        if not metric or row[0] == "geomean":
            continue
        row += [""] * max(0, 7 - len(row))

        change = row[5].strip()
        if change not in ("~", "", "?") and not CHANGE.fullmatch(change):
            raise ValueError(f"Invalid benchstat change on line {line_number}: {change!r}")
        p_text = row[6].strip()
        match = P_VALUE.fullmatch(p_text) if p_text else None
        if change not in ("", "?") and not match:
            raise ValueError(f"Missing benchstat p-value on line {line_number}")
        key = package, row[0], metric
        if key in keys:
            raise ValueError(f"Duplicate benchmark metric on line {line_number}: {key!r}")
        keys.add(key)
        measurements.append(
            Measurement(
                package=package,
                name=row[0],
                metric=metric,
                master=_number(row[1], line_number),
                master_ci=row[2].strip(),
                pr=_number(row[3], line_number),
                pr_ci=row[4].strip(),
                change=change,
                p=match.group(1) if match else "",
                n=int(match.group(2)) if match else None,
            )
        )

    if not seen_tables or not measurements:
        raise ValueError("No benchmark comparisons found in benchstat CSV")
    return measurements


def _number(value, line_number):
    if value in ("", "?"):
        return None
    try:
        number = float(value)
    except ValueError as error:
        raise ValueError(f"Invalid benchmark median on line {line_number}: {value!r}") from error
    if not math.isfinite(number) or number < 0:
        raise ValueError(f"Invalid benchmark median on line {line_number}: {value!r}")
    return number


def _package_name(package):
    if package == MODULE:
        return "SDK root package"
    if package.startswith(MODULE + "/"):
        return package[len(MODULE) + 1 :]
    return package


def _metric_phrase(measurement):
    metric = measurement.metric
    if measurement.direction == "incomplete":
        return f"not comparable ({metric})"
    if measurement.direction == "unknown":
        return f"changed ({measurement.change} {metric}; direction unknown)"
    phrases = {
        "sec/op": ("slower", "faster"),
        "sec/op(p50)": ("higher p50 latency", "lower p50 latency"),
        "sec/op(p99)": ("higher p99 latency", "lower p99 latency"),
        "B/op": ("allocates more memory", "allocates less memory"),
        "allocs/op": ("performs more allocations", "performs fewer allocations"),
    }
    regression, improvement = phrases[metric]
    word = regression if measurement.direction == "regression" else improvement
    return f"{word} ({measurement.change} {metric})"


def _format_median(value, ci, metric):
    if value is None:
        return "—"
    if metric.startswith("sec/op"):
        for scale, unit in ((1, "s/op"), (1e-3, "ms/op"), (1e-6, "µs/op"), (1e-9, "ns/op")):
            if value >= scale or scale == 1e-9:
                rendered = f"{value / scale:.4g} {unit}"
                break
    elif metric == "B/op":
        for scale, unit in ((1024**3, "GiB/op"), (1024**2, "MiB/op"), (1024, "KiB/op"), (1, "B/op")):
            if value >= scale or scale == 1:
                rendered = f"{value / scale:.4g} {unit}"
                break
    elif metric == "allocs/op":
        rendered = f"{value:.4g} allocs/op"
    else:
        rendered = f"{value:.4g} {metric}"
    if ci and ci != "?":
        rendered += f" ±{ci}"
    return html.escape(rendered)


def _p_value(measurement):
    if not measurement.p:
        return "—"
    return "p &lt; 0.001" if measurement.p == "0.000" else f"p = {measurement.p}"


def _card_status(measurements):
    directions = {measurement.direction for measurement in measurements}
    if "regression" in directions:
        return "regression"
    if "incomplete" in directions or "unknown" in directions:
        return "incomplete"
    if "improvement" in directions:
        return "improvement"
    return "unchanged"


def _cards(measurements):
    groups = defaultdict(list)
    for measurement in measurements:
        groups[measurement.package, measurement.name].append(measurement)
    return {key: sorted(value, key=lambda item: (METRIC_ORDER.get(item.metric, 99), item.metric))
            for key, value in groups.items()}


def _time_trend(measurements):
    ratios = [math.log(row.pr / row.master) for row in measurements
              if row.metric == "sec/op" and row.master and row.pr]
    if not ratios:
        return "—"
    change = 100 * math.expm1(math.fsum(ratios) / len(ratios))
    return f"{change:+.1f}%"


def _scope_counts(cards):
    counts = defaultdict(int)
    for measurements in cards.values():
        counts[_card_status(measurements)] += 1
    return counts


def _card_priority(measurements):
    rank = {"regression": 0, "incomplete": 1, "improvement": 2, "unchanged": 3}
    changes = [abs(float(item.change[:-1])) for item in measurements if CHANGE.fullmatch(item.change)]
    return rank[_card_status(measurements)], -max(changes, default=0)


def _card_lines(name, measurements):
    changed = [row for row in measurements if row.direction in ("regression", "improvement")]
    status = _card_status(changed)
    directions = {row.direction for row in changed}
    icon = "🔴🟢" if "regression" in directions and "improvement" in directions else {
        "regression": "🔴", "improvement": "🟢"
    }[status]
    summary = "; ".join(_metric_phrase(row) for row in changed)
    lines = [
        "<details>",
        f"<summary>{icon} <code>{html.escape(name)}</code> — {html.escape(summary)}</summary>",
        "",
        "| Metric | Master median (95% CI) | PR median (95% CI) | Change | Comparison |",
        "|:--|--:|--:|--:|--:|",
    ]
    for row in changed:
        comparison = _p_value(row)
        if row.n:
            comparison += f"; n={row.n} each"
        lines.append(
            f"| {html.escape(row.metric)} | {_format_median(row.master, row.master_ci, row.metric)} "
            f"| {_format_median(row.pr, row.pr_ci, row.metric)} | {html.escape(row.change or '—')} | {comparison} |"
        )
    return lines + ["", "</details>", ""]


def _package_lines(measurements, max_cards=None):
    cards = _cards(measurements)
    visible = [(key, value) for key, value in cards.items()
               if _card_status(value) in ("regression", "improvement")]
    packages = defaultdict(list)
    for key, value in visible:
        packages[key[0]].append((key[1], value))
    ordered_packages = sorted(
        packages,
        key=lambda package: (min(_card_priority(card) for _, card in packages[package]), package),
    )
    lines = ["### Changed benchmarks by package", ""]
    shown = 0
    for package in ordered_packages:
        package_cards = sorted(packages[package], key=lambda item: (*_card_priority(item[1]), item[0]))
        selected = package_cards if max_cards is None else package_cards[:max(0, max_cards - shown)]
        if not selected:
            break
        counts = _scope_counts({name: rows for name, rows in package_cards})
        labels = []
        for direction, icon, singular in (("regression", "🔴", "regression"),
                                          ("improvement", "🟢", "improvement")):
            if counts[direction]:
                n = counts[direction]
                labels.append(f"{icon} {n} {singular}{'s' if n != 1 else ''}")
        lines += [f"#### <code>{html.escape(_package_name(package))}</code> — {' · '.join(labels)}", ""]
        for name, rows in selected:
            lines += _card_lines(name, rows)
            shown += 1
    omitted = len(visible) - shown
    if omitted:
        lines += [f"{omitted} additional changed benchmark(s) are available in the full artifact.", ""]
    if not visible:
        lines += ["No individual changes were reported by benchstat.", ""]
    return lines


def render_report(csv_source, *, artifact_url, preview_url, master_sha, head_sha, max_chars=55000):
    """Render one package-grouped comment with an overall review signal."""
    artifact_pattern = r"https://github\.com/[^/]+/[^/]+/actions/runs/\d+/artifacts/\d+"
    if not all(re.fullmatch(artifact_pattern, url) for url in (artifact_url, preview_url)):
        raise ValueError("Invalid GitHub artifact URL")
    if not all(re.fullmatch(r"[0-9a-f]{40}", sha) for sha in (master_sha, head_sha)):
        raise ValueError("Invalid benchmark revision")
    measurements = parse_benchstat_csv(csv_source)
    totals = _scope_counts(_cards(measurements))
    if totals["regression"]:
        outcome = "🔴 Performance regressions reported"
    elif totals["incomplete"]:
        outcome = "🟡 Comparison incomplete"
    elif totals["improvement"]:
        outcome = "🟢 Improvements reported; no regressions detected"
    else:
        outcome = "⚪ No performance change detected"

    footer = [
        "The time trend is the geometric mean of per-benchmark median `sec/op` ratios, including rows marked `~`. "
        "Each matched benchmark has equal weight. It is descriptive, has no confidence interval, and can hide individual regressions.",
        "Percentages in each benchmark card describe changes in the stated metric. "
        "Benchstat reports individual changes using unadjusted p-values; `~` means no change was detected, not proof of equality. "
        "The 95% confidence intervals in the details describe each median, not the percentage change.",
        "",
        f"[Open full benchstat in browser]({preview_url}) · "
        f"[Download full benchstat and raw results]({artifact_url})",
    ]

    def build(max_cards):
        return "\n".join([
            MARKER,
            f"## Benchmark review: {outcome}",
            "",
            f"**{totals['regression']} benchmarks with regressions · "
            f"{totals['improvement']} with improvements only · "
            f"{totals['incomplete']} not comparable · "
            f"{totals['unchanged']} with no detected change.**",
            "",
            f"**Time trend across all packages:** {_time_trend(measurements)}",
            "",
            f"Master: `{master_sha[:12]}` · PR: `{head_sha[:12]}`",
            "",
            *_package_lines(measurements, max_cards=max_cards),
            *footer,
        ])

    report = build(None)
    if len(report) > max_chars:
        report = build(25)
    if len(report) > max_chars:
        report = build(0)
    if len(report) > max_chars:
        raise ValueError("Benchmark summary exceeds GitHub comment limit")
    return report


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--csv", type=Path, required=True)
    parser.add_argument("--artifact-url", required=True)
    parser.add_argument("--preview-url", required=True)
    parser.add_argument("--master-sha", required=True)
    parser.add_argument("--head-sha", required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    report = render_report(
        args.csv.read_text(),
        artifact_url=args.artifact_url,
        preview_url=args.preview_url,
        master_sha=args.master_sha,
        head_sha=args.head_sha,
    )
    args.output.write_text(report + "\n")


if __name__ == "__main__":
    main()
