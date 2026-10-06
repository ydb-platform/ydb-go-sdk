#!/usr/bin/env python3
"""Render the committed measurements without third-party Python packages."""

from collections import defaultdict
from html import escape
import json
from pathlib import Path
from statistics import median


def main():
    directory = Path(__file__).resolve().parent / "results"
    cases = defaultdict(dict)
    for line in (directory / "linux-runs.jsonl").read_text().splitlines():
        run = json.loads(line)
        cases[run["nodes"], run["scenario"], run["rate"]][run["algorithm"], run["repeat"]] = run
    svg = ['<svg xmlns="http://www.w3.org/2000/svg" width="1200" height="780" viewBox="0 0 1200 780">',
           '<rect width="1200" height="780" fill="white"/>',
           '<style>text{font-family:Arial,sans-serif;fill:#243447;font-size:16px}</style>']

    def text(x, y, value, anchor="start", size=16):
        svg.append(f'<text x="{x}" y="{y}" text-anchor="{anchor}" style="font-size:{size}px">'
                   f'{escape(str(value))}</text>')

    def line(x1, y1, x2, y2, color="#dbe3eb", width=1):
        svg.append(f'<path d="M{x1},{y1} L{x2},{y2}" stroke="{color}" stroke-width="{width}"/>')

    def axis(x, y, width, height, low, high, ticks):
        def scale(value):
            return y + height * (high - value) / (high - low)
        for value in ticks:
            point = scale(value)
            line(x, point, x + width, point)
            text(x - 10, point + 5, f"{value:g}", "end", 14)
        line(x, y, x, y + height, "#8091a3")
        return scale

    def values(key, algorithm, field):
        runs = cases[key]
        return [runs[algorithm, i][field] for i in range(5)]

    def bar(x, baseline, scale, data, color):
        center, minimum, maximum = median(data), min(data), max(data)
        svg.append(f'<rect x="{x-18}" y="{scale(center)}" width="36" '
                   f'height="{baseline-scale(center)}" fill="{color}"/>')
        line(x, scale(minimum), x, scale(maximum), "#243447", 2)
        for value in (minimum, maximum):
            line(x - 6, scale(value), x + 6, scale(value), "#243447", 2)
        text(x, scale(maximum) - 8, f"{center:.1f}", "middle", 14)

    text(600, 30, "P2C: less queuing on slow nodes; measurable selection overhead", "middle", 23)
    text(600, 56, "Linux/arm64, 5 alternating repeats of 4 s. Whiskers = min/max, not confidence intervals.", "middle", 15)
    for x, color, label in ((420, "#697a8c", "Random"), (580, "#197f85", "P2C")):
        svg.append(f'<rect x="{x}" y="72" width="18" height="16" fill="{color}"/>')
        text(x + 27, 86, label)

    text(315, 126, "Slow nodes, lower load: successful RPC p95 (ms)", "middle", 18)
    scale = axis(80, 160, 450, 190, 0, 100, (0, 25, 50, 75, 100))
    for x, nodes, rate in ((205, 3, 240), (405, 9, 720)):
        key = (nodes, "slow", rate)
        bar(x - 24, 350, scale, values(key, "random", "p95Ms"), "#697a8c")
        bar(x + 24, 350, scale, values(key, "p2c", "p95Ms"), "#197f85")
        text(x, 377, f"{nodes} nodes / {rate} RPC/s", "middle", 15)
    text(315, 406, "No errors in either revision", "middle", 14)

    text(910, 126, "Slow nodes, higher load: errors per 4 s", "middle", 18)
    scale = axis(675, 160, 450, 190, 0, 600, (0, 200, 400, 600))
    for x, nodes, rate in ((800, 3, 750), (1000, 9, 2250)):
        key = (nodes, "slow", rate)
        bar(x - 24, 350, scale, values(key, "random", "errors"), "#697a8c")
        bar(x + 24, 350, scale, values(key, "p2c", "errors"), "#197f85")
        text(x, 377, f"{nodes} nodes / {rate} RPC/s", "middle", 15)
    text(910, 406, "P2C: zero errors in all five repeats", "middle", 14)

    text(600, 458, "Homogeneous load: client CPU ratio (P2C / random)", "middle", 18)
    scale = axis(85, 490, 1030, 190, 0.4, 1.8, (0.4, 0.6, 0.8, 1, 1.2, 1.4, 1.6, 1.8))
    line(85, scale(1), 1115, scale(1), "#8091a3", 2)
    line(85, scale(1.05), 1115, scale(1.05), "#bd3c4e", 2)
    equal = [key for key in cases if key[1] == "equal"]
    for index, key in enumerate(equal):
        x = 135 + index * 150
        data = [b / a for a, b in zip(values(key, "random", "clientCpuSeconds"),
                                     values(key, "p2c", "clientCpuSeconds"))]
        low, high, middle = min(data), max(data), median(data)
        line(x, scale(low), x, scale(high), "#197f85", 2)
        for value in (low, high):
            line(x - 7, scale(value), x + 7, scale(value), "#197f85", 2)
        svg.append(f'<circle cx="{x}" cy="{scale(middle)}" r="5" fill="#197f85"/>')
        text(x, 707, f"{key[0]} nodes", "middle", 14)
        text(x, 729, f"{key[2]} RPC/s", "middle", 14)
    text(600, 767, "Dots = median paired ratio. Red line = +5% budget; individual runs can exceed it.", "middle", 15)
    svg.append("</svg>")
    (directory / "summary.svg").write_text("\n".join(svg) + "\n")


if __name__ == "__main__":
    main()
