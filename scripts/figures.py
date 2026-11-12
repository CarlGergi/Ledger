#!/usr/bin/env python3
"""Draws the README's figures from the benchmarks' raw results, and prints their tables.

    python3 scripts/figures.py            # reads results/, writes docs/images/*.svg

Standard library only. Every figure uses the same fonts, colours and white background,
and carries no title: the README's caption says what it shows. Intervals are 95%:
t-intervals for means over seeds, bootstrap (2,000 resamples, fixed seed) for
percentiles, and order statistics for medians.
"""

import json
import math
import random
import shutil
import statistics
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
RESULTS = ROOT / "results"
IMAGES = ROOT / "docs" / "images"

FONT = "-apple-system, 'Segoe UI', Helvetica, Arial, sans-serif"
INK = "#1f2328"
MUTED = "#59636e"
GRID = "#e6e8eb"
DIRECT = "#9aa4af"  # without Ledger
LEDGER = "#1f6feb"  # with Ledger
LEDGER_2 = "#8cb4f5"  # a second Ledger series


# Statistics.


def mean_ci(values, resamples=2000, seed=1):
    """Mean, with a bootstrap interval: counts are skewed and never negative, which a
    t-interval over ten seeds would not respect."""
    m = statistics.fmean(values)
    if len(values) < 2:
        return m, m, m
    rng = random.Random(seed)
    means = sorted(statistics.fmean(rng.choices(values, k=len(values))) for _ in range(resamples))
    return m, means[int(0.025 * resamples)], means[int(0.975 * resamples) - 1]


def percentile(sorted_values, p):
    """Nearest rank, as the benchmarks report."""
    rank = max(1, math.ceil(p / 100 * len(sorted_values)))
    return sorted_values[min(rank, len(sorted_values)) - 1]


def bootstrap_ci(values, p, resamples=2000, seed=1):
    rng = random.Random(seed)
    n = len(values)
    estimates = sorted(
        percentile(sorted(rng.choices(values, k=n)), p) for _ in range(resamples)
    )
    return (percentile(sorted(values), p), estimates[int(0.025 * resamples)],
            estimates[int(0.975 * resamples) - 1])


def median_ci(values):
    """Median, with the order-statistic interval that covers it at least 95% of the time."""
    xs = sorted(values)
    n = len(xs)
    med = statistics.median(xs)
    # Largest k with P(Binomial(n, 1/2) < k) <= 2.5%: then (x_k, x_{n+1-k}) covers >= 95%.
    k, below = 0, 0.0
    while True:
        below_next = below + math.comb(n, k) / 2**n
        if below_next > 0.025:
            break
        below, k = below_next, k + 1
    if k == 0:
        return med, xs[0], xs[-1]
    return med, xs[k - 1], xs[n - k]


# Drawing.


def nice_ticks(top):
    if top <= 0:
        return [0, 1]
    step = 10 ** math.floor(math.log10(top))
    for m in (1, 2, 2.5, 5, 10):
        if top / (step * m) <= 5:
            step *= m
            break
    ticks, t = [], 0.0
    while t < top + step * 0.999:
        ticks.append(round(t, 10))
        t += step
    return ticks


def fmt(v):
    return f"{v:g}" if abs(v) >= 1 or v == 0 else f"{v:.2g}"


class Svg:
    def __init__(self, width, height):
        self.w, self.h, self.parts = width, height, []

    def add(self, s):
        self.parts.append(s)

    def text(self, x, y, s, size=13, anchor="start", color=INK, weight="normal", rotate=None):
        tr = f' transform="rotate({rotate} {x} {y})"' if rotate else ""
        s = s.replace("&", "&amp;").replace("<", "&lt;")
        self.add(f'<text x="{x:.1f}" y="{y:.1f}" font-size="{size}" text-anchor="{anchor}" '
                 f'fill="{color}" font-weight="{weight}"{tr}>{s}</text>')

    def rect(self, x, y, w, h, fill):
        self.add(f'<rect x="{x:.1f}" y="{y:.1f}" width="{max(w, 0):.1f}" '
                 f'height="{max(h, 0):.1f}" fill="{fill}"/>')

    def line(self, x1, y1, x2, y2, color=INK, width=1):
        self.add(f'<line x1="{x1:.1f}" y1="{y1:.1f}" x2="{x2:.1f}" y2="{y2:.1f}" '
                 f'stroke="{color}" stroke-width="{width}"/>')

    def save(self, path):
        body = "\n".join(self.parts)
        path.write_text(
            f'<svg xmlns="http://www.w3.org/2000/svg" width="{self.w}" height="{self.h}" '
            f'viewBox="0 0 {self.w} {self.h}" font-family="{FONT}">\n'
            f'<rect width="100%" height="100%" fill="#ffffff"/>\n{body}\n</svg>\n'
        )


def grouped_bars(path, groups, series, ylabel, log=False, width=760, height=380, note=None):
    """groups: [label]; series: [(name, colour, [(value, low, high) or None per group])]."""
    svg = Svg(width, height)
    left, right, top, bottom = 72, 24, 62, 64
    plot_w, plot_h = width - left - right, height - top - bottom
    highs = [v[2] for _, _, vals in series for v in vals if v]
    ymax = max(highs) * 1.12 if highs else 1
    ticks = nice_ticks(ymax)
    ymax = ticks[-1]

    def y(v):
        return top + plot_h - (v / ymax) * plot_h

    for t in ticks:
        svg.line(left, y(t), left + plot_w, y(t), GRID)
        svg.text(left - 10, y(t) + 4, fmt(t), 12, "end", MUTED)
    svg.text(18, top + plot_h / 2, ylabel, 13, "middle", MUTED, rotate=-90)
    svg.line(left, top + plot_h, left + plot_w, top + plot_h, INK)

    slot = plot_w / len(groups)
    bar = min(46, slot * 0.8 / len(series))
    for g, label in enumerate(groups):
        cx = left + slot * (g + 0.5)
        start = cx - bar * len(series) / 2
        for s, (name, colour, vals) in enumerate(series):
            v = vals[g]
            if v is None:
                continue
            x = start + s * bar
            value, low, high = v
            # At least a line, so that a zero still shows whose it is.
            svg.rect(x + 2, min(y(value), top + plot_h - 2.5), bar - 4,
                     max(top + plot_h - y(value), 2.5), colour)
            if high > low:
                mid = x + bar / 2
                svg.line(mid, y(high), mid, y(low), INK, 1.2)
                svg.line(mid - 5, y(high), mid + 5, y(high), INK, 1.2)
                svg.line(mid - 5, y(low), mid + 5, y(low), INK, 1.2)
            svg.text(x + bar / 2, y(high) - 6, fmt(round(value, 2)), 11, "middle", INK)
        for i, part in enumerate(label.split("\n")):
            svg.text(cx, top + plot_h + 20 + 15 * i, part, 12.5, "middle", INK)

    # Legend, top left.
    lx = left
    for name, colour, _ in series:
        svg.rect(lx, 16, 12, 12, colour)
        svg.text(lx + 18, 26, name, 12.5, "start", INK)
        lx += 30 + 7.2 * len(name)
    if note:
        svg.text(left, 48, note, 11.5, "start", MUTED)
    svg.save(path)


# The figures.


def load_jsonl(path):
    return [json.loads(line) for line in path.read_text().splitlines() if line.strip()]


def duplicates(out):
    runs = load_jsonl(RESULTS / "duplicates.jsonl")
    if not runs:
        sys.exit("results/duplicates.jsonl is empty: run make bench-duplicates")
    losses = sorted({r["loss"] for r in runs})
    arrangements = ["direct", "ledger, answer lost to the agent", "ledger, answer lost from the tool"]
    names = {"direct": "Straight to the tool",
             "ledger, answer lost to the agent": "Ledger, answer lost on the way to the agent",
             "ledger, answer lost from the tool": "Ledger, answer lost on the way from the tool"}
    colours = [DIRECT, LEDGER, LEDGER_2]
    seeds = len({r["seed"] for r in runs})
    rows = []
    for tool in ("email (keyed)", "append (no key)"):
        for loss in losses:
            for a in arrangements:
                pick = [r for r in runs if r["tool"] == tool and r["loss"] == loss and r["arrangement"] == a]
                rows.append((tool, loss, a, {k: mean_ci([p[k] for p in pick])
                                             for k in ("duplicates", "flagged", "gave_up", "answers_lost")}))

    def stat(tool, loss, a, key):
        return next(s[key] for t, l, x, s in rows if (t, l, x) == (tool, loss, a))

    groups = [f"Keyed email tool\n{loss:.0%} of answers lost" for loss in losses] + \
             [f"Append tool, no key\n{loss:.0%} of answers lost" for loss in losses]
    series = []
    for a, colour in zip(arrangements, colours):
        vals = [stat("email (keyed)", loss, a, "duplicates") for loss in losses] + \
               [stat("append (no key)", loss, a, "duplicates") for loss in losses]
        series.append((names[a], colour, vals))
    grouped_bars(IMAGES / "duplicates.svg", groups, series,
                 "Duplicate side effects per 1,000 calls", width=920,
                 note=f"mean of {seeds} seeds, 95% CI")

    print(f"### Duplicates: 1,000 calls per run, mean of {seeds} seeds [95% CI]\n")
    print("| Tool | Answers lost | Arrangement | Duplicates | Flagged in doubt | Gave up |")
    print("| --- | --: | --- | --: | --: | --: |")
    for tool, loss, a, s in rows:
        cell = lambda k: f"{s[k][0]:.1f} [{s[k][1]:.1f}, {s[k][2]:.1f}]"
        print(f"| {tool} | {loss:.0%} | {names[a]} | {cell('duplicates')} | {cell('flagged')} | {cell('gave_up')} |")
    print()


def overhead(out):
    path = RESULTS / "overhead.json"
    if not path.exists():
        print("(no overhead results yet: make bench-overhead on a quiet machine)\n")
        return
    r = json.loads(path.read_text())
    ps = (50, 90, 99)
    direct = [bootstrap_ci(r["direct_ms"], p) for p in ps]
    ledger = [bootstrap_ci(r["ledger_ms"], p) for p in ps]
    grouped_bars(IMAGES / "overhead.svg", [f"p{p}" for p in ps],
                 [("Straight to the tool", DIRECT, direct), ("Through Ledger", LEDGER, ledger)],
                 "Latency per call (ms)", width=620,
                 note=f"{len(r['ledger_ms']):,} calls each, 95% bootstrap CI")
    print(f"### Overhead: {len(r['ledger_ms']):,} sequential calls each\n")
    print(f"Machine at start: {r['machine_before']}  \nMachine at end: {r['machine_after']}\n")
    print("| | p50 ms [95% CI] | p90 ms [95% CI] | p99 ms [95% CI] | calls/s, 16 clients |")
    print("| --- | --: | --: | --: | --: |")
    for name, v, rate in (("Straight to the tool", direct, r["direct_per_second"]),
                          ("Through Ledger", ledger, r["ledger_per_second"])):
        cells = " | ".join(f"{m:.2f} [{lo:.2f}, {hi:.2f}]" for m, lo, hi in v)
        print(f"| {name} | {cells} | {rate:,.0f} |")
    print(f"\nThrough Ledger, median of a session's first 100 calls: "
          f"{r['ledger_early_median_ms']:.2f} ms; of its last 100: {r['ledger_late_median_ms']:.2f} ms\n")


def takeover(out):
    path = RESULTS / "takeover.json"
    if not path.exists():
        print("(no takeover results yet: make bench-takeover)\n")
        return
    r = json.loads(path.read_text())
    leases = sorted({t["lease_secs"] for t in r["trials"]})
    stats = {}
    for stop in ("kill", "term"):
        for lease in leases:
            xs = [t["after_exit_s"] for t in r["trials"] if t["stop"] == stop and t["lease_secs"] == lease]
            stats[stop, lease] = (median_ci(xs), len(xs))
    grouped_bars(IMAGES / "takeover.svg", [f"lease_secs = {l}" for l in leases],
                 [("Copy killed (SIGKILL)", DIRECT, [stats["kill", l][0] for l in leases]),
                  ("Copy stopped (SIGTERM)", LEDGER, [stats["term", l][0] for l in leases])],
                 "Seconds until another copy answers", width=620,
                 note="median, 95% CI")
    n = stats["kill", leases[0]][1]
    print(f"### Takeover: {n} stops of each kind per lease length\n")
    print(f"Machine: {r['machine']}; the next copy tried every {r['poll_ms']} ms\n")
    print("| lease_secs | Killed: median s [95% CI] | Stopped: median ms [95% CI] |")
    print("| --: | --: | --: |")
    for lease in leases:
        (km, kl, kh), _ = stats["kill", lease]
        (tm, tl, th), _ = stats["term", lease]
        print(f"| {lease} | {km:.3f} [{kl:.3f}, {kh:.3f}] | {tm*1000:.0f} [{tl*1000:.0f}, {th*1000:.0f}] |")
    print()


def screenshots():
    for source, target in ((RESULTS / "email-demo" / "mailpit-inbox.png", "mailpit-inbox.png"),
                           (RESULTS / "azure" / "insights.png", "azure-insights.png")):
        if source.exists():
            shutil.copyfile(source, IMAGES / target)


def main():
    IMAGES.mkdir(parents=True, exist_ok=True)
    if not (RESULTS / "duplicates.jsonl").exists():
        sys.exit("no results/duplicates.jsonl: run make bench-duplicates first")
    duplicates(sys.stdout)
    overhead(sys.stdout)
    takeover(sys.stdout)
    screenshots()


if __name__ == "__main__":
    main()
