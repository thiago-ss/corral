#!/usr/bin/env python3
"""Regenerate the README benchmark figure from `go run ./cmd/bench` output.

The figure visualizes only the three measured benchmark results. It carries no
number that the benchmark does not produce, and the same numbers stay in the
README as a Markdown table so they remain searchable and screen-reader
accessible (see docs/brand.md).

Usage:
    go run ./cmd/bench | python3 scripts/make_bench_figure.py
    python3 scripts/make_bench_figure.py --self-test
"""

from __future__ import annotations

import argparse
import re
import sys
from pathlib import Path

OUT_DIR = Path(__file__).resolve().parent.parent / "docs" / "assets"

# docs/brand.md palette. Blue is work, amber is proof, green is accepted,
# red is a failed outcome. Nothing else may be recolored.
THEMES = {
    "light": dict(
        paper="#F3F0E8", ink="#191A16", muted="#66665E",
        blue="#2B63D9", amber="#D8890B", green="#1F7A50", red="#B8453F",
        grid="#DCD7C9",
    ),
    "dark": dict(
        paper="#141612", ink="#ECE8DC", muted="#A3A096",
        blue="#78A4FF", amber="#F2AA2A", green="#57C78B", red="#FF8178",
        grid="#2B2D27",
    ),
}

W, H = 900, 246
PANEL_W = 268
PANEL_X = [22, 316, 610]
PLOT_TOP = 78          # y of the top of the plot area
PLOT_H = 132           # height of the plot area
BASELINE = PLOT_TOP + PLOT_H


def parse_bench(text: str) -> dict:
    """Pull the three measured results out of cmd/bench stdout."""
    scaling = [
        (int(w), int(t))
        for w, t in re.findall(r"^\s{2}(\d+)\s+(\d+)\s+[\d.]+x\s+\d+%", text, re.M)
    ]
    naive = re.search(r"naive re-run:\s*(\d+)", text)
    resume = re.search(r"corral resume:\s*(\d+)", text)
    total = re.search(r"agents claiming done:\s*(\d+)", text)
    caught = re.search(r"failed their gate:\s*(\d+)", text)

    missing = [
        n for n, v in
        [("scaling sweep", scaling), ("naive re-run", naive),
         ("corral resume", resume), ("gate total", total), ("gate failures", caught)]
        if not v
    ]
    if missing:
        raise SystemExit(f"could not parse from bench output: {', '.join(missing)}")

    return {
        "scaling": scaling,
        "naive": int(naive.group(1)),
        "resume": int(resume.group(1)),
        "total": int(total.group(1)),
        "caught": int(caught.group(1)),
    }


def esc(s: str) -> str:
    return s.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")


def bar(x, y, w, h, fill, radius=3) -> str:
    return f'<rect x="{x:.1f}" y="{y:.1f}" width="{w:.1f}" height="{h:.1f}" rx="{radius}" fill="{fill}"/>'


def text(x, y, s, fill, size=13, weight="400", anchor="middle") -> str:
    return (
        f'<text x="{x:.1f}" y="{y:.1f}" fill="{fill}" font-size="{size}" '
        f'font-weight="{weight}" text-anchor="{anchor}" '
        f'font-family="-apple-system,BlinkMacSystemFont,Segoe UI,Helvetica,Arial,sans-serif">{esc(s)}</text>'
    )


def panel_header(px, title, subtitle, c) -> str:
    return (
        text(px, 30, title, c["ink"], size=15, weight="700", anchor="start")
        + text(px, 50, subtitle, c["muted"], size=11.5, anchor="start")
        + f'<line x1="{px}" y1="{BASELINE + 0.5}" x2="{px + PANEL_W}" y2="{BASELINE + 0.5}" stroke="{c["grid"]}" stroke-width="1"/>'
    )


def panel_scaling(px, data, c) -> str:
    """Wall time falling as workers are added."""
    pts = data["scaling"]
    peak = max(t for _, t in pts)
    out = [panel_header(px, "Parallelism", "8 tasks — wall time in ticks", c)]
    slot = PANEL_W / len(pts)
    bw = 34
    for i, (workers, ticks) in enumerate(pts):
        cx = px + slot * (i + 0.5)
        h = max(3.0, ticks / peak * PLOT_H)
        out.append(bar(cx - bw / 2, BASELINE - h, bw, h, c["blue"]))
        out.append(text(cx, BASELINE - h - 8, str(ticks), c["ink"], size=12.5, weight="700"))
        out.append(text(cx, BASELINE + 17, f"{workers}w", c["muted"], size=11.5))
    return "".join(out)


def panel_recovery(px, data, c) -> str:
    """Cost of a crash at 50%: full redo vs resume."""
    naive, resume = data["naive"], data["resume"]
    peak = max(naive, resume)
    out = [panel_header(px, "Crash recovery", "ticks to finish after a crash at 50%", c)]
    bw = 62
    for i, (label, val, fill) in enumerate(
        [("re-run", naive, c["muted"]), ("resume", resume, c["blue"])]
    ):
        cx = px + PANEL_W * (0.30 + 0.40 * i)
        h = max(3.0, val / peak * PLOT_H)
        out.append(bar(cx - bw / 2, BASELINE - h, bw, h, fill))
        out.append(text(cx, BASELINE - h - 8, str(val), c["ink"], size=12.5, weight="700"))
        out.append(text(cx, BASELINE + 17, label, c["muted"], size=11.5))
    return "".join(out)


def panel_gates(px, data, c) -> str:
    """Every agent claimed done; the gates disagreed with some of them."""
    total, caught = data["total"], data["caught"]
    out = [panel_header(px, "Evidence gates", f"all {total} agents reported success", c)]
    cols, cell, gap = 5, 34, 10
    grid_w = cols * cell + (cols - 1) * gap
    x0 = px + (PANEL_W - grid_w) / 2
    y0 = PLOT_TOP + 14
    for i in range(total):
        r, col = divmod(i, cols)
        x = x0 + col * (cell + gap)
        y = y0 + r * (cell + gap)
        rejected = i >= total - caught
        out.append(bar(x, y, cell, cell, c["red"] if rejected else c["green"], radius=4))
        if rejected:  # strike through the ones the gates threw out
            out.append(
                f'<path d="M{x + 9} {y + 9} L{x + cell - 9} {y + cell - 9} '
                f'M{x + cell - 9} {y + 9} L{x + 9} {y + cell - 9}" '
                f'stroke="{c["paper"]}" stroke-width="3" stroke-linecap="round"/>'
            )
    legend_y = y0 + 2 * (cell + gap) + 16
    out.append(text(px + PANEL_W / 2, legend_y,
                    f"{total - caught} passed · {caught} rejected by their gate",
                    c["muted"], size=11.5))
    return "".join(out)


def render(data: dict, theme: str) -> str:
    c = THEMES[theme]
    alt = (
        f"Three measured benchmark results. Left: wall time for 8 tasks falls from "
        f"{data['scaling'][0][1]} ticks at 1 worker to {data['scaling'][-1][1]} ticks at "
        f"{data['scaling'][-1][0]} workers. Middle: after a crash at 50 percent, a naive "
        f"re-run costs {data['naive']} ticks against {data['resume']} to resume. Right: all "
        f"{data['total']} agents reported success and the evidence gates rejected "
        f"{data['caught']} of them."
    )
    body = "".join([
        f'<rect width="{W}" height="{H}" fill="{c["paper"]}"/>',
        panel_scaling(PANEL_X[0], data, c),
        panel_recovery(PANEL_X[1], data, c),
        panel_gates(PANEL_X[2], data, c),
    ])
    return (
        f'<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 {W} {H}" width="{W}" '
        f'height="{H}" role="img" aria-label="{esc(alt)}">{body}</svg>\n'
    )


SELF_TEST_INPUT = """8 independent tasks — wall time in ticks
  single agent (concurrency 1): 81
  corral (concurrency 4):       21
  speedup: 3.9x

crash at 50% done — work re-executed (ticks)
  naive re-run: 81
  corral resume: 11
  saved: 7.4x

concurrency scaling — 8 independent tasks
  workers      ticks    speedup  efficiency
  1            81       1.0x     100%
  2            41       2.0x     99%
  4            21       3.9x     96%
  8            11       7.4x     92%

evidence gates vs trusting the prose
  agents claiming done: 10
  failed their gate: 3
  would have merged broken work (no gates): 3/10
"""


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--self-test", action="store_true",
                    help="parse a known bench output and assert the values")
    args = ap.parse_args()

    if args.self_test:
        d = parse_bench(SELF_TEST_INPUT)
        assert d["scaling"] == [(1, 81), (2, 41), (4, 21), (8, 11)], d["scaling"]
        assert (d["naive"], d["resume"]) == (81, 11), (d["naive"], d["resume"])
        assert (d["total"], d["caught"]) == (10, 3), (d["total"], d["caught"])
        print("self-test ok:", d)
        return

    raw = sys.stdin.read()
    if not raw.strip():
        raise SystemExit("no input; pipe `go run ./cmd/bench` into this script")
    data = parse_bench(raw)
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    for theme in THEMES:
        path = OUT_DIR / f"bench-{theme}.svg"
        path.write_text(render(data, theme))
        print(f"wrote {path.relative_to(OUT_DIR.parent.parent)} ({path.stat().st_size} B)")


if __name__ == "__main__":
    main()
