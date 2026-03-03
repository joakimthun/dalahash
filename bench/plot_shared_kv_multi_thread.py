#!/usr/bin/env python3

import argparse
import html
import json
import math
import pathlib
import sys

PALETTE = (
    "#0f766e",
    "#2563eb",
    "#dc2626",
    "#ca8a04",
    "#7c3aed",
    "#0891b2",
    "#ea580c",
    "#4f46e5",
    "#059669",
    "#be123c",
    "#1d4ed8",
)


def fail(message: str) -> int:
    print(f"error: {message}", file=sys.stderr)
    return 1


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Render an SVG graph from shared_kv_multi_thread_bench Google Benchmark JSON output."
    )
    parser.add_argument("input_json", type=pathlib.Path, help="Path to --benchmark_out JSON file.")
    parser.add_argument(
        "output_svg",
        nargs="?",
        type=pathlib.Path,
        help="Output SVG path. Defaults to the input path with a .svg suffix.",
    )
    parser.add_argument(
        "--title",
        default="Shared KV Multi-Thread Throughput",
        help="Chart title.",
    )
    parser.add_argument(
        "--workload",
        action="append",
        default=[],
        help="Only include a specific workload (repeatable).",
    )
    parser.add_argument(
        "--dataset",
        action="append",
        default=[],
        help="Only include a specific dataset tuple like 4096/16/64 (repeatable).",
    )
    return parser.parse_args()


def dataset_sort_key(dataset: str):
    parts = dataset.split("/")
    if len(parts) != 3:
        return (sys.maxsize, dataset)
    try:
        return tuple(int(part) for part in parts)
    except ValueError:
        return (sys.maxsize, dataset)


def parse_series_name(name: str):
    parts = name.split("/")
    if len(parts) < 6:
        return None
    if parts[0] != "SharedKvMultiFixture":
        return None
    if len(parts) < 5:
        return None
    workload = parts[1]
    dataset = "/".join(parts[2:5])
    return workload, dataset


def collect_panels(payload, workloads_filter, datasets_filter):
    grouped = {}
    benchmarks = payload.get("benchmarks")
    if not isinstance(benchmarks, list):
        raise ValueError("JSON payload is missing a benchmarks array")

    allowed_workloads = set(workloads_filter) if workloads_filter else None
    allowed_datasets = set(datasets_filter) if datasets_filter else None

    for run in benchmarks:
        if not isinstance(run, dict):
            continue
        if run.get("run_type") != "iteration":
            continue
        name = run.get("name")
        items_per_second = run.get("items_per_second")
        threads = run.get("threads")
        if not isinstance(name, str):
            continue
        if not isinstance(items_per_second, (int, float)):
            continue
        if not isinstance(threads, int):
            continue
        parsed = parse_series_name(name)
        if parsed is None:
            continue
        workload, dataset = parsed
        if allowed_workloads is not None and workload not in allowed_workloads:
            continue
        if allowed_datasets is not None and dataset not in allowed_datasets:
            continue
        workload_entry = grouped.setdefault(workload, {})
        dataset_entry = workload_entry.setdefault(dataset, {})
        thread_samples = dataset_entry.setdefault(threads, [])
        thread_samples.append(float(items_per_second))

    panels = []
    for workload, series_by_dataset in grouped.items():
        series_list = []
        for dataset in sorted(series_by_dataset, key=dataset_sort_key):
            points = []
            for threads in sorted(series_by_dataset[dataset]):
                samples = series_by_dataset[dataset][threads]
                points.append((threads, sum(samples) / len(samples)))
            if points:
                series_list.append({"dataset": dataset, "points": points})
        if series_list:
            panels.append({"workload": workload, "series": series_list})
    return panels


def choose_rate_unit(max_rate: float):
    for divisor, suffix in ((1_000_000_000.0, "G"), (1_000_000.0, "M"), (1_000.0, "K")):
        if max_rate >= divisor:
            return divisor, suffix
    return 1.0, ""


def format_rate(value: float, divisor: float) -> str:
    scaled = value / divisor
    if scaled >= 100.0:
        text = f"{scaled:.0f}"
    elif scaled >= 10.0:
        text = f"{scaled:.1f}"
    else:
        text = f"{scaled:.2f}"
    return text.rstrip("0").rstrip(".")


def map_x(value: int, minimum: int, maximum: int, left: float, width: float) -> float:
    if minimum == maximum:
        return left + width * 0.5
    return left + (float(value - minimum) / float(maximum - minimum)) * width


def map_y(value: float, maximum: float, top: float, height: float) -> float:
    if maximum <= 0.0:
        return top + height
    clamped = min(max(value, 0.0), maximum)
    return top + height - (clamped / maximum) * height


def escape_text(value: str) -> str:
    return html.escape(value, quote=True)


def build_svg(panels, title: str) -> str:
    panel_count = len(panels)
    if panel_count == 0:
        raise ValueError("no matching SharedKvMultiFixture runs found")

    all_threads = sorted(
        {
            thread
            for panel in panels
            for series in panel["series"]
            for thread, _ in series["points"]
        }
    )
    if not all_threads:
        raise ValueError("no thread counts found")

    max_rate = max(
        rate
        for panel in panels
        for series in panel["series"]
        for _, rate in series["points"]
    )
    y_max = max_rate * 1.1 if max_rate > 0.0 else 1.0
    rate_divisor, rate_suffix = choose_rate_unit(y_max)
    y_axis_label = "items_per_second"
    if rate_suffix:
        y_axis_label = f"items_per_second ({rate_suffix})"

    cols = 1 if panel_count == 1 else 2
    rows = math.ceil(panel_count / cols)

    outer_margin_x = 28
    outer_margin_top = 60
    outer_margin_bottom = 60
    gap_x = 18
    gap_y = 18
    panel_w = 540
    panel_h = 320

    width = outer_margin_x * 2 + cols * panel_w + max(0, cols - 1) * gap_x
    height = outer_margin_top + outer_margin_bottom + rows * panel_h + max(0, rows - 1) * gap_y

    plot_offset_x = 62
    plot_offset_y = 54
    plot_w = 300
    plot_h = 210
    legend_offset_x = 382
    legend_offset_y = 68
    legend_row_h = 18

    svg = [
        f'<svg xmlns="http://www.w3.org/2000/svg" width="{width}" height="{height}" '
        f'viewBox="0 0 {width} {height}">',
        "<style>",
        "text { font-family: 'DejaVu Sans Mono', monospace; fill: #111827; }",
        ".title { font-size: 20px; font-weight: 700; }",
        ".panel-title { font-size: 14px; font-weight: 700; }",
        ".axis-label { font-size: 13px; font-weight: 700; }",
        ".tick { font-size: 11px; }",
        ".legend { font-size: 11px; }",
        ".panel { fill: #ffffff; stroke: #d1d5db; stroke-width: 1; }",
        ".plot-bg { fill: #f9fafb; }",
        ".grid { stroke: #e5e7eb; stroke-width: 1; }",
        ".axis { stroke: #111827; stroke-width: 1.25; }",
        "</style>",
        f'<rect x="0" y="0" width="{width}" height="{height}" fill="#f3f4f6" />',
        f'<text class="title" x="{width / 2:.1f}" y="30" text-anchor="middle">{escape_text(title)}</text>',
        f'<text class="axis-label" x="{width / 2:.1f}" y="{height - 16}" text-anchor="middle">threads</text>',
        f'<text class="axis-label" x="16" y="{height / 2:.1f}" transform="rotate(-90 16 {height / 2:.1f})" '
        f'text-anchor="middle">{escape_text(y_axis_label)}</text>',
    ]

    min_thread = all_threads[0]
    max_thread = all_threads[-1]

    for index, panel in enumerate(panels):
        row = index // cols
        col = index % cols
        panel_x = outer_margin_x + col * (panel_w + gap_x)
        panel_y = outer_margin_top + row * (panel_h + gap_y)
        plot_x = panel_x + plot_offset_x
        plot_y = panel_y + plot_offset_y
        legend_x = panel_x + legend_offset_x
        legend_y = panel_y + legend_offset_y

        svg.append(
            f'<rect class="panel" x="{panel_x}" y="{panel_y}" width="{panel_w}" height="{panel_h}" rx="8" />'
        )
        svg.append(
            f'<text class="panel-title" x="{panel_x + 16}" y="{panel_y + 28}">{escape_text(panel["workload"])}</text>'
        )
        svg.append(
            f'<rect class="plot-bg" x="{plot_x}" y="{plot_y}" width="{plot_w}" height="{plot_h}" rx="4" />'
        )

        y_tick_count = 5
        for tick_index in range(y_tick_count):
            tick_value = y_max * (tick_index / float(y_tick_count - 1))
            y = map_y(tick_value, y_max, plot_y, plot_h)
            svg.append(f'<line class="grid" x1="{plot_x}" y1="{y:.1f}" x2="{plot_x + plot_w}" y2="{y:.1f}" />')
            svg.append(
                f'<text class="tick" x="{plot_x - 8}" y="{y + 4:.1f}" text-anchor="end">'
                f"{escape_text(format_rate(tick_value, rate_divisor))}</text>"
            )

        for thread in all_threads:
            x = map_x(thread, min_thread, max_thread, plot_x, plot_w)
            svg.append(f'<line class="grid" x1="{x:.1f}" y1="{plot_y}" x2="{x:.1f}" y2="{plot_y + plot_h}" />')
            svg.append(
                f'<text class="tick" x="{x:.1f}" y="{plot_y + plot_h + 18}" text-anchor="middle">'
                f"{thread}</text>"
            )

        svg.append(f'<line class="axis" x1="{plot_x}" y1="{plot_y}" x2="{plot_x}" y2="{plot_y + plot_h}" />')
        svg.append(
            f'<line class="axis" x1="{plot_x}" y1="{plot_y + plot_h}" x2="{plot_x + plot_w}" y2="{plot_y + plot_h}" />'
        )

        for series_index, series in enumerate(panel["series"]):
            color = PALETTE[series_index % len(PALETTE)]
            points = [
                (
                    map_x(thread, min_thread, max_thread, plot_x, plot_w),
                    map_y(rate, y_max, plot_y, plot_h),
                )
                for thread, rate in series["points"]
            ]
            if len(points) >= 2:
                polyline = " ".join(f"{x:.1f},{y:.1f}" for x, y in points)
                svg.append(
                    f'<polyline points="{polyline}" fill="none" stroke="{color}" '
                    'stroke-width="2.25" stroke-linecap="round" stroke-linejoin="round" />'
                )
            for x, y in points:
                svg.append(
                    f'<circle cx="{x:.1f}" cy="{y:.1f}" r="3.5" fill="{color}" stroke="#ffffff" stroke-width="1" />'
                )

            legend_item_y = legend_y + series_index * legend_row_h
            svg.append(
                f'<line x1="{legend_x}" y1="{legend_item_y}" x2="{legend_x + 16}" y2="{legend_item_y}" '
                f'stroke="{color}" stroke-width="2.25" stroke-linecap="round" />'
            )
            svg.append(
                f'<circle cx="{legend_x + 8}" cy="{legend_item_y}" r="3" fill="{color}" stroke="#ffffff" stroke-width="1" />'
            )
            svg.append(
                f'<text class="legend" x="{legend_x + 24}" y="{legend_item_y + 4}">'
                f'{escape_text(series["dataset"])}</text>'
            )

    svg.append("</svg>")
    return "\n".join(svg) + "\n"


def main() -> int:
    args = parse_args()
    input_path = args.input_json
    output_path = args.output_svg or input_path.with_suffix(".svg")

    try:
        payload = json.loads(input_path.read_text(encoding="utf-8"))
    except FileNotFoundError:
        return fail(f"{input_path} does not exist")
    except json.JSONDecodeError as exc:
        return fail(f"{input_path} is not valid JSON: {exc}")

    try:
        panels = collect_panels(payload, args.workload, args.dataset)
        svg = build_svg(panels, args.title)
    except ValueError as exc:
        return fail(str(exc))

    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(svg, encoding="utf-8")
    print(f"wrote {output_path}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
