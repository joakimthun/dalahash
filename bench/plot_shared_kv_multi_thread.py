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
        read_ops = run.get("read_ops", 0.0)
        write_ops = run.get("write_ops", 0.0)
        threads = run.get("threads")
        if not isinstance(name, str):
            continue
        if not isinstance(read_ops, (int, float)):
            continue
        if not isinstance(write_ops, (int, float)):
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
        thread_samples = dataset_entry.setdefault(
            threads,
            {"read_ops": [], "write_ops": []},
        )
        thread_samples["read_ops"].append(float(read_ops))
        thread_samples["write_ops"].append(float(write_ops))

    panels = []
    for workload, series_by_dataset in grouped.items():
        series_list = []
        for dataset in sorted(series_by_dataset, key=dataset_sort_key):
            points = []
            for threads in sorted(series_by_dataset[dataset]):
                samples = series_by_dataset[dataset][threads]
                points.append(
                    {
                        "thread": threads,
                        "read_ops": average(samples["read_ops"]),
                        "write_ops": average(samples["write_ops"]),
                    }
                )
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


def average(values) -> float:
    return sum(values) / len(values)


def map_x(value: int, minimum: int, maximum: int, left: float, width: float) -> float:
    if minimum == maximum:
        return left + width * 0.5
    return left + (float(value - minimum) / float(maximum - minimum)) * width


def map_y(value: float, maximum: float, top: float, height: float) -> float:
    if maximum <= 0.0:
        return top + height
    clamped = min(max(value, 0.0), maximum)
    return top + height - (clamped / maximum) * height


def offset_plot_y(value: float, delta: float, top: float, height: float, margin: float = 3.0) -> float:
    lower = top + margin
    upper = top + height - margin
    return min(max(value + delta, lower), upper)


def escape_text(value: str) -> str:
    return html.escape(value, quote=True)


def build_svg(panels, title: str) -> str:
    panel_count = len(panels)
    if panel_count == 0:
        raise ValueError("no matching SharedKvMultiFixture runs found")

    all_threads = sorted(
        {
            point["thread"]
            for panel in panels
            for series in panel["series"]
            for point in series["points"]
        }
    )
    if not all_threads:
        raise ValueError("no thread counts found")

    max_read_rate = max(
        point["read_ops"]
        for panel in panels
        for series in panel["series"]
        for point in series["points"]
    )
    max_write_rate = max(
        point["write_ops"]
        for panel in panels
        for series in panel["series"]
        for point in series["points"]
    )
    left_y_max = max_read_rate * 1.1 if max_read_rate > 0.0 else 1.0
    right_y_max = max_write_rate * 1.1 if max_write_rate > 0.0 else 1.0
    left_divisor, left_suffix = choose_rate_unit(left_y_max)
    right_divisor, right_suffix = choose_rate_unit(right_y_max)
    left_y_axis_label = "read_ops"
    if left_suffix:
        left_y_axis_label = f"read_ops ({left_suffix})"
    right_y_axis_label = "write_ops"
    if right_suffix:
        right_y_axis_label = f"write_ops ({right_suffix})"

    cols = 1 if panel_count == 1 else 2
    rows = math.ceil(panel_count / cols)

    outer_margin_x = 28
    outer_margin_top = 60
    outer_margin_bottom = 60
    gap_x = 18
    gap_y = 18
    panel_w = 580
    panel_h = 320

    width = outer_margin_x * 2 + cols * panel_w + max(0, cols - 1) * gap_x
    height = outer_margin_top + outer_margin_bottom + rows * panel_h + max(0, rows - 1) * gap_y

    plot_offset_x = 62
    plot_offset_y = 54
    plot_w = 290
    plot_h = 210
    legend_offset_x = 410
    dataset_legend_offset_y = 120
    legend_row_h = 18
    plot_bg_fill = "#f9fafb"
    write_dash = "10 6"
    write_offset_px = 4.0

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
        f'text-anchor="middle">{escape_text(left_y_axis_label)}</text>',
        f'<text class="axis-label" x="{width - 16}" y="{height / 2:.1f}" '
        f'transform="rotate(90 {width - 16} {height / 2:.1f})" '
        f'text-anchor="middle">{escape_text(right_y_axis_label)}</text>',
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
        dataset_legend_y = panel_y + dataset_legend_offset_y

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
            tick_value = left_y_max * (tick_index / float(y_tick_count - 1))
            y = map_y(tick_value, left_y_max, plot_y, plot_h)
            svg.append(f'<line class="grid" x1="{plot_x}" y1="{y:.1f}" x2="{plot_x + plot_w}" y2="{y:.1f}" />')
            svg.append(
                f'<text class="tick" x="{plot_x - 8}" y="{y + 4:.1f}" text-anchor="end">'
                f"{escape_text(format_rate(tick_value, left_divisor))}</text>"
            )
            right_tick_value = right_y_max * (tick_index / float(y_tick_count - 1))
            right_y = map_y(right_tick_value, right_y_max, plot_y, plot_h)
            svg.append(
                f'<text class="tick" x="{plot_x + plot_w + 8}" y="{right_y + 4:.1f}" text-anchor="start">'
                f"{escape_text(format_rate(right_tick_value, right_divisor))}</text>"
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
            f'<line class="axis" x1="{plot_x + plot_w}" y1="{plot_y}" x2="{plot_x + plot_w}" y2="{plot_y + plot_h}" />'
        )
        svg.append(
            f'<line class="axis" x1="{plot_x}" y1="{plot_y + plot_h}" x2="{plot_x + plot_w}" y2="{plot_y + plot_h}" />'
        )
        metric_legend_y = panel_y + 62
        for label, dash in (
            ("read_ops", None),
            ("write_ops", write_dash),
        ):
            line_attrs = ""
            line_cap = "round"
            if dash is not None:
                line_attrs = f' stroke-dasharray="{dash}"'
                line_cap = "butt"
            svg.append(
                f'<line x1="{legend_x}" y1="{metric_legend_y}" x2="{legend_x + 16}" y2="{metric_legend_y}" '
                f'stroke="#111827" stroke-width="2.25" stroke-linecap="{line_cap}"{line_attrs} />'
            )
            svg.append(
                f'<text class="legend" x="{legend_x + 24}" y="{metric_legend_y + 4}">{escape_text(label)}</text>'
            )
            metric_legend_y += legend_row_h

        for series_index, series in enumerate(panel["series"]):
            color = PALETTE[series_index % len(PALETTE)]
            has_read_points = any(point["read_ops"] > 0.0 for point in series["points"])
            has_write_points = any(point["write_ops"] > 0.0 for point in series["points"])
            read_points = [
                (
                    map_x(point["thread"], min_thread, max_thread, plot_x, plot_w),
                    map_y(point["read_ops"], left_y_max, plot_y, plot_h),
                )
                for point in series["points"]
            ]
            write_points = [
                (
                    map_x(point["thread"], min_thread, max_thread, plot_x, plot_w),
                    offset_plot_y(
                        map_y(point["write_ops"], right_y_max, plot_y, plot_h),
                        write_offset_px,
                        plot_y,
                        plot_h,
                    ),
                )
                for point in series["points"]
            ]
            if has_read_points and len(read_points) >= 2:
                polyline = " ".join(f"{x:.1f},{y:.1f}" for x, y in read_points)
                svg.append(
                    f'<polyline points="{polyline}" fill="none" stroke="{color}" '
                    'stroke-width="2.25" stroke-linecap="round" stroke-linejoin="round" />'
                )
            if has_write_points and len(write_points) >= 2:
                polyline = " ".join(f"{x:.1f},{y:.1f}" for x, y in write_points)
                svg.append(
                    f'<polyline points="{polyline}" fill="none" stroke="{plot_bg_fill}" '
                    f'stroke-width="4.5" stroke-linecap="butt" stroke-linejoin="round" '
                    f'stroke-dasharray="{write_dash}" />'
                )
                svg.append(
                    f'<polyline points="{polyline}" fill="none" stroke="{color}" '
                    f'stroke-width="2.25" stroke-linecap="butt" stroke-linejoin="round" '
                    f'stroke-dasharray="{write_dash}" />'
                )
            if has_read_points:
                for x, y in read_points:
                    svg.append(
                        f'<circle cx="{x:.1f}" cy="{y:.1f}" r="3.5" fill="{color}" stroke="#ffffff" stroke-width="1" />'
                    )
            if has_write_points:
                for x, y in write_points:
                    svg.append(
                        f'<rect x="{x - 2.6:.1f}" y="{y - 2.6:.1f}" width="5.2" height="5.2" fill="#ffffff" '
                        f'stroke="{color}" stroke-width="1.4" />'
                    )

            legend_item_y = dataset_legend_y + series_index * legend_row_h
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
