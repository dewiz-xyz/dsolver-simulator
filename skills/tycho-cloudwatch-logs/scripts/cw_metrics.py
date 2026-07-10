#!/usr/bin/env python3
import argparse
import datetime as dt
import json
import math
import os
import re
import subprocess
import sys
from collections import defaultdict
from statistics import mean

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
CW_TIME = os.path.join(SCRIPT_DIR, "cw_time.py")

METRIC_MAP = {
    "memory": "TaskMemoryUtilization",
    "cpu": "TaskCpuUtilization",
}


def run_cmd(cmd):
    return subprocess.check_output(cmd, text=True)


def parse_time_spec(spec: str) -> dt.datetime:
    epoch = int(run_cmd(["python3", CW_TIME, spec]).strip())
    return dt.datetime.fromtimestamp(epoch, tz=dt.timezone.utc)


def parse_duration(spec: str) -> int:
    spec = spec.strip()
    if re.fullmatch(r"\d+", spec):
        return int(spec)
    match = re.fullmatch(r"(\d+)([smhdw])", spec)
    if not match:
        raise ValueError(f"Unsupported duration spec: {spec}")
    amount = int(match.group(1))
    unit = match.group(2)
    return amount * {"s": 1, "m": 60, "h": 3600, "d": 86400, "w": 604800}[unit]


def isoformat_utc(ts: dt.datetime) -> str:
    return ts.astimezone(dt.timezone.utc).isoformat().replace("+00:00", "Z")


def normalize_metrics(metrics_spec: str):
    metrics = []
    for raw in metrics_spec.split(","):
        item = raw.strip()
        if not item:
            continue
        metrics.append(METRIC_MAP.get(item, item))
    return metrics


def task_ids_from_arns(arns):
    ids = []
    for arn in arns:
        arn = arn.strip()
        if not arn:
            continue
        ids.append(arn.split("/")[-1])
    return ids


def fetch_task_ids(cluster: str, service: str):
    cmd = [
        "aws",
        "ecs",
        "list-tasks",
        "--cluster",
        cluster,
        "--service-name",
        service,
        "--query",
        "taskArns",
        "--output",
        "text",
        "--no-cli-pager",
    ]
    output = run_cmd(cmd).strip()
    if not output:
        return []
    return task_ids_from_arns(output.split())


def build_metric_queries(task_ids, metrics, cluster, service, period, stat):
    queries = []
    query_map = {}
    idx = 1
    for task_id in task_ids:
        for metric in metrics:
            query_id = f"m{idx}"
            idx += 1
            queries.append(
                {
                    "Id": query_id,
                    "MetricStat": {
                        "Metric": {
                            "Namespace": "ECS/ContainerInsights",
                            "MetricName": metric,
                            "Dimensions": [
                                {"Name": "ClusterName", "Value": cluster},
                                {"Name": "ServiceName", "Value": service},
                                {"Name": "TaskId", "Value": task_id},
                            ],
                        },
                        "Period": period,
                        "Stat": stat,
                    },
                    "ReturnData": True,
                    "Label": f"{task_id}:{metric}",
                }
            )
            query_map[query_id] = (task_id, metric)
    return queries, query_map


def parse_results(result, query_map):
    per_task_metric = defaultdict(list)
    series = defaultdict(lambda: defaultdict(list))

    for item in result.get("MetricDataResults", []):
        task_id, metric = query_map.get(item.get("Id"), (None, None))
        if not task_id or not metric:
            label = item.get("Label", "")
            if ":" in label:
                task_id, metric = label.split(":", 1)
            else:
                continue

        timestamps = item.get("Timestamps", [])
        values = item.get("Values", [])
        pairs = []
        for ts, val in zip(timestamps, values):
            try:
                dt_ts = dt.datetime.fromisoformat(ts.replace("Z", "+00:00"))
            except ValueError:
                continue
            pairs.append((dt_ts, val))
            series[metric][ts].append(val)
        per_task_metric[(task_id, metric)] = sorted(pairs, key=lambda x: x[0])

    return per_task_metric, series


def summary_stats(pairs, pivot=None, window_seconds=None):
    if not pairs:
        return None

    values = [v for _, v in pairs]
    min_val = min(values)
    max_val = max(values)
    mean_val = mean(values)
    max_time = pairs[values.index(max_val)][0]

    summary = {
        "min": min_val,
        "max": max_val,
        "mean": mean_val,
        "max_time": isoformat_utc(max_time),
    }

    if pivot is not None and window_seconds is not None:
        pivot_dt = pivot
        window = dt.timedelta(seconds=window_seconds)
        before_start = pivot_dt - window
        after_end = pivot_dt + window

        before_vals = [v for t, v in pairs if before_start <= t < pivot_dt]
        after_vals = [v for t, v in pairs if pivot_dt <= t <= after_end]
        closest = min(pairs, key=lambda pair: abs(pair[0] - pivot_dt))

        summary.update(
            {
                "min_before": min(before_vals) if before_vals else None,
                "value_at_pivot": closest[1] if closest else None,
                "max_after": max(after_vals) if after_vals else None,
            }
        )
        if summary["min_before"] is not None and summary["value_at_pivot"] is not None:
            summary["delta"] = summary["value_at_pivot"] - summary["min_before"]
        else:
            summary["delta"] = None

    return summary


def format_float(value):
    if value is None or (isinstance(value, float) and math.isnan(value)):
        return "-"
    if isinstance(value, float):
        return f"{value:.3f}"
    return str(value)


def output_table(summary_rows, series_rows, include_series, include_summary):
    if include_summary:
        headers = list(summary_rows[0].keys()) if summary_rows else []
        print("SUMMARY")
        print_table(headers, summary_rows)

    if include_series:
        if include_summary:
            print("")
        headers = list(series_rows[0].keys()) if series_rows else []
        print("SERIES")
        print_table(headers, series_rows)


def print_table(headers, rows):
    if not headers:
        print("(no data)")
        return
    widths = {h: len(h) for h in headers}
    for row in rows:
        for h in headers:
            widths[h] = max(widths[h], len(str(row.get(h, ""))))
    header_line = "  ".join(h.ljust(widths[h]) for h in headers)
    print(header_line)
    print("  ".join("-" * widths[h] for h in headers))
    for row in rows:
        print("  ".join(str(row.get(h, "")).ljust(widths[h]) for h in headers))


def output_tsv(summary_rows, series_rows, include_series, include_summary):
    if include_summary:
        print("# summary")
        if summary_rows:
            headers = list(summary_rows[0].keys())
            print("\t".join(headers))
            for row in summary_rows:
                print("\t".join(str(row.get(h, "")) for h in headers))
        else:
            print("(no data)")
    if include_series:
        print("# series")
        if series_rows:
            headers = list(series_rows[0].keys())
            print("\t".join(headers))
            for row in series_rows:
                print("\t".join(str(row.get(h, "")) for h in headers))
        else:
            print("(no data)")


def output_json(summary_rows, series_rows, include_series, include_summary, meta):
    payload = {"meta": meta}
    if include_summary:
        payload["summary"] = summary_rows
    if include_series:
        payload["series"] = series_rows
    print(json.dumps(payload, indent=2))


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--since", default="12h")
    parser.add_argument("--until", default="now")
    parser.add_argument("--metrics", default="memory,cpu")
    parser.add_argument("--cluster", default=os.getenv("TYCHO_ECS_CLUSTER", "quoter-base"))
    parser.add_argument("--service", default=os.getenv("TYCHO_ECS_SERVICE", "tycho-simulator"))
    parser.add_argument("--task-ids", default="")
    parser.add_argument("--period", type=int, default=60)
    parser.add_argument("--stat", default="Average", choices=["Average", "Maximum", "Minimum"])
    parser.add_argument("--output", default="table", choices=["table", "json", "tsv"])
    parser.add_argument("--mode", default="both", choices=["summary", "series", "both"])
    parser.add_argument("--pivot", default=None)
    parser.add_argument("--window", default="10m")
    args = parser.parse_args()

    since_dt = parse_time_spec(args.since)
    until_dt = parse_time_spec(args.until)

    metrics = normalize_metrics(args.metrics)
    if not metrics:
        print("No metrics selected", file=sys.stderr)
        sys.exit(1)

    task_ids = [t for t in (args.task_ids.split(",") if args.task_ids else []) if t]
    if not task_ids:
        task_ids = fetch_task_ids(args.cluster, args.service)

    if not task_ids:
        print("No tasks found", file=sys.stderr)
        sys.exit(1)

    queries, query_map = build_metric_queries(
        task_ids,
        metrics,
        args.cluster,
        args.service,
        args.period,
        args.stat,
    )

    payload = {
        "StartTime": isoformat_utc(since_dt),
        "EndTime": isoformat_utc(until_dt),
        "MetricDataQueries": queries,
        "ScanBy": "TimestampAscending",
    }

    result = json.loads(
        run_cmd(
            [
                "aws",
                "cloudwatch",
                "get-metric-data",
                "--no-cli-pager",
                "--cli-input-json",
                json.dumps(payload),
            ]
        )
    )

    per_task_metric, series = parse_results(result, query_map)

    pivot_dt = parse_time_spec(args.pivot) if args.pivot else None
    window_seconds = parse_duration(args.window) if args.pivot else None

    summary_rows = []
    for task_id in task_ids:
        for metric in metrics:
            pairs = per_task_metric.get((task_id, metric), [])
            stats = summary_stats(pairs, pivot_dt, window_seconds)
            if stats is None:
                row = {
                    "metric": metric,
                    "task_id": task_id,
                    "min": "-",
                    "max": "-",
                    "mean": "-",
                    "max_time": "-",
                }
                if args.pivot:
                    row.update(
                        {
                            "min_before": "-",
                            "value_at_pivot": "-",
                            "max_after": "-",
                            "delta": "-",
                        }
                    )
            else:
                row = {
                    "metric": metric,
                    "task_id": task_id,
                    "min": format_float(stats["min"]),
                    "max": format_float(stats["max"]),
                    "mean": format_float(stats["mean"]),
                    "max_time": stats["max_time"],
                }
                if args.pivot:
                    row.update(
                        {
                            "min_before": format_float(stats.get("min_before")),
                            "value_at_pivot": format_float(stats.get("value_at_pivot")),
                            "max_after": format_float(stats.get("max_after")),
                            "delta": format_float(stats.get("delta")),
                        }
                    )
            summary_rows.append(row)

    series_rows = []
    for metric in metrics:
        metric_series = series.get(metric, {})
        for ts in sorted(metric_series.keys()):
            values = metric_series[ts]
            avg = sum(values) / len(values) if values else 0
            series_rows.append({"metric": metric, "timestamp": ts, "avg": format_float(avg)})

    include_summary = args.mode in ("summary", "both")
    include_series = args.mode in ("series", "both")

    meta = {
        "since": isoformat_utc(since_dt),
        "until": isoformat_utc(until_dt),
        "cluster": args.cluster,
        "service": args.service,
        "metrics": metrics,
        "period": args.period,
        "stat": args.stat,
    }
    if args.pivot:
        meta["pivot"] = isoformat_utc(pivot_dt)
        meta["window_seconds"] = window_seconds

    if args.output == "json":
        output_json(summary_rows, series_rows, include_series, include_summary, meta)
    elif args.output == "tsv":
        output_tsv(summary_rows, series_rows, include_series, include_summary)
    else:
        output_table(summary_rows, series_rows, include_series, include_summary)


if __name__ == "__main__":
    main()
