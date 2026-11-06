#!/usr/bin/env python3
"""Summarize A/A noise and optional A/B median deltas for accept-latency samples.

Reads one or more JSONL files produced by measure_protocol_accept_latency.py
(--jsonl) or equivalent {seconds: float} rows. Exit 0 always when IO succeeds;
prints a machine-readable decision block for stop-or-ship.
"""
from __future__ import annotations

import argparse
import json
import statistics
import sys
from pathlib import Path


def load_jsonl(path):
    rows = []
    for line in path.read_text(encoding="utf-8").splitlines():
        if not line.strip():
            continue
        row = json.loads(line)
        if "seconds" not in row:
            raise ValueError(f"missing_seconds:{path}")
        rows.append(float(row["seconds"]))
    return rows


def summarize(values):
    if not values:
        return {"count": 0}
    values = sorted(values)
    return {
        "count": len(values),
        "median": statistics.median(values),
        "mean": statistics.mean(values),
        "min": values[0],
        "max": values[-1],
        "stdev": statistics.pstdev(values) if len(values) > 1 else 0.0,
        "range": values[-1] - values[0],
    }


def decide(aa_a, aa_b, baseline, candidate, threshold=1.0):
    aa_noise = abs(aa_a["median"] - aa_b["median"]) if aa_a.get("count") and aa_b.get("count") else None
    gate = max(0.3, aa_noise or 0.0)
    if not baseline.get("count") or not candidate.get("count"):
        return {"adopt": False, "reason": "insufficient_samples", "gate_seconds": gate, "aa_noise_seconds": aa_noise}
    improvement = baseline["median"] - candidate["median"]
    adopt = improvement > max(threshold, gate)
    return {
        "adopt": adopt,
        "reason": "median_improvement_clears_gate" if adopt else "median_improvement_below_gate",
        "improvement_seconds": improvement,
        "gate_seconds": gate,
        "aa_noise_seconds": aa_noise,
        "ship_threshold_seconds": threshold,
    }


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--aa-a", type=Path, help="A/A side A JSONL")
    parser.add_argument("--aa-b", type=Path, help="A/A side B JSONL")
    parser.add_argument("--baseline", type=Path, action="append", default=[],
                        help="baseline JSONL (repeatable; concatenated)")
    parser.add_argument("--candidate", type=Path, action="append", default=[],
                        help="candidate JSONL (repeatable; concatenated)")
    parser.add_argument("--threshold", type=float, default=1.0,
                        help="stop-or-ship median improvement threshold seconds")
    args = parser.parse_args(argv)

    def merge(paths):
        values = []
        for path in paths:
            values.extend(load_jsonl(path))
        return summarize(values)

    report = {
        "aa_a": summarize(load_jsonl(args.aa_a)) if args.aa_a else {"count": 0},
        "aa_b": summarize(load_jsonl(args.aa_b)) if args.aa_b else {"count": 0},
        "baseline": merge(args.baseline) if args.baseline else {"count": 0},
        "candidate": merge(args.candidate) if args.candidate else {"count": 0},
    }
    report["decision"] = decide(report["aa_a"], report["aa_b"], report["baseline"],
                                report["candidate"], threshold=args.threshold)
    print(json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
