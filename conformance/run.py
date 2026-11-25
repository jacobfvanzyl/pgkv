#!/usr/bin/env python3
"""Run the conformance suite: upstream-derived scenarios and/or seeded fuzz sequences.

  python conformance/run.py                       # scenarios.jsonl
  python conformance/run.py --fuzz 1 500          # 500 fuzz sequences from seed 1
  python conformance/run.py --all                 # scenarios + the default fuzz corpus

Exits non-zero if pgkv disagrees with the oracle anywhere that is not listed in
expected.json (differences pgkv documents and intends to keep).
"""
from __future__ import annotations

import argparse
import collections
import json
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import fuzz  # noqa: E402
import harness  # noqa: E402

HERE = os.path.dirname(os.path.abspath(__file__))


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--scenarios", default=None, help="JSONL file of scenarios (default: scenarios.jsonl)")
    ap.add_argument("--fuzz", nargs=2, type=int, metavar=("SEED", "COUNT"), help="run COUNT fuzz sequences from SEED")
    ap.add_argument("--all", action="store_true", help="scenarios.jsonl plus fuzz seeds 1-3, 300 sequences each")
    ap.add_argument("--pipeline", action="store_true",
                    help="replay each scenario as one MULTI/EXEC against one atomic pgkv.pipeline")
    ap.add_argument("--expected", default=os.path.join(HERE, "expected.json"))
    ap.add_argument("--out", default=None, help="write every divergence to this JSON file")
    ap.add_argument("--show", type=int, default=15, help="how many unexpected divergences to print")
    args = ap.parse_args()

    scenarios = []
    if args.all or (not args.fuzz) or args.scenarios:
        scenarios += harness.load_scenarios(args.scenarios or os.path.join(HERE, "scenarios.jsonl"))
    if args.fuzz:
        scenarios += list(fuzz.generate(args.fuzz[0], args.fuzz[1]))
    if args.all:
        for seed in (1, 2, 3):
            scenarios += list(fuzz.generate(seed, 300))

    expected = harness.load_expected(args.expected)
    oracle, pg = harness.connect()
    commands = matched = 0
    unexpected, known = [], collections.Counter()
    all_divergences = []
    skipped = 0
    for sc in scenarios:
        if args.pipeline:
            ran, div = harness.run_pipeline_scenario(oracle, pg, sc)
            if ran == 0:
                skipped += 1
                continue
        else:
            ran, div = harness.run_scenario(oracle, pg, sc)
        commands += ran
        if div is None:
            matched += 1
            continue
        all_divergences.append(div)
        why = harness.expected_reason(expected, div)
        if why:
            known[why] += 1
        else:
            unexpected.append(div)

    if args.pipeline:
        print(f"pipeline mode: {skipped} scenarios skipped (sleeps, random or clock-dependent commands)")
    print(f"{len(scenarios) - skipped} scenarios, {commands} commands: {matched} match, "
          f"{sum(known.values())} stop at a documented difference, {len(unexpected)} diverge")
    for why, count in known.most_common():
        print(f"  documented ({count}): {why}")
    by_cmd = collections.Counter(d["cmd"][0].upper() for d in unexpected)
    if unexpected:
        print("  unexpected, by command: " + ", ".join(f"{c} {n}" for c, n in by_cmd.most_common()))
    for d in unexpected[: args.show]:
        print(f"\n[{d['kind']}] {d['scenario']} step {d['step']} ({d['source'] or 'fuzz'})")
        for p in d["prev"]:
            print("    " + json.dumps(p, ensure_ascii=False))
        print("  > " + json.dumps(d["cmd"], ensure_ascii=False))
        print(f"    oracle: {d['oracle']}\n    pgkv:   {d['pgkv']}")
        if d["state"]:
            print(f"    state:  {d['state']}")
    if args.out:
        with open(args.out, "w", encoding="utf-8") as f:
            json.dump(all_divergences, f, indent=1, ensure_ascii=False)
    sys.exit(1 if unexpected else 0)


if __name__ == "__main__":
    main()
