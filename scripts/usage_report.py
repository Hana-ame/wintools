#!/usr/bin/env python3
"""Query opencode daily usage from the local sqlite database.

Usage:
  usage_report.py                # today's usage summary
  usage_report.py --days 7       # last 7 days summary
  usage_report.py --from 2026-08-01 --to 2026-08-06
  usage_report.py --sessions     # include per-session detail
  usage_report.py --no-dedup     # keep fork snapshot duplicates
  usage_report.py --db PATH      # override db location

Pricing (per million tokens), used for the est$ column:
  cache read      $0.02
  input           $1.00
  output+reasoning $2.00

Dedup: fork snapshots share a title like "X (fork #1)" and carry full copies
of the same conversation; by default only the largest-token session per
title group (suffix stripped) is kept, so forked context is not counted
multiple times.
"""
import argparse
import json
import os
import re
import sqlite3
import sys
from datetime import date, datetime, timedelta

PRICE_CACHE_READ_M = 0.02
PRICE_INPUT_M = 1.0
PRICE_OUTPUT_M = 2.0  # output + reasoning combined

FORK_SUFFIX = re.compile(r"\s*\(fork\s*#?\d*\)\s*$")


def est_price(in_tok: float, out_tok: float, rsn_tok: float, cache_r: float) -> float:
    """Estimated cost: cache*0.02/M + input*1/M + (output+reasoning)*2/M."""
    return (float(cache_r) / 1e6 * PRICE_CACHE_READ_M
            + float(in_tok) / 1e6 * PRICE_INPUT_M
            + (float(out_tok) + float(rsn_tok)) / 1e6 * PRICE_OUTPUT_M)


def default_db() -> str:
    path = os.environ.get("OPENCODE_DB")
    if path:
        return path
    return os.path.expanduser("~/.local/share/opencode/opencode.db")


def parse_day(d: date) -> int:
    return int(datetime(d.year, d.month, d.day).timestamp()) * 1000


def dedup(rows, key_idx, dedup: bool) -> list:
    """Keep the largest-token session per fork-title group."""
    if not dedup:
        return list(rows)
    best = {}
    for r in rows:
        title = r[key_idx] or ""
        group = FORK_SUFFIX.sub("", title)
        toks = (r[5] or 0) + (r[6] or 0) + (r[7] or 0) + (r[8] or 0)
        if group not in best or toks > best[group][1]:
            best[group] = (r, toks)
    return [b[0] for b in best.values()]


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--days", type=int, help="last N days (inclusive of today)")
    ap.add_argument("--from", dest="from_day", help="start date YYYY-MM-DD")
    ap.add_argument("--to", dest="to_day", help="end date YYYY-MM-DD (default today)")
    ap.add_argument("--sessions", action="store_true", help="list sessions instead of daily summary")
    ap.add_argument("--no-dedup", action="store_true", help="keep fork snapshot duplicates")
    ap.add_argument("--json", action="store_true", help="output raw JSON")
    ap.add_argument("--db", default=default_db(), help="sqlite db path")
    args = ap.parse_args()

    today = date.today()
    if args.days:
        from_day, to_day = today - timedelta(days=args.days - 1), today
    else:
        from_day = datetime.strptime(args.from_day, "%Y-%m-%d").date() if args.from_day else today
        to_day = datetime.strptime(args.to_day, "%Y-%m-%d").date() if args.to_day else today
    if from_day > to_day:
        print("error: --from must be <= --to", file=sys.stderr)
        return 1

    lo, hi = parse_day(from_day), parse_day(to_day) + 86400000
    try:
        con = sqlite3.connect(f"file:{args.db}?mode=ro", uri=True)
    except sqlite3.Error as e:
        print(f"error: cannot open db: {e}", file=sys.stderr)
        return 1

    def day_row(ts_ms: int) -> str:
        return datetime.fromtimestamp(ts_ms / 1000).date().isoformat()

    if args.sessions:
        rows = con.execute(
            """SELECT time_created, title, directory, agent, model,
                      tokens_input, tokens_output, tokens_reasoning,
                      tokens_cache_read, tokens_cache_write, cost
               FROM session
               WHERE time_created >= ? AND time_created < ?
               ORDER BY time_created""",
            (lo, hi),
        ).fetchall()
        rows = dedup(rows, 1, not args.no_dedup)
        if args.json:
            print(json.dumps([
                {"time_created": r[0], "date": day_row(r[0]), "title": r[1],
                 "directory": r[2], "agent": r[3], "model": r[4],
                 "tokens_input": r[5], "tokens_output": r[6], "tokens_reasoning": r[7],
                 "tokens_cache_read": r[8], "tokens_cache_write": r[9], "cost": r[10],
                 "est_cost": round(est_price(r[5], r[6], r[7], r[8]), 4)}
                for r in rows
            ], indent=2))
            return 0
        if not rows:
            print("no sessions in range")
            return 0
        hdr = f"{'date':<11}{'input':>14}{'output':>10}{'reasoning':>12}{'cache_r':>14}{'est$':>9}  title"
        print(hdr)
        for r in rows:
            print(f"{day_row(r[0]):<11}{r[5]:>14,}{r[6]:>10,}{r[7]:>12,}{r[8]:>14,}"
                  f"{est_price(r[5], r[6], r[7], r[8]):>9.4f}  {(r[1] or '')[:50]}")
        return 0

    rows = con.execute(
        """SELECT time_created, title, directory, agent, model,
                  tokens_input, tokens_output, tokens_reasoning,
                  tokens_cache_read, tokens_cache_write, cost
           FROM session
           WHERE time_created >= ? AND time_created < ?
           ORDER BY time_created""",
        (lo, hi),
    ).fetchall()
    rows = dedup(rows, 1, not args.no_dedup)
    daily = {}
    for r in rows:
        d = day_row(r[0])
        agg = daily.setdefault(d, [0, 0, 0, 0, 0, 0, 0.0])  # sessions,in,out,rsn,cr,cw,cost
        agg[0] += 1
        for i, idx in enumerate((5, 6, 7, 8, 9)):  # in,out,rsn,cr,cw
            agg[i + 1] += (r[idx] or 0)
        agg[6] += (r[10] or 0)
    if args.json:
        print(json.dumps([
            {"date": d, "sessions": a[0], "tokens_input": a[1], "tokens_output": a[2],
             "tokens_reasoning": a[3], "tokens_cache_read": a[4], "tokens_cache_write": a[5],
             "cost": a[6], "est_cost": round(est_price(a[1], a[2], a[3], a[4]), 4)}
            for d, a in sorted(daily.items())
        ], indent=2))
        return 0
    if not daily:
        print("no sessions in range")
        return 0
    hdr = f"{'date':<11}{'sessions':>9}{'input':>14}{'output':>10}{'reasoning':>12}{'cache_r':>14}{'est$':>9}"
    print(hdr)
    print("-" * len(hdr))
    for d in sorted(daily):
        a = daily[d]
        print(f"{d:<11}{a[0]:>9,}{a[1]:>14,}{a[2]:>10,}{a[3]:>12,}{a[4]:>14,}"
              f"{est_price(a[1], a[2], a[3], a[4]):>9.4f}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
