"""Write a static HTML dashboard from the monitor CSV (inline SVG, no server, no JS libs).

python -m bus.dashboard --csv data/monitor.csv --out data/dashboard.html [--title T] [--source S]
Prints a JSON summary of the CSV (rows, lag rows without committed offsets, peaks) to stdout.
"""
from __future__ import annotations

import argparse
import csv
import json
from html import escape

CSS = """
:root{--bg:#fbfbfa;--fg:#1d1d1f;--muted:#6b6b70;--card:#fff;--line:#e3e3e6;--accent:#1f6feb;--warn:#b54708}
@media (prefers-color-scheme:dark){:root:not([data-theme=light]){--bg:#141416;--fg:#ececef;--muted:#9a9aa2;
 --card:#1d1d21;--line:#2e2e34;--accent:#6ea8ff;--warn:#f5a25d}}
*{box-sizing:border-box}body{margin:0;background:var(--bg);color:var(--fg);
 font:15px/1.5 system-ui,-apple-system,Segoe UI,Roboto,sans-serif}
main{max-width:820px;margin:0 auto;padding:24px 16px 48px}
h1{font-size:1.4rem;margin:0 0 4px}p{margin:4px 0}.muted{color:var(--muted)}
.banner{border-left:3px solid var(--warn);padding:8px 12px;margin:16px 0;background:var(--card)}
.kpis{display:grid;grid-template-columns:repeat(auto-fit,minmax(150px,1fr));gap:12px;margin:16px 0}
.kpi{background:var(--card);border:1px solid var(--line);border-radius:10px;padding:12px}
.kpi b{display:block;font-size:1.35rem;font-variant-numeric:tabular-nums}
.kpi span{color:var(--muted);font-size:.85rem}
section{background:var(--card);border:1px solid var(--line);border-radius:10px;padding:12px 14px;margin:12px 0}
h3{font-size:1rem;margin:0 0 6px}h3 small{color:var(--muted);font-weight:400}
svg text{fill:var(--muted);font-size:10px}svg .ax{stroke:var(--line)}svg .ln{stroke:var(--accent)}
"""


def spark(xs, ys, title, unit, w=760, h=170, pad=44):
    if len(ys) < 2:
        return f"<section><h3>{escape(title)}</h3><p>not enough rows</p></section>"
    x0, x1 = xs[0], xs[-1]
    lo, hi = min(min(ys), 0), max(max(ys), 1e-9)
    sx = lambda x: pad + (x - x0) / ((x1 - x0) or 1) * (w - pad - 8)
    sy = lambda y: h - 22 - (y - lo) / ((hi - lo) or 1) * (h - 38)
    pts = " ".join(f"{sx(x):.1f},{sy(y):.1f}" for x, y in zip(xs, ys))
    return (f"<section><h3>{escape(title)} <small>last {ys[-1]:.4g} {unit}, max {max(ys):.4g}</small></h3>"
            f'<svg viewBox="0 0 {w} {h}" width="100%" role="img" aria-label="{escape(title)}">'
            f'<line class="ax" x1="{pad}" y1="{h-22}" x2="{w-8}" y2="{h-22}"/>'
            f'<line class="ax" x1="{pad}" y1="8" x2="{pad}" y2="{h-22}"/>'
            f'<text x="2" y="14">{hi:.4g}</text><text x="2" y="{h-24}">{lo:.4g}</text>'
            f'<text x="{pad}" y="{h-6}">0 s</text><text x="{w-8}" y="{h-6}" text-anchor="end">{x1:.0f} s</text>'
            f'<polyline class="ln" fill="none" stroke-width="2" points="{pts}"/></svg></section>')


def summarise(rows):
    lag = [int(float(r["lag"])) for r in rows]
    real = [v for v in lag if v >= 0]
    return dict(rows=len(rows), span_s=round(float(rows[-1]["wall_ts"]) - float(rows[0]["wall_ts"]), 1),
                lag_rows_without_commit=sum(v < 0 for v in lag), lag_rows_measured=len(real),
                max_lag=max(real, default=None), final_lag=real[-1] if real else None,
                max_msg_rate=max(float(r["msg_rate"]) for r in rows),
                msgs_total=int(float(rows[-1]["msgs_total"])),
                max_vehicles_seen=max(int(float(r["vehicles_seen"])) for r in rows),
                max_stale_frac=max(float(r["stale_frac"]) for r in rows))


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--csv", default="data/monitor.csv")
    ap.add_argument("--out", default="data/dashboard.html")
    ap.add_argument("--title", default="Bus stream monitor")
    ap.add_argument("--source", default="SYNTHETIC simulator",
                    help="what produced the stream; shown in the banner")
    a = ap.parse_args()
    with open(a.csv) as f:
        rows = list(csv.DictReader(f))
    if not rows:
        raise SystemExit(f"{a.csv} has no rows; run bus.monitor first")
    s = summarise(rows)
    t = [float(r["wall_ts"]) - float(rows[0]["wall_ts"]) for r in rows]
    col = lambda k: [float(r[k]) for r in rows]
    kpi = lambda v, k: f'<div class="kpi"><b>{escape(str(v))}</b><span>{escape(k)}</span></div>'
    kpis = "".join([kpi(s["msgs_total"], "messages in topic"), kpi(f'{s["max_msg_rate"]:.0f}', "peak msg/s"),
                    kpi(s["max_lag"] if s["max_lag"] is not None else "n/a", "peak consumer lag (msgs)"),
                    kpi(s["final_lag"] if s["final_lag"] is not None else "n/a", "final consumer lag"),
                    kpi(f'{s["max_stale_frac"]:.1%}', "peak stale-vehicle share")])
    body = "".join([
        spark(t, col("msg_rate"), "Message rate into the topic", "msg/s"),
        spark(t, [max(v, 0) for v in col("lag")], "Consumer lag of the scoring group", "msgs"),
        spark(t, col("stale_frac"), "Stale vehicle fraction (newest ping older than 120 s)", ""),
        spark(t, col("vehicles_seen"), "Active vehicles seen by the monitor", ""),
    ])
    banner = (f"Data source: {a.source}. Numbers describe the pipeline on one shared machine, "
              "not Delhi traffic.")
    html = (f'<!doctype html><html lang="en"><head><meta charset="utf-8">'
            f'<meta name="viewport" content="width=device-width,initial-scale=1">'
            f"<title>{escape(a.title)}</title><style>{CSS}</style></head><body><main>"
            f"<h1>{escape(a.title)}</h1><p class=muted>{s['rows']} samples over {s['span_s']} s from "
            f"{escape(a.csv)}. Lag rows before the group's first commit: {s['lag_rows_without_commit']}.</p>"
            f'<p class="banner">{escape(banner)}</p><div class="kpis">{kpis}</div>{body}</main></body></html>')
    with open(a.out, "w") as f:
        f.write(html)
    print(json.dumps(dict(out=a.out, **s)))


if __name__ == "__main__":
    main()
