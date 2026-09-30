import argparse
import re
import sys
from collections import defaultdict, deque
from dataclasses import dataclass
from html import escape
from pathlib import Path
from typing import Optional

LINE = re.compile(r"^(\d\d:\d\d:\d\d\.\d+)\s+(\w+)\s+(.*)$")
SEND = re.compile(r"Send: to = (\d+), id = (\d+), clock = (\d+), type = (\w+)")
RECV = re.compile(r"Receive: from = (\d+), id = (\d+), clock = (\d+), type = (\w+)")
DISCARD = re.compile(r"Discard: (?:from = (\d+), )?id = (\d+), clock = (\d+)")
MAXID = re.compile(r"State: maxId (\d+) -> (\d+), clock = (\d+)")
LEADER_STATE = re.compile(r"State: isLeader = true, clock = (\d+)")
ELECTED = re.compile(r"Leader elected: (\d+), clock = (\d+)")
NODE_FILE = re.compile(r"node-(\d+)\.log$")


@dataclass
class Event:
    node: int
    seq: int
    kind: str
    clock: int
    eff: int = 0
    peer: Optional[int] = None
    uid: Optional[int] = None
    mtype: str = ""
    extra: str = ""


def parse_file(path: Path, node: int):
    events = []
    eff = 0
    for seq, raw in enumerate(path.read_text(encoding="utf-8").splitlines()):
        m = LINE.match(raw)
        if not m:
            continue
        msg = m.group(3)
        ev = None
        if g := SEND.match(msg):
            ev = Event(node, seq, "send", int(g[3]), peer=int(g[1]), uid=int(g[2]), mtype=g[4])
        elif g := RECV.match(msg):
            ev = Event(node, seq, "recv", int(g[3]), peer=int(g[1]), uid=int(g[2]), mtype=g[4])
        elif g := DISCARD.match(msg):
            ev = Event(node, seq, "discard", int(g[3]),
                       peer=int(g[1]) if g[1] else None, uid=int(g[2]))
        elif g := MAXID.match(msg):
            ev = Event(node, seq, "maxid", int(g[3]), extra=f"maxId {g[1]}→{g[2]}")
        elif g := LEADER_STATE.match(msg):
            ev = Event(node, seq, "leader", int(g[1]), extra="isLeader = true")
        elif g := ELECTED.match(msg):
            ev = Event(node, seq, "elected", int(g[2]), uid=int(g[1]))
        if ev is None:
            continue
        eff = max(eff, ev.clock)
        ev.eff = eff
        events.append(ev)
    return events


def load(log_dir: Path):
    events = []
    for f in sorted(log_dir.glob("node-*.log")):
        m = NODE_FILE.search(f.name)
        if m:
            events.extend(parse_file(f, int(m.group(1))))
    return events


def merge(events):
    return sorted(events, key=lambda e: (e.eff, e.node, e.seq))


def pair_messages(merged):
    pending = defaultdict(deque)
    pairs, warnings = {}, []
    for i, e in enumerate(merged):
        if e.kind == "send":
            pending[(e.node, e.peer)].append(i)
        elif e.kind == "recv":
            q = pending[(e.peer, e.node)]
            if not q:
                warnings.append(f"recv without matching send: node {e.node} <- {e.peer}, "
                                f"{e.mtype} {e.uid}, clock {e.eff}")
                continue
            s = q.popleft()
            se = merged[s]
            if (se.uid, se.mtype) != (e.uid, e.mtype):
                warnings.append(f"inconsistent pair {se.node}->{e.node}: "
                                f"send {se.mtype} {se.uid} vs recv {e.mtype} {e.uid}")
            pairs[s] = i
    for (a, b), q in pending.items():
        for s in q:
            warnings.append(f"send without matching recv: {a} -> {b}, {merged[s].mtype} {merged[s].uid}")
    return pairs, warnings


def describe(e: Event) -> str:
    if e.kind == "send":
        return f"send    -> {e.peer}   {e.mtype} {e.uid}"
    if e.kind == "recv":
        return f"recv    <- {e.peer}   {e.mtype} {e.uid}"
    if e.kind == "discard":
        return f"discard {e.uid}"
    if e.kind == "elected":
        return f"leader elected: {e.uid}"
    return f"state   {e.extra}"


def write_merged(merged, path: Path):
    lines = [f"{i + 1:>3}  c={e.eff:<3} node {e.node:<4} {describe(e)}"
             for i, e in enumerate(merged)]
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def summarize(merged, nodes):
    sends = [e for e in merged if e.kind == "send"]
    n_elec = sum(e.mtype == "ELECTION" for e in sends)
    n_elct = sum(e.mtype == "ELECTED" for e in sends)
    n_disc = sum(e.kind == "discard" for e in merged)
    leaders = [e.node for e in merged if e.kind == "leader"]
    agreed = {e.uid for e in merged if e.kind == "elected"}

    print(f"Nodes: {sorted(nodes)}   events: {len(merged)}")
    print(f"ELECTION sent: {n_elec} | ELECTED sent: {n_elct} | discards: {n_disc}")
    if len(leaders) == 1:
        print(f"Safety:    OK (one leader: {leaders[0]})")
    elif not leaders:
        print("Liveness:  FAILED (no node declared itself leader)")
    else:
        print(f"Safety:    VIOLATED ({len(leaders)} leaders: {leaders})")
    if len(agreed) == 1:
        print(f"Agreement: OK (all nodes that finished report leader {next(iter(agreed))})")
    elif len(agreed) > 1:
        print(f"Agreement: VIOLATED (different leaders: {sorted(agreed)})")
    done = {e.node for e in merged if e.kind == "elected"}
    missing = sorted(set(nodes) - done)
    if missing:
        print(f"Nodes without 'Leader elected': {missing}")


CSS = """
:root{--bg:#fff;--fg:#222;--mut:#999;--elec:#2b6cb0;--elct:#2f855a;--bad:#c53030;--gold:#b7791f}
@media (prefers-color-scheme:dark){:root{--bg:#1a1a1a;--fg:#e5e5e5;--mut:#777;
--elec:#63b3ed;--elct:#68d391;--bad:#fc8181;--gold:#f6ad55}}
body{background:var(--bg);color:var(--fg);font-family:system-ui,sans-serif;margin:16px}
.wrap{overflow-x:auto}
svg text{fill:var(--fg);font-size:12px}
.hdr{font-weight:600;font-size:13px}
.life{stroke:var(--mut);stroke-width:1;stroke-dasharray:3 4}
.arrow{stroke-width:1.6;fill:none}
.elec{stroke:var(--elec)}.elct{stroke:var(--elct)}
.lbl{paint-order:stroke;stroke:var(--bg);stroke-width:3px;font-size:11px}
.clk{fill:var(--mut)!important;font-size:10px}
.ev{fill:var(--fg)}
.lead{fill:var(--gold)!important;font-weight:600}
.bad{fill:var(--bad)!important}
.note{font-size:12px;color:var(--mut)}
"""


def ring_order(merged):
    succ = {}
    for e in merged:
        if e.kind == "send":
            succ.setdefault(e.node, e.peer)
    nodes = sorted({e.node for e in merged})
    order = [nodes[0]]
    while True:
        nxt = succ.get(order[-1])
        if nxt is None or nxt in order:
            break
        order.append(nxt)
    return order if len(order) == len(nodes) else nodes


def render_html(merged, pairs, title):
    order = ring_order(merged)
    COL, LEFT, TOP, ROW = 180, 90, 80, 28
    xs = {n: LEFT + i * COL for i, n in enumerate(order)}
    ys = [TOP + (i + 1) * ROW for i in range(len(merged))]
    H = TOP + (len(merged) + 2) * ROW
    W = 2 * LEFT + (len(order) - 1) * COL

    o = [f'<svg xmlns="http://www.w3.org/2000/svg" width="{W}" height="{H}" viewBox="0 0 {W} {H}">']
    for mid, cls in (("m-elec", "elec"), ("m-elct", "elct")):
        o.append(f'<defs><marker id="{mid}" markerWidth="8" markerHeight="8" refX="7" refY="4" '
                 f'orient="auto"><path d="M0,0 L8,4 L0,8 z" style="fill:var(--{cls})"/></marker></defs>')
    for n in order:
        o.append(f'<text x="{xs[n]}" y="{TOP - 34}" class="hdr" text-anchor="middle">node {n}</text>')
        o.append(f'<line x1="{xs[n]}" y1="{TOP - 20}" x2="{xs[n]}" y2="{H - 10}" class="life"/>')

    for s, r in pairs.items():
        a, b = merged[s], merged[r]
        cls = "elec" if a.mtype == "ELECTION" else "elct"
        x1, y1, x2, y2 = xs[a.node], ys[s], xs[b.node], ys[r]
        o.append(f'<line x1="{x1}" y1="{y1}" x2="{x2}" y2="{y2}" class="arrow {cls}" '
                 f'marker-end="url(#m-{cls})"/>')
        tag = "E" if a.mtype == "ELECTION" else "L"
        o.append(f'<text x="{(x1 + x2) / 2}" y="{(y1 + y2) / 2 - 3}" class="lbl" '
                 f'text-anchor="middle">{tag}({a.uid})</text>')

    for i, e in enumerate(merged):
        x, y = xs[e.node], ys[i]
        o.append(f'<text x="{x - 9}" y="{y + 3}" class="clk" text-anchor="end">{e.eff}</text>')
        if e.kind in ("send", "recv"):
            o.append(f'<circle cx="{x}" cy="{y}" r="3.5" class="ev"/>')
        elif e.kind == "discard":
            o.append(f'<text x="{x}" y="{y + 4}" class="bad" text-anchor="middle" '
                     f'style="font-size:13px">✕</text>')
            o.append(f'<text x="{x + 10}" y="{y + 4}" class="bad">discard {e.uid}</text>')
        elif e.kind == "maxid":
            o.append(f'<rect x="{x - 3}" y="{y - 3}" width="6" height="6" class="ev"/>')
            o.append(f'<text x="{x + 10}" y="{y + 4}">{escape(e.extra)}</text>')
        elif e.kind == "leader":
            o.append(f'<circle cx="{x}" cy="{y}" r="6" class="lead"/>')
            o.append(f'<text x="{x + 12}" y="{y + 4}" class="lead">LEADER</text>')
        elif e.kind == "elected":
            o.append(f'<rect x="{x - 3}" y="{y - 3}" width="6" height="6" class="lead"/>')
            o.append(f'<text x="{x + 10}" y="{y + 4}" class="lead">elected {e.uid}</text>')
    o.append("</svg>")

    legend = ("E(uid) = ELECTION &nbsp; L(uid) = ELECTED &nbsp; number on the left = Lamport clock "
              "&nbsp; ● send/recv &nbsp; ■ state change &nbsp; ✕ discard")
    return (f'<!doctype html><html lang="en"><head><meta charset="utf-8"><title>{escape(title)}</title>'
            f'<style>{CSS}</style></head><body><h3>{escape(title)}</h3>'
            f'<p class="note">{legend}</p><div class="wrap">{"".join(o)}</div></body></html>')


def main():
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8")
    ap = argparse.ArgumentParser(description="Merge per-node LCR logs into a global trace.")
    ap.add_argument("log_dir", nargs="?", default="logs",
                    help="directory with node-<uid>.log files (default: logs)")
    ap.add_argument("--out", default="trace_out", help="output directory (default: trace_out)")
    args = ap.parse_args()

    log_dir, out = Path(args.log_dir), Path(args.out)
    events = load(log_dir)
    if not events:
        sys.exit(f"No events found in '{log_dir}' (expected node-<uid>.log files).")

    merged = merge(events)
    pairs, warnings = pair_messages(merged)

    out.mkdir(parents=True, exist_ok=True)
    write_merged(merged, out / "merged.txt")
    (out / "trace.html").write_text(render_html(merged, pairs, f"LCR trace — {log_dir}"),
                                    encoding="utf-8")

    summarize(merged, {e.node for e in merged})
    for w in warnings:
        print("WARNING:", w)
    print(f"\nWritten: {out / 'merged.txt'} and {out / 'trace.html'}")


if __name__ == "__main__":
    main()