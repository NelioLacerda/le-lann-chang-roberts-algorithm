import argparse
import re
import sys
from collections import defaultdict
from dataclasses import dataclass
from html import escape
from pathlib import Path
from typing import Optional

LINE = re.compile(r"^(\d\d:\d\d:\d\d\.\d+)\s+(?:\[[^\]]*\]\s+)?(\w+)\s+(.*)$")

# Formatos exatos produzidos pelo Java (Node / Observer)
SEND = re.compile(r"Send: to = (\d+), nodeIdSend = (\d+), type = (\w+), seq = (\d+)")
RECV = re.compile(r"Receive: from = (\d+), nodeIdToSend = (\d+), type = (\w+), seq = (\d+)")
DISCARD = re.compile(r"Discard: from = (\d+), nodeIdSent = (\d+), seq = (\d+)")
MAXID = re.compile(r"State: maxId (\d+) -> (\d+), seq = (\d+)")
LEADER_STATE = re.compile(r"State: isLeader = true, seq = (\d+)")
ELECTED = re.compile(r"Leader elected node: (\d+), isLeader = (\w+), seq = (\d+)")
NODE_FILE = re.compile(r"node-(\d+)\.log$")

OBS = re.compile(r"Observer stored event: node = (\d+), kind = (\w+), peerNodeId = (\d+), "
                 r"nodeIdSent = (\d+), type = (\w+), seq = (\d+)")
KIND_MAP = {"SEND": "send", "RECV": "recv", "DISCARD": "discard",
            "REQUEST_TO_BECOME_LEADER": "leader", "LEADER": "elected"}


@dataclass
class Event:
    node: int
    line: int
    kind: str
    seq: int
    # Sem msgId no log: chave de emparelhamento (origem, destino, uid, tipo).
    # É única num anel LCR (cada nó reencaminha cada uid no máximo uma vez).
    msgid: Optional[tuple] = None
    peer: Optional[int] = None
    uid: Optional[int] = None
    mtype: str = ""
    extra: str = ""


# ---------------------------------------------------------------- parsing

def parse_file(path: Path, node: int):
    events = []
    for line, raw in enumerate(path.read_text(encoding="utf-8").splitlines()):
        m = LINE.match(raw)
        if not m:
            continue
        msg = m.group(3)
        ev = None
        if g := SEND.match(msg):
            to, uid, t, seq = int(g[1]), int(g[2]), g[3], int(g[4])
            ev = Event(node, line, "send", seq, (node, to, uid, t), peer=to, uid=uid, mtype=t)
        elif g := RECV.match(msg):
            frm, uid, t, seq = int(g[1]), int(g[2]), g[3], int(g[4])
            ev = Event(node, line, "recv", seq, (frm, node, uid, t), peer=frm, uid=uid, mtype=t)
        elif g := DISCARD.match(msg):
            # o discard só acontece para ELECTION (o log não traz o tipo)
            frm, uid, seq = int(g[1]), int(g[2]), int(g[3])
            ev = Event(node, line, "discard", seq, (frm, node, uid, "ELECTION"), peer=frm, uid=uid)
        elif g := MAXID.match(msg):
            ev = Event(node, line, "maxid", int(g[3]), extra=f"maxId {g[1]}→{g[2]}")
        elif g := LEADER_STATE.match(msg):
            ev = Event(node, line, "leader", int(g[1]), extra="isLeader = true")
        elif g := ELECTED.match(msg):
            ev = Event(node, line, "elected", int(g[3]), uid=int(g[1]))
        if ev is not None:
            events.append(ev)
    return events


def load(log_dir: Path):
    events = []
    for f in sorted(log_dir.glob("node-*.log")):
        m = NODE_FILE.search(f.name)
        if m:
            events.extend(parse_file(f, int(m.group(1))))
    return events


def load_observer(path: Path):
    events = []
    for line, raw in enumerate(path.read_text(encoding="utf-8").splitlines()):
        m = LINE.match(raw)
        g = OBS.match(m.group(3)) if m else None
        if not g or g[2] not in KIND_MAP:
            continue
        kind = KIND_MAP[g[2]]
        node, peer, uid, t, seq = int(g[1]), int(g[3]), int(g[4]), g[5], int(g[6])
        key = {"send": (node, peer, uid, t),
               "recv": (peer, node, uid, t),
               "discard": (peer, node, uid, "ELECTION")}.get(kind)
        ev = Event(node, line, kind, seq, key, peer=peer, uid=uid, mtype=t)
        if kind == "leader":
            ev.extra = "isLeader = true"
        events.append(ev)
    return events


def merge(events):
    """Ground truth: ordena por seq global."""
    return sorted(events, key=lambda e: (e.seq, e.node, e.line))


# ---------------------------------------------------------------- análise

def pair_messages(trace):
    """Emparelha send/recv pela chave (origem, destino, uid, tipo)."""
    sends = {e.msgid: i for i, e in enumerate(trace) if e.kind == "send"}
    pairs, warnings = {}, []
    for i, e in enumerate(trace):
        if e.kind != "recv":
            continue
        s = sends.get(e.msgid)
        if s is None:
            warnings.append(f"recv without matching send: node {e.node} <- {e.peer}, "
                            f"{e.mtype} {e.uid}")
            continue
        pairs[s] = i
    for s in sends.values():
        if s not in pairs:
            warnings.append(f"send without matching recv: {trace[s].node} -> {trace[s].peer}, "
                            f"{trace[s].mtype} {trace[s].uid}")
    return pairs, warnings


def find_illegal(trace):
    """Mesmas três verificações do Observer. Cada entrada é (índice do evento
    observado cedo demais, índice do evento que o devia preceder ou None se nunca observado)."""
    sends = {e.msgid: i for i, e in enumerate(trace) if e.kind == "send"}
    recvs = {e.msgid: i for i, e in enumerate(trace) if e.kind == "recv"}
    leaders = {e.node: i for i, e in enumerate(trace) if e.kind == "leader"}
    out = {"recv_before_send": [], "discard_before_recv": [], "elected_before_leader": []}
    for i, e in enumerate(trace):
        if e.kind == "recv":
            s = sends.get(e.msgid)
            if s is None or i < s:
                out["recv_before_send"].append((i, s))
        elif e.kind == "discard":
            r = recvs.get(e.msgid)
            if r is None or i < r:
                out["discard_before_recv"].append((i, r))
        elif e.kind == "send" and e.mtype == "ELECTED":
            p = leaders.get(e.uid)
            if p is None or i < p:
                out["elected_before_leader"].append((i, p))
    return out


def illegal_indices(illegal):
    return {i for lst in illegal.values() for i, _ in lst}


def local_inversions(trace):
    by_node = defaultdict(list)
    for e in trace:
        by_node[e.node].append(e.seq)
    return sum(1 for seqs in by_node.values()
               for a in range(len(seqs)) for b in range(a + 1, len(seqs)) if seqs[a] > seqs[b])


# ---------------------------------------------------------------- texto

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


def write_trace(trace, path: Path, bad_idx=frozenset()):
    lines = [f"{i + 1:>3} {'!' if i in bad_idx else ' '} seq={e.seq:<4} node {e.node:<4} {describe(e)}"
             for i, e in enumerate(trace)]
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def summarize(merged, nodes):
    sends = [e for e in merged if e.kind == "send"]
    n_elec = sum(e.mtype == "ELECTION" for e in sends)
    n_elct = sum(e.mtype == "ELECTED" for e in sends)
    n_disc = sum(e.kind == "discard" for e in merged)
    leaders = [e.node for e in merged if e.kind == "leader"]
    agreed = {e.uid for e in merged if e.kind == "elected"}

    n_state = sum(e.kind == "maxid" for e in merged)
    print(f"Nodes: {sorted(nodes)}   events: {len(merged)} "
          f"({len(merged) - n_state} reportable to the observer + {n_state} internal maxId)")
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


def summarize_observer(observed, merged, illegal):
    n_rep = sum(e.kind != "maxid" for e in merged)
    print(f"\nObserver: {len(observed)} of {n_rep} reportable events observed")
    print(f"  RECV before SEND:                   {len(illegal['recv_before_send'])}")
    print(f"  DISCARD before RECV:                {len(illegal['discard_before_recv'])}")
    print(f"  ELECTED SEND before BECOME_LEADER:  {len(illegal['elected_before_leader'])}")
    print(f"  local-order inversions (same node): {local_inversions(observed)}")
    truth = {e.seq for e in merged if e.kind != "maxid"}   # maxid não é reportado ao observador
    seen = {e.seq for e in observed}
    miss, extra = sorted(truth - seen), sorted(seen - truth)
    if miss:
        print(f"  Events in node logs but not observed: seq {miss}")
    if extra:
        print(f"  Observed events not in node logs: seq {extra}")
    if not miss and not extra:
        print("  Completeness: OK (observer saw every event)")


# ---------------------------------------------------------------- HTML

CSS = """
:root{--bg:#fff;--fg:#222;--mut:#999;--elec:#2b6cb0;--elct:#2f855a;--bad:#c53030;--gold:#b7791f;--ill:#6b46c1}
@media (prefers-color-scheme:dark){:root{--bg:#1a1a1a;--fg:#e5e5e5;--mut:#777;
--elec:#63b3ed;--elct:#68d391;--bad:#fc8181;--gold:#f6ad55;--ill:#b794f4}}
body{background:var(--bg);color:var(--fg);font-family:system-ui,sans-serif;margin:16px}
.wrap{overflow-x:auto}
svg text{fill:var(--fg);font-size:12px}
.hdr{font-weight:600;font-size:13px}
.life{stroke:var(--mut);stroke-width:1;stroke-dasharray:3 4}
.arrow{stroke-width:1.6;fill:none}
.elec{stroke:var(--elec)}.elct{stroke:var(--elct)}
.rep{stroke:var(--bad);stroke-width:1;stroke-opacity:.55;fill:none}
.rep-ill{stroke-width:2.2;stroke-opacity:1}
.rep-dot{fill:var(--bad)}
.ill{fill:var(--ill)!important;font-weight:600}
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


REASON = {"recv_before_send": "RECV before its SEND",
          "discard_before_recv": "DISCARD before its RECV",
          "elected_before_leader": "ELECTED sent before BECOME_LEADER"}


def illegal_reasons(illegal):
    out = {}
    for key, lst in illegal.items():
        for i, other in lst:
            out.setdefault(i, []).append(REASON[key] + (" [counterpart missing]" if other is None else ""))
    return out


def short(e: Event) -> str:
    tag = "E" if e.mtype == "ELECTION" else "L"
    if e.kind == "send":
        return f"n{e.node} send→{e.peer} {tag}({e.uid})"
    if e.kind == "recv":
        return f"n{e.node} recv←{e.peer} {tag}({e.uid})"
    if e.kind == "discard":
        return f"n{e.node} discard {e.uid}"
    if e.kind == "leader":
        return f"n{e.node} LEADER"
    return f"n{e.node} elected {e.uid}"


def render_svg(trace, pairs, order, observed=None, illegal=None, pid="g"):
    """Um só diagrama: lifelines dos nós (tempo real, por seq) + lifeline do observador à direita.
    Cada evento reportado é uma seta vermelha do nó para o observador; as linhas do observador
    seguem a ordem de chegada."""
    COL, LEFT, TOP, ROW, DROP, LBL = 180, 90, 80, 28, 14, 420
    xs = {n: LEFT + i * COL for i, n in enumerate(order)}
    ys = [TOP + (i + 1) * ROW for i in range(len(trace))]
    x_obs = LEFT + len(order) * COL

    oy, src_of = [], []
    if observed:
        seq_idx = {e.seq: i for i, e in enumerate(trace) if e.kind != "maxid"}
        prev = TOP
        for e in observed:
            src = seq_idx.get(e.seq)
            y = prev + ROW
            if src is not None:
                y = max(y, ys[src] + DROP)      # a seta desce sempre: o reporte tem atraso
            oy.append(y)
            src_of.append(src)
            prev = y
    H = max(ys + oy + [TOP]) + 2 * ROW
    W = (x_obs + LBL) if observed else (2 * LEFT + (len(order) - 1) * COL)
    reasons = illegal_reasons(illegal) if illegal else {}

    o = [f'<svg xmlns="http://www.w3.org/2000/svg" width="{W}" height="{H}" viewBox="0 0 {W} {H}">']
    for mid, cls in ((f"{pid}-elec", "elec"), (f"{pid}-elct", "elct"), (f"{pid}-bad", "bad")):
        o.append(f'<defs><marker id="{mid}" markerWidth="8" markerHeight="8" refX="7" refY="4" markerUnits="userSpaceOnUse" '
                 f'orient="auto"><path d="M0,0 L8,4 L0,8 z" style="fill:var(--{cls})"/></marker></defs>')
    for n in order:
        o.append(f'<text x="{xs[n]}" y="{TOP - 34}" class="hdr" text-anchor="middle">node {n}</text>')
        o.append(f'<line x1="{xs[n]}" y1="{TOP - 20}" x2="{xs[n]}" y2="{H - 10}" class="life"/>')
    if observed:
        o.append(f'<text x="{x_obs}" y="{TOP - 34}" class="hdr" text-anchor="middle">observer</text>')
        o.append(f'<line x1="{x_obs}" y1="{TOP - 20}" x2="{x_obs}" y2="{H - 10}" class="life"/>')

    for s, r in pairs.items():
        a, b = trace[s], trace[r]
        cls = "elec" if a.mtype == "ELECTION" else "elct"
        x1, y1, x2, y2 = xs[a.node], ys[s], xs[b.node], ys[r]
        o.append(f'<line x1="{x1}" y1="{y1}" x2="{x2}" y2="{y2}" class="arrow {cls}" '
                 f'marker-end="url(#{pid}-{cls})"/>')
        tag = "E" if a.mtype == "ELECTION" else "L"
        o.append(f'<text x="{(x1 + x2) / 2}" y="{(y1 + y2) / 2 - 3}" class="lbl" '
                 f'text-anchor="middle">{tag}({a.uid})</text>')

    for k, e in enumerate(observed or []):                  # reportes ao observador
        src = src_of[k]
        if src is not None:
            cls = "rep rep-ill" if k in reasons else "rep"
            o.append(f'<line x1="{xs[trace[src].node]}" y1="{ys[src]}" x2="{x_obs}" y2="{oy[k]}" '
                     f'class="{cls}" marker-end="url(#{pid}-bad)"/>')
        txt = escape(f"#{k + 1} {short(e)}")
        extra = (f'<tspan class="ill" dx="10">⚠ {escape("; ".join(reasons[k]))}</tspan>'
                 if k in reasons else "")
        o.append(f'<text x="{x_obs + 10}" y="{oy[k] + 4}">{txt}{extra}</text>')

    for i, e in enumerate(trace):
        x, y = xs[e.node], ys[i]
        lab = "" if e.kind == "maxid" else e.seq
        o.append(f'<text x="{x - 9}" y="{y + 3}" class="clk" text-anchor="end">{lab}</text>')
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
    return "".join(o)


def render_html(svg, title, with_observer):
    legend = ("E(uid) = ELECTION &nbsp; L(uid) = ELECTED &nbsp; number on the left = true seq "
              "&nbsp; ● send/recv &nbsp; ■ state change &nbsp; ✕ discard")
    if with_observer:
        legend += ("<br>red arrow = event reported to the observer (rows #k = arrival order) "
                   "&nbsp; ⚠ = illegal in that arrival order (thick arrow)")
    return (f'<!doctype html><html lang="en"><head><meta charset="utf-8"><title>{escape(title)}</title>'
            f'<style>{CSS}</style></head><body><h3>{escape(title)}</h3>'
            f'<p class="note">{legend}</p><div class="wrap">{svg}</div></body></html>')


# ---------------------------------------------------------------- main

def main():
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8")
    ap = argparse.ArgumentParser(description="Merge per-node LCR logs and the observer log into traces.")
    ap.add_argument("log_dir", nargs="?", default="logs",
                    help="directory with node-<uid>.log and observer.log (default: logs)")
    ap.add_argument("--out", default="trace_out", help="output directory (default: trace_out)")
    args = ap.parse_args()

    log_dir, out = Path(args.log_dir), Path(args.out)
    events = load(log_dir)
    if not events:
        sys.exit(f"No events found in '{log_dir}' (expected node-<uid>.log files).")

    merged = merge(events)
    pairs, warnings = pair_messages(merged)
    order = ring_order(merged)

    out.mkdir(parents=True, exist_ok=True)
    write_trace(merged, out / "merged.txt")

    obs_path = log_dir / "observer.log"
    observed = load_observer(obs_path) if obs_path.exists() else []
    illegal = None
    if observed:
        o_pairs, o_warnings = pair_messages(observed)
        warnings += [f"[observer] {w}" for w in o_warnings]
        illegal = find_illegal(observed)
        write_trace(observed, out / "observed.txt", illegal_indices(illegal))
    elif obs_path.exists():
        warnings.append(f"{obs_path} has no parsable observer events")

    svg = render_svg(merged, pairs, order, observed, illegal)
    (out / "trace.html").write_text(render_html(svg, f"LCR trace — {log_dir}", bool(observed)),
                                    encoding="utf-8")

    summarize(merged, {e.node for e in merged})
    if illegal is not None:
        summarize_observer(observed, merged, illegal)
    for w in warnings:
        print("WARNING:", w)
    written = f"{out / 'merged.txt'}, {out / 'trace.html'}" + (f", {out / 'observed.txt'}" if observed else "")
    print(f"\nWritten: {written}")


if __name__ == "__main__":
    main()