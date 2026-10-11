#!/usr/bin/env python3
"""
check_legal.py - checks whether a history produced by the Observer (CSV) is legal.

Usage:
    python check_legal.py runs/observed-<seed>.csv
    python check_legal.py corrected.csv --reference runs/observed-<seed>.csv
    python check_legal.py runs/truth-<seed>.csv --selftest

Expected columns: pos,kind,node,peer,msgId,nodeIdToSend,type,trueSeq,localSeq
The history order is the row order (the pos column is ignored).
trueSeq is NOT used: the checker only relies on local information (localSeq, msgId).

A history is legal if it is a linear extension of the happens-before order, which is
the transitive closure of two generating relations:
  (a) program order: within each node, localSeq increasing;
  (b) message order: SEND(m) before RECV(m).
It is enough to check the generating relations: the transitive closure follows.

Local state events (DISCARD, MAX_ID_UPDATE, LEADER) take part in the history like any
other event. On top of program order, DISCARD and MAX_ID_UPDATE are tied to the RECV that
caused them (same msgId), and MAX_ID_UPDATE must raise the node's maxId (it starts as the
node's own id, which is the value of the node column).

Exit code: 0 = legal, 1 = illegal, 2 = selftest failed.
"""
import argparse
import csv
import random
import sys
from collections import Counter, defaultdict


def load(path):
    with open(path, newline="") as f:
        return [
            {
                "kind": r["kind"],
                "node": int(r["node"]),
                "peer": int(r["peer"]),
                "msg": int(r["msgId"]),
                "to_send": int(r["nodeIdToSend"]),
                "type": r["type"],
                "local": int(r["localSeq"]),
            }
            for r in csv.DictReader(f)
        ]


def check(history, reference=None):
    """Returns (errors, warnings). The history is legal <=> the error list is empty."""
    evs = [dict(e, idx=i) for i, e in enumerate(history)]
    errs, warns = [], []

    def err(rule, text):
        errs.append((rule, text))

    # 1. Identity: (node, localSeq) identifies an event and may appear only once.
    seen = {}
    for e in evs:
        k = (e["node"], e["local"])
        if k in seen:
            err("DUPLICATE",
                f"event (node {k[0]}, localSeq {k[1]}) at positions {seen[k]} and {e['idx']}")
        else:
            seen[k] = e["idx"]

    per_node = defaultdict(list)
    for e in evs:
        per_node[e["node"]].append(e)

    for node, event_list in sorted(per_node.items()):
        # 2. localSeq contiguous from 0 (a gap means a missing event).
        locs = {e["local"] for e in event_list}
        missing = sorted(set(range(max(locs) + 1)) - locs)
        if missing:
            more = "..." if len(missing) > 10 else ""
            err("INCOMPLETE", f"node {node}: missing localSeq values {missing[:10]}{more}")

        # 3. Program order: within a node, localSeq must not decrease.
        for a, b in zip(event_list, event_list[1:]):
            if b["local"] < a["local"]:
                err("PROGRAM-ORDER",
                    f"node {node}: localSeq {b['local']} ({b['kind']}, pos {b['idx']}) "
                    f"appears after localSeq {a['local']} ({a['kind']}, pos {a['idx']})")

        # 4. A node's receives follow msgId order (FIFO mailbox).
        rs = [e for e in event_list if e["kind"] == "RECV"]
        for a, b in zip(rs, rs[1:]):
            if b["msg"] < a["msg"]:
                err("FIFO",
                    f"node {node}: RECV msgId {b['msg']} (pos {b['idx']}) "
                    f"after RECV msgId {a['msg']} (pos {a['idx']})")

    # 5. Messages: SEND before RECV, and consistency between the two.
    sends, recvs, discards = {}, {}, []
    for e in evs:
        if e["kind"] == "SEND":
            if e["msg"] in sends:
                err("MESSAGE", f"msgId {e['msg']}: more than one SEND (pos {sends[e['msg']]['idx']} and {e['idx']})")
            sends.setdefault(e["msg"], e)
        elif e["kind"] == "RECV":
            if e["msg"] in recvs:
                err("MESSAGE", f"msgId {e['msg']}: more than one RECV (pos {recvs[e['msg']]['idx']} and {e['idx']})")
            recvs.setdefault(e["msg"], e)
        elif e["kind"] == "DISCARD":
            discards.append(e)

    for m, r in sorted(recvs.items()):
        s = sends.get(m)
        if s is None:
            err("MESSAGE", f"RECV of msgId {m} (pos {r['idx']}) has no SEND")
            continue
        if s["idx"] > r["idx"]:
            err("SEND-BEFORE-RECV",
                f"msgId {m}: RECV at pos {r['idx']} is before its SEND at pos {s['idx']}")
        if (s["node"], s["peer"], s["to_send"], s["type"]) != (r["peer"], r["node"], r["to_send"], r["type"]):
            err("INCONSISTENT", f"msgId {m}: SEND and RECV fields do not match")

    for m in sorted(set(sends) - set(recvs)):
        warns.append(f"msgId {m} was sent but never received "
                     f"(acceptable if the run ended with the message in transit)")

    # 6. DISCARD: only after the RECV of the same message, on the same node.
    for d in discards:
        r = recvs.get(d["msg"])
        if r is None or r["node"] != d["node"]:
            err("DISCARD", f"DISCARD of msgId {d['msg']} (pos {d['idx']}) has no RECV on node {d['node']}")
        elif r["idx"] > d["idx"]:
            err("DISCARD", f"DISCARD of msgId {d['msg']} (pos {d['idx']}) is before its RECV (pos {r['idx']})")

    # 7. MAX_ID_UPDATE: a node only raises its maxId upon receiving, from its predecessor,
    #    a message carrying a larger id. The new maxId (nodeIdToSend) must match the id in
    #    the RECV that caused it, the RECV must come first, and maxId must strictly increase
    #    (it starts as the node's own id).
    for node, event_list in sorted(per_node.items()):
        cur = node
        updates = sorted((x for x in event_list if x["kind"] == "MAX_ID_UPDATE"), key=lambda x: x["local"])
        for e in updates:
            r = recvs.get(e["msg"])
            if r is None or r["node"] != node:
                err("MAX-ID", f"node {node}: MAX_ID_UPDATE to {e['to_send']} (pos {e['idx']}) "
                              f"has no RECV of msgId {e['msg']} on this node")
            else:
                if r["idx"] > e["idx"]:
                    err("MAX-ID", f"node {node}: MAX_ID_UPDATE to {e['to_send']} (pos {e['idx']}) "
                                  f"is before its RECV (pos {r['idx']})")
                if r["to_send"] != e["to_send"]:
                    err("MAX-ID", f"node {node}: MAX_ID_UPDATE to {e['to_send']} (pos {e['idx']}) "
                                  f"does not match the id {r['to_send']} carried by msgId {e['msg']}")
            if e["to_send"] <= cur:
                err("MAX-ID", f"node {node}: maxId must increase, but went {cur} -> {e['to_send']} "
                              f"(pos {e['idx']})")
            cur = max(cur, e["to_send"])

    # 8. Optional: the history must be a permutation of the reference history.
    if reference is not None:
        def sig(l):
            return Counter((e["node"], e["local"], e["kind"], e["msg"]) for e in l)
        got, want = sig(history), sig(reference)
        for k in want - got:
            err("NOT-A-PERMUTATION", f"missing event (node, localSeq, kind, msgId) = {k}")
        for k in got - want:
            err("NOT-A-PERMUTATION", f"extra event (node, localSeq, kind, msgId) = {k}")

    return errs, warns


def report(history, errs, warns, max_errors):
    n_nodes = len({e["node"] for e in history})
    n_msgs = len({e["msg"] for e in history if e["kind"] == "SEND"})
    if not errs:
        print(f"LEGAL ({len(history)} events, {n_nodes} nodes, {n_msgs} messages)")
    else:
        print(f"ILLEGAL - {len(errs)} violations ({len(history)} events, {n_nodes} nodes, {n_msgs} messages)")
        for rule, c in Counter(r for r, _ in errs).most_common():
            print(f"  {rule}: {c}")
        print()
        for rule, text in errs[:max_errors]:
            print(f"  [{rule}] {text}")
        if len(errs) > max_errors:
            print(f"  ... (+{len(errs) - max_errors} more; use --max-errors)")
    for w in warns:
        print(f"WARNING: {w}")


def selftest(base):
    """Validates the checker itself: the legal base passes, illegal mutations are caught,
    and harmless swaps do not produce false positives."""
    errs, _ = check(base)
    if errs:
        print("The base history must be legal (use truth-<seed>.csv). Errors:")
        for rule, text in errs[:10]:
            print(f"  [{rule}] {text}")
        return 2

    rnd = random.Random(0)
    failed = 0

    def expect(name, hist, rule, reference=None):
        nonlocal failed
        got, _ = check(hist, reference)
        ok = any(r == rule for r, _ in got)
        print(f"{'OK    ' if ok else 'FAILED'} {name} (expected {rule})")
        failed += 0 if ok else 1

    def skip(name):
        print(f"SKIP   {name} (the base history has no such case)")

    sends = {e["msg"]: i for i, e in enumerate(base) if e["kind"] == "SEND"}
    recvs = {e["msg"]: i for i, e in enumerate(base) if e["kind"] == "RECV"}
    by_node = defaultdict(list)
    for i, e in enumerate(base):
        by_node[e["node"]].append(i)

    # M1: RECV placed immediately before its SEND
    both = [m for m in recvs if m in sends]
    if both:
        m = rnd.choice(both)
        h = list(base)
        h.insert(sends[m], h.pop(recvs[m]))
        expect("RECV before its SEND", h, "SEND-BEFORE-RECV")
    else:
        skip("RECV before its SEND")

    # M2: two events of the same node swapped
    multi = [idxs for idxs in by_node.values() if len(idxs) >= 2]
    if multi:
        idxs = rnd.choice(multi)
        h = list(base)
        h[idxs[0]], h[idxs[-1]] = h[idxs[-1]], h[idxs[0]]
        expect("two events of the same node swapped", h, "PROGRAM-ORDER")
    else:
        skip("two events of the same node swapped")

    # M3: duplicated event
    h = list(base)
    h.insert(1, dict(base[0]))
    expect("duplicated event", h, "DUPLICATE")

    # M4: a middle event of a node removed
    mid = [(i, e) for i, e in enumerate(base) if e["local"] == 1 and len(by_node[e["node"]]) >= 3]
    if mid:
        i, _ = rnd.choice(mid)
        h = base[:i] + base[i + 1:]
        expect("middle event removed", h, "INCOMPLETE")
    else:
        skip("middle event removed")

    # M5: DISCARD before its RECV
    ds = [(i, e) for i, e in enumerate(base) if e["kind"] == "DISCARD" and e["msg"] in recvs]
    if ds:
        i, e = rnd.choice(ds)
        h = list(base)
        j = recvs[e["msg"]]
        h[i], h[j] = h[j], h[i]
        expect("DISCARD before its RECV", h, "DISCARD")
    else:
        skip("DISCARD before its RECV")

    # M5b: MAX_ID_UPDATE before the RECV that caused it
    ms = [(i, e) for i, e in enumerate(base) if e["kind"] == "MAX_ID_UPDATE" and e["msg"] in recvs]
    if ms:
        i, e = rnd.choice(ms)
        h = list(base)
        j = recvs[e["msg"]]
        h[i], h[j] = h[j], h[i]
        expect("MAX_ID_UPDATE before its RECV", h, "MAX-ID")

        # M5c: MAX_ID_UPDATE that does not raise maxId (new value = the node's own id)
        i, e = rnd.choice(ms)
        h = list(base)
        h[i] = dict(e, to_send=e["node"])
        expect("MAX_ID_UPDATE that does not raise maxId", h, "MAX-ID")
    else:
        skip("MAX_ID_UPDATE mutations")

    # M6: last event removed (only visible with a reference)
    expect("last event removed (with --reference)", base[:-1], "NOT-A-PERMUTATION", reference=base)

    # No false positives: swapping neighbours of different nodes without a direct edge
    # between them keeps the history legal.
    def harmless(a, b):
        return a["node"] != b["node"] and not (
                a["kind"] == "SEND" and b["kind"] == "RECV" and a["msg"] == b["msg"])

    cand = [i for i in range(len(base) - 1) if harmless(base[i], base[i + 1])]
    cand = rnd.sample(cand, min(len(cand), 500))
    bad = 0
    for i in cand:
        h = list(base)
        h[i], h[i + 1] = h[i + 1], h[i]
        if check(h)[0]:
            bad += 1
    print(f"{'OK    ' if bad == 0 else 'FAILED'} {len(cand)} harmless swaps keep the history legal")
    failed += 1 if bad else 0

    print("\nselftest:", "all OK" if failed == 0 else f"{failed} failure(s)")
    return 0 if failed == 0 else 2


def main():
    ap = argparse.ArgumentParser(description="Checks whether an Observer history is legal.")
    ap.add_argument("history", help="Observer CSV (observed-<seed>.csv, truth-<seed>.csv or legalizer output)")
    ap.add_argument("--reference", help="original CSV; the history must be a permutation of it")
    ap.add_argument("--max-errors", type=int, default=20, help="maximum number of violations to list")
    ap.add_argument("--selftest", action="store_true",
                    help="validate the checker itself, using the given history as the legal base")
    a = ap.parse_args()

    history = load(a.history)
    if a.selftest:
        sys.exit(selftest(history))

    reference = load(a.reference) if a.reference else None
    errs, warns = check(history, reference)
    report(history, errs, warns, a.max_errors)
    sys.exit(1 if errs else 0)


if __name__ == "__main__":
    main()