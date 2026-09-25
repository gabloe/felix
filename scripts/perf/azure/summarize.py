#!/usr/bin/env python3
"""Reduce a session's cell directories to one row per cell and one per group.

    summarize.py <results-dir>

Reads <results-dir>/cells/*/ as written by cells.sh and writes:

  cells.csv   one row per trial: build, knobs, broker-side throughput and
              storage counters, client-side numbers, CPU, UDP errors,
              per-listener datagram share
  summary.md  trials of a cell grouped (name minus -tN): mean and spread

Broker append MB/s is the headline throughput. The loadgen's throughput is
an enqueue rate for fire-and-forget publishes and is kept beside it, not
instead of it. Standard library only, so it runs anywhere the drivers do.
"""

import csv
import json
import re
import statistics
import sys
from pathlib import Path

BASE_KNOBS = {
    "FELIX_QUIC_LISTENERS": "listeners",
    "FELIX_IO_RUNTIME_THREADS": "io_threads",
    "FELIX_DURABLE_FSYNC_MODE": "fsync",
    "FELIX_ACK_ON_COMMIT": "ack_on_commit",
    "FELIX_STORAGE_IO_URING": "io_uring",
    "FELIX_PUB_INGRESS_WAIT": "ingress_wait",
}
CLIENT_PORTS = range(5000, 5064)


def kv_lines(path):
    out = {}
    if not path.exists():
        return out
    for line in path.read_text(errors="replace").splitlines():
        line = line.strip()
        if "=" not in line or line.startswith(("!!", ">>")):
            continue
        key, value = line.split("=", 1)
        if " " in key:
            continue
        out[key] = value
    return out


def num(d, key):
    try:
        return float(d[key])
    except (KeyError, ValueError):
        return None


def delta(before, after, key):
    a, b = num(after, key), num(before, key)
    if a is None or b is None:
        return None
    # A counter that went backwards means the broker restarted mid-cell.
    return a - b if a >= b else None


def loadgen_json(path):
    for line in reversed(path.read_text(errors="replace").splitlines()):
        if line.startswith("LOADGEN_JSON "):
            try:
                return json.loads(line[len("LOADGEN_JSON "):])
            except json.JSONDecodeError:
                return None
    return None


def parse_args(args):
    toks = args.split()
    flags = {}
    for i, t in enumerate(toks):
        if t.startswith("--"):
            nxt = toks[i + 1] if i + 1 < len(toks) and not toks[i + 1].startswith("--") else "1"
            flags[t[2:]] = nxt
    return flags


def fsum(values):
    vals = [v for v in values if v is not None]
    return sum(vals) if vals else None


def fmax(values):
    vals = [v for v in values if v is not None]
    return max(vals) if vals else None


def cell_row(cdir):
    meta = kv_lines(cdir / "meta.env")
    if not meta:
        return None
    row = {"cell": cdir.name, "done": (cdir / "done").exists()}
    m = re.match(r"^(.*)-t(\d+)$", cdir.name)
    row["group"], row["trial"] = (m.group(1), int(m.group(2))) if m else (cdir.name, 1)
    row["ref"] = meta.get("ref", "")
    flags = parse_args(meta.get("args", ""))
    gens = meta.get("generators", "").split()
    row["generators"] = len(gens)
    for f in ("scenario", "stream", "payload-bytes", "batch", "concurrency", "keys", "fanout"):
        row[f.replace("-", "_")] = flags.get(f, "")
    row["loadgen_env"] = meta.get("loadgen_env", "").strip()
    overrides = {k[len("override."):]: v for k, v in meta.items() if k.startswith("override.")}
    for env, col in BASE_KNOBS.items():
        row[col] = overrides.get(env, "")
    row["other_overrides"] = " ".join(f"{k}={v}" for k, v in sorted(overrides.items()) if k not in BASE_KNOBS)

    brokers = sorted({p.name.split(".")[0] for p in cdir.glob("felixperf-broker-*.before.txt")})
    shas, appends, peaks, gbytes, syncs, sdur_s, sdur_c, bsum, bcnt = [], [], [], [], [], [], [], [], []
    fails, quorum, fwd, busy, cores, si, rcvbuf, inerr, active = [], [], [], [], [], [], [], [], []
    ports = {}
    listen = []
    for b in brokers:
        before = kv_lines(cdir / f"{b}.before.txt")
        after = kv_lines(cdir / f"{b}.after.txt")
        ref = before.get("bin.broker.ref", "")
        shas.append(ref.split()[1][:12] if len(ref.split()) > 1 else "")
        listen.append(before.get("listen.ports", ""))
        appends.append(num(after, "s.append_mb_s"))
        peaks.append(num(after, "s.append_peak_mb_s"))
        d = delta(before, after, "m.append_bytes")
        gbytes.append(d / 1e9 if d is not None else None)
        syncs.append(delta(before, after, "m.sync_total"))
        sdur_s.append(delta(before, after, "m.sync_dur_sum"))
        sdur_c.append(delta(before, after, "m.sync_dur_count"))
        bsum.append(delta(before, after, "m.sync_batch_sum"))
        bcnt.append(delta(before, after, "m.sync_batch_count"))
        fails.append(delta(before, after, "m.sync_failures"))
        quorum.append(delta(before, after, "m.quorum_failed"))
        fwd.append(delta(before, after, "m.forwarded"))
        active.append(num(after, "m.append_series_active"))
        busy.append(num(after, "s.cpu_busy"))
        cores.append(num(after, "s.proc_cores"))
        si.append(num(after, "s.cpu_si"))
        rcvbuf.append(delta(before, after, "udp.RcvbufErrors"))
        inerr.append(delta(before, after, "udp.InErrors"))
        for key in after:
            mp = re.match(r"^port\.(\d+)\.pkts$", key)
            if mp and int(mp.group(1)) in CLIENT_PORTS:
                d = delta(before, after, key)
                if d:
                    ports[int(mp.group(1))] = ports.get(int(mp.group(1)), 0) + d
    row["build_sha"] = ",".join(sorted(set(s for s in shas if s)))
    row["listen_ports"] = "|".join(listen)
    row["broker_append_mb_s"] = fsum(appends)
    row["broker_append_peak_mb_s"] = fsum(peaks)
    row["broker_append_gb"] = fsum(gbytes)
    row["active_logs"] = fsum(active)
    row["syncs"] = fsum(syncs)
    sc = fsum(sdur_c)
    row["sync_mean_ms"] = (fsum(sdur_s) / sc * 1e3) if sc else None
    bc = fsum(bcnt)
    row["sync_fanin"] = (fsum(bsum) / bc) if bc else None
    row["sync_failures"] = fsum(fails)
    row["quorum_failed"] = fsum(quorum)
    row["forwarded"] = fsum(fwd)
    row["broker_cpu_busy_max"] = fmax(busy)
    row["broker_proc_cores"] = fsum(cores)
    row["broker_cpu_si_max"] = fmax(si)
    row["udp_rcvbuf_errors"] = fsum(rcvbuf)
    row["udp_in_errors"] = fsum(inerr)
    total = sum(ports.values())
    row["port_share"] = " ".join(f"{p}:{100 * c / total:.0f}%" for p, c in sorted(ports.items())) if total else ""
    row["port_imbalance"] = (max(ports.values()) / min(ports.values())) if len(ports) > 1 else None

    client_mb, client_msg, p50, p99, dp50, lcpu = [], [], None, None, None, []
    for g in gens:
        j = loadgen_json(cdir / f"{g}.run.txt") if (cdir / f"{g}.run.txt").exists() else None
        lcpu.append(num(kv_lines(cdir / f"{g}.after.txt"), "s.cpu_busy"))
        if not j:
            continue
        client_mb.append(j.get("throughput_mb_s"))
        client_msg.append(j.get("throughput_msg_s", j.get("publish_throughput_msg_s")))
        if p50 is None:
            lat = j.get("ack_latency_us") or ((j.get("put") or {}).get("latency_us"))
            if lat:
                p50, p99 = lat.get("p50"), lat.get("p99")
            dl = j.get("delivery_latency_us")
            if dl:
                dp50 = dl.get("p50")
    row["client_mb_s"] = fsum(client_mb)
    row["client_msg_s"] = fsum(client_msg)
    row["lat_p50_us"], row["lat_p99_us"], row["delivery_p50_us"] = p50, p99, dp50
    row["loadgen_cpu_busy_max"] = fmax(lcpu)
    row["results"] = sum(1 for g in gens if (cdir / f"{g}.run.txt").exists() and loadgen_json(cdir / f"{g}.run.txt"))
    return row


def fmt(v, digits=1):
    if v is None or v == "":
        return "-"
    if isinstance(v, float):
        return f"{v:.{digits}f}"
    return str(v)


def spread(vals):
    vals = [v for v in vals if v is not None]
    if not vals:
        return None, None
    mean = statistics.mean(vals)
    return mean, (100 * (max(vals) - min(vals)) / mean if mean and len(vals) > 1 else None)


def main():
    root = Path(sys.argv[1] if len(sys.argv) > 1 else ".")
    rows = [r for r in (cell_row(d) for d in sorted((root / "cells").glob("*")) if d.is_dir()) if r]
    if not rows:
        print(f"no cells under {root}/cells")
        return
    cols = list(rows[0].keys())
    with open(root / "cells.csv", "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=cols)
        w.writeheader()
        for r in rows:
            w.writerow({k: fmt(v, 3) if isinstance(v, float) else v for k, v in r.items()})

    groups = {}
    for r in rows:
        groups.setdefault(r["group"], []).append(r)
    lines = [f"# {root.name}", ""]
    sj = root / "session.json"
    if sj.exists():
        s = json.loads(sj.read_text())
        lines += [
            f"Session `{s.get('session')}`, tier {s.get('tier')}, {s.get('location')}; "
            f"brokers {s.get('broker_vm_size')} x{len((s.get('broker_ips') or '').split(','))}, "
            f"generators {s.get('loadgens')}; builds: {', '.join(s.get('builds') or []) or s.get('release_url') or s.get('release_tag')}; "
            f"loadgen {s.get('loadgen_spec')}; RTT ms {s.get('rtt_ms')}.",
            "",
        ]
    fio = []
    for p in sorted((root / "system").glob("*.fio.txt")):
        kv = kv_lines(p)
        for k, v in kv.items():
            if k.startswith("fio."):
                fio.append((p.name.split(".")[0], k[4:], v))
    if fio:
        lines += ["## Device baseline (fio, buffered write + fdatasync)", "", "| host | test | value |", "|---|---|---|"]
        lines += [f"| {h} | {k} | {v} |" for h, k, v in fio]
        lines.append("")
    lines += [
        "## Cells",
        "",
        "Broker MB/s is appended bytes (header included) over the window the counter moved, "
        "summed over brokers; spread is (max-min)/mean over trials.",
        "",
        "| cell | n | ref | knobs | broker MB/s (spread) | client MB/s | p50 / p99 us | sync ms | fan-in | broker CPU % | gen CPU % | rcvbuf err | ports |",
        "|---|---|---|---|---|---|---|---|---|---|---|---|---|",
    ]
    for g, rs in groups.items():
        ok = [r for r in rs if r["done"]]
        use = ok or rs
        bm, bs = spread([r["broker_append_mb_s"] for r in use])
        cm, _ = spread([r["client_mb_s"] for r in use])
        p50, _ = spread([r["lat_p50_us"] for r in use])
        p99, _ = spread([r["lat_p99_us"] for r in use])
        sm, _ = spread([r["sync_mean_ms"] for r in use])
        fi, _ = spread([r["sync_fanin"] for r in use])
        cpu, _ = spread([r["broker_cpu_busy_max"] for r in use])
        lcpu, _ = spread([r["loadgen_cpu_busy_max"] for r in use])
        rb = fsum([r["udp_rcvbuf_errors"] for r in use])
        r0 = use[0]
        knobs = " ".join(
            f"{c}={r0[c]}" for c in ("listeners", "io_threads", "fsync", "ack_on_commit", "io_uring", "ingress_wait") if r0[c]
        )
        if r0["other_overrides"]:
            knobs += " " + r0["other_overrides"]
        if r0["loadgen_env"]:
            knobs += " client:" + r0["loadgen_env"]
        lines.append(
            f"| {g} | {len(ok)}/{len(rs)} | {r0['ref']} {r0['build_sha']} | {knobs} | "
            f"{fmt(bm)} ({fmt(bs, 0)}%) | {fmt(cm)} | {fmt(p50, 0)} / {fmt(p99, 0)} | {fmt(sm, 2)} | {fmt(fi)} | "
            f"{fmt(cpu, 0)} | {fmt(lcpu, 0)} | {fmt(rb, 0)} | {r0['port_share']} |"
        )
    lines.append("")
    (root / "summary.md").write_text("\n".join(lines))
    print(f">> {root / 'cells.csv'} ({len(rows)} rows), {root / 'summary.md'} ({len(groups)} groups)")


if __name__ == "__main__":
    main()
