#!/usr/bin/env python3
"""
CRAFTS reprocessing - Step 0 (simple): file-count audit of the raw FAST data.

Uses file NAMES only (no FITS header reads, no file-size lookups), so it is
fast even on /aphid and needs nothing beyond standard Python 3.

Per observation (pointing / epoch dir / filename prefix) it reports:
  - files per beam, which of the 19 beams are present
  - file numbers missing in some beams (or all beams)
  - whether a pos.fits file exists for it (= processable now)
  - estimated observing time  (files x file_dur_s from the config)
  - the best files-per-chunk k for that epoch: among chunk lengths between
    min_chunk_s and max_chunk_s, an exact divisor of the file count if one
    exists (closest to target_chunk_s), otherwise the k leaving the fewest
    remainder files, preferring k near the target when the remainder is
    <= max_remainder_files. Remainder files are reported separately (not chunked).
  - every exact divisor in the allowed range, and the SLURM array size

Outputs in audit_dir:
  audit_summary.txt    human-readable table
  audit_epochs.csv     one row per observation
  audit_status.json    machine-readable, for the next pipeline step
  audit.log

Usage:
  python audit_simple.py -c crafts_config.ini
  python audit_simple.py -c crafts_config.ini --only Dec+3114_06_07
"""

import argparse
import configparser
import csv
import datetime as dt
import json
import math
import os
import re
import sys
import time
from collections import defaultdict


def log(msg, fh=None):
    line = f"{dt.datetime.now():%Y-%m-%d %H:%M:%S} {msg}"
    print(line, flush=True)
    if fh:
        fh.write(line + "\n")
        fh.flush()


def load_config(path):
    cp = configparser.ConfigParser(interpolation=None)
    if not cp.read(path):
        sys.exit(f"Cannot read config {path}")
    p, a = cp["paths"], cp["audit"]
    return {
        "raw_root": p.get("raw_root"),
        "raw_projects": [s.strip() for s in p.get("raw_projects").split(",") if s.strip()],
        "pos_dir": p.get("pos_dir"),
        "audit_dir": p.get("audit_dir"),
        "file_regex": a.get("file_regex"),
        "pos_regex": a.get("pos_regex"),
        "n_beams": a.getint("n_beams", 19),
        "file_dur_s": a.getfloat("file_dur_s", 12.885),
        "target_chunk_s": a.getfloat("target_chunk_s", 245.0),
        "min_chunk_s": a.getfloat("min_chunk_s", 120.0),
        "max_chunk_s": a.getfloat("max_chunk_s", 480.0),
        "max_remainder_files": a.getint("max_remainder_files", 3),
    }


def list_fits(pdir):
    """Recursively yield (relative_dir, filename) for *.fits using scandir (no stat)."""
    stack = [pdir]
    while stack:
        d = stack.pop()
        try:
            with os.scandir(d) as it:
                for e in it:
                    if e.is_dir(follow_symlinks=False):
                        stack.append(e.path)
                    elif e.name.endswith(".fits"):
                        rel = os.path.relpath(d, pdir)
                        yield ("" if rel == "." else rel), e.name
        except OSError as err:
            print(f"WARNING cannot list {d}: {err}", flush=True)


def obs_identifier(pointing, epoch_dir, prefix):
    tag = epoch_dir.replace(os.sep, "-") if epoch_dir else "nodate"
    pre = prefix[len(pointing):].lstrip("_-") if prefix.startswith(pointing) else prefix
    return f"{pointing}__{tag}" + (f"__{pre}" if pre else "")


def compress_ranges(nums, limit=8):
    """[1,2,3,7,9,10] -> '1-3,7,9-10' (truncated after `limit` ranges)."""
    nums = sorted(nums)
    out, i = [], 0
    while i < len(nums):
        j = i
        while j + 1 < len(nums) and nums[j + 1] == nums[j] + 1:
            j += 1
        out.append(f"{nums[i]}" if i == j else f"{nums[i]}-{nums[j]}")
        i = j + 1
    s = ",".join(out[:limit])
    return s + (f",...(+{len(out) - limit} more)" if len(out) > limit else "")


def best_chunking(n, cfg):
    """Return (k, n_chunks, remainder, exact, divisors_in_range, kmin, kmax)."""
    dur = cfg["file_dur_s"]
    kmin = max(1, math.ceil(cfg["min_chunk_s"] / dur))
    kmax = max(kmin, math.floor(cfg["max_chunk_s"] / dur))
    ktgt = cfg["target_chunk_s"] / dur
    if n <= 0:
        return 0, 0, 0, False, [], kmin, kmax
    if n < kmin:  # whole observation shorter than the shortest allowed chunk
        return n, 1, 0, True, [], kmin, kmax
    ks = range(kmin, min(kmax, n) + 1)
    divisors = [k for k in ks if n % k == 0]
    near = [k for k in ks if n % k <= cfg["max_remainder_files"]]
    if divisors:            # 1) exact divisor closest to target
        k = min(divisors, key=lambda k: abs(k - ktgt))
    elif near:              # 2) remainder within tolerance, closest to target
        k = min(near, key=lambda k: (abs(k - ktgt), n % k))
    else:                   # 3) fewest remainder files
        k = min(ks, key=lambda k: (n % k, abs(k - ktgt)))
    return k, n // k, n % k, n % k == 0, divisors, kmin, kmax


def analyse(obs_id, entries, cfg):
    """entries: list of (beam, idx)."""
    nb = cfg["n_beams"]
    by_beam = defaultdict(set)
    dup = 0
    for b, i in entries:
        if i in by_beam[b]:
            dup += 1
        by_beam[b].add(i)
    expected = set(range(1, nb + 1))
    present = sorted(b for b in by_beam if b in expected)
    missing_beams = sorted(expected - set(by_beam))
    extra_beams = sorted(set(by_beam) - expected)
    counts = {b: len(by_beam[b]) for b in present}

    all_idx = set().union(*(by_beam[b] for b in present)) if present else set()
    common = set.intersection(*(by_beam[b] for b in present)) if present else set()
    lo, hi = (min(all_idx), max(all_idx)) if all_idx else (0, 0)
    gaps_all = sorted(set(range(lo, hi + 1)) - all_idx)          # missing in every beam
    partial = sorted(all_idx - common)                            # missing in some beams
    partial_beams = sorted({b for b in present if by_beam[b] != all_idx})

    n_use = len(common)  # files usable in all present beams
    issues = []
    if missing_beams:
        issues.append("missing beams " + ",".join(f"M{b:02d}" for b in missing_beams))
    if extra_beams:
        issues.append(f"unexpected beams {extra_beams}")
    if counts and min(counts.values()) != max(counts.values()):
        issues.append(f"unequal files per beam ({min(counts.values())}-{max(counts.values())})")
    if partial:
        issues.append(f"{len(partial)} file numbers missing in some beams "
                      f"({','.join(f'M{b:02d}' for b in partial_beams[:6])}"
                      f"{'...' if len(partial_beams) > 6 else ''}): {compress_ranges(partial)}")
    if gaps_all:
        issues.append(f"{len(gaps_all)} file numbers missing in all beams: {compress_ranges(gaps_all)}")
    if lo != 1 and all_idx:
        issues.append(f"numbering starts at {lo}")
    if dup:
        issues.append(f"{dup} duplicate file numbers")

    k, nchk, rem, exact, divs, kmin, kmax = best_chunking(n_use, cfg)
    return {
        "obs_id": obs_id,
        "n_beams_present": len(present),
        "missing_beams": ";".join(f"M{b:02d}" for b in missing_beams),
        "files_total": len(entries),
        "files_per_beam_min": min(counts.values()) if counts else 0,
        "files_per_beam_max": max(counts.values()) if counts else 0,
        "first_idx": lo, "last_idx": hi,
        "files_all_beams": n_use,
        "n_missing_some_beams": len(partial),
        "n_missing_all_beams": len(gaps_all),
        "est_obs_time_min": round(n_use * cfg["file_dur_s"] / 60.0, 1),
        "files_per_chunk": k,
        "chunk_min": round(k * cfg["file_dur_s"] / 60.0, 2),
        "n_chunks": nchk,
        "remainder_files": rem,
        "exact_divide": exact,
        "divisors_in_range": ";".join(map(str, divs)),
        "k_range": f"{kmin}-{kmax}",
        "slurm_array": f"0-{nchk * len(present) - 1}" if nchk else "",
        "clean": not issues,
        "issues": issues,
    }


def scan_pos(cfg):
    rx = re.compile(cfg["pos_regex"])
    out = defaultdict(list)
    names = sorted(os.listdir(cfg["pos_dir"])) if os.path.isdir(cfg["pos_dir"]) else []
    for fn in names:
        m = rx.match(fn)
        if m:
            d = m.groupdict()
            out[(d["pointing"], d["date"])].append(fn)
    return out


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("-c", "--config", required=True)
    ap.add_argument("--only", default="", help="comma-separated pointings (for a quick test)")
    ap.add_argument("-o", "--audit-dir", default="", help="override audit_dir")
    args = ap.parse_args()
    cfg = load_config(args.config)
    if args.audit_dir:
        cfg["audit_dir"] = args.audit_dir
    out = cfg["audit_dir"]
    if os.path.abspath(out).startswith(os.path.abspath(cfg["raw_root"]) + os.sep):
        sys.exit("audit_dir is inside raw_root - refusing to write into the raw data tree")
    os.makedirs(out, exist_ok=True)
    lf = open(os.path.join(out, "audit.log"), "w")
    t0 = time.time()
    only = {s.strip() for s in args.only.split(",") if s.strip()}
    rx = re.compile(cfg["file_regex"])
    log(f"config {args.config} -> {out}", lf)

    results, unparsed = [], []
    for project in cfg["raw_projects"]:
        pdir_root = os.path.join(cfg["raw_root"], project)
        if not os.path.isdir(pdir_root):
            log(f"WARNING project dir missing: {pdir_root}", lf)
            continue
        for pointing in sorted(os.listdir(pdir_root)):
            pdir = os.path.join(pdir_root, pointing)
            if not os.path.isdir(pdir) or (only and pointing not in only):
                continue
            groups = defaultdict(list)
            meta = {}
            n = 0
            for rel, fn in list_fits(pdir):
                n += 1
                m = rx.match(fn)
                if not m:
                    unparsed.append(os.path.join(pdir, rel, fn))
                    continue
                oid = obs_identifier(pointing, rel, m.group("prefix"))
                groups[oid].append((int(m.group("beam")), int(m.group("idx"))))
                if oid not in meta:
                    d = re.search(r"(\d{8})", rel)
                    meta[oid] = {"project": project, "pointing": pointing, "epoch_dir": rel,
                                 "epoch_date": d.group(1) if d else "", "prefix": m.group("prefix")}
            for oid in sorted(groups):
                r = analyse(oid, groups[oid], cfg)
                r.update(meta[oid])
                results.append(r)
            log(f"  {project} {pointing:<22} {n:>7} fits  {len(groups)} observation(s)", lf)

    # ---- pos files
    pos = scan_pos(cfg)
    by_key = defaultdict(list)
    for r in results:
        by_key[(r["pointing"], r["epoch_date"])].append(r)
    used = set()
    for key, robs in by_key.items():
        cands = pos.get(key, [])
        for r in robs:
            r["pos_file"] = ""
            if len(cands) == 1:
                r["pos_file"] = cands[0]
                if len(robs) > 1:
                    r["issues"].append("pos file shared by several observations on this date - check")
            elif len(cands) > 1:
                # several pos files that day (e.g. _12_10 / _12_24): try to match HHMM in the prefix
                hit = [c for c in cands if re.search(r"_(\d{2})_(\d{2})_pos", c)
                       and "".join(re.search(r"_(\d{2})_(\d{2})_pos", c).groups()) in r["prefix"]]
                if len(hit) == 1:
                    r["pos_file"] = hit[0]
                else:
                    r["pos_file"] = ";".join(cands)
                    r["issues"].append("several pos files for this date - match by hand")
            used.update(cands)
            r["processable_now"] = bool(r["pos_file"]) and r["files_all_beams"] > 0
    unused_pos = sorted(fn for v in pos.values() for fn in v if fn not in used)
    if only:
        unused_pos = [p for p in unused_pos if any(p.startswith(o + "_") for o in only)]

    # ---- csv
    fields = ["obs_id", "project", "pointing", "epoch_dir", "epoch_date", "prefix", "processable_now",
              "pos_file", "n_beams_present", "missing_beams", "files_total", "files_per_beam_min",
              "files_per_beam_max", "first_idx", "last_idx", "files_all_beams", "n_missing_some_beams",
              "n_missing_all_beams", "est_obs_time_min", "files_per_chunk", "chunk_min", "n_chunks",
              "remainder_files", "exact_divide", "divisors_in_range", "k_range", "slurm_array",
              "clean", "issues"]
    with open(os.path.join(out, "audit_epochs.csv"), "w", newline="") as fh:
        w = csv.DictWriter(fh, fieldnames=fields, extrasaction="ignore")
        w.writeheader()
        for r in results:
            w.writerow({**r, "issues": " | ".join(r["issues"])})

    # ---- json
    with open(os.path.join(out, "audit_status.json"), "w") as fh:
        json.dump({"step": "audit_simple", "status": "completed",
                   "finished": dt.datetime.now().isoformat(timespec="seconds"),
                   "elapsed_s": round(time.time() - t0, 1), "config": cfg,
                   "observations": results, "pos_files_without_raw": unused_pos,
                   "unparsed_fits": unparsed}, fh, indent=1)

    # ---- summary
    now = [r for r in results if r["processable_now"]]
    L = ["=" * 110,
         f"CRAFTS RAW DATA AUDIT (file counts)   {dt.datetime.now():%Y-%m-%d %H:%M}   took {(time.time() - t0) / 60:.1f} min",
         "=" * 110,
         f"raw_root {cfg['raw_root']}   pos_dir {cfg['pos_dir']}",
         f"time estimate assumes {cfg['file_dur_s']} s per file; chunk length allowed {cfg['min_chunk_s']:.0f}-{cfg['max_chunk_s']:.0f} s, "
         f"target {cfg['target_chunk_s']:.0f} s",
         "",
         f"pointings              : {len({r['pointing'] for r in results})}",
         f"observations           : {len(results)}   (clean: {sum(r['clean'] for r in results)})",
         f"fits files             : {sum(r['files_total'] for r in results)}",
         f"est. observing time    : {sum(r['est_obs_time_min'] for r in results) / 60:.1f} h (per beam)",
         f"processable now (pos)  : {len(now)} obs, {sum(r['est_obs_time_min'] for r in now) / 60:.1f} h, "
         f"{sum(r['n_chunks'] * r['n_beams_present'] for r in now)} filterbanks",
         f"exact divide           : {sum(r['exact_divide'] for r in results)}/{len(results)} observations; "
         f"remainder files (per beam, all obs): {sum(r['remainder_files'] for r in results)}",
         f"unparsed fits names    : {len(unparsed)}",
         ""]
    hdr = (f"  {'observation':<44}{'pos':>4}{'beams':>6}{'files/beam':>12}{'est.min':>8}"
           f"{'k':>5}{'chk.min':>8}{'nchk':>6}{'rem':>5}{'array':>12}  {'divisors in range':<18} issues")
    for proj in cfg["raw_projects"]:
        rs = [r for r in results if r["project"] == proj]
        if not rs:
            continue
        L += ["-" * 110, f"{proj}: {len(rs)} observations", hdr]
        for r in rs:
            fpb = (str(r["files_per_beam_min"]) if r["files_per_beam_min"] == r["files_per_beam_max"]
                   else f"{r['files_per_beam_min']}-{r['files_per_beam_max']}")
            L.append(f"  {r['obs_id']:<44}{'Y' if r['pos_file'] else '-':>4}{r['n_beams_present']:>6}{fpb:>12}"
                     f"{r['est_obs_time_min']:>8.1f}{r['files_per_chunk']:>5}{r['chunk_min']:>8.2f}{r['n_chunks']:>6}"
                     f"{r['remainder_files']:>5}{r['slurm_array']:>12}  {(r['divisors_in_range'] or '-'):<18} "
                     f"{'; '.join(r['issues']) or 'ok'}")
    L += ["-" * 110, "",
          f"OBSERVATIONS WITHOUT pos.fits ({sum(not r['pos_file'] for r in results)}):"]
    L += [f"  {r['obs_id']}" for r in results if not r["pos_file"]]
    L += ["", f"pos.fits WITHOUT RAW DATA ({len(unused_pos)}):"] + [f"  {p}" for p in unused_pos]
    if unparsed:
        L += ["", f"UNPARSED FITS NAMES ({len(unparsed)}), first 20:"] + [f"  {u}" for u in unparsed[:20]]
    L += ["", "files/beam = files per beam; est.min = files present in ALL beams x file_dur_s;",
          "k = best files per chunk (exact divisor closest to target if one exists, else fewest remainder);",
          "chk.min = chunk length; nchk = chunks per beam; rem = remainder files per beam (handled separately);",
          "array = SLURM --array for digifil (nchk x beams); divisors in range = all k that divide exactly."]
    txt = "\n".join(L)
    with open(os.path.join(out, "audit_summary.txt"), "w") as fh:
        fh.write(txt + "\n")
    print(txt)
    log(f"done: {len(results)} observations, {len(now)} processable now", lf)


if __name__ == "__main__":
    main()
