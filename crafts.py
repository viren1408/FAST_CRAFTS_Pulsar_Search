#!/usr/bin/env python3
"""
crafts.py - automation layer for the FAST CRAFTS reprocessing pipeline.

It does NOT change any science code. It plans each epoch from the raw files,
writes one task list per step, submits your existing scripts through thin
sbatch wrappers (steps/*.sbatch), and after every step runs a CHECK job that
verifies the outputs, records time/memory from sacct, writes machine-readable
state, and only then submits the next step. The chain stops at the first
failed check.

Steps (in order)
    1 update_pointing   update_pointing_v2.py on the raw epoch (skipped if marker exists)
    2 digifil           fits -> .fil, one task per (beam, chunk), chunk plan from the audit logic
    3 clean             clean_filterbank.py (rfifind + channel mask) per .fil
    4 birdies           run_sliding_search_for_birdies.py (GPU, DM 0-5) per masked .fil
    5 zaplist           make_zap_list.py for the epoch (one .zap per chunk)
    6 search            run_sliding_search_test_z.py (GPU, DM 5-500, zap) per masked .fil
    7 prepare_fold      prepare_final_candidates_to_fold.py -> fold commands
    8 fold              fold_pipeline_v3.py, one task per fold command (split by candidate count)
      report            written automatically when the last step passes

Commands
    plan          POINTING [--date D] [--k N] [--force]      make/refresh the epoch plan
    run           POINTING... [--date D] [--from STEP] [--only STEP] [--dry-run]
    run           --all-ready [--limit N]                     every clean epoch with a pos file
    status        [POINTING...]                               table of all epochs
    show          POINTING --date D                           detail of one epoch
    rerun-failed  POINTING --date D --step STEP               resubmit only the failed tasks
    mark-done     POINTING --date D --step STEP               accept a step by hand
    check         (internal, run by the check job)
    report        POINTING --date D                           (re)write the final report
    resources                                                 time/memory summary per step
    sync-cmd      POINTING [--date D] [--dest DIR]            rsync command for your laptop
    cancel        POINTING [--date D]                         scancel this epoch's jobs

Only the Python standard library is used.
"""

import argparse
import configparser
import csv
import datetime as dt
import glob
import json
import math
import os
import re
import shlex
import shutil
import statistics
import subprocess
import sys
import time

HERE = os.path.dirname(os.path.abspath(__file__))
STEPS = ["update_pointing", "digifil", "clean", "birdies", "zaplist", "search", "prepare_fold", "fold"]
SHORT = {"update_pointing": "UP", "digifil": "DF", "clean": "CL", "birdies": "BD", "zaplist": "ZP",
         "search": "SR", "prepare_fold": "PF", "fold": "FO"}
ARRAY_STEPS = {"digifil", "clean", "birdies", "search", "fold"}
WRAPPER = {s: os.path.join(HERE, "steps", f"{i + 1:02d}_{s}.sbatch") for i, s in enumerate(STEPS)}
CHECK_WRAPPER = os.path.join(HERE, "steps", "check.sbatch")


# =============================================================================
# config / small utils
# =============================================================================
def load_cfg(path):
    cp = configparser.ConfigParser(interpolation=None)
    if not cp.read(path):
        sys.exit(f"cannot read config {path}")
    cp.path = os.path.abspath(path)
    return cp


def now():
    return dt.datetime.now().isoformat(timespec="seconds")


def sbatch_bin():
    return os.environ.get("CRAFTS_SBATCH", "sbatch")


def sacct_bin():
    return os.environ.get("CRAFTS_SACCT", "sacct")


def squeue_bin():
    return os.environ.get("CRAFTS_SQUEUE", "squeue")


def read_json(path, default=None):
    try:
        with open(path) as fh:
            return json.load(fh)
    except (OSError, ValueError):
        return default


def write_json(path, obj):
    tmp = path + ".tmp"
    with open(tmp, "w") as fh:
        json.dump(obj, fh, indent=1)
    os.replace(tmp, path)


def parse_mem_mb(s):
    if not s:
        return None
    m = re.match(r"([\d.]+)([KMGT]?)", s.strip())
    if not m:
        return None
    v = float(m.group(1))
    return v * {"": 1 / 1024 / 1024, "K": 1 / 1024, "M": 1, "G": 1024, "T": 1024 ** 2}[m.group(2)]


def fmt_s(sec):
    if sec is None:
        return "-"
    sec = int(sec)
    return f"{sec // 3600:d}:{sec % 3600 // 60:02d}:{sec % 60:02d}"


# =============================================================================
# Epoch: paths + state
# =============================================================================
class Epoch:
    def __init__(self, cfg, project, pointing, epoch):
        self.cfg, self.project, self.pointing, self.epoch = cfg, project, pointing, epoch
        p = cfg["pipeline"]
        self.raw_dir = os.path.join(cfg["paths"]["raw_root"], project, pointing, epoch)
        self.proc_dir = os.path.join(p["proc_root"], project, pointing, epoch)
        self.pipe = os.path.join(self.proc_dir, "pipeline")
        self.tag = f"{pointing}/{epoch}"

    # --- paths
    def f(self, *parts):
        return os.path.join(self.pipe, *parts)

    @property
    def plan_path(self):
        return self.f("epoch.json")

    def state_path(self, step):
        return self.f("state", f"{step}.json")

    def plan(self):
        return read_json(self.plan_path)

    def state(self, step):
        return read_json(self.state_path(step), {})

    def save_state(self, step, st):
        if self.dry:
            return
        os.makedirs(self.f("state"), exist_ok=True)
        write_json(self.state_path(step), st)

    def marker(self):
        return self.f("state", "update_pointing.done")

    dry = False

    def log(self, msg):
        if self.dry:
            print(f"[{self.tag}] (dry-run) {msg}")
            return
        os.makedirs(self.pipe, exist_ok=True)
        line = f"{now()}  {msg}"
        with open(self.f("pipeline.log"), "a") as fh:
            fh.write(line + "\n")
        glog = os.path.join(self.cfg["pipeline"]["proc_root"], "automation_logs")
        os.makedirs(glog, exist_ok=True)
        with open(os.path.join(glog, "events.log"), "a") as fh:
            fh.write(f"{now()}  [{self.tag}] {msg}\n")
        print(f"[{self.tag}] {msg}")

    def step_status(self, step):
        if step == "update_pointing" and os.path.exists(self.marker()):
            return "complete"
        return self.state(step).get("status", "")

    @staticmethod
    def from_dir(cfg, proc_dir):
        root = cfg["pipeline"]["proc_root"].rstrip("/")
        rel = os.path.relpath(proc_dir, root).split(os.sep)
        return Epoch(cfg, rel[0], rel[1], os.sep.join(rel[2:]))


def find_raw_epochs(cfg, pointing, date=None):
    """Return Epoch objects for every epoch directory of a pointing (or one date)."""
    out = []
    for proj in [s.strip() for s in cfg["paths"]["raw_projects"].split(",")]:
        pdir = os.path.join(cfg["paths"]["raw_root"], proj, pointing)
        if not os.path.isdir(pdir):
            continue
        for e in sorted(os.listdir(pdir)):
            if date and e != date:
                continue
            if os.path.isdir(os.path.join(pdir, e)):
                out.append(Epoch(cfg, proj, pointing, e))
    if not out:
        sys.exit(f"no raw epoch found for {pointing}" + (f"/{date}" if date else ""))
    return out


def find_proc_epochs(cfg, pointings=None):
    root = cfg["pipeline"]["proc_root"]
    out = []
    for plan in sorted(glob.glob(os.path.join(root, "*", "*", "*", "pipeline", "epoch.json"))):
        ep = Epoch.from_dir(cfg, os.path.dirname(os.path.dirname(plan)))
        if not pointings or ep.pointing in pointings:
            out.append(ep)
    return out


def get_epoch(cfg, pointing, date):
    eps = [e for e in find_proc_epochs(cfg, [pointing]) if e.epoch == date]
    if not eps:
        eps = find_raw_epochs(cfg, pointing, date)
    return eps[0]


# =============================================================================
# PLAN: files, beams, chunking, pos file
# =============================================================================
def best_chunking(n, cfg, k_force=None):
    a = cfg["audit"]
    dur = a.getfloat("file_dur_s", 12.885)
    if k_force:
        return k_force, n // k_force, n % k_force
    kmin = max(1, math.ceil(a.getfloat("min_chunk_s", 120) / dur))
    kmax = max(kmin, math.floor(a.getfloat("max_chunk_s", 480) / dur))
    ktgt = a.getfloat("target_chunk_s", 245) / dur
    tol = a.getint("max_remainder_files", 3)
    if n < kmin:
        return n, 1, 0
    ks = range(kmin, min(kmax, n) + 1)
    div = [k for k in ks if n % k == 0]
    near = [k for k in ks if n % k <= tol]
    if div:
        k = min(div, key=lambda k: abs(k - ktgt))
    elif near:
        k = min(near, key=lambda k: (abs(k - ktgt), n % k))
    else:
        k = min(ks, key=lambda k: (n % k, abs(k - ktgt)))
    return k, n // k, n % k


def match_pos(cfg, pointing, epoch, prefix):
    pos_dir = cfg["paths"]["pos_dir"]
    d = re.search(r"(\d{8})", epoch)
    if not d:
        return None, "no date in epoch name"
    cands = sorted(glob.glob(os.path.join(pos_dir, f"{pointing}_{d.group(1)}*_pos.fits")))
    cands = [c for c in cands if re.fullmatch(rf"{re.escape(pointing)}_{d.group(1)}(_\d\d_\d\d)?_pos\.fits",
                                              os.path.basename(c))]
    if len(cands) == 1:
        return cands[0], ""
    if not cands:
        return None, "no pos file"
    return None, "several pos files: " + ", ".join(os.path.basename(c) for c in cands)


def make_plan(ep, k_force=None, force=False, pos_force=None, prefix_force=None):
    cfg = ep.cfg
    rx = re.compile(cfg["audit"]["file_regex"])
    nb = cfg["audit"].getint("n_beams", 19)
    names = {}
    prefixes, suffixes = set(), set()
    with os.scandir(ep.raw_dir) as it:
        for e in it:
            if not e.name.endswith(".fits"):
                continue
            m = rx.match(e.name)
            if not m:
                continue
            if prefix_force and m.group("prefix") != prefix_force:
                continue
            prefixes.add(m.group("prefix"))
            suffixes.add(m.group("suffix") or "")
            names[(int(m.group("beam")), int(m.group("idx")))] = e.name
    issues = []
    if not names:
        sys.exit(f"[{ep.tag}] no fits files in {ep.raw_dir}")
    if len(prefixes) > 1:
        sys.exit(f"[{ep.tag}] several observations in one folder {sorted(prefixes)}; use --prefix")
    if len(suffixes) > 1:
        sys.exit(f"[{ep.tag}] mixed file suffixes {sorted(suffixes)}")
    prefix, suffix = prefixes.pop(), suffixes.pop()

    by_beam = {}
    for (b, i) in names:
        by_beam.setdefault(b, set()).add(i)
    beams = list(range(1, nb + 1))
    missing_beams = [b for b in beams if b not in by_beam]
    if missing_beams:
        issues.append("missing beams " + ",".join(f"M{b:02d}" for b in missing_beams))
    common = sorted(set.intersection(*(by_beam[b] for b in by_beam)))
    all_idx = sorted(set().union(*by_beam.values()))
    partial = sorted(set(all_idx) - set(common))
    # runs of consecutive file numbers present in every beam
    runs, cur = [], []
    for i in common:
        if cur and i != cur[-1] + 1:
            runs.append(cur)
            cur = []
        cur.append(i)
    if cur:
        runs.append(cur)
    trailing_only = len(runs) == 1 and all(p > common[-1] for p in partial)
    if partial and not trailing_only:
        issues.append(f"{len(partial)} file numbers missing in some beams")
    if len(runs) > 1:
        issues.append(f"{len(runs)} separate runs of file numbers (holes)")
    if partial and trailing_only:
        issues.append(f"note: last {len(partial)} file number(s) not in every beam - dropped")

    pos, pos_msg = (pos_force, "") if pos_force else match_pos(cfg, ep.pointing, ep.epoch, prefix)
    if not pos:
        issues.append(pos_msg)
    blocking = [x for x in issues if not x.startswith("note:")]
    if blocking and not force:
        sys.exit(f"[{ep.tag}] not planned: {'; '.join(blocking)}  (use --force to plan anyway)")

    # chunks inside runs
    k = None
    chunks = []
    n_total = sum(len(r) for r in runs)
    if k_force:
        k = k_force
    elif len(runs) == 1:
        k = best_chunking(n_total, cfg)[0]
    else:
        k = best_chunking(max(len(r) for r in runs), cfg)[0]
    dropped = 0
    for r in runs:
        for c in range(len(r) // k):
            chunks.append(r[c * k:(c + 1) * k])
        dropped += len(r) % k
    plan = {
        "project": ep.project, "pointing": ep.pointing, "epoch": ep.epoch,
        "raw_dir": ep.raw_dir, "proc_dir": ep.proc_dir, "prefix": prefix, "suffix": suffix,
        "pos_file": pos, "n_beams": nb, "beams": beams,
        "files_per_beam": len(common), "n_fits_total": len(names),
        "files_per_chunk": k,
        "chunk_s": round(k * cfg["audit"].getfloat("file_dur_s", 12.885), 2),
        "n_chunks": len(chunks), "remainder_files_per_beam": dropped,
        "chunks": [{"chunk": c, "idx_first": ch[0], "idx_last": ch[-1], "n": len(ch)} for c, ch in enumerate(chunks)],
        "issues": issues, "forced": bool(blocking), "created": now(),
    }
    os.makedirs(ep.pipe, exist_ok=True)
    for d in ("state", "tasks", "logs"):
        os.makedirs(ep.f(d), exist_ok=True)
    write_json(ep.plan_path, plan)
    write_epoch_env(ep, plan)
    ep.log(f"PLAN  {plan['files_per_beam']} files/beam, k={k} ({plan['chunk_s']}s), "
           f"{plan['n_chunks']} chunks x {nb} beams, remainder {dropped}/beam"
           + (f"  issues: {'; '.join(issues)}" if issues else ""))
    return plan


def write_epoch_env(ep, plan):
    cfg = ep.cfg
    p, s = cfg["pipeline"], cfg["science"]
    env = {
        "PROJECT": ep.project, "POINTING": ep.pointing, "EPOCH": ep.epoch,
        "RAW_DIR": ep.raw_dir, "PROC_DIR": ep.proc_dir, "PIPE_DIR": ep.pipe,
        "POS_FILE": plan.get("pos_file") or "", "PREFIX": plan["prefix"],
        "NBEAMS": str(plan["n_beams"]),
        "SCRIPTS_DIR": p["scripts_dir"], "UPDATE_SCRIPT": p["update_pointing_script"],
        "ENV_ACTIVATE": p["env_activate"],
        "CRAFTS_PY": os.path.join(HERE, "crafts.py"), "CRAFTS_CONFIG": cfg.path,
        "CRAFTS_STEPS_DIR": os.path.join(HERE, "steps"),
        "CRAFTS_PYTHON": p.get("python", "python3"),
        "DIGIFIL_ARGS": s.get("digifil_args", "-cont -f 4"),
        "BIRDIE_MIN_SN": s.get("birdie_min_sn"), "BIRDIE_DM_START": s.get("birdie_dm_start"),
        "BIRDIE_DM_END": s.get("birdie_dm_end"),
        "SEARCH_MIN_SN": s.get("search_min_sn"), "SEARCH_DM_START": s.get("search_dm_start"),
        "SEARCH_DM_END": s.get("search_dm_end"),
        "T_WINDOW": s.get("t_window"), "T_SHIFT": s.get("t_shift"), "ZAP_T": s.get("zap_t"),
    }
    with open(ep.f("epoch.env"), "w") as fh:
        for k, v in env.items():
            fh.write(f"{k}={shlex.quote(str(v))}\n")


# =============================================================================
# TASK LISTS (tab separated; first column = task id = line number)
# =============================================================================
def fil_stem(plan, beam, chunk):
    return f"{plan['prefix']}-M{beam:02d}_{chunk:04d}"


def build_tasks(ep, step):
    plan = ep.plan()
    pd_, beams = plan["proc_dir"], plan["beams"]
    rows = []
    if step in ("update_pointing", "zaplist", "prepare_fold"):
        rows = [[ep.epoch]]
    elif step == "digifil":
        for b in beams:  # beam-major, like the original array
            for ch in plan["chunks"]:
                files = [os.path.join(plan["raw_dir"], f"{plan['prefix']}-M{b:02d}_{i:04d}{plan['suffix']}.fits")
                         for i in range(ch["idx_first"], ch["idx_last"] + 1)]
                out = os.path.join(pd_, f"{b:02d}", fil_stem(plan, b, ch["chunk"]) + ".fil")
                rows.append([f"{b:02d}", f"{ch['chunk']:04d}", out, " ".join(files)])
    elif step == "clean":
        for b in beams:
            for ch in plan["chunks"]:
                rows.append([f"{b:02d}", f"{ch['chunk']:04d}",
                             os.path.join(pd_, f"{b:02d}", fil_stem(plan, b, ch["chunk"]) + ".fil")])
    elif step in ("birdies", "search"):
        suff = "_birdies" if step == "birdies" else "_candidates"
        for b in beams:
            for ch in plan["chunks"]:
                stem = fil_stem(plan, b, ch["chunk"]) + "_masked"
                row = [f"{b:02d}", f"{ch['chunk']:04d}", os.path.join(pd_, f"{b:02d}", stem + ".fil"),
                       os.path.join(pd_, f"{b:02d}", stem + suff)]
                if step == "search":
                    row.append(os.path.join(pd_, "birdie_lists", f"birdies_{ch['chunk']:04d}.zap"))
                rows.append(row)
    elif step == "fold":
        cmdfile = fold_commands_path(ep)
        with open(cmdfile) as fh:
            for line in fh:
                line = line.strip()
                if not line or line.startswith("#"):
                    continue
                toks = shlex.split(line)
                outdir = toks[toks.index("--outdir") + 1] if "--outdir" in toks else ""
                cands = toks[toks.index("--cands") + 1] if "--cands" in toks else ""
                rows.append([str(count_rows(cands)), outdir, line])
    path = ep.f("tasks", f"{step}.tsv")
    with open(path, "w") as fh:
        for i, r in enumerate(rows):
            fh.write("\t".join([str(i)] + r) + "\n")
    return path, len(rows)


def count_rows(csv_path):
    try:
        with open(csv_path) as fh:
            return max(0, sum(1 for _ in fh) - 1)
    except OSError:
        return -1


def read_tasks(path):
    with open(path) as fh:
        return [ln.rstrip("\n").split("\t") for ln in fh if ln.strip()]


def fold_commands_path(ep):
    return os.path.join(ep.proc_dir, "sifting_output", f"fold_commands_{ep.epoch}.sh")


# =============================================================================
# SUBMISSION
# =============================================================================
def slurm_opts(cfg, step, override=None):
    sec = f"slurm.{step}"
    opts = []
    o = dict(cfg[sec]) if cfg.has_section(sec) else {}
    if override:
        o.update(override)
    for key in ("time", "mem", "cpus-per-task"):
        if o.get(key):
            opts.append(f"--{key}={o[key]}")
    if o.get("extra"):
        opts += shlex.split(o["extra"])
    return opts, int(o.get("throttle", 0) or 0)


def run_sbatch(args, dry):
    cmd = [sbatch_bin(), "--parsable"] + args
    if dry:
        print("  DRY:", " ".join(shlex.quote(c) for c in cmd))
        return f"DRY{abs(hash(tuple(cmd))) % 100000}"
    r = subprocess.run(cmd, capture_output=True, text=True)
    if r.returncode != 0:
        raise RuntimeError(f"sbatch failed: {r.stderr.strip()}")
    return r.stdout.strip().split(";")[0]


def submit_step(ep, step, task_file=None, attempt=None, cont=True, dry=False, override=None, label=""):
    """Submit one step (all tasks, or the tasks in task_file) plus its check job."""
    cfg = ep.cfg
    if task_file is None:
        task_file, n = build_tasks(ep, step)
    else:
        n = len(read_tasks(task_file))
    st = ep.state(step) if attempt else {}
    if not attempt:
        st = {"step": step, "tasks_file": task_file, "n_tasks": n, "ok": [], "attempts": []}
    if n == 0:
        st.update(status="complete", note="no tasks", finished=now())
        ep.save_state(step, st)
        ep.log(f"{step:<15} no tasks - nothing to do")
        return after_complete(ep, step, cont, dry)

    os.makedirs(ep.f("logs", step), exist_ok=True)
    parts = []
    if step == "fold":
        groups = split_fold_classes(cfg, task_file, ep, label)
    else:
        groups = [(task_file, None, "")]
    for tfile, ovr, cls in groups:
        m = len(read_tasks(tfile))
        if m == 0:
            continue
        opts, throttle = slurm_opts(cfg, step, dict(ovr or {}, **(override or {})))
        maxa = cfg["pipeline"].getint("max_array_size", 1000)
        offsets = range(0, m, maxa) if step in ARRAY_STEPS else [0]
        for off in offsets:
            cnt = min(maxa, m - off) if step in ARRAY_STEPS else 1
            name = f"crafts_{step}" + (f"_{cls}" if cls else "")
            args = [f"--job-name={name}",
                    f"--export=ALL,CRAFTS_TASKS={tfile},CRAFTS_OFFSET={off},"
                    f"CRAFTS_EPOCH_ENV={ep.f('epoch.env')},CRAFTS_STEP={step}"]
            if step in ARRAY_STEPS:
                arr = f"0-{cnt - 1}" + (f"%{throttle}" if throttle else "")
                args += [f"--array={arr}",
                         f"--output={ep.f('logs', step)}/{step}{('_' + cls) if cls else ''}_%A_%a.out",
                         f"--error={ep.f('logs', step)}/{step}{('_' + cls) if cls else ''}_%A_%a.err"]
            else:
                args += [f"--output={ep.f('logs', step)}/{step}_%j.out",
                         f"--error={ep.f('logs', step)}/{step}_%j.err"]
            jid = run_sbatch(args + opts + [WRAPPER[step]], dry)
            parts.append({"job": jid, "offset": off, "n": cnt, "task_file": tfile, "class": cls})
    att = {"n": len(st["attempts"]) + 1, "submitted": now(), "parts": parts, "task_file": task_file}
    st["attempts"].append(att)
    st["status"] = "submitted"
    ep.save_state(step, st)
    ep.log(f"{step:<15} SUBMITTED attempt {att['n']}: {n} task(s) in job(s) "
           f"{','.join(p['job'] for p in parts)}")
    jobs = ":".join(p["job"] for p in parts)
    ck_args = [f"--job-name=crafts_check_{step}", f"--dependency=afterany:{jobs}",
               "--kill-on-invalid-dep=yes",
               f"--output={ep.f('logs', 'check')}_{step}_%j.out",
               f"--export=ALL,CRAFTS_EPOCH_ENV={ep.f('epoch.env')},CRAFTS_STEP={step},"
               f"CRAFTS_CONTINUE={1 if cont else 0}"]
    ck_opts, _ = slurm_opts(cfg, "check")
    ck = run_sbatch(ck_args + ck_opts + [CHECK_WRAPPER], dry)
    st2 = ep.state(step)          # the check may already have run and updated the state
    if st2.get("attempts") and len(st2["attempts"]) == att["n"]:
        st2["attempts"][-1]["check_job"] = ck
        ep.save_state(step, st2)
    ep.log(f"{step:<15} check job {ck} (runs when the step's jobs end)")
    return True


def split_fold_classes(cfg, task_file, ep, label=""):
    """Split fold tasks by candidate count so each group gets a sensible time limit."""
    classes = []
    for name, val in cfg["fold_classes"].items():
        lim, t, mem = [v.strip() for v in val.split(",")]
        classes.append((int(lim), name, {"time": t, "mem": mem}))
    classes.sort()
    rows = read_tasks(task_file)
    groups = []
    base = os.path.splitext(task_file)[0]
    for lim, name, ovr in classes:
        sel = [r for r in rows if int(r[1]) <= lim]
        rows = [r for r in rows if int(r[1]) > lim]
        path = f"{base}_{name}.tsv"
        with open(path, "w") as fh:
            for r in sel:
                fh.write("\t".join(r) + "\n")
        groups.append((path, ovr, name))
    if rows:  # anything above the largest class
        path = f"{base}_{classes[-1][1]}.tsv"
        with open(path, "a") as fh:
            for r in rows:
                fh.write("\t".join(r) + "\n")
    return groups


def next_step(step):
    i = STEPS.index(step)
    return STEPS[i + 1] if i + 1 < len(STEPS) else None


def after_complete(ep, step, cont, dry=False):
    nxt = next_step(step)
    if nxt is None:
        write_report(ep)
        ep.log("PIPELINE COMPLETE - final report written")
        return True
    if not cont:
        ep.log(f"{step:<15} complete; not continuing (--only / --no-continue)")
        return True
    if nxt == "fold":
        if not os.path.exists(fold_commands_path(ep)):
            ep.log("fold            no fold commands file - nothing to fold")
            ep.save_state("fold", {"step": "fold", "status": "complete", "note": "no commands", "finished": now()})
            write_report(ep)
            return True
    return submit_step(ep, nxt, cont=cont, dry=dry)


# =============================================================================
# CHECK (runs in the check job after each step)
# =============================================================================
def sacct_tasks(parts):
    """Return {(job, array_idx or None): dict(state, exit, elapsed, maxrss_mb)}."""
    ids = ",".join(p["job"] for p in parts)
    fmt = "JobID,State,ExitCode,ElapsedRaw,MaxRSS"
    res = {}
    for attempt in range(4):
        try:
            r = subprocess.run([sacct_bin(), "-j", ids, "-P", "-n", f"--format={fmt}"],
                               capture_output=True, text=True, timeout=120)
            out = r.stdout
        except (OSError, subprocess.TimeoutExpired):
            out = ""
        for line in out.splitlines():
            f = line.split("|")
            if len(f) < 5:
                continue
            jid, stp = f[0], ""
            if "." in jid:
                jid, stp = jid.split(".", 1)
            if "_" in jid:
                j, a = jid.split("_", 1)
                if not a.isdigit():
                    continue  # pending range like 123_[0-10]
                key = (j, int(a))
            else:
                key = (jid, None)
            d = res.setdefault(key, {"state": "", "exit": "", "elapsed": None, "maxrss_mb": None})
            if not stp:
                d["state"], d["exit"] = f[1].split()[0], f[2]
                d["elapsed"] = int(f[3]) if f[3].isdigit() else None
            mem = parse_mem_mb(f[4])
            if mem is not None:
                d["maxrss_mb"] = max(d["maxrss_mb"] or 0, mem)
        if res and all(v["state"] not in ("PENDING", "RUNNING", "") for v in res.values()):
            break
        time.sleep(20)
    return res


def task_log(ep, step, part, aidx):
    cls = f"_{part['class']}" if part.get("class") else ""
    if step in ARRAY_STEPS:
        base = ep.f("logs", step, f"{step}{cls}_{part['job']}_{aidx}")
    else:
        base = ep.f("logs", step, f"{step}_{part['job']}")
    return base + ".out", base + ".err"


def grep_fail(paths, patterns):
    for p in paths:
        try:
            with open(p, errors="replace") as fh:
                txt = fh.read()
        except OSError:
            continue
        for pat in patterns:
            if pat and pat in txt:
                return pat
    return None


def verify_task(ep, step, row, logs):
    """Return '' if the task's outputs look right, else a reason."""
    cfg, plan = ep.cfg, ep.plan()
    pats = [p.strip() for p in cfg["fail_patterns"].get(step, "").split("|")] if cfg.has_section("fail_patterns") else []
    bad = grep_fail(logs, pats)
    if bad:
        return f"log contains '{bad}'"
    if step == "update_pointing":
        logf = glob.glob(ep.f("logs", "update_pointing", "update_pointing_*.log"))
        if not logf:
            return "no update_pointing log"
        txt = open(max(logf, key=os.path.getmtime), errors="replace").read()
        n_up = len(re.findall(r" Updated ", txt))
        n_skip = len(re.findall(r"skipping", txt))
        n_err = len(re.findall(r" - ERROR - ", txt))
        exp = sum(1 for f in os.listdir(ep.raw_dir) if f.endswith(".fits"))
        if n_up < exp or n_err or n_skip:
            return f"updated {n_up}/{exp} files, {n_skip} skipped, {n_err} errors"
        return ""
    if step == "digifil":
        out = row[3]
        if not os.path.exists(out) or os.path.getsize(out) == 0:
            return "no .fil"
        return ""
    if step == "clean":
        stem = os.path.basename(row[3])[:-4]
        masked = os.path.join(os.path.dirname(row[3]), stem + "_masked.fil")
        kill = os.path.join(plan["proc_dir"], "rfi_masks", stem + "_peasoup.kill")
        if not (os.path.exists(masked) and os.path.getsize(masked) > 0):
            return "no _masked.fil"
        if not os.path.exists(kill):
            return "no _peasoup.kill"
        return ""
    if step in ("birdies", "search"):
        if not os.path.isdir(row[4]):
            return "no output directory"
        return ""  # empty output is legitimate (no candidates); failures caught by exit code + log patterns
    if step == "zaplist":
        missing = [c["chunk"] for c in plan["chunks"]
                   if not os.path.exists(os.path.join(plan["proc_dir"], "birdie_lists", f"birdies_{c['chunk']:04d}.zap"))]
        if missing and cfg["pipeline"].getboolean("empty_zap_if_missing", True):
            for c in missing:
                open(os.path.join(plan["proc_dir"], "birdie_lists", f"birdies_{c:04d}.zap"), "a").close()
            ep.log(f"zaplist         WARNING created EMPTY zap files for chunks with no birdie data: {missing}")
            return ""
        return f"missing zap for chunks {missing}" if missing else ""
    if step == "prepare_fold":
        cf = fold_commands_path(ep)
        if not os.path.exists(cf):
            txt = "".join(open(p, errors="replace").read() for p in logs if os.path.exists(p))
            return "" if "No chunks found" in txt else "no fold commands file"
        return ""
    if step == "fold":
        if not os.path.exists(os.path.join(row[2], "sorted.cands")):
            return "no sorted.cands"
        return ""
    return ""


def cmd_check(cfg, args):
    ep = Epoch.from_dir(cfg, os.path.dirname(os.path.dirname(args.epoch_env)))
    step = args.step
    st = ep.state(step)
    att = st["attempts"][-1]
    acct = sacct_tasks(att["parts"])
    all_rows = {int(r[0]): r for r in read_tasks(st["tasks_file"])}
    ok = set(st.get("ok", []))
    newly_failed = {}
    res_rows = []
    for part in att["parts"]:
        rows = read_tasks(part["task_file"])[part["offset"]:part["offset"] + part["n"]]
        for aidx, row in enumerate(rows):
            tid = int(row[0])
            key = (part["job"], aidx) if step in ARRAY_STEPS else (part["job"], None)
            a = acct.get(key, {})
            logs = task_log(ep, step, part, aidx)
            reason = ""
            if a and a.get("state") != "COMPLETED":
                reason = f"slurm {a.get('state')} exit {a.get('exit')}"
            if not reason:
                reason = verify_task(ep, step, all_rows.get(tid, row), logs)
            if reason:
                newly_failed[tid] = reason
                ok.discard(tid)
            else:
                ok.add(tid)
            res_rows.append([ep.pointing, ep.epoch, step, tid, a.get("state", "?"),
                             "" if a.get("elapsed") is None else a["elapsed"],
                             f"{a['maxrss_mb']:.0f}" if a.get("maxrss_mb") else "", part["job"],
                             part.get("class", ""), reason])
    failed = sorted(set(all_rows) - ok)
    el = [r[5] for r in res_rows if r[5] != ""]
    mm = [float(r[6]) for r in res_rows if r[6] != ""]
    att.update(checked=now(), n_checked=len(res_rows), n_failed=len(newly_failed),
               elapsed_s={"median": statistics.median(el) if el else None, "max": max(el) if el else None,
                          "sum": sum(el) if el else None},
               maxrss_mb_max=max(mm) if mm else None)
    st["ok"] = sorted(ok)
    st["failed"] = {str(t): newly_failed.get(t, "not run / earlier failure") for t in failed}
    # failed task list for rerun-failed
    ff = ep.f("tasks", f"{step}.failed.tsv")
    with open(ff, "w") as fh:
        for t in failed:
            fh.write("\t".join(all_rows[t]) + "\n")
    # global resource log for tuning time/mem requests later
    rdir = os.path.join(cfg["pipeline"]["proc_root"], "automation_logs")
    os.makedirs(rdir, exist_ok=True)
    rpath = os.path.join(rdir, "resources.csv")
    new = not os.path.exists(rpath)
    with open(rpath, "a", newline="") as fh:
        w = csv.writer(fh)
        if new:
            w.writerow(["pointing", "epoch", "step", "task", "state", "elapsed_s", "maxrss_mb", "job", "class", "reason"])
        w.writerows(res_rows)

    if failed:
        st["status"] = "failed"
        st["finished"] = now()
        ep.save_state(step, st)
        ex = "; ".join(f"{t}: {newly_failed.get(t, '')}" for t in failed[:5])
        ep.log(f"{step:<15} FAILED {len(failed)}/{len(all_rows)} tasks (e.g. {ex}). Chain stopped. "
               f"Fix, then: crafts.py rerun-failed {ep.pointing} --date {ep.epoch} --step {step}")
        return 1
    st["status"] = "complete"
    st["finished"] = now()
    ep.save_state(step, st)
    if step == "update_pointing":
        with open(ep.marker(), "w") as fh:
            fh.write(f"update_pointing complete {now()}\n")
    ep.log(f"{step:<15} OK {len(all_rows)}/{len(all_rows)} tasks; elapsed median {fmt_s(att['elapsed_s']['median'])}, "
           f"max {fmt_s(att['elapsed_s']['max'])}; peak mem {att['maxrss_mb_max'] or 0:.0f} MB")
    after_complete(ep, step, cont=os.environ.get("CRAFTS_CONTINUE", "1") == "1")
    return 0


# =============================================================================
# REPORT
# =============================================================================
def write_report(ep):
    plan = ep.plan()
    pd_ = plan["proc_dir"]
    so = os.path.join(pd_, "sifting_output")
    rep = {"pointing": ep.pointing, "epoch": ep.epoch, "written": now(), "plan": {
        k: plan[k] for k in ("files_per_beam", "files_per_chunk", "chunk_s", "n_chunks", "remainder_files_per_beam")},
        "steps": {}}
    L = [f"CRAFTS FINAL REPORT  {ep.tag}   {now()}", "=" * 90,
         f"files/beam {plan['files_per_beam']}, {plan['files_per_chunk']} files/chunk ({plan['chunk_s']} s), "
         f"{plan['n_chunks']} chunks x {plan['n_beams']} beams", "",
         f"{'step':<16}{'status':<10}{'tasks':>7}{'failed':>7}{'attempts':>9}{'sum time':>11}{'median':>9}"
         f"{'max':>9}{'peak MB':>9}"]
    for s in STEPS:
        st = ep.state(s)
        atts = st.get("attempts", [])
        tot = sum((a.get("elapsed_s") or {}).get("sum") or 0 for a in atts)
        med = [(a.get("elapsed_s") or {}).get("median") for a in atts
               if (a.get("elapsed_s") or {}).get("median") is not None]
        mx = [(a.get("elapsed_s") or {}).get("max") for a in atts if (a.get("elapsed_s") or {}).get("max") is not None]
        mem = [a.get("maxrss_mb_max") for a in atts if a.get("maxrss_mb_max")]
        status = ep.step_status(s) or "-"
        rep["steps"][s] = {"status": status, "n_tasks": st.get("n_tasks"), "failed": len(st.get("failed", {})),
                           "attempts": len(atts), "elapsed_sum_s": tot, "maxrss_mb": max(mem) if mem else None}
        L.append(f"{s:<16}{status:<10}{st.get('n_tasks', '-')!s:>7}{len(st.get('failed', {})):>7}{len(atts):>9}"
                 f"{fmt_s(tot):>11}{fmt_s(med[-1] if med else None):>9}{fmt_s(max(mx) if mx else None):>9}"
                 f"{(max(mem) if mem else 0):>9.0f}")
    # candidate funnel from prepare_fold stdout
    funnel = {"raw": 0, "period_nh": 0, "after_birdies": 0, "to_fold": 0}
    keys = {"Total raw candidates": "raw", "After period and nh filtering": "period_nh",
            "After residual birdie removal": "after_birdies", "Candidates ready for folding": "to_fold"}
    for p in glob.glob(ep.f("logs", "prepare_fold", "*.out")):
        for line in open(p, errors="replace"):
            for k, v in keys.items():
                if line.strip().startswith(k):
                    try:
                        funnel[v] += int(line.split(":")[-1])
                    except ValueError:
                        pass
    rep["candidates"] = funnel
    # concatenate outputs
    sorted_files = sorted(glob.glob(os.path.join(so, "fold_inputs", "fold_results", "**", "sorted.cands"), recursive=True))
    n_folded = 0
    if sorted_files:
        with open(os.path.join(so, "all_sorted_cands.txt"), "w") as out:
            hdr_done = False
            for sf in sorted_files:
                lines = open(sf, errors="replace").read().splitlines()
                if not lines:
                    continue
                if not hdr_done:
                    out.write(lines[0] + "  sorted_cands_path\n")
                    hdr_done = True
                for ln in lines[1:]:
                    out.write(f"{ln}  {sf}\n")
                    n_folded += 1
    sifted = sorted(glob.glob(os.path.join(so, "sifted_candidates_*.txt")))
    if sifted:
        with open(os.path.join(so, "all_sifted_candidates.txt"), "w") as out:
            for i, sf in enumerate(sifted):
                lines = open(sf, errors="replace").read().splitlines()
                out.write("\n".join(lines if i == 0 else lines[1:]) + "\n")
    rep["candidates"]["final_rows_in_sorted_cands"] = n_folded
    birdie_summaries = sorted(glob.glob(os.path.join(pd_, "birdie_lists", "birdie_summary_*.csv")))
    nearby = sorted(glob.glob(os.path.join(so, "**", "*_nearby_pulsars.txt"), recursive=True))
    rep["paths"] = {"all_sorted_cands": os.path.join(so, "all_sorted_cands.txt") if sorted_files else None,
                    "all_sifted_candidates": os.path.join(so, "all_sifted_candidates.txt") if sifted else None,
                    "birdie_summaries": birdie_summaries, "nearby_pulsar_files": nearby}
    L += ["", "CANDIDATES (summed over chunks)",
          f"  raw {funnel['raw']}  ->  period/nh {funnel['period_nh']}  ->  after birdies "
          f"{funnel['after_birdies']}  ->  to fold {funnel['to_fold']}  ->  rows in sorted.cands {n_folded}",
          "", "FILES",
          f"  all sorted.cands   : {rep['paths']['all_sorted_cands']}  ({len(sorted_files)} files)",
          f"  all sifted cands   : {rep['paths']['all_sifted_candidates']}  ({len(sifted)} files)",
          f"  birdie summaries   : {len(birdie_summaries)} in {os.path.join(pd_, 'birdie_lists')}",
          f"  nearby-pulsar lists: {len(nearby)}" + ("   (none - fold cleanup 'rm *.txt' deletes them)" if not nearby else ""),
          "", "COPY TO LAPTOP", "  " + sync_cmd(ep)]
    write_json(ep.f("final_report.json"), rep)
    with open(ep.f("final_report.txt"), "w") as fh:
        fh.write("\n".join(L) + "\n")
    return "\n".join(L)


def sync_cmd(ep, dest="."):
    so = os.path.join(ep.proc_dir, "sifting_output") + "/"
    host = ep.cfg["pipeline"].get("ssh_host", "vmandaog@nt.swin.edu.au")
    d = os.path.join(dest, ep.pointing, ep.epoch) + "/"
    return ("rsync -avm --include='*/' --include='sorted.cands' --include='*.png' "
            "--include='*_nearby_pulsars.txt' --include='all_*.txt' --exclude='*' "
            f"{host}:{so} {d}sifting_output/ && "
            f"rsync -av {host}:{ep.pipe}/final_report.txt {host}:{ep.pipe}/pipeline.log {d}")


# =============================================================================
# CLI commands
# =============================================================================
def cmd_plan(cfg, a):
    for ep in find_raw_epochs(cfg, a.pointing, a.date):
        try:
            make_plan(ep, k_force=a.k, force=a.force, pos_force=a.pos, prefix_force=a.prefix)
        except SystemExit as e:
            print(e)


def epoch_busy(ep):
    for s in STEPS:
        if ep.state(s).get("status") == "submitted":
            return s
    return None


def start_epoch(ep, a):
    if not os.path.exists(ep.plan_path) or a.replan:
        make_plan(ep, k_force=a.k, force=a.force)
    busy = epoch_busy(ep)
    ep.dry = a.dry_run
    if busy and not a.force:
        print(f"[{ep.tag}] step {busy} is already submitted - skipping (see status)")
        return
    if a.only:
        steps = [a.only]
    elif a.from_step:
        steps = STEPS[STEPS.index(a.from_step):]
    else:
        steps = [s for s in STEPS if ep.step_status(s) != "complete"]
    if not steps:
        print(f"[{ep.tag}] all steps complete")
        return
    first = steps[0]
    redo_up = a.only == "update_pointing" or a.from_step == "update_pointing"
    if first == "update_pointing" and redo_up and os.path.exists(ep.marker()) and not a.dry_run:
        os.remove(ep.marker())
    if first == "update_pointing" and os.path.exists(ep.marker()):
        first = steps[1] if len(steps) > 1 else None
        ep.log("update_pointing marker found - skipping step 1")
    if first == "fold" and not os.path.exists(fold_commands_path(ep)):
        sys.exit(f"[{ep.tag}] no fold commands file yet")
    if first is None:
        return
    for s in STEPS[STEPS.index(first):]:  # reset state of steps that will re-run
        p = ep.state_path(s)
        if not a.dry_run and os.path.exists(p) and (a.from_step or a.only == s or ep.step_status(s) != "complete"):
            os.makedirs(ep.f("state", "old"), exist_ok=True)
            shutil.move(p, ep.f("state", "old", f"{s}_{int(time.time())}.json"))
    if first == "update_pointing" and not ep.plan().get("pos_file"):
        sys.exit(f"[{ep.tag}] no pos file - cannot update pointing")
    ep.log(f"RUN   starting at step '{first}'" + (" (only this step)" if a.only else ""))
    submit_step(ep, first, cont=not a.only, dry=a.dry_run)


def cmd_run(cfg, a):
    eps = []
    if a.all_ready:
        path = os.path.join(cfg["paths"]["audit_dir"], "audit_epochs.csv")
        rows = [r for r in csv.DictReader(open(path)) if r["clean"] == "True" and r["processable_now"] == "True"]
        for r in rows[:a.limit or None]:
            eps.append(Epoch(cfg, r["project"], r["pointing"], r["epoch_dir"]))
        print(f"{len(eps)} ready epochs from {path}")
    for p in a.pointing:
        eps += find_raw_epochs(cfg, p, a.date)
    for ep in eps:
        try:
            start_epoch(ep, a)
        except SystemExit as e:
            print(e)
        except RuntimeError as e:
            ep.log(f"ERROR {e}")


def live_jobs(job_ids):
    if not job_ids:
        return set()
    try:
        r = subprocess.run([squeue_bin(), "-h", "-o", "%i", "-j", ",".join(job_ids)],
                           capture_output=True, text=True, timeout=60)
        return {x.split("_")[0] for x in r.stdout.split()}
    except (OSError, subprocess.TimeoutExpired):
        return set(job_ids)  # unknown -> assume alive


def cmd_status(cfg, a):
    eps = find_proc_epochs(cfg, a.pointing or None)
    if not eps:
        print("no planned epochs")
        return
    sym = {"complete": "ok", "submitted": "..", "failed": "XX", "": " -"}
    print(f"{'pointing':<20}{'epoch':<12}" + "".join(f"{SHORT[s]:>4}" for s in STEPS) + "   note")
    for ep in eps:
        cells, note = [], ""
        for s in STEPS:
            stt = ep.step_status(s)
            if stt == "submitted":
                att = ep.state(s)["attempts"][-1]
                ids = [p["job"] for p in att["parts"]] + [att.get("check_job", "")]
                if not live_jobs([i for i in ids if i]):
                    stt, note = "", f"{s} STALLED (jobs gone, no check) -> crafts.py check by hand or rerun"
                    cells.append("!!")
                    continue
                note = note or f"{s} running (attempt {att['n']})"
            if stt == "failed":
                note = f"{s}: {len(ep.state(s).get('failed', {}))} failed -> rerun-failed"
            cells.append(sym.get(stt, stt[:2]))
        if all(c == "ok" for c in cells):
            note = "DONE"
        print(f"{ep.pointing:<20}{ep.epoch:<12}" + "".join(f"{c:>4}" for c in cells) + f"   {note}")
    print("\nok=complete  ..=submitted/running  XX=failed  !!=stalled  -=not started")


def cmd_show(cfg, a):
    ep = get_epoch(cfg, a.pointing, a.date)
    plan = ep.plan() or {}
    for k, v in plan.items():
        if k not in ("chunks", "beams"):
            print(f"  {k:<26} {v}")
    for s in STEPS:
        st = ep.state(s)
        if not st:
            continue
        print(f"\n== {s}: {ep.step_status(s)}  tasks={st.get('n_tasks')} ok={len(st.get('ok', []))} "
              f"failed={len(st.get('failed', {}))}")
        for t, r in list(st.get("failed", {}).items())[:10]:
            print(f"   task {t}: {r}")
    print("\n-- last log lines --")
    try:
        print("".join(open(ep.f("pipeline.log")).readlines()[-15:]))
    except OSError:
        pass


def cmd_rerun(cfg, a):
    ep = get_epoch(cfg, a.pointing, a.date)
    st = ep.state(a.step)
    ff = ep.f("tasks", f"{a.step}.failed.tsv")
    if st.get("status") != "failed" or not os.path.exists(ff) or not read_tasks(ff):
        sys.exit(f"[{ep.tag}] step {a.step} has no failed tasks to rerun")
    ep.dry = a.dry_run
    rr = ep.f("tasks", f"{a.step}.rerun{len(st['attempts']) + 1}.tsv")
    shutil.copy(ff, rr)
    ovr = {}
    if a.time:
        ovr["time"] = a.time
    if a.mem:
        ovr["mem"] = a.mem
    ep.log(f"{a.step:<15} RERUN of {len(read_tasks(rr))} failed task(s)" + (f" with {ovr}" if ovr else ""))
    submit_step(ep, a.step, task_file=rr, attempt=True, cont=not a.no_continue, dry=a.dry_run,
                override=ovr or None)


def cmd_mark_done(cfg, a):
    ep = get_epoch(cfg, a.pointing, a.date)
    st = ep.state(a.step) or {"step": a.step, "attempts": []}
    st.update(status="complete", finished=now(), note=f"marked done by hand: {a.reason}")
    ep.save_state(a.step, st)
    if a.step == "update_pointing":
        open(ep.marker(), "w").write(f"marked done by hand {now()}: {a.reason}\n")
    ep.log(f"{a.step:<15} MARKED DONE by hand ({a.reason})")


def cmd_resources(cfg, a):
    path = os.path.join(cfg["pipeline"]["proc_root"], "automation_logs", "resources.csv")
    if not os.path.exists(path):
        sys.exit("no resources.csv yet - it fills up as steps are checked")
    data = {}
    for r in csv.DictReader(open(path)):
        if r["state"] != "COMPLETED":
            continue
        key = r["step"] + (f"/{r['class']}" if r["class"] else "")
        d = data.setdefault(key, {"el": [], "mem": []})
        if r["elapsed_s"]:
            d["el"].append(float(r["elapsed_s"]))
        if r["maxrss_mb"]:
            d["mem"].append(float(r["maxrss_mb"]))
    print(f"{'step':<18}{'n':>7}{'median':>9}{'p95':>9}{'max':>9}{'mem p95 MB':>12}{'mem max':>9}   suggested")
    for k, d in sorted(data.items()):
        el, mem = sorted(d["el"]), sorted(d["mem"])
        p95 = el[int(0.95 * (len(el) - 1))] if el else None
        m95 = mem[int(0.95 * (len(mem) - 1))] if mem else None
        sug_t = fmt_s(max(120, 1.5 * max(el))) if el else "-"
        sug_m = f"{max(256, math.ceil(1.5 * max(mem) / 256) * 256):.0f}M" if mem else "-"
        print(f"{k:<18}{len(el):>7}{fmt_s(statistics.median(el)) if el else '-':>9}{fmt_s(p95):>9}"
              f"{fmt_s(max(el)) if el else '-':>9}{(m95 or 0):>12.0f}{(max(mem) if mem else 0):>9.0f}"
              f"   --time={sug_t} --mem={sug_m}")


def cmd_report(cfg, a):
    print(write_report(get_epoch(cfg, a.pointing, a.date)))


def cmd_sync(cfg, a):
    eps = [e for e in find_proc_epochs(cfg, [a.pointing]) if not a.date or e.epoch == a.date]
    for ep in eps:
        print(sync_cmd(ep, a.dest))


def cmd_cancel(cfg, a):
    eps = [e for e in find_proc_epochs(cfg, [a.pointing]) if not a.date or e.epoch == a.date]
    for ep in eps:
        for s in STEPS:
            st = ep.state(s)
            if st.get("status") == "submitted":
                att = st["attempts"][-1]
                ids = [p["job"] for p in att["parts"]] + [att.get("check_job", "")]
                subprocess.run(["scancel"] + [i for i in ids if i])
                st["status"] = "failed"
                st["failed"] = {"all": "cancelled by user"}
                ep.save_state(s, st)
                ep.log(f"{s:<15} CANCELLED jobs {ids}")


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("-c", "--config", default=os.environ.get("CRAFTS_CONFIG", os.path.join(HERE, "crafts_config.ini")))
    sp = ap.add_subparsers(dest="cmd", required=True)

    p = sp.add_parser("plan")
    p.add_argument("pointing")
    p.add_argument("--date")
    p.add_argument("--k", type=int, help="force files per chunk")
    p.add_argument("--force", action="store_true", help="plan even with missing files / no pos")
    p.add_argument("--pos", help="pos file to use (when several match)")
    p.add_argument("--prefix", help="observation prefix (when several in one folder)")

    p = sp.add_parser("run")
    p.add_argument("pointing", nargs="*")
    p.add_argument("--date")
    p.add_argument("--all-ready", action="store_true", help="every clean epoch with pos file (from the audit csv)")
    p.add_argument("--limit", type=int, default=0)
    p.add_argument("--from", dest="from_step", choices=STEPS)
    p.add_argument("--only", choices=STEPS)
    p.add_argument("--k", type=int)
    p.add_argument("--replan", action="store_true")
    p.add_argument("--force", action="store_true")
    p.add_argument("--dry-run", action="store_true")

    p = sp.add_parser("status")
    p.add_argument("pointing", nargs="*")

    for name in ("show", "report"):
        p = sp.add_parser(name)
        p.add_argument("pointing")
        p.add_argument("--date", required=True)

    p = sp.add_parser("rerun-failed")
    p.add_argument("pointing")
    p.add_argument("--date", required=True)
    p.add_argument("--step", required=True, choices=STEPS)
    p.add_argument("--time", help="override time limit, e.g. 00:20:00")
    p.add_argument("--mem", help="override memory, e.g. 2G")
    p.add_argument("--no-continue", action="store_true")
    p.add_argument("--dry-run", action="store_true")

    p = sp.add_parser("mark-done")
    p.add_argument("pointing")
    p.add_argument("--date", required=True)
    p.add_argument("--step", required=True, choices=STEPS)
    p.add_argument("--reason", required=True)

    p = sp.add_parser("check")
    p.add_argument("--epoch-env", required=True)
    p.add_argument("--step", required=True)

    sp.add_parser("resources")

    p = sp.add_parser("sync-cmd")
    p.add_argument("pointing")
    p.add_argument("--date")
    p.add_argument("--dest", default=".")

    p = sp.add_parser("cancel")
    p.add_argument("pointing")
    p.add_argument("--date")

    a = ap.parse_args()
    cfg = load_cfg(a.config)
    fn = {"plan": cmd_plan, "run": cmd_run, "status": cmd_status, "show": cmd_show,
          "rerun-failed": cmd_rerun, "mark-done": cmd_mark_done, "check": cmd_check,
          "resources": cmd_resources, "report": cmd_report, "sync-cmd": cmd_sync, "cancel": cmd_cancel}[a.cmd]
    rc = fn(cfg, a)
    sys.exit(rc or 0)


if __name__ == "__main__":
    main()
