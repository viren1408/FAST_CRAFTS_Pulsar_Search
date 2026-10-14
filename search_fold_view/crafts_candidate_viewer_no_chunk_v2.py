"""
CRAFTS Pulsar Candidate Viewer Pipeline
======================================

PNG candidate reviewer — by default shows ALL candidates across all chunks
in one continuous session.  Use --by_chunk to review chunk-by-chunk instead.

Background PNG preloading uses a worker pool (--preload_workers, default 4)
so the next N images are decoded while you classify the current one.

For each candidate:
    - Opens the candidate PNG (rotated 90° CCW)
    - Candidates are sorted: folded_sn > 9 first, then folded_sn > zero_dm_sn
    - User classifies using keyboard/buttons

Keys:
    P -> PULSAR
    N -> NOISE
    R -> RFI
    S -> SKIP
    B -> BACK
    Q -> QUIT

Outputs:
    candidate_classifications_<timestamp>.txt  — all classifications
    pulsar_candidates_<timestamp>.txt          — PULSAR-only list

Usage
-----
# All candidates in one session (default):
python crafts_candidate_viewer.py \
    --base_dir sifting_output_Dec+2045_01_05 \
    --obs_tag Dec+2045_01_05

# Chunk-by-chunk (original behaviour):
python crafts_candidate_viewer.py \
    --base_dir sifting_output_Dec+2045_01_05 \
    --obs_tag Dec+2045_01_05 \
    --by_chunk

# Faster preloading (up to 32 workers):
python crafts_candidate_viewer.py \
    --base_dir sifting_output_Dec+2045_01_05 \
    --obs_tag Dec+2045_01_05 \
    --preload_workers 18
"""

import os
import sys
import glob
import argparse
import re
from datetime import datetime
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor
import threading

import numpy as np
import matplotlib

matplotlib.use("TkAgg")

import matplotlib.pyplot as plt
import matplotlib.image as mpimg

from matplotlib.gridspec import GridSpec
from matplotlib.widgets import Button


# ─────────────────────────────────────────────────────────────────────────────
# COLUMN DEFINITIONS
# Updated: png_path and fil_path are now explicit columns from build_pdmp_summary.
# archive_name is removed; derive_png_path() is no longer needed.
# ra, dec, start_sec, window are also now present.
# ─────────────────────────────────────────────────────────────────────────────
COLUMNS = [
    "fft_cand", "fft_period", "fft_acc", "fft_dm", "fft_nh", "fft_sn",
    "pdmp_per", "pdmp_per_corr", "pdmp_per_err",
    "period_diff", "period_corr_fact",
    "pdmp_dm", "pdmp_dm_err", "pulse_width",
    "folded_sn", "zero_dm_pulse_width", "zero_dm_folded_sn",
    "sn_ratio", "dc", "sn_boost", "dm_correction",
    "pdmp_acc", "acc_change",
    "ra", "dec", "start_sec", "window",
    "png_path", "fil_path",
]

# Columns that are strings, not floats
STRING_COLS = {"fft_cand", "ra", "dec", "window", "png_path", "fil_path"}

HIGH_SN_THRESHOLD = 9.5
SN_RATIO          = 0.8


# ─────────────────────────────────────────────────────────────────────────────
# PARSING
# ─────────────────────────────────────────────────────────────────────────────
def parse_sorted_cands(filepath):
    candidates = []
    with open(filepath, "r") as fh:
        for line in fh:
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            parts = line.split()
            if len(parts) < len(COLUMNS):
                continue
            try:
                rec = {}
                for i, col in enumerate(COLUMNS):
                    rec[col] = parts[i] if col in STRING_COLS else float(parts[i])
                candidates.append(rec)
            except (ValueError, IndexError):
                continue
    return candidates


# ─────────────────────────────────────────────────────────────────────────────
# CANDIDATE SORTING & FILTERING
# ─────────────────────────────────────────────────────────────────────────────
def sort_candidates(cands):
    """
    Tier 0: folded_sn > HIGH_SN_THRESHOLD  AND  folded_sn > zero_dm_folded_sn
    Tier 1: folded_sn > HIGH_SN_THRESHOLD  (regardless of zero_dm comparison)
    Tier 2: folded_sn > zero_dm_folded_sn  (but folded_sn <= HIGH_SN_THRESHOLD)
    Tier 3: everything else
    Within each tier: descending folded_sn.
    """
    def priority_key(c):
        fsn  = c.get("folded_sn", 0.0)
        zdsn = c.get("zero_dm_folded_sn", 0.0)
        high_sn      = fsn > HIGH_SN_THRESHOLD
        sn_gt_zerodm = fsn > zdsn
        if high_sn and sn_gt_zerodm:
            tier = 0
        elif high_sn:
            tier = 1
        elif sn_gt_zerodm:
            tier = 2
        else:
            tier = 3
        return (tier, -fsn)

    return sorted(cands, key=priority_key)


def filter_tier1(cands):
    return [
        c for c in cands
        if c.get("folded_sn", 0.0) > HIGH_SN_THRESHOLD
        and c.get("zero_dm_folded_sn", 0.0) / c.get("folded_sn", 0.0) <= SN_RATIO
    ]


# ─────────────────────────────────────────────────────────────────────────────
# DISCOVERY
# ─────────────────────────────────────────────────────────────────────────────
def discover_chunks(base_dir, obs_tag, nbeams=19):
    pattern = os.path.join(
        base_dir, "fold_inputs", "fold_results",
        f"{obs_tag}_arcdrift-M*_*_masked", "*", "*", "sorted.cands"
    )
    found = sorted(glob.glob(pattern))
    if not found:
        found = sorted(
            glob.glob(os.path.join(base_dir, "**", "sorted.cands"), recursive=True)
        )

    chunks_dict = defaultdict(list)
    for path in found:
        parts = path.replace("\\", "/").split("/")
        chunk_label = [p for p in parts if p.startswith("chunk")][0]
        chunks_dict[chunk_label].append(path)

    chunks = []
    for chunk_label, paths in sorted(chunks_dict.items()):
        if len(paths) != nbeams:
            print(f"Warning: {chunk_label} has {len(paths)} beams (expected {nbeams})")
        chunks.append({
            "chunk_label":        chunk_label,
            "sorted_cands_paths": sorted(paths),
        })
    return chunks


def load_chunk_candidates(chunk_info, global_id_offset):
    """
    Load and filter candidates for one chunk.
    png_path is read directly from the sorted.cands column — no reconstruction needed.
    """
    cands = []
    idx   = 0
    for path in chunk_info["sorted_cands_paths"]:
        for c in parse_sorted_cands(path):
            c["cand_number"]       = global_id_offset + idx
            c["sorted_cands_path"] = path
            c["chunk_label"]       = chunk_info["chunk_label"]
            # png_path already populated from COLUMNS — no derive_png_path call
            cands.append(c)
            idx += 1

    cands = sort_candidates(cands)
    cands = filter_tier1(cands)

    for new_idx, c in enumerate(cands):
        c["cand_number"] = global_id_offset + new_idx

    return cands



def load_chunk_candidates(chunk_info, global_id_offset):
    cands = []
    idx   = 0
    for path in chunk_info["sorted_cands_paths"]:
        cands_dir = os.path.dirname(path)          # directory containing sorted.cands
        for c in parse_sorted_cands(path):
            c["cand_number"]       = global_id_offset + idx
            c["sorted_cands_path"] = path
            c["chunk_label"]       = chunk_info["chunk_label"]
            # Join the sorted.cands directory with the bare png filename
            c["png_path"]          = os.path.join(cands_dir, c["png_path"])
            cands.append(c)
            idx += 1

    cands = sort_candidates(cands)
    cands = filter_tier1(cands)

    for new_idx, c in enumerate(cands):
        c["cand_number"] = global_id_offset + new_idx

    return cands


# ─────────────────────────────────────────────────────────────────────────────
# BACKGROUND PNG PRELOADER
# Uses a ThreadPoolExecutor so disk I/O + PNG decode happens off the main thread
# while the user is classifying the current candidate.
# ─────────────────────────────────────────────────────────────────────────────
from collections import OrderedDict

class PNGPreloader:
    def __init__(self, n_workers=4, preload_ahead=8, cache_window=40):
        self._executor      = ThreadPoolExecutor(max_workers=n_workers,
                                                 thread_name_prefix="png_loader")
        self._preload_ahead = preload_ahead
        self._cache_window  = cache_window          # how many recent images to keep
        self._cache         = OrderedDict()          # path -> ndarray | None (ordered = insertion order)
        self._pending       = set()
        self._lock          = threading.Lock()

    def load_around(self, cands, current_idx):
        for i in range(current_idx, min(current_idx + self._preload_ahead, len(cands))):
            path = cands[i]["png_path"]
            with self._lock:
                if path not in self._cache and path not in self._pending:
                    self._pending.add(path)
                    self._executor.submit(self._decode, path)
        self._evict_old(cands, current_idx)

    def _evict_old(self, cands, current_idx):
        # drop cached images that are well behind current position
        keep_paths = {
            cands[i]["png_path"]
            for i in range(max(0, current_idx - 5), min(current_idx + self._preload_ahead, len(cands)))
        }
        with self._lock:
            for path in list(self._cache.keys()):
                if path not in keep_paths:
                    del self._cache[path]



    # ── public ───────────────────────────────────────────────────────────────

    def load_around(self, cands, current_idx):
        """Submit preload jobs for the next _preload_ahead candidates."""
        for i in range(current_idx, min(current_idx + self._preload_ahead, len(cands))):
            path = cands[i]["png_path"]
            with self._lock:
                if path not in self._cache and path not in self._pending:
                    self._pending.add(path)
                    self._executor.submit(self._decode, path)

    def get(self, path):
        """Return cached image (or None), blocking until ready if needed."""
        with self._lock:
            if path in self._cache:
                return self._cache[path]
        # Not yet ready — decode synchronously on calling thread
        return self._decode_sync(path)

    def shutdown(self):
        self._executor.shutdown(wait=False)

    # ── private ──────────────────────────────────────────────────────────────

    def _decode(self, path):
        img = self._decode_sync(path)
        with self._lock:
            self._cache[path] = img
            self._pending.discard(path)

    def _decode_sync(self, path):
        try:
            if not os.path.exists(path):
                return None
            img = mpimg.imread(path)
            return np.rot90(img, k=1)   # rotate 90° CCW once, store result
        except Exception:
            return None


# ─────────────────────────────────────────────────────────────────────────────
# STYLE
# ─────────────────────────────────────────────────────────────────────────────
DARK_BG  = "#0d1117"
PANEL_BG = "#161b22"
ACCENT   = "#58a6ff"
GREEN    = "#3fb950"
RED      = "#f85149"
YELLOW   = "#e3b341"
TEXT     = "#c9d1d9"
MUTED    = "#8b949e"
GRID_C   = "#21262d"


def apply_dark_style():
    plt.rcParams.update({
        "figure.facecolor": DARK_BG,
        "axes.facecolor":   PANEL_BG,
        "axes.edgecolor":   GRID_C,
        "axes.labelcolor":  TEXT,
        "xtick.color":      MUTED,
        "ytick.color":      MUTED,
        "text.color":       TEXT,
        "grid.color":       GRID_C,
        "grid.linestyle":   "--",
        "grid.linewidth":   0.5,
        "font.family":      "monospace",
        "figure.dpi":       110,
    })


# ─────────────────────────────────────────────────────────────────────────────
# PNG VIEWER  (single session — receives the full flat candidate list)
# ─────────────────────────────────────────────────────────────────────────────
def run_png_viewer(
    cands,
    total_chunks,           # for display only
    chunk_label_for_title,  # for display only ("ALL" in flat mode)
    classifications,
    output_file,
    preloader: PNGPreloader,
    start_chunk_idx=None,   # used only when --by_chunk
):
    apply_dark_style()

    if not cands:
        return "next"

    total = len(cands)

    state = {
        "idx":     0,
        "result":  "next",
        "done":    False,
        "history": [],
    }

    # ── Figure ────────────────────────────────────────────────────────────────
    fig = plt.figure(figsize=(22, 12), facecolor=DARK_BG)

    # Attempt to maximise window safely across backends/platforms
    try:
        mgr = fig.canvas.manager
        if hasattr(mgr, "window"):
            try:
                mgr.window.state("zoomed")
            except Exception:
                try:
                    mgr.window.attributes("-zoomed", True)
                except Exception:
                    try:
                        mgr.full_screen_toggle()
                    except Exception:
                        pass
        elif hasattr(mgr, "full_screen_toggle"):
            mgr.full_screen_toggle()
    except Exception:
        pass

    # Layout — two columns, two rows
    #   col 0 (70%): image top, button bar bottom
    #   col 1 (30%): info panel (spans both rows)
    gs = GridSpec(
        2, 2,
        figure=fig,
        width_ratios=[7, 3],
        height_ratios=[17, 1],
        left=0.01, right=0.99,
        top=0.95,  bottom=0.02,
        hspace=0.10,
        wspace=0.03,
    )

    ax_img  = fig.add_subplot(gs[0, 0])
    ax_info = fig.add_subplot(gs[0, 1])
    ax_img.axis("off")
    ax_info.axis("off")
    ax_info.set_facecolor(PANEL_BG)

    title_obj = fig.text(
        0.37, 0.970, "",
        ha="center", va="top",
        color=TEXT, fontsize=11,
        fontfamily="monospace", fontweight="bold",
    )

    def make_btn(rect, label, bg):
        bax = fig.add_axes(rect)
        b   = Button(bax, label, color=bg, hovercolor=bg)
        b.label.set_color("white")
        b.label.set_fontsize(10)
        b.label.set_fontweight("bold")
        b.label.set_fontfamily("monospace")
        return b

    btn_p = make_btn([0.01, 0.016, 0.12, 0.055], "P  PULSAR", "#1a5c2a")
    btn_r = make_btn([0.15, 0.016, 0.12, 0.055], "R  RFI",    "#5c1a1a")
    btn_n = make_btn([0.29, 0.016, 0.12, 0.055], "N  NOISE",  "#5c4a1a")
    btn_s = make_btn([0.43, 0.016, 0.09, 0.055], "S  SKIP",   "#2a2a3a")
    btn_b = make_btn([0.54, 0.016, 0.08, 0.055], "B  BACK",   "#2a2a4a")
    btn_q = make_btn([0.64, 0.016, 0.07, 0.055], "Q  QUIT",   "#3a1a3a")

    # Progress bar
    pb_ax = fig.add_axes([0.01, 0.082, 0.70, 0.007])
    pb_ax.set_xlim(0, max(total, 1))
    pb_ax.set_ylim(0, 1)
    pb_ax.axis("off")

    info_text_obj = ax_info.text(
        0.05, 0.97, "",
        transform=ax_info.transAxes,
        ha="left", va="top",
        color=TEXT, fontsize=9,
        fontfamily="monospace",
        linespacing=1.8,
    )

    def draw_progress(idx):
        pb_ax.clear()
        pb_ax.set_xlim(0, max(total, 1))
        pb_ax.set_ylim(0, 1)
        pb_ax.axis("off")
        pb_ax.barh(0, idx,         height=1, color=ACCENT, left=0)
        pb_ax.barh(0, total - idx, height=1, color=GRID_C, left=idx)

    def build_info_text(c, label=""):
        badge = f"  ▶ {label}" if label else ""
        # ra/dec/start_sec/window are now real values from the CSV
        ra        = c.get("ra",        "N/A")
        dec       = c.get("dec",       "N/A")
        start_sec = c.get("start_sec", 0.0)
        window    = c.get("window",    "N/A")
        lines = [
            f"{'─'*28}",
            f" Cand #{c['cand_number']}{badge}",
            f"{'─'*28}",
            "",
            f" {'FFT Period':<18}  {c['fft_period']:.8f} ms",
            f" {'PDMP Period':<18}  {c['pdmp_per']:.8f} ms",
            f" {'PDMP Period corr':<18}  {c['pdmp_per_corr']:.8f} ms",
            f" {'Period err':<18}  {c['pdmp_per_err']:.8f} ms",
            "",
            f" {'FFT DM':<18}  {c['fft_dm']:.3f}",
            f" {'PDMP DM':<18}  {c['pdmp_dm']:.3f}",
            f" {'PDMP DM err':<18}  {c['pdmp_dm_err']:.3f}",
            "",
            f" {'FFT S/N':<18}  {c['fft_sn']:.2f}",
            f" {'Folded S/N':<18}  {c['folded_sn']:.2f}",
            f" {'ZeroDM S/N':<18}  {c['zero_dm_folded_sn']:.2f}",
            f" {'S/N boost':<18}  {c['sn_boost']:.2f}",
            f" {'S/N ratio':<18}  {c['sn_ratio']:.2f}",
            "",
            f" {'Pulse width':<18}  {c['pulse_width']:.4f} ms",
            f" {'ZeroDM width':<18}  {c['zero_dm_pulse_width']:.4f} ms",
            "",
            f" {'FFT acc':<18}  {c['fft_acc']:.4f}",
            f" {'PDMP acc':<18}  {c['pdmp_acc']:.4f}",
            f" {'Acc change':<18}  {c['acc_change']:.4f}",
            "",
            f" {'DM correction':<18}  {c['dm_correction']:.4f}",
            f" {'FFT nharm':<18}  {int(c['fft_nh'])}",
            "",
            f" {'RA':<18}  {ra}",
            f" {'Dec':<18}  {dec}",
            f" {'Start sec':<18}  {start_sec}",
            f" {'Window':<18}  {window}",
            "",
            f"{'─'*28}",
            f" {c['chunk_label']}",
        ]
        return "\n".join(lines)

    def load(idx):
        c = cands[idx]

        # Kick off background preloads for upcoming candidates
        preloader.load_around(cands, idx + 1)

        # ── Left: PNG ────────────────────────────────────────────────────────
        ax_img.clear()
        ax_img.axis("off")

        img = preloader.get(c["png_path"])

        if img is not None:
            ax_img.imshow(img, aspect="equal", interpolation="bilinear")
        else:
            ax_img.text(
                0.5, 0.5,
                f"PNG not found:\n{c['png_path']}",
                ha="center", va="center",
                color=RED, fontsize=10,
                transform=ax_img.transAxes,
            )

        # ── Right: properties ────────────────────────────────────────────────
        existing = classifications.get(c["cand_number"], "")
        info_text_obj.set_text(build_info_text(c, existing))

        # ── Title ────────────────────────────────────────────────────────────
        badge = f"  [{existing}]" if existing else ""

        if chunk_label_for_title == "ALL":
            chunk_str = c["chunk_label"]
        else:
            chunk_str = chunk_label_for_title

        title_obj.set_text(
            f"Cand {idx+1}/{total}  (#{c['cand_number']}){badge}  │  "
            f"{chunk_str}"
        )

        draw_progress(idx)
        fig.canvas.draw_idle()

    def classify(label):
        if state["done"]:
            return
        c   = cands[state["idx"]]
        num = c["cand_number"]
        classifications[num] = label
        state["history"].append(num)
        append_classification(output_file, c, label)
        print(f"    Cand #{num:5d}  ({state['idx']+1}/{total})  ->  {label}")
        state["idx"] += 1
        if state["idx"] >= total:
            state["done"] = True
            plt.close(fig)
        else:
            load(state["idx"])

    def go_back(_e=None):
        if not state["history"]:
            return
        num = state["history"].pop()
        classifications.pop(num, None)
        state["idx"] = max(0, state["idx"] - 1)
        load(state["idx"])

    def do_quit(_e=None):
        state["result"] = "quit"
        state["done"]   = True
        plt.close(fig)

    def on_key(event):
        k = (event.key or "").lower()
        if   k == "p":         classify("PULSAR")
        elif k == "r":         classify("RFI")
        elif k == "n":         classify("NOISE")
        elif k in ("s", " "): classify("SKIP")
        elif k == "b":         go_back()
        elif k == "q":         do_quit()

    btn_p.on_clicked(lambda e: classify("PULSAR"))
    btn_r.on_clicked(lambda e: classify("RFI"))
    btn_n.on_clicked(lambda e: classify("NOISE"))
    btn_s.on_clicked(lambda e: classify("SKIP"))
    btn_b.on_clicked(go_back)
    btn_q.on_clicked(do_quit)

    fig.canvas.mpl_connect("key_press_event", on_key)

    # Seed the preloader for the very first candidate
    preloader.load_around(cands, 0)

    load(0)
    plt.show()

    return state["result"]


# ─────────────────────────────────────────────────────────────────────────────
# FILE I/O
# ─────────────────────────────────────────────────────────────────────────────
_HEADER_COLS = (
    "# Columns: cand_number  label  "
    "fft_period  pdmp_per  "
    "fft_dm  pdmp_dm  "
    "fft_sn  folded_sn  "
    "zero_dm_folded_sn  "
    "sn_boost  "
    "ra  dec  start_sec  window  "
    "chunk_label  "
    "png_path  fil_path\n"
)


def init_output_file(output_file):
    with open(output_file, "w") as fh:
        fh.write("# CRAFTS Candidate Classifications\n")
        fh.write(f"# Started: {datetime.now().isoformat()}\n")
        fh.write(_HEADER_COLS)
        fh.write("#" + "-" * 120 + "\n")


def init_pulsar_file(pulsar_file):
    with open(pulsar_file, "w") as fh:
        fh.write("# CRAFTS Pulsar Candidates\n")
        fh.write(f"# Started: {datetime.now().isoformat()}\n")
        fh.write(
            "# Sorting criteria applied:\n"
            "#   Tier 0 — folded_sn > 9  AND  folded_sn > zero_dm_folded_sn\n"
            "#   Tier 1 — folded_sn > 9\n"
            "#   Tier 2 — folded_sn > zero_dm_folded_sn\n"
            "#   Tier 3 — other\n"
        )
        fh.write(_HEADER_COLS)
        fh.write("#" + "-" * 120 + "\n")


def _format_cand_line(c, label):
    ra        = c.get("ra",        "N/A")
    dec       = c.get("dec",       "N/A")
    start_sec = c.get("start_sec", 0.0)
    window    = c.get("window",    "N/A")
    png_path  = c.get("png_path",  "N/A")
    fil_path  = c.get("fil_path",  "N/A")
    return (
        f"{c['cand_number']:6d}  "
        f"{label:<8s}  "
        f"{c['fft_period']:14.8f}  "
        f"{c['pdmp_per']:14.8f}  "
        f"{c['fft_dm']:10.3f}  "
        f"{c['pdmp_dm']:10.3f}  "
        f"{c['fft_sn']:8.2f}  "
        f"{c['folded_sn']:8.2f}  "
        f"{c['zero_dm_folded_sn']:8.2f}  "
        f"{c['sn_boost']:8.2f}  "
        f"{ra}  {dec}  {start_sec}  {window}  "
        f"{c['chunk_label']}  "
        f"{png_path}  {fil_path}\n"
    )


def append_classification(output_file, c, label):
    with open(output_file, "a") as fh:
        fh.write(_format_cand_line(c, label))


def append_pulsar(pulsar_file, c):
    with open(pulsar_file, "a") as fh:
        fh.write(_format_cand_line(c, "PULSAR"))


def write_master_cands(all_cands, master_file):
    with open(master_file, "w") as fh:
        fh.write("# CRAFTS Master Sorted Candidates\n")
        fh.write(f"# Generated: {datetime.now().isoformat()}\n")
        fh.write("# cand_number  " + "  ".join(COLUMNS) + "  chunk_label\n")
        for c in all_cands:
            vals = [str(c["cand_number"])]
            for col in COLUMNS:
                v = c.get(col, "")
                vals.append(f"{v:.10g}" if isinstance(v, float) else str(v))
            vals.append(c["chunk_label"])
            fh.write("  ".join(vals) + "\n")
    print(f"[SAVED] Master candidate list: {master_file}")


# ─────────────────────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────────────────────
def main():
    parser = argparse.ArgumentParser(description="CRAFTS pulsar candidate viewer")
    parser.add_argument("--base_dir",   required=True)
    parser.add_argument("--obs_tag",    required=True)
    parser.add_argument("--output_dir", default=".")
    parser.add_argument(
        "--start_chunk", type=int, default=0,
        help="0-based chunk index to resume from (only meaningful with --by_chunk)",
    )
    parser.add_argument(
        "--by_chunk", action="store_true",
        help="Review candidates chunk-by-chunk (original behaviour). "
             "Default: all candidates in one continuous session.",
    )
    parser.add_argument(
        "--preload_workers", type=int, default=4,
        help="Number of background threads for PNG preloading (default: 4, max useful: ~32).",
    )
    parser.add_argument(
        "--preload_ahead", type=int, default=16,
        help="How many upcoming PNGs to preload at a time (default: 16).",
    )

    args = parser.parse_args()

    os.makedirs(args.output_dir, exist_ok=True)

    timestamp    = datetime.now().strftime("%Y%m%d_%H%M%S")
    classif_file = os.path.join(args.output_dir, f"candidate_classifications_{timestamp}.txt")
    pulsar_file  = os.path.join(args.output_dir, f"pulsar_candidates_{timestamp}.txt")
    master_file  = os.path.join(args.output_dir, f"master_sorted_cands_{timestamp}.txt")

    chunks = discover_chunks(args.base_dir, args.obs_tag)
    if not chunks:
        print("[ERROR] No sorted.cands files found.")
        sys.exit(1)

    print(f"[INFO] Found {len(chunks)} chunks.")

    all_cands        = []
    chunk_cands_list = []
    offset           = 0

    for ci in chunks:
        cands = load_chunk_candidates(ci, offset)
        chunk_cands_list.append(cands)
        all_cands.extend(cands)
        offset += len(cands)

    print(f"[INFO] Total candidates after filtering: {len(all_cands)}")

    write_master_cands(all_cands, master_file)
    init_output_file(classif_file)
    init_pulsar_file(pulsar_file)

    print(f"[INFO] Saving classifications to : {classif_file}")
    print(f"[INFO] Pulsar candidates mirrored: {pulsar_file}")
    print(f"[INFO] Preload workers           : {args.preload_workers}")
    print(f"[INFO] Preload ahead             : {args.preload_ahead}")
    if args.by_chunk:
        print("[INFO] Mode: chunk-by-chunk")
    else:
        print("[INFO] Mode: all candidates in one session")
    print()

    classifications = {}
    total_chunks    = len(chunks)

    # ── Shared preloader — lives for the whole session ────────────────────────
    preloader = PNGPreloader(
        n_workers=args.preload_workers,
        preload_ahead=args.preload_ahead,
    )

    try:
        # ── FLAT MODE (default) ───────────────────────────────────────────────
        if not args.by_chunk:
            print(f"  Total candidates: {len(all_cands)}")

            result = run_png_viewer(
                cands=all_cands,
                total_chunks=total_chunks,
                chunk_label_for_title="ALL",
                classifications=classifications,
                output_file=classif_file,
                preloader=preloader,
            )

            for c in all_cands:
                if classifications.get(c["cand_number"]) == "PULSAR":
                    append_pulsar(pulsar_file, c)

            if result == "quit":
                print("\n[QUIT] Pipeline stopped.")

        # ── CHUNK-BY-CHUNK MODE (--by_chunk) ─────────────────────────────────
        else:
            for chunk_idx in range(args.start_chunk, total_chunks):
                cands = chunk_cands_list[chunk_idx]
                label = chunks[chunk_idx]["chunk_label"]

                print(f"\n{'='*64}")
                print(f"  Chunk {chunk_idx+1}/{total_chunks} — {label}")
                print(f"  Candidates in this chunk: {len(cands)}")
                print(f"{'='*64}")

                if not cands:
                    print("  (empty — skipping)")
                    continue

                result = run_png_viewer(
                    cands=cands,
                    total_chunks=total_chunks,
                    chunk_label_for_title=label,
                    classifications=classifications,
                    output_file=classif_file,
                    preloader=preloader,
                    start_chunk_idx=chunk_idx,
                )

                for c in cands:
                    if classifications.get(c["cand_number"]) == "PULSAR":
                        append_pulsar(pulsar_file, c)

                if result == "quit":
                    print("\n[QUIT] Pipeline stopped.")
                    break

                print(
                    f"  Chunk {chunk_idx+1} done.  "
                    f"(Resume with --start_chunk {chunk_idx+1})"
                )

    finally:
        preloader.shutdown()

    # ── Summary ───────────────────────────────────────────────────────────────
    counts = {}
    for lbl in classifications.values():
        counts[lbl] = counts.get(lbl, 0) + 1

    print(f"\n{'='*64}")
    print(
        f"  DONE — classified "
        f"{len(classifications)} / {len(all_cands)} candidates"
    )
    for lbl, n in sorted(counts.items()):
        print(f"    {lbl:<8s}: {n}")

    n_pulsar = counts.get("PULSAR", 0)
    print(f"\n  Master list          : {master_file}")
    print(f"  All classifications  : {classif_file}")
    print(f"  Pulsar candidates    : {pulsar_file}  ({n_pulsar} found)")
    print(f"{'='*64}")


if __name__ == "__main__":
    main()
