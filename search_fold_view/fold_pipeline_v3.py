"""
1. Read filterbank
2. Apply RFI mask
3. Read & filter candidates
4. Fold candidates (dspsr -S -T -A)
5. clfd cleaning (batched)
6. pdmp fine tuning
7. Filter weak candidates
8. Refold survivors with corrected P/DM
9. 0-DM folding & comparison
10. Final table generation
"""

import os
import sys
import time
import re
import glob
import shutil
import argparse
import subprocess
import tempfile
import errno
from collections import defaultdict
import psrchive
import numpy as np
import pandas as pd
from decimal import Decimal
from sigpyproc.readers import FilReader
from pathos.multiprocessing import ProcessingPool as Pool
import natsort
import logging
from pathlib import Path

from typing import Iterable, Tuple, List, Union
import math


### USEFUL SUBROUTINES ###

def run_cmd(cmd, dry_run=False):
    logging.info("Running: %s", " ".join(cmd))
    if dry_run:
        return 0
    result = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    if result.returncode != 0:
        logging.error("Command failed:")
        logging.error(result.stderr)
    return result.returncode


c = 299792458.0


def power_of_2(a, mode='nearest'):
    if a <= 0:
        raise ValueError("a must be greater than 0")
    if mode == 'nearest':
        power = round(math.log(a, 2))
    elif mode == 'next_higher':
        power = math.ceil(math.log(a, 2))
    else:
        raise ValueError("mode must be 'nearest' or 'next_higher'")
    return 2 ** power


### READ FILTERBANK HEADER ###

def get_fil_info(fil_path):
    if os.path.isfile(fil_path):
        fil_read = FilReader(fil_path)
        total_samples = fil_read.header.nsamples
        fft_size = power_of_2(total_samples, mode='nearest')
        source_name = fil_read.header.source
        total_samples = fft_size
        mid_mjd = fil_read.header.tstart + (total_samples * fil_read.header.tsamp) / (2.0 * 86400.0)
        mid_ob_mjd = "%.12f" % Decimal(mid_mjd)
        ra = fil_read.header.ra
        dec = fil_read.header.dec
        tobs = fil_read.header.tobs
    else:
        print("filterbank file to get info does not exist")
    return source_name, tobs, mid_ob_mjd, ra, dec, total_samples, tobs


### FIRST FILTERING STEP ###

def create_filtered_cands(cand_file_path, folding_dir, cand_prefix_name, ra, dec):
    """
    Filter candidates and write CSV. ra/dec passed in from cached fil header
    so they are stored in the CSV for all downstream functions to use.
    """
    try:
        with open(cand_file_path, 'r') as _f:
            _first_line = _f.readline()
        _sep = ',' if ',' in _first_line else r'\s+'
        df = pd.read_csv(cand_file_path, sep=_sep)

        c1 = (df['fft_sn'] >= 8.0) & (df['nh'] > 0)
        c2 = (df['fft_sn'] >= 10) & (df['nh'] == 0)
        c3 = (df['period'] >= 0.03) & (df['period'] <= 0.100) & (df['fft_sn'] >= 7.5) & (df['fft_sn'] < 8.0) & (df['nh'] > 0) & (df['nh'] <= 4)
        c4 = (df['period'] >  0.1) & (df['period'] <= 0.2)  & (df['fft_sn'] >= 7.0) & (df['fft_sn'] < 8.0) & (df['nh'] >= 2)
        c5 = (df['period'] >  0.2) & (df['fft_sn'] >= 7.0) & (df['fft_sn'] < 7.5) & (df['nh'] >= 2)
        c6 = (df['period'] >= 0.3) & (df['fft_sn'] >= 6.0) & (df['fft_sn'] <= 7.0) & (df['nh'] >= 3)
        c7 = (df['period'] >= 0.5) & (df['fft_sn'] >= 5.5) & (df['fft_sn'] <= 6.0) & (df['nh'] >= 3)
        c8 = (df['period'] >= 1.0) & (df['fft_sn'] >= 5.0) & (df['fft_sn'] < 5.5)  & (df['nh'] >= 3)

        df_filtered = df[c1 | c2 | c3 | c4 | c5 | c6 | c7 | c8].copy()
        #c1 = (df['period'] >= 2.0) #test

        #df_filtered = df[c1].copy() #test
        

        # Store ra/dec from filterbank header — written once here, read by all
        # downstream functions from the CSV (no repeated get_fil_info calls)
        df_filtered["ra"]  = ra
        df_filtered["dec"] = dec

        df_filtered.to_csv(
            "%s/%s_filtered_cands.csv" % (folding_dir, cand_prefix_name),
            sep='\t', index=False
        )
        return df_filtered

    except Exception as e:
        print("An error occurred while filtering candidates:", e)
        return None


def create_cands_chunk_files_from_filtered_cand_table(
    cand_table_path: str,
    uniq_fil_prefix: str,
    mid_ob_mjd: str,           # passed in from cached get_fil_info — no re-read
    chunk_size: int = 500,
    output_file_base: str = "cands_chunk",
) -> Tuple[int, List[Tuple[str, float]]]:
    """
    Reads the filtered candidate table, groups by window,
    and writes dspsr chunk plan files.

    ra/dec are read from the CSV itself (written there by create_filtered_cands).
    mid_ob_mjd is passed in from the pipeline-level cached get_fil_info call.

    Returns:
        (total_written, [(chunk_filename, start_sec), ...])
    """
    df = pd.read_csv(cand_table_path, sep=r'\s+', comment="#")

    required = ["period", "dm", "acc"]
    missing = [col for col in required if col not in df.columns]
    if missing:
        raise ValueError(f"Missing required columns in {cand_table_path}: {missing}")

    has_cand_no   = "cand_no"   in df.columns
    has_start_sec = "start_sec" in df.columns
    has_window    = "window"    in df.columns
    has_ra_dec    = "ra" in df.columns and "dec" in df.columns

    header = "SOURCE EPOCH PERIOD DM ACC RA DEC\n"
    chunk_files: List[Tuple[str, float]] = []
    total_written = 0

    groups = df.groupby("window") if has_window else [("all", df)]

    for window_id, df_win in groups:
        start_sec = float(df_win["start_sec"].iloc[0]) if has_start_sec else 0.0

        # ra/dec from the CSV row (same value for all rows in one fil, but
        # stored per-row so the CSV is self-contained)
        row0 = df_win.iloc[0]
        ra  = row0["ra"]  if has_ra_dec else "00:00:00.00"
        dec = row0["dec"] if has_ra_dec else "00:00:00.00"

        lines_buffer: List[str] = []
        chunk_counter = 1

        for idx, row in df_win.iterrows():
            period  = float(row["period"])
            dm      = float(row["dm"])
            acc     = float(row["acc"])
            cand_no = int(row["cand_no"]) if has_cand_no else int(idx)

            per = round(period, 3)
            d   = int(dm)
            source_str = f"{uniq_fil_prefix}_{cand_no}_candid_{per}_p_{d}_dm"

            lines_buffer.append(
                f"{source_str} {mid_ob_mjd} "
                f"{period:.11f} {dm:.6f} {acc:.6f} {ra} {dec}\n"
            )
            total_written += 1

            if len(lines_buffer) >= chunk_size:
                chunk_filename = f"{output_file_base}_win{window_id}_{chunk_counter}.txt"
                with open(chunk_filename, "w", encoding="utf-8") as out:
                    out.write(header)
                    out.writelines(lines_buffer)
                chunk_files.append((chunk_filename, start_sec))
                lines_buffer = []
                chunk_counter += 1

        if lines_buffer:
            chunk_filename = f"{output_file_base}_win{window_id}_{chunk_counter}.txt"
            with open(chunk_filename, "w", encoding="utf-8") as out:
                out.write(header)
                out.writelines(lines_buffer)
            chunk_files.append((chunk_filename, start_sec))

    return total_written, chunk_files


LONG_PERIOD_THRESHOLD = 1.0  


def create_cands_chunk_files_from_filtered_cand_table(
    cand_table_path: str,
    uniq_fil_prefix: str,
    mid_ob_mjd: str,
    chunk_size: int = 500,
    output_file_base: str = "cands_chunk",
    long_period_threshold: float = LONG_PERIOD_THRESHOLD,
) -> Tuple[int, List[Tuple[str, float]], int, List[Tuple[str, float]]]:
    """
    Reads the filtered candidate table, groups by window, and writes dspsr
    chunk plan files.

    Returns:
        (n_short, short_chunk_files, n_long, long_chunk_files)

        ms_chunk_files -> passed to run_dspsr_multifold        (P <= threshold)
        sec_chunk_files  -> passed to run_dspsr_multifold_singlepulse 
    """
    df = pd.read_csv(cand_table_path, sep=r'\s+', comment="#")

    required = ["period", "dm", "acc"]
    missing = [col for col in required if col not in df.columns]
    if missing:
        raise ValueError(f"Missing required columns in {cand_table_path}: {missing}")

    has_cand_no   = "cand_no"   in df.columns
    has_start_sec = "start_sec" in df.columns
    has_window    = "window"    in df.columns
    has_ra_dec    = "ra" in df.columns and "dec" in df.columns

    header = "SOURCE EPOCH PERIOD DM ACC RA DEC\n"

    ms_chunk_files: List[Tuple[str, float]] = []
    sec_chunk_files:  List[Tuple[str, float]] = []
    total_ms  = 0
    total_sec = 0

    groups = df.groupby("window") if has_window else [("all", df)]

    for window_id, df_win in groups:
        start_sec = float(df_win["start_sec"].iloc[0]) if has_start_sec else 0.0

        row0 = df_win.iloc[0]
        ra  = row0["ra"]  if has_ra_dec else "00:00:00.00"
        dec = row0["dec"] if has_ra_dec else "00:00:00.00"

        ms_buf: List[str] = []
        sec_buf:  List[str] = []
        ms_counter = 1
        sec_counter = 1

        def _flush(buf, chunk_files, counter, tag):
            fn = f"{output_file_base}_{tag}_win{window_id}_{counter}.txt"
            with open(fn, "w", encoding="utf-8") as out:
                out.write(header)
                out.writelines(buf)
            chunk_files.append((fn, start_sec))
            buf.clear()
            return counter + 1

        for idx, row in df_win.iterrows():
            period  = float(row["period"])
            dm      = float(row["dm"])
            acc     = float(row["acc"])
            cand_no = int(row["cand_no"]) if has_cand_no else int(idx)

            per = round(period, 3)
            d   = int(dm)
            source_str = (
                f"{uniq_fil_prefix}_{cand_no}_candid_{per}_p_{d}_dm"
            )
            line = (
                f"{source_str} {mid_ob_mjd} "
                f"{period:.11f} {dm:.6f} {acc:.6f} {ra} {dec}\n"
            )

            if period > long_period_threshold:
                sec_buf.append(line)
                total_sec += 1
                if len(sec_buf) >= chunk_size:
                    sec_counter = _flush(sec_buf, sec_chunk_files, sec_counter, "long_period")
            else:
                ms_buf.append(line)
                total_ms += 1
                if len(ms_buf) >= chunk_size:
                    ms_counter = _flush(ms_buf, ms_chunk_files, ms_counter, "short")

        if ms_buf:
            _flush(ms_buf, ms_chunk_files, ms_counter, "short")
        if sec_buf:
            _flush(sec_buf,  sec_chunk_files,  sec_counter,  "long_period")

    print(
        f"[INFO] Chunk split: {total_ms} short-period (P ≤ {long_period_threshold}s) "
        f"->{len(ms_chunk_files)} files; "
        f"->{total_sec} long-period (P > {long_period_threshold}s) "
        f"→ {len(sec_chunk_files)} files."
    )
    return total_ms, ms_chunk_files, total_sec, sec_chunk_files

def create_cands_chunk_files_from_pdmp(
    survived_clfds: Iterable[str],
    pdmp_extract_values,
    uniq_fil_prefix: str,
    mid_ob_mjd: str,
    filtered_cand_table_path: str,
    chunk_size: int = 500,
    output_file_base: str = "cands_chunk",
    long_period_threshold: float = LONG_PERIOD_THRESHOLD,
) -> Tuple[int, List[Tuple[str, float]], int, List[Tuple[str, float]]]:
    """
    Builds re-fold chunk files for pdmp survivors, split by period.

    Returns:
        (n_short, short_chunk_files, n_long, long_chunk_files)

        ms_chunk_files  — passed to run_dspsr_multifold             (P <= threshold)
        sec_chunk_files   — passed to run_dspsr_multifold_singlepulse (P > threshold)
    """
    df_orig = pd.read_csv(filtered_cand_table_path, sep=r'\s+', comment="#")

    has_start_sec = "start_sec" in df_orig.columns
    has_window    = "window"    in df_orig.columns
    has_ra_dec    = "ra" in df_orig.columns and "dec" in df_orig.columns

    if "cand_no" in df_orig.columns:
        start_sec_lookup = df_orig.set_index("cand_no")["start_sec"].to_dict() if has_start_sec else {}
        window_lookup    = df_orig.set_index("cand_no")["window"].to_dict()    if has_window    else {}
        ra_lookup        = df_orig.set_index("cand_no")["ra"].to_dict()        if has_ra_dec    else {}
        dec_lookup       = df_orig.set_index("cand_no")["dec"].to_dict()       if has_ra_dec    else {}
    else:
        start_sec_lookup = window_lookup = ra_lookup = dec_lookup = {}

    header = "SOURCE EPOCH PERIOD DM ACC RA DEC\n"

    # Group survivors by (window_id, start_sec) — dspsr needs homogeneous
    # start_sec per chunk, same as before
    groups: dict = defaultdict(list)

    for clfd in survived_clfds:
        m = re.search(r'_(\d+)_candid', Path(clfd).name)
        cand_no = int(m.group(1)) if m else -1

        if cand_no not in start_sec_lookup:
            print(f"[WARN] cand_no {cand_no} not found in filtered cand table; defaulting start_sec=0.0")

        start_sec = float(start_sec_lookup.get(cand_no, 0.0))
        window_id = window_lookup.get(cand_no, "all")
        groups[(window_id, start_sec)].append((cand_no, clfd))

    ms_chunk_files: List[Tuple[str, float]] = []
    sec_chunk_files:  List[Tuple[str, float]] = []
    total_short = 0
    total_long = 0
    chunk_counter = 1

    def _flush(buf, chunk_files, window_id, start_sec, tag):
        nonlocal chunk_counter
        fn = f"{output_file_base}_{tag}_win{window_id}_{chunk_counter}.txt"
        with open(fn, "w") as out:
            out.write(header)
            out.writelines(buf)
        chunk_files.append((fn, start_sec))
        buf.clear()
        chunk_counter += 1

    for (window_id, start_sec), cand_list in groups.items():
        ms_buf: List[str] = []
        sec_buf:  List[str] = []

        for cand_no, clfd in cand_list:
            txt = f"{clfd}.txt"
            p_corr, _, _, best_dm = pdmp_extract_values(txt)

            period = float(p_corr)
            dm     = float(best_dm)
            per    = round(period, 3)
            dmi    = int(dm)

            ra  = ra_lookup.get(cand_no,  "00:00:00.00")
            dec = dec_lookup.get(cand_no, "00:00:00.00")

            source_str = f"{uniq_fil_prefix}_{cand_no}_candid_{per}_p_{dmi}_dm"
            line = (
                f"{source_str} {mid_ob_mjd} "
                f"{period:.11f} {dm:.6f} 0.000000 {ra} {dec}\n"
            )

            if period > long_period_threshold:
                sec_buf.append(line)
                total_long += 1
                if len(sec_buf) >= chunk_size:
                    _flush(sec_buf, sec_chunk_files, window_id, start_sec, "long")
            else:
                ms_buf.append(line)
                total_short += 1
                if len(ms_buf) >= chunk_size:
                    _flush(ms_buf, ms_chunk_files, window_id, start_sec, "short")

        if ms_buf:
            _flush(ms_buf, ms_chunk_files, window_id, start_sec, "short")
        if sec_buf:
            _flush(sec_buf, sec_chunk_files, window_id, start_sec, "long")

    print(
        f"[INFO] Re-fold chunk split: {total_short} short-period (P ≤ {long_period_threshold}s) "
        f"→ {len(ms_chunk_files)} files; "
        f"{total_long} long-period (P > {long_period_threshold}s) "
        f"→ {len(sec_chunk_files)} files."
    )
    return total_short, ms_chunk_files, total_long, sec_chunk_files



### DSPSR MULTI-FOLD ###

def run_dspsr_multifold(
    chunk_file: str,
    fil_file: str,
    ar_outdir: str,
    start_sec: float,
    T_window: float,
    dspsr_threads: int = 4,
    verbose: bool = False,
    cmd_log_file: str = None,
):
    ar_out     = Path(ar_outdir).resolve()
    chunk_path = Path(chunk_file).resolve()
    fil_path   = Path(fil_file).resolve()

    ar_out.mkdir(parents=True, exist_ok=True)

    if not chunk_path.exists():
        raise FileNotFoundError(f"Chunk file not found: {chunk_path}")
    if not fil_path.exists():
        raise FileNotFoundError(f"Filterbank file not found: {fil_path}")

    sources = []
    with chunk_path.open("r", encoding="utf-8", errors="replace") as f:
        f.readline()  # discard header
        for line in f:
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            parts = line.split()
            if parts:
                sources.append(parts[0])

    cmd = [
        "dspsr",
        "-k", "FAST",
        "-L", "1.0",
        "-Lmin", "0.999",
        "-S", str(start_sec),
        "-T", str(T_window),
        "-t", str(dspsr_threads),
        "-A",
        "-b", "128",
        "-U", "1024",
        "-w", str(chunk_path),
        str(fil_path),
    ]

    cmd_str = " ".join(cmd)
    print(f"[INFO] Running: {cmd_str}")

    # --- verbose: log every dspsr command to file ---
    if verbose and cmd_log_file:
        with open(cmd_log_file, "a") as log:
            log.write(f"# chunk: {chunk_path.name}  start_sec: {start_sec}  T_window: {T_window}\n")
            log.write(cmd_str + "\n\n")

    subprocess.run(cmd, check=True)

    moved = missing_dirs = missing_ar = multi_ar = 0

    for src in sources:
        src_dir = Path(src)
        if not src_dir.is_dir():
            missing_dirs += 1
            print(f"[WARN] Expected dspsr output dir not found: {src_dir}")
            continue

        ar_files = sorted(src_dir.glob("*.ar"))
        if len(ar_files) == 0:
            missing_ar += 1
            print(f"[WARN] No .ar found in: {src_dir}")
            continue
        if len(ar_files) > 1:
            multi_ar += 1
            print(f"[WARN] Multiple .ar files in {src_dir}; leaving directory untouched.")
            continue

        ar_in  = ar_files[0]
        ar_tmp = src_dir / f"{src_dir.name}.ar"
        if ar_tmp.exists():
            ar_tmp = src_dir / f"{src_dir.name}_{ar_in.stem}.ar"

        try:
            ar_in.rename(ar_tmp)
        except Exception as e:
            print(f"[WARN] Could not rename {ar_in} -> {ar_tmp}: {e}")
            continue

        ar_dest = ar_out / ar_tmp.name
        if ar_dest.exists():
            base, suf = ar_dest.stem, 1
            while True:
                candidate = ar_out / f"{base}_{suf}.ar"
                if not candidate.exists():
                    ar_dest = candidate
                    break
                suf += 1

        try:
            shutil.move(str(ar_tmp), str(ar_dest))
            moved += 1
        except Exception as e:
            print(f"[WARN] Could not move {ar_tmp} -> {ar_dest}: {e}")
            continue

        try:
            leftovers = list(src_dir.iterdir())
            if not leftovers:
                src_dir.rmdir()
            else:
                print(f"[INFO] Not deleting {src_dir} ({len(leftovers)} items remain).")
        except Exception as e:
            print(f"[WARN] Could not delete directory {src_dir}: {e}")

    print(
        f"[INFO] Chunk {chunk_path.name}: moved {moved} .ar files; "
        f"missing_dirs={missing_dirs}, missing_ar={missing_ar}, multi_ar={multi_ar}"
    )

##DSPSR for single_pulse_mode###

def run_dspsr_multifold_singlepulse(
    chunk_file: str,
    fil_file: str,
    ar_outdir: str,
    start_sec: float,
    T_window: float,
    dspsr_threads: int = 4,
    verbose: bool = False,
    cmd_log_file: str = None,
):
    ar_out     = Path(ar_outdir).resolve()
    chunk_path = Path(chunk_file).resolve()
    fil_path   = Path(fil_file).resolve()

    ar_out.mkdir(parents=True, exist_ok=True)

    if not chunk_path.exists():
        raise FileNotFoundError(f"Chunk file not found: {chunk_path}")
    if not fil_path.exists():
        raise FileNotFoundError(f"Filterbank file not found: {fil_path}")

    sources = []
    with chunk_path.open("r", encoding="utf-8", errors="replace") as f:
        f.readline()  # discard header
        for line in f:
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            parts = line.split()
            if parts:
                sources.append(parts[0])

    cmd = [
        "dspsr",
        "-k", "FAST",
        "-s",    
        "-K" ,              
        "-S", str(start_sec),
        "-T", str(T_window),
        "-t", str(dspsr_threads),
        "-A",
        "-b", "128",
        "-U", "1024",
        "-w", str(chunk_path),
        str(fil_path),
    ]

    cmd_str = " ".join(cmd)
    print(f"[INFO] Running (single-pulse mode): {cmd_str}")

    if verbose and cmd_log_file:
        with open(cmd_log_file, "a") as log:
            log.write(f"# [single-pulse] chunk: {chunk_path.name}  start_sec: {start_sec}  T_window: {T_window}\n")
            log.write(cmd_str + "\n\n")

    subprocess.run(cmd, check=True)

    moved = missing_dirs = missing_ar = multi_ar = 0

    for src in sources:
        src_dir = Path(src)
        if not src_dir.is_dir():
            missing_dirs += 1
            print(f"[WARN] Expected dspsr output dir not found: {src_dir}")
            continue

        ar_files = sorted(src_dir.glob("*.ar"))
        if len(ar_files) == 0:
            missing_ar += 1
            print(f"[WARN] No .ar found in: {src_dir}")
            continue
        if len(ar_files) > 1:
            multi_ar += 1
            print(f"[WARN] Multiple .ar files in {src_dir}; leaving directory untouched.")
            continue

        ar_in  = ar_files[0]
        ar_tmp = src_dir / f"{src_dir.name}.ar"
        if ar_tmp.exists():
            ar_tmp = src_dir / f"{src_dir.name}_{ar_in.stem}.ar"

        try:
            ar_in.rename(ar_tmp)
        except Exception as e:
            print(f"[WARN] Could not rename {ar_in} -> {ar_tmp}: {e}")
            continue

        ar_dest = ar_out / ar_tmp.name
        if ar_dest.exists():
            base, suf = ar_dest.stem, 1
            while True:
                candidate = ar_out / f"{base}_{suf}.ar"
                if not candidate.exists():
                    ar_dest = candidate
                    break
                suf += 1

        try:
            shutil.move(str(ar_tmp), str(ar_dest))
            moved += 1
        except Exception as e:
            print(f"[WARN] Could not move {ar_tmp} -> {ar_dest}: {e}")
            continue

        try:
            leftovers = list(src_dir.iterdir())
            if not leftovers:
                src_dir.rmdir()
            else:
                print(f"[INFO] Not deleting {src_dir} ({len(leftovers)} items remain).")
        except Exception as e:
            print(f"[WARN] Could not delete directory {src_dir}: {e}")

    print(
        f"[INFO] Chunk {chunk_path.name} [single-pulse]: moved {moved} .ar files; "
        f"missing_dirs={missing_dirs}, missing_ar={missing_ar}, multi_ar={multi_ar}"
    )
### CLFD ###

def clfd(archive_files, batch_size=100):
    for i in range(0, len(archive_files), batch_size):
        batch = archive_files[i:i + batch_size]
        cmd = [
            "clfd",
            *batch,
            "--features", "std", "ptp", "lfamp",
            "--qmask", "2.0",
            "--despike",
            "--qspike", "4.0",
            "--processes", "1",
        ]
        run_cmd(cmd)


### PDMP ###

def pdmp_fine_tune(
    archive_file,
    fil_file_path,
    png_filename,
    acc_search=0,
    dm_search=1,
    acc_range=None,
    acc_step_size=None,
):
    ar = psrchive.Archive_load(archive_file)
    n_subint = ar.get_nsubint()
    #ms = min(24, max(1, n_subint - 1))
    # Pick base command depending on archive length
    if n_subint >= 24:
        pdmp_cmd = "pdmp -mc 32 -ms 24 -mb 128"
    else:
        pdmp_cmd = f"pdmp -mc 32 -mb 128 -ms {n_subint-1}"

    # Append optional acc/dm search flags
    do_acc = (acc_search == 1) and (acc_range is not None) and (acc_step_size is not None)

    if do_acc and dm_search == 1:
        pdmp_cmd += f" -ar {acc_range} -as {acc_step_size}"
    elif do_acc and dm_search == 0:
        pdmp_cmd += f" -ar {acc_range} -as {acc_step_size} -dr 0.1 -ds 0.1"
    elif (not do_acc) and dm_search == 0:
        pdmp_cmd += " -do 0 -dr 0.1 -ds 0.01"

    pdmp_cmd += f" -g {png_filename}.eps/vcps {archive_file} > {archive_file}.txt"
    print(f"pdmp cmd is {pdmp_cmd}")
    return pdmp_cmd

def pdmp_extract_values(fname):
    tc_line = dm_line = None
    with open(fname) as f:
        for line in f:
            if "Best TC Period (ms) = " in line:
                tc_line = line
            if "Best DM = " in line:
                dm_line = line
    if not tc_line or not dm_line:
        raise ValueError("Required line(s) not found.")

    def extract(line, key, end=None):
        val = line.split(key, 1)[1]
        if end:
            val = val.split(end, 1)[0]
        return float(val.strip())

    best_tc_period       = extract(tc_line, "Best TC Period (ms) = ", "  Correction (ms)") / 1000.0
    tc_period_correction = extract(tc_line, "Correction (ms) = ",     "  Error (ms)")
    tc_period_error      = extract(tc_line, "Error (ms) = ")
    best_dm              = extract(dm_line,  "Best DM = ",             "  Correction")
    return best_tc_period, tc_period_correction, tc_period_error, best_dm


def filter_table_by_snr(table_path, snr_threshold, dm_threshold):
    print("table path is", table_path)
    with open(table_path, 'r') as f_in, open('pdmp_filtered.txt', 'w') as f_out:
        for line in f_in:
            columns = line.split()
            snr = float(columns[6])
            dm  = float(columns[3])
            if (snr >= snr_threshold) and (dm > dm_threshold):
                f_out.write(line)
            else:
                file_path = columns[7]
                png_file  = "%s.eps" % file_path.strip().split("/")[-1].split(".ar.clfd")[0]
                text_file = "%s.txt" % file_path
                ar_file   = "%s"     % file_path.strip().split("/")[-1].split(".clfd")[0]
                for fp in [file_path, png_file, ar_file, text_file]:
                    if os.path.exists(fp):
                        os.remove(fp)
    return 'pdmp_filtered.txt'


def zero_dm_cleaning_clfd_pdmp_step(fil_file_path):
    archives_at_0dm = glob.glob("*.0dm")
    clfd(archives_at_0dm, batch_size=100)
    clfd_files = glob.glob("*.0dm.clfd")

    pdmp_list = []
    for ii in clfd_files:
        modified  = re.sub(r"acc_\d+", "acc_0", ii.strip())
        png_fname = modified.split("_dm")[0] + "_dm_0dm"
        pdmp_list.append(pdmp_fine_tune(ii, fil_file_path, png_fname, acc_search=0, dm_search=0))

    def use_pdmp(cmd):
        os.system(cmd)

    pool = Pool(4)
    pool.map(use_pdmp, pdmp_list)


def build_pdmp_summary(
    filtered_cand_list: str,
    fil_file_path: str,                        # for fil_path column in output
    output_txt: str = "output.txt",
    sorted_out: str = "sorted.cands",
    write_sorted: bool = True,
):
    """
    Builds the final candidate summary table.
    ra, dec, start_sec, window are read from the filtered cands CSV.
    archive_name replaced by png_path and fil_path.
    """
    df_orig = pd.read_csv(filtered_cand_list, sep=r'\s+', comment="#")

    has_ra_dec    = "ra"        in df_orig.columns and "dec" in df_orig.columns
    has_start_sec = "start_sec" in df_orig.columns
    has_window    = "window"    in df_orig.columns

    if "cand_no" in df_orig.columns:
        ra_lookup        = df_orig.set_index("cand_no")["ra"].to_dict()        if has_ra_dec    else {}
        dec_lookup       = df_orig.set_index("cand_no")["dec"].to_dict()       if has_ra_dec    else {}
        start_sec_lookup = df_orig.set_index("cand_no")["start_sec"].to_dict() if has_start_sec else {}
        window_lookup    = df_orig.set_index("cand_no")["window"].to_dict()    if has_window    else {}
    else:
        ra_lookup = dec_lookup = start_sec_lookup = window_lookup = {}

    os.system("cat pdmp.per | grep .0dm > 0_DM.per")

    with open("0_DM.per",          'r') as f2_h: f2 = f2_h.readlines()
    with open("pdmp_filtered.txt", 'r') as f3_h: f3 = f3_h.readlines()
    with open(filtered_cand_list,  'r') as f4_h: f4 = f4_h.readlines()[1:]

    header = (
        "fft_cand fft_period fft_acc fft_dm fft_nh fft_sn "
        "pdmp_per pdmp_per_corr pdmp_per_err "
        "period_diff period_corr_fact(%) "
        "pdmp_dm pdmp_dm_err "
        "pulse_width folded_sn "
        "zero_dm_pulse_width zero_dm_folded_sn "
        "sn_ratio dc(%) sn_boost dm_correction(%) "
        "pdmp_acc acc_change(%) "
        "ra dec start_sec window "
        "png_path fil_path\n"
    )

    with open(output_txt, "w") as outfile:
        outfile.write(header)

        for ii in f2:
            parts_ii = ii.strip().split()
            if len(parts_ii) < 8:
                continue
            zero_dm_pulse_width = parts_ii[5]
            zero_dm_folded_sn   = float(parts_ii[6])
            cand_id = parts_ii[7].strip().split("_candid")[0].split("_")[-1]

            found_in_pdmp = False

            for jj in f3:
                parts_jj = jj.strip().split()
                if len(parts_jj) < 12:
                    continue

                pdmp_acc    = float(parts_jj[11])
                pdmp_dm     = float(parts_jj[3])
                pdmp_dm_err = parts_jj[4]
                pulse_width = parts_jj[5]
                folded_sn   = float(parts_jj[6])
                name2       = parts_jj[7]

                cand_no_str = name2.strip().split("_candid")[0].split("_")[-1]
                if cand_id != cand_no_str:
                    continue

                found_in_pdmp = True

                # png_path: eps converted to png before this function is called
                clfd_stem = name2.strip().split(".ar.clfd")[0]
                png_path  = clfd_stem + ".png"

                tc_per_correct, tc_per_corr, tc_per_err, _ = pdmp_extract_values(f"{name2}.txt")
                tc_per_correct = float(tc_per_correct) * 1000.0

                for kk in f4:
                    parts_kk = kk.strip().split()
                    if len(parts_kk) < 6:
                        continue
                    if cand_id != parts_kk[0]:
                        continue

                    fft_cand   = parts_kk[0]
                    fft_period = float(parts_kk[1]) * 1000.0
                    fft_acc    = float(parts_kk[2])
                    fft_dm     = float(parts_kk[3])
                    fft_nh     = parts_kk[4]
                    fft_sn     = float(parts_kk[5])

                    pulse_width_f    = float(pulse_width)
                    dc               = (pulse_width_f / fft_period) * 100.0
                    sn_ratio         = zero_dm_folded_sn / folded_sn
                    sn_boost         = folded_sn / fft_sn
                    dm_correction    = (abs(pdmp_dm - fft_dm) / fft_dm) * 100.0
                    period_diff      = abs(fft_period - tc_per_correct)
                    period_corr_fact = (period_diff / fft_period) * 100.0

                    if fft_acc == 0.0:
                        acc_change = abs(pdmp_acc)
                    elif pdmp_acc == 0.0:
                        acc_change = 0.0
                    elif (fft_acc < 0) == (pdmp_acc < 0):
                        acc_change = max(abs(pdmp_acc), abs(fft_acc)) / min(abs(pdmp_acc), abs(fft_acc))
                    else:
                        acc_change = (abs(fft_acc) + abs(pdmp_acc)) / abs(fft_acc)

                    # Per-candidate metadata from CSV lookups
                    cand_no_int = int(cand_id)
                    ra        = ra_lookup.get(cand_no_int,        "N/A")
                    dec       = dec_lookup.get(cand_no_int,       "N/A")
                    start_sec = start_sec_lookup.get(cand_no_int, 0.0)
                    window    = window_lookup.get(cand_no_int,    "N/A")

                    outfile.write(
                        "%s %.4f %.2f %.3f %s %.2f "
                        "%.4f %s %s "
                        "%.4f %.4f "
                        "%.3f %s "
                        "%s %.2f "
                        "%s %.2f "
                        "%.2f %.2f %.2f %.2f "
                        "%.2f %.2f "
                        "%s %s %.2f %s "
                        "%s %s\n"
                        % (
                            fft_cand, fft_period, fft_acc, fft_dm, fft_nh, fft_sn,
                            tc_per_correct, tc_per_corr, tc_per_err,
                            period_diff, period_corr_fact,
                            pdmp_dm, pdmp_dm_err,
                            pulse_width, folded_sn,
                            zero_dm_pulse_width, zero_dm_folded_sn,
                            sn_ratio, dc, sn_boost, dm_correction,
                            pdmp_acc, acc_change,
                            ra, dec, start_sec, window,
                            png_path, fil_file_path,
                        )
                    )
                    break
                else:
                    print(f"cand_id {cand_id} found in only two files")
                break

            if not found_in_pdmp:
                print(f"cand_id {cand_id} not found in pdmp_filtered.txt")

    if write_sorted:
        os.system(f"cat {output_txt} | column -t > {sorted_out}")

    return output_txt




def query_atnf_nearby_pulsars(
    ra_str: str,
    dec_str: str,
    radius_arcmin: float = 75.0,
    output_file: str = "nearby_pulsars.txt",
) -> pd.DataFrame:
    """
    Query ATNF catalogue for pulsars within radius_arcmin of the filterbank
    pointing. ra_str/dec_str are sigpyproc format e.g. "02:25:58.8986".
    """
    from psrqpy import QueryATNF
    import astropy.units as u
    from astropy.coordinates import SkyCoord

    coord   = SkyCoord(ra=ra_str, dec=dec_str, unit=(u.hourangle, u.deg))
    ra_deg  = coord.ra.deg
    dec_deg = coord.dec.deg

    print(f"[ATNF] Querying pulsars within {radius_arcmin} arcmin of RA={ra_deg:.4f} DEC={dec_deg:.4f}")

    query = QueryATNF(
        params=["JNAME", "RAJ", "DECJ", "P0", "DM", "S1400", "DIST"],
        circular_boundary=(ra_deg, dec_deg, radius_arcmin / 60.0),
    )

    df = query.pandas
    print(df)

    if df.empty:
        print(f"[ATNF] No pulsars found within {radius_arcmin} arcmin.")
    else:
        print(f"[ATNF] Found {len(df)} pulsar(s):")
        pulsar_coords = SkyCoord(
            ra=df["RAJ"].values,
            dec=df["DECJ"].values,
            unit=(u.hourangle, u.deg),
        )
        sep = coord.separation(pulsar_coords).to(u.arcmin)
        df = df.copy()
        df["sep_arcmin"] = sep.value
        df = df.sort_values("sep_arcmin")
        print(df[["JNAME", "RAJ", "DECJ", "P0", "DM", "sep_arcmin"]].to_string(index=False))

    with open(output_file, "w") as f:
        f.write(f"# ATNF pulsars within {radius_arcmin} arcmin of RA={ra_deg:.6f} DEC={dec_deg:.6f}\n")
        f.write(f"# Beam FWHM assumed = {radius_arcmin} arcmin\n")
        if df.empty:
            f.write("# No pulsars found\n")
        else:
            f.write(df.to_csv(sep="\t", index=False))

    return df

def query_atnf_nearby_pulsars(
    ra_str: str,
    dec_str: str,
    radius_arcmin: float = 6.0,
    output_file: str = "nearby_pulsars.txt",
) -> pd.DataFrame:
    """
    Query ATNF catalogue for pulsars within radius_arcmin of the pointing.
    ra_str/dec_str are sigpyproc format e.g. "02:25:58.8986".
    """

    import pandas as pd
    from psrqpy import QueryATNF
    import astropy.units as u
    from astropy.coordinates import SkyCoord

    # Pointing coordinate
    coord = SkyCoord(ra=ra_str, dec=dec_str, unit=(u.hourangle, u.deg))

    print(
        f"[ATNF] Querying pulsars within {radius_arcmin} arcmin of "
        f"RA={coord.ra.deg:.4f} DEC={coord.dec.deg:.4f}"
    )

    # Query full catalogue (no spatial filter — IMPORTANT)
    query = QueryATNF(
        params=["JNAME", "RAJ", "DECJ", "P0", "DM", "S1400", "DIST"],
    )

    df = query.pandas

    if df.empty:
        print("[ATNF] No pulsars returned from catalogue query.")
        with open(output_file, "w") as f:
            f.write("# No pulsars found in ATNF query\n")
        return df

    # Convert catalogue positions to SkyCoord
    pulsar_coords = SkyCoord(
        ra=df["RAJ"].values,
        dec=df["DECJ"].values,
        unit=(u.hourangle, u.deg),
    )

    # Angular separation
    sep = coord.separation(pulsar_coords)

    # Apply radius cut
    mask = sep < (radius_arcmin * u.arcmin)

    df = df.loc[mask].copy()

    if df.empty:
        print(f"[ATNF] No pulsars found within {radius_arcmin} arcmin.")
    else:
        df["sep_arcmin"] = sep[mask].to_value(u.arcmin)
        df = df.sort_values("sep_arcmin")

        print(f"[ATNF] Found {len(df)} pulsar(s):")
        print(
            df[["JNAME", "RAJ", "DECJ", "P0", "DM", "sep_arcmin"]]
            .to_string(index=False)
        )

    # Write output
    with open(output_file, "w") as f:
        f.write(
            f"# ATNF pulsars within {radius_arcmin} arcmin of "
            f"RA={coord.ra.deg:.6f} DEC={coord.dec.deg:.6f}\n"
        )

        if df.empty:
            f.write("# No pulsars found\n")
        else:
            df.to_csv(f, sep="\t", index=False)

    return df

def convert_eps_list_to_png(
    eps_files: Iterable[Union[str, Path]],
    rotate_deg: int = 0,
    density_in: int = 150,
    density_out: int = 225,
    skip_existing: bool = False,
) -> List[Path]:
    outputs: List[Path] = []
    for eps in eps_files:
        eps_path = Path(eps).expanduser().resolve()
        if not eps_path.exists():
            raise FileNotFoundError(f"EPS not found: {eps_path}")
        png_path = eps_path.with_suffix(".png")
        if skip_existing and png_path.exists():
            outputs.append(png_path)
            continue
        cmd = [
            "convert",
            "-colorspace", "sRGB",
            "-density", str(density_in),
            str(eps_path),
            "-rotate", str(rotate_deg),
            "-background", "white", "-flatten",
            "-units", "pixelsperinch",
            "-density", str(density_out),
            str(png_path),
        ]
        png_path.parent.mkdir(parents=True, exist_ok=True)
        subprocess.run(cmd, check=True, stdout=subprocess.DEVNULL, stderr=subprocess.PIPE, text=True)
        outputs.append(png_path)
    return outputs


# ---------------------------------------------------------------------------
# MAIN PIPELINE
# ---------------------------------------------------------------------------

def run_multifold_window_pipeline(
    fil_file_path,
    candidate_list_path,
    T_shift,
    T_window,
    tmp_dir,
    obs_folding_dir,
    uniq_prefix,
    killfilename=None,
    backend="FAST",
    verbose=False,
):
    print("=" * 60)
    print("Filterbank       :", fil_file_path)
    print("Candidate file   :", candidate_list_path)
    print("Unique prefix    :", uniq_prefix)
    print("Window shift (s) :", T_shift)
    print("Integration (s)  :", T_window)
    print("=" * 60)

    # Resolve all paths to absolute BEFORE chdir
    fil_file_path       = str(Path(fil_file_path).resolve())
    candidate_list_path = str(Path(candidate_list_path).resolve())
    tmp_dir             = str(Path(tmp_dir).resolve())
    obs_folding_dir     = str(Path(obs_folding_dir).resolve())
    if killfilename is not None:
        killfilename = str(Path(killfilename).resolve())

    os.makedirs(tmp_dir, exist_ok=True)
    os.makedirs(obs_folding_dir, exist_ok=True)

    print("tmpdir is", tmp_dir)
    os.chdir(tmp_dir)

    # --- Read filterbank header ONCE — ra/dec/mid_ob_mjd cached here,
    #     passed to all downstream functions; never call get_fil_info again ---
    source_name, tobs, mid_ob_mjd, ra, dec, total_samples, _ = get_fil_info(fil_file_path)
    print(f"[INFO] Filterbank: source={source_name}, tobs={tobs:.1f}s, ra={ra}, dec={dec}")

    # --- verbose: set up command log file ---
    cmd_log_file = os.path.join(obs_folding_dir, f"{uniq_prefix}_dspsr_commands.log") if verbose else None
    if cmd_log_file and os.path.exists(cmd_log_file):
        os.remove(cmd_log_file)  # fresh log each run

    # --- Query ATNF for known pulsars in this beam ---
    atnf_output = os.path.join(obs_folding_dir, f"{uniq_prefix}_nearby_pulsars.txt")
    query_atnf_nearby_pulsars(
        ra_str=ra,
        dec_str=dec,
        radius_arcmin=75.0,
        output_file=atnf_output,
    )
    

    # --- RFI masking (optional) ---
    if killfilename is not None:
        apply_mask(fil_file_path, killfilename)
        os.system(f"mv -f {masked_fil_path} {fil_file_path}")
    else:
        print("Running without cleaning RFI from filterbank!")

    # --- Filter candidates; ra/dec written into the CSV here ---
    create_filtered_cands(candidate_list_path, obs_folding_dir, uniq_prefix, ra=ra, dec=dec)

    filtered_cand_list = os.path.join(obs_folding_dir, f"{uniq_prefix}_filtered_cands.csv")
    print("Filtered cand list:", filtered_cand_list)

    test_df = pd.read_csv(filtered_cand_list, sep=r"\s+")
    n_filtered = len(test_df)
    print(f"Filtered candidates: {n_filtered}")


   # --- Build per-window chunk files (short and long period separated) ---
    n_ms, ms_chunks, n_sec, sec_chunks = create_cands_chunk_files_from_filtered_cand_table(
        cand_table_path=filtered_cand_list,
        uniq_fil_prefix=uniq_prefix,
        mid_ob_mjd=mid_ob_mjd,
        chunk_size=100,
        output_file_base=f"{uniq_prefix}_chunk",
    )
    print(f"[INFO] Wrote {n_ms} short-period and {n_sec} long-period candidates.")

    # --- First fold ---
    t_start_time = time.time()

    for chunk_file, start_sec in ms_chunks:
        run_dspsr_multifold(
            chunk_file=chunk_file,
            fil_file=fil_file_path,
            ar_outdir=obs_folding_dir,
            start_sec=start_sec,
            T_window=T_window,
            dspsr_threads=1,
            verbose=verbose,
            cmd_log_file=cmd_log_file,
        )

    for chunk_file, start_sec in sec_chunks:
        run_dspsr_multifold_singlepulse(
            chunk_file=chunk_file,
            fil_file=fil_file_path,
            ar_outdir=obs_folding_dir,
            start_sec=start_sec,
            T_window=T_window,
            dspsr_threads=1,
            verbose=verbose,
            cmd_log_file=cmd_log_file,
        )
    fold_time = time.time() - t_start_time

    #import re as _re
    #_ts_pat = _re.compile(r'^\d{4}-\d{2}-\d{2}-\d{2}:\d{2}:\d{2}\.ar$')
    #for stray in glob.glob("*.ar"):
    #	if _ts_pat.match(Path(stray).name):
    #       print(f"[INFO] Removing stray timestamp archive: {stray}")
    #       os.remove(stray)

    archive_files = glob.glob("*.ar")
    cand_len = len(archive_files)
    print(f"dspsr folding time for {cand_len} candidates: {fold_time:.2f}s")

    # --- CLFD ---
    t0 = time.time()
    clfd(archive_files, batch_size=100)
    print(f"clfd cleaning time for {len(archive_files)} archives: {time.time()-t0:.2f}s")

    # --- PDMP (first pass) ---
    clfd_files = glob.glob("*.ar.clfd")
    pdmp_list = [
        pdmp_fine_tune(ii, fil_file_path, ii.split(".ar.clfd")[0], acc_search=0, dm_search=1)
        for ii in clfd_files
    ]

    t0 = time.time()
    pool = Pool(1)
    pool.map(os.system, pdmp_list)
    for i, cmd in enumerate(pdmp_list, start=1):
        os.system(cmd)
        print(f"[{i}/{len(pdmp_list)}] Done", flush=True)

    print(f"pdmp time for {len(clfd_files)} candidates: {time.time()-t0:.2f}s")

    # --- Filter by folded S/N ---
    filter_table_by_snr("pdmp.per", snr_threshold=7.0, dm_threshold=2.0)

    # --- Re-fold survivors with corrected P/DM ---
    survived_clfds = glob.glob("*.clfd")
    print(f"Survivors after first pdmp filter: {len(survived_clfds)}")

    # --- Re-fold survivors with corrected P/DM ---
    survived_clfds = glob.glob("*.clfd")
    print(f"Survivors after first pdmp filter: {len(survived_clfds)}")

    n_short, short_survived, n_long, long_survived = create_cands_chunk_files_from_pdmp(
        survived_clfds=survived_clfds,
        pdmp_extract_values=pdmp_extract_values,
        uniq_fil_prefix=uniq_prefix,
        mid_ob_mjd=mid_ob_mjd,
        filtered_cand_table_path=filtered_cand_list,
        chunk_size=500,
        output_file_base="survived_cands_chunk",
    )

    for chunk_file, start_sec in short_survived:
        run_dspsr_multifold(
            chunk_file=chunk_file,
            fil_file=fil_file_path,
            ar_outdir=obs_folding_dir,
            start_sec=start_sec,
            T_window=T_window,
            dspsr_threads=1,
            verbose=verbose,
            cmd_log_file=cmd_log_file,
        )

    for chunk_file, start_sec in long_survived:
        run_dspsr_multifold_singlepulse(
            chunk_file=chunk_file,
            fil_file=fil_file_path,
            ar_outdir=obs_folding_dir,
            start_sec=start_sec,
            T_window=T_window,
            dspsr_threads=1,
            verbose=verbose,
            cmd_log_file=cmd_log_file,
        )


    filter_table_by_snr("pdmp.per", snr_threshold=7.0, dm_threshold=2.0)

    # --- 0-DM comparison ---
    os.system("pam -d 0 -D 0 *.clfd -e 0dm")
    zero_dm_cleaning_clfd_pdmp_step(fil_file_path)

    # --- EPS -> PNG must happen BEFORE build_pdmp_summary so png_path exists ---
    eps_files = sorted(Path(".").glob("*.eps"))
    if eps_files:
        out_pngs = convert_eps_list_to_png(eps_files, rotate_deg=90, skip_existing=True)
        print(f"Converted {len(out_pngs)} EPS -> PNG in: {Path('.').resolve()}")
    else:
        print("No .eps files found; nothing to convert.")

    # --- Final summary table ---
    build_pdmp_summary(
        filtered_cand_list=filtered_cand_list,
        fil_file_path=fil_file_path,
    )

    # --- Cleanup ---
    os.system("rm -f *.clfd *.h5 *_0dm.eps *.txt *.ar.0dm *.ar pdmp.per pdmp.posn 0_DM.per")

    total_time = time.time() - t_start_time
    print(f"Total processing time for {cand_len} candidates: {total_time:.2f}s")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Window-based folding + CLFD + PDMP pipeline")

    parser.add_argument("--fil_file_path", required=True,
                        help="Input filterbank file (.fil)")
    parser.add_argument("--cands", required=True,
                        help="Candidate list (.cand or .csv)")
    parser.add_argument("--tshift", type=float, required=True,
                        help="Window shift in seconds")
    parser.add_argument("--twindow", type=float, required=True,
                        help="Window length / integration time in seconds")
    parser.add_argument("--prefix", required=True,
                        help="Unique prefix for all output files (e.g. 'Dec+2045_01_05_beam01')")
    parser.add_argument("--outdir", required=True,
                        help="Directory where folded archives and results are written")
    parser.add_argument("--mask", default=None,
                        help="RFI mask / kill file (optional)")
    parser.add_argument("--backend", default="FAST",
                        help="Backend name passed to dspsr -k (default: FAST)")
    parser.add_argument("--verbose", action="store_true",
                        help="Log all dspsr commands to <prefix>_dspsr_commands.log")

    args = parser.parse_args()

    run_multifold_window_pipeline(
        fil_file_path=args.fil_file_path,
        candidate_list_path=args.cands,
        T_shift=args.tshift,
        T_window=args.twindow,
        tmp_dir=args.outdir,
        obs_folding_dir=args.outdir,
        uniq_prefix=args.prefix,
        killfilename=args.mask,
        backend=args.backend,
        verbose=args.verbose,
    )
