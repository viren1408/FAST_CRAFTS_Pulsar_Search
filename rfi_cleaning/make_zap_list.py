#!/usr/bin/env python3

import pandas as pd
import numpy as np
import os
import argparse


# ---------------------------------------------------------------------------
# Directory parsing
# ---------------------------------------------------------------------------

def parse_chunk_id_from_dirname(dirname, beam_num):
    """Extract chunk_id from a directory name like ..._M01_CHUNKID_..."""
    marker = f"M{beam_num:02d}_"
    idx = dirname.find(marker)
    if idx == -1:
        return None
    after = dirname[idx + len(marker):]
    chunk_id = after.split("_")[0]
    return chunk_id


def discover_all_chunks(base_path, nbeams=19):
    """
    Scan beam 01's subdirectories to discover all chunk IDs present.
    Expects: base_path/01/<dirname containing M01_CHUNKID_>
    """
    chunk_ids = set()
    beam_folder = os.path.join(base_path, "01")
    if not os.path.isdir(beam_folder):
        # fallback: try zero-padded "1"
        beam_folder = os.path.join(base_path, "1")
    if not os.path.isdir(beam_folder):
        print(f"ERROR: Could not find beam-01 folder under {base_path}")
        return []
    for entry in os.scandir(beam_folder):
        if entry.is_dir():
            chunk_id = parse_chunk_id_from_dirname(entry.name, 1)
            if chunk_id is not None:
                chunk_ids.add(chunk_id)
    return sorted(chunk_ids)


# ---------------------------------------------------------------------------
# Data loading
# ---------------------------------------------------------------------------

def load_beams_for_chunk(base_path, chunk_id, nbeams=19):
    """
    For a given chunk_id, load candidates_with_beam.csv from every beam
    directory that matches  base_path/{beam_num:02d}/*_M{beam_num:02d}_{chunk_id}_*
    """
    dfs = []
    for beam_num in range(1, nbeams + 1):
        beam_folder = os.path.join(base_path, f"{beam_num:02d}")
        if not os.path.isdir(beam_folder):
            beam_folder = os.path.join(base_path, str(beam_num))
        if not os.path.isdir(beam_folder):
            print(f"  [WARN] Beam folder not found: {beam_folder}")
            continue

        matched = [
            e.path for e in os.scandir(beam_folder)
            if e.is_dir() and f"M{beam_num:02d}_{chunk_id}_" in e.name
        ]
        if not matched:
            print(f"  [WARN] No directory for beam {beam_num:02d}, chunk {chunk_id}")
            continue

        csv_path = os.path.join(matched[0], "candidates_with_beam.csv")
        if not os.path.exists(csv_path):
            print(f"  [WARN] CSV not found: {csv_path}")
            continue

        df = pd.read_csv(csv_path)
        dfs.append(df)

    if not dfs:
        return pd.DataFrame()
    return pd.concat(dfs, ignore_index=True)


# ---------------------------------------------------------------------------
# Birdie detection
# ---------------------------------------------------------------------------

def find_birdies(df_all, T=25.8, sn_min=10, dm_min=0, dm_max=5,
                 min_windows=7, min_beams=10):
    """
    Identify RFI birdies: signals that appear in many beams AND many windows
    at low DM with high S/N.
    """
    df_res = 1 / T
    df = df_all[(df_all["fft_sn"] > sn_min) &(df_all["dm"] >= dm_min) &(df_all["dm"] <= dm_max)].copy()

    if df.empty:
        return [], pd.DataFrame()

    df["frequency"] = 1.0 / df["period"]
    df["freq_bin"] = (df["frequency"] / df_res).round() * df_res

    # Must persist across many windows within a single beam
    beam_window = (df.groupby(["beam", "freq_bin"]).agg(window_count=("window", "nunique")).reset_index())
    beam_persistent = beam_window[beam_window["window_count"] > min_windows]
    freq_beam_set = set(beam_persistent["freq_bin"])

    # Must also appear in many beams within the same window
    window_beam = (df.groupby(["window", "freq_bin"]).agg(beam_count=("beam", "nunique")).reset_index())
    window_persistent = window_beam[window_beam["beam_count"] > min_beams]
    freq_window_set = set(window_persistent["freq_bin"])

    final_freqs = sorted(freq_beam_set.intersection(freq_window_set))

    if not final_freqs:
        return [], pd.DataFrame()

    final_group = (df[df["freq_bin"].isin(final_freqs)].groupby("freq_bin").agg(total_count=("freq_bin", "count"),beams_present=("beam", "nunique"),windows_present=("window", "nunique"),max_sn=("fft_sn", "max"),mean_dm=("dm", "mean"),max_nh=("nh", "max")).sort_values("total_count", ascending=False))

    return final_freqs, final_group


def write_zap_file(final_freqs, chunk_id, T=25.6, outdir="."):
    """Write a PRESTO-compatible .zap birdie list for this chunk."""
    df_res = 1 / T
    width = 2 * df_res
    os.makedirs(outdir, exist_ok=True)
    outfile = os.path.join(outdir, f"birdies_{chunk_id}.zap")
    with open(outfile, "w") as f:
        f.write("#Frequency Width\n")
        for freq in final_freqs:
            f.write(f"{freq:.6f} {width:.6f}\n")
        # Fixed known birdie always appended
        f.write("5086.263020833333   1.07\n")
    print(f"  [OK] Zap file written: {outfile}")
    return outfile


def save_candidate_list(df, chunk_id, outdir=".", prefix="candidates"):
    """Save the raw merged candidate table for a chunk."""
    os.makedirs(outdir, exist_ok=True)
    outfile = os.path.join(outdir, f"{prefix}_{chunk_id}.txt")
    df.to_csv(outfile, sep=" ", index=False)
    print(f"  [OK] Candidate list saved: {outfile}")


def save_birdie_summary(final_group, chunk_id, outdir="."):
    """Save the birdie summary table for a chunk."""
    if final_group.empty:
        return
    os.makedirs(outdir, exist_ok=True)
    outfile = os.path.join(outdir, f"birdie_summary_{chunk_id}.csv")
    final_group.to_csv(outfile)
    print(f"  Birdie summary saved: {outfile}")


def process_chunk(base_path, chunk_id, nbeams=19, outdir=".", T=25.6):
    print(f"\n{'='*55}")
    print(f"  Chunk: {chunk_id}")
    print(f"{'='*55}")

    df_all = load_beams_for_chunk(base_path, chunk_id, nbeams=nbeams)
    if df_all.empty:
        print(f"  [SKIP] No data loaded for chunk {chunk_id}.")
        return None

    save_candidate_list(df_all, chunk_id, outdir=outdir, prefix="raw_candidates")

    final_freqs, final_group = find_birdies(df_all, T=T)

    if not final_freqs:
        print("  [INFO] No birdies found for this chunk.")
    else:
        print(f"  [INFO] {len(final_freqs)} birdie(s) found:")
        print(final_group.to_string())

    write_zap_file(final_freqs, chunk_id, T=T, outdir=outdir)
    save_birdie_summary(final_group, chunk_id, outdir=outdir)

    return final_freqs, final_group


def process_all_chunks(base_path, nbeams=19, outdir=".", T=25.6):
    os.makedirs(outdir, exist_ok=True)

    chunk_ids = discover_all_chunks(base_path, nbeams=nbeams)
    if not chunk_ids:
        print(f"ERROR: No chunks discovered under {base_path}")
        return {}

    print(f"Discovered {len(chunk_ids)} chunk(s): {chunk_ids}")

    results = {}
    for chunk_id in chunk_ids:
        result = process_chunk(base_path, chunk_id, nbeams=nbeams,outdir=outdir, T=T)
        if result is not None:
            results[chunk_id] = result

    print(f"\nDone. Processed {len(results)}/{len(chunk_ids)} chunks successfully.")
    return results


def main():
    parser = argparse.ArgumentParser(
        description="Find RFI birdies across all multibeam chunks and write .zap files."
    )
    parser.add_argument(
        "--base_path", required=True,
        help="Root directory of peasoup output, e.g. test_pulsars/"
    )
    parser.add_argument(
        "--outdir", default="birdie_lists",
        help="Output directory for .zap files and summaries (default: birdie_lists/)"
    )
    parser.add_argument(
        "--nbeams", type=int, default=19,
        help="Number of beams (default: 19)"
    )
    parser.add_argument(
        "--T", type=float, default=25.6,
        help="FFT window length in seconds used for frequency resolution (default: 25.6)"
    )
    args = parser.parse_args()

    process_all_chunks(
        base_path=args.base_path,
        nbeams=args.nbeams,
        outdir=args.outdir,
        T=args.T,
    )


if __name__ == "__main__":
    main()
