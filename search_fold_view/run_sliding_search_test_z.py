#!/usr/bin/env python3
import os
import argparse
import logging
import pandas as pd
import numpy as np

from peasoup_runner_test_z import read_header_params, run_peasoup

# ----------------------------------------------------------------------
# Logging
# ----------------------------------------------------------------------
log = logging.getLogger("sliding_search")
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)

# ----------------------------------------------------------------------
# Core logic
# ----------------------------------------------------------------------
def run_sliding_search(
    filterbank,
    beam_id,
    min_sn,
    zap_file,
    dm_start=0.0,
    dm_end=0.0,
    T_window=27.4,
    T_shift=13.7,
    outdir="sliding_results",
):
    beam_tag = f"beam_{beam_id:02d}"
#    outdir = os.path.join(outdir, beam_tag)
    os.makedirs(outdir, exist_ok=True)

    hdr = read_header_params(filterbank)
    tsamp_sec = hdr["tsamp_us"] / 1e6
    nsamp_total = hdr["nsamples"]
    total_time = nsamp_total * tsamp_sec

    ns_window = int(T_window / tsamp_sec)
    ns_shift = int(T_shift / tsamp_sec)

    if total_time <= T_window:
        starts_sec = np.array([0.0])
        log.info(
            f"[Beam {beam_id:02d}] Short file "
            f"(Tobs={total_time:.2f}s < Twindow={T_window:.2f}s): single window"
        )
    else:
        starts_sec = np.arange(
            0, total_time - T_window + tsamp_sec, T_shift
        )

    log.info(f"[Beam {beam_id:02d}] Total windows = {len(starts_sec)}")

    all_results = []
    base_name = os.path.splitext(os.path.basename(filterbank))[0]

    for idx, start_sec in enumerate(starts_sec):
        start_sample = int(start_sec / tsamp_sec)
        remaining = nsamp_total - start_sample
        samples_to_read = min(ns_window, remaining)

        if samples_to_read <= 0:
            continue

        win_dir = os.path.join(outdir, f"win_{idx:05d}")
        os.makedirs(win_dir, exist_ok=True)

        log.info(
            f"[Beam {beam_id:02d}] "
            f"Window {idx:05d} start={start_sec:.2f}s "
            f"samples={samples_to_read}"
        )

        run_peasoup(
            filterbank_path=filterbank,
            results_path=win_dir,
            min_sn = min_sn,
            dm_start=dm_start,
            dm_end=dm_end,
            acc_start=0,
            acc_end=0,
            dm_tol=1.1,
            fft_size=samples_to_read,
            start_sample=start_sample,
            samples_to_read=samples_to_read,
            zap_file = zap_file
        )

        cand_file = os.path.join(win_dir, f"{base_name}.cand")
        if not os.path.exists(cand_file):
            continue

        df = pd.read_csv(cand_file, sep="\t")
        if df.empty:
            continue

        df["beam"] = beam_id
        df["window"] = idx
        df["start_sec"] = start_sec
        df["T_window"] = samples_to_read * tsamp_sec
        df["filterbank"] = filterbank
        df["cand_file"] = cand_file

        all_results.append(df)

    if not all_results:
        log.warning(f"[Beam {beam_id:02d}] No candidates found")
        return None

    combined = pd.concat(all_results, ignore_index=True)
    out_csv = os.path.join(outdir, "candidates_with_beam.csv")
    combined.to_csv(out_csv, index=False)

    log.info(
        f"[Beam {beam_id:02d}] Saved {len(combined)} candidates {out_csv}"
    )
    return combined


# ----------------------------------------------------------------------
# Argparse entry point
# ----------------------------------------------------------------------
def main():
    parser = argparse.ArgumentParser(
        description="Sliding-window Peasoup search on a single filterbank"
    )
    

    parser.add_argument(
        "--fil",
        required=True,
        help="Path to input filterbank (.fil)"
    )

    parser.add_argument(
        "--beam",
        type=int,
        required=True,
        help="Beam number (for bookkeeping)"
    )

    parser.add_argument(
        "--min_sn",
        type=float,
        required=True,
        help="S/N threshold"
    )


    parser.add_argument(
        "--dm_start",
        type=float,
        default=0.0,
        help="DM start (default: 0)"
    )

    parser.add_argument(
        "--dm_end",
        type=float,
        default=0.0,
        help="DM end (default: 0)"
    )

    parser.add_argument(
        "--T-window",
        type=float,
        default=27.4,
        help="Sliding window length in seconds"
    )

    parser.add_argument(
        "--T-shift",
        type=float,
        default=13.7,
        help="Sliding window shift in seconds"
    )

    parser.add_argument(
        "--zap_file",
        type=str,
        default=None,
        help="Full birdie zap list path for peasoup"
    )

    parser.add_argument(
        "--outdir",
        default="sliding_results",
        help="Output directory"
    )

    args = parser.parse_args()

    if not os.path.exists(args.fil):
        raise FileNotFoundError(f"Filterbank not found: {args.fil}")

    run_sliding_search(
        filterbank=args.fil,
        beam_id=args.beam,
	min_sn = args.min_sn,
        dm_start=args.dm_start,
        dm_end=args.dm_end,
        T_window=args.T_window,
        T_shift=args.T_shift,
	zap_file = args.zap_file,
        outdir=args.outdir,
    )


if __name__ == "__main__":
    main()
