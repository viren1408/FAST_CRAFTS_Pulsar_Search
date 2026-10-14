#!/usr/bin/env python3

import sys
import re
import os
import argparse
import logging
import numpy as np
from astropy.io import fits
from scipy.interpolate import interp1d


def setup_logging(log_level="INFO", log_file=None):
    numeric_level = getattr(logging, log_level.upper(), logging.INFO)
    handlers = [logging.StreamHandler()]
    if log_file:
        handlers.append(logging.FileHandler(log_file, mode='a'))

    logging.basicConfig(
        level=numeric_level,
        format='%(asctime)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S',
        handlers=handlers
    )


def read_pointing_file(filename):
    with fits.open(filename) as hdul:
        table_hdu = hdul[1]
        header = table_hdu.header
        data = table_hdu.data
        pos = {}

        for key in header:
            if key.startswith('TTYPE'):
                col_name = header[key]
                pos[col_name] = data[col_name]

    return pos


def get_obs_start_mjd(hdr):
    if 'STT_IMJD' in hdr and 'STT_SMJD' in hdr:
        imjd = hdr['STT_IMJD']
        smjd = hdr['STT_SMJD']
        offs = hdr.get('STT_OFFS', 0.0)
        return imjd + (smjd + offs) / 86400.0
    else:
        raise ValueError("Missing STT_IMJD/STT_SMJD in header")


def interpolate_pointing(mjd_ref, ra_deg, dec_deg, target_mjds, kind='linear'):
    if not np.all(np.diff(mjd_ref) > 0):
        idx = np.argsort(mjd_ref)
        mjd_ref = mjd_ref[idx]
        ra_deg = ra_deg[idx]
        dec_deg = dec_deg[idx]

    f_ra = interp1d(mjd_ref, ra_deg, kind=kind,
                    bounds_error=False, fill_value='extrapolate')
    f_dec = interp1d(mjd_ref, dec_deg, kind=kind,
                     bounds_error=False, fill_value='extrapolate')

    return f_ra(target_mjds), f_dec(target_mjds)


def update_pointing_for_file(input_fits, output_fits, pointing_data,
                             beam_id, interp_kind='linear'):

    basename = os.path.basename(input_fits)

    try:
        mjd_ref = pointing_data['mjd']
        ra_deg_ref = pointing_data[f'ra_deg_{beam_id}']
        dec_deg_ref = pointing_data[f'dec_deg_{beam_id}']
        ra_str_ref = pointing_data[f'ra_{beam_id}']
        dec_str_ref = pointing_data[f'dec_{beam_id}']
    except KeyError as e:
        raise KeyError(f"Missing column for beam {beam_id}: {e}")

    with fits.open(input_fits, mode='update' if output_fits == input_fits else 'readonly') as hdul:

        start_mjd = get_obs_start_mjd(hdul[0].header)

        idx = np.argmin(np.abs(mjd_ref - start_mjd))

        ra_deg = ra_deg_ref[idx]
        dec_deg = dec_deg_ref[idx]
        ra_str = ra_str_ref[idx]
        dec_str = dec_str_ref[idx]

        time_diff_sec = (mjd_ref[idx] - start_mjd) * 86400.0

        if abs(time_diff_sec) > 1:
            logging.warning(
                f"{basename} Time mismatch too large "
                f"(diff={time_diff_sec:.5f} s), skipping"
            )
            return

        logging.info(
            f"{basename} Observation start MJD: {start_mjd:.12f} "
            f"Closest pointing: MJD={mjd_ref[idx]:.12f} "
            f"(diff={time_diff_sec:.5f} s)"
        )
        logging.info(
            f"{basename} RA={ra_str} ({ra_deg:.6f} deg), "
            f"DEC={dec_str} ({dec_deg:.6f} deg)"
        )

        pri_hdr = hdul[0].header
        pri_hdr['RA'] = ra_str
        pri_hdr['DEC'] = dec_str
        pri_hdr['RA_DEG'] = ra_deg
        pri_hdr['DEC_DEG'] = dec_deg

        if output_fits != input_fits:
            hdul.writeto(output_fits, overwrite=True)
        else:
            hdul.flush()

        logging.info(f"{basename} Updated {output_fits}")


def batch_update(directory, pointing_file, output_dir=None, interp_kind='linear'):

    logging.info(f"Reading pointing file: {pointing_file}")
    pointing_data = read_pointing_file(pointing_file)

    if output_dir and not os.path.exists(output_dir):
        os.makedirs(output_dir)

    fits_files = [f for f in os.listdir(directory) if f.endswith('.fits')]

    if not fits_files:
        logging.warning("No FITS files found.")
        return

    for fname in fits_files:
        full_input = os.path.join(directory, fname)

        match = re.search(r'M\d{2}', fname)
        if not match:
            logging.warning(f"Skipping {fname}: no beam ID")
            continue

        beam_id = match.group(0)

        full_output = os.path.join(output_dir, fname) if output_dir else full_input

        try:
            update_pointing_for_file(full_input, full_output,
                                     pointing_data, beam_id, interp_kind)
        except Exception as e:
            logging.error(f"Error processing {fname}: {e}", exc_info=True)


def main():
    parser = argparse.ArgumentParser(
        description="Update pointing in pulsar FITS files."
    )

    parser.add_argument("input", nargs='?', default=None)
    parser.add_argument("output", nargs='?', default=None)

    parser.add_argument("--dir", dest="directory", default=None)
    parser.add_argument("--outdir", dest="output_dir", default=None)

    parser.add_argument("--pointing-file", required=True)

    parser.add_argument("--interp", default="linear",
                        choices=['linear', 'nearest', 'cubic'])

    parser.add_argument("--log-level", default="INFO",
                        choices=['DEBUG', 'INFO', 'WARNING', 'ERROR'])

    parser.add_argument("--log-file", default=None)

    args = parser.parse_args()

    setup_logging(args.log_level, args.log_file)

    if args.directory:
        batch_update(args.directory, args.pointing_file,
                     args.output_dir, args.interp)
    else:
        if not args.input:
            parser.error("Input FITS file required")

        output = args.output if args.output else args.input

        match = re.search(r'M\d{2}', args.input)
        if not match:
            logging.error("Cannot determine beam ID")
            sys.exit(1)

        beam_id = match.group(0)

        pointing_data = read_pointing_file(args.pointing_file)

        update_pointing_for_file(args.input, output,
                                 pointing_data, beam_id, args.interp)


if __name__ == "__main__":
    main()
