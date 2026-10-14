#!/usr/bin/env python3
import os
import sys
import numpy as np
from sigpyproc.readers import FilReader

def create_bad_channel_mask(fil_file_path, rfi_mask_path):
    fil_prefix = os.path.basename(fil_file_path).replace(".fil", "")
    peasoup_kill_path = os.path.join(rfi_mask_path, f"{fil_prefix}_peasoup.kill")

    if not os.path.isfile(peasoup_kill_path):
        # Run rfifind
        rfifind_cmd = (
            f"rfifind -filterbank -time 5.0 -noweights -noscales -nooffsets  -zapchan 0:74,449:506,586:624,649:687,999:1024 "
            f"{fil_file_path} -o {os.path.join(rfi_mask_path, fil_prefix)}"
        )
        print("Running:", rfifind_cmd)
        os.system(rfifind_cmd)

        # Read zap channels
        mask_file = f"{os.path.join(rfi_mask_path, fil_prefix)}_rfifind.mask"
        with open(mask_file, "rb") as f:
            time_sig, freq_sig, MJD, dtint, lofreq, df = np.fromfile(f, np.float64, 6)
            nchan, nint, ptsperint = np.fromfile(f, np.int32, 3)
            nzap = np.fromfile(f, np.int32, 1)[0]
            mask_zap_chans = np.fromfile(f, np.int32, nzap)

        mask_values = np.ones(nchan, dtype=int)
        for ch in mask_zap_chans:
            mask_values[ch] = 0

        reversed_arr = np.fliplr([mask_values])[0]
        np.savetxt(peasoup_kill_path, reversed_arr, fmt="%i", delimiter=",")

    return peasoup_kill_path


def apply_chn_mask(fil_file_path, killfilename, output_dir):
    fil_prefix = os.path.basename(fil_file_path).replace(".fil", "")
    masked_fil = f"{fil_prefix}_masked.fil"
    masked_fil_path = os.path.join(output_dir, masked_fil)

    # Create inverted kill file
    inverted_killfile = os.path.join(output_dir, f"masked_{fil_prefix}_folding.kill")
    os.system(f"tr '01' '10' < {killfilename} > {inverted_killfile}")

    mask = np.loadtxt(inverted_killfile, dtype="ubyte")
    read_fil = FilReader(fil_file_path)
    print(f"Applying channel mask:{masked_fil_path}")
    read_fil.apply_channel_mask(mask,filename=masked_fil_path)
    return masked_fil_path


def main():
    if len(sys.argv) != 4:
        print("Usage: clean_filterbank.py <filterbank_file> <rfi_mask_dir> <cleaned_fil_dir>")
        sys.exit(1)

    fil_file_path = sys.argv[1]
    rfi_mask_path = sys.argv[2]
    cleaned_fil_dir = sys.argv[3]

    os.makedirs(rfi_mask_path, exist_ok=True)
    os.makedirs(cleaned_fil_dir, exist_ok=True)

    killfile = create_bad_channel_mask(fil_file_path, rfi_mask_path)
    masked_fil_path = apply_chn_mask(fil_file_path, killfile, cleaned_fil_dir)

    print(f"Cleaned filterbank saved at: {masked_fil_path}")


if __name__ == "__main__":
    main()
