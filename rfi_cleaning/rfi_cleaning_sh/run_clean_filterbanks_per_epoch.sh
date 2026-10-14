#!/bin/bash

bash /aphid/temp/fast_crafts/crafts_processing/final_scripts/rfi_cleaning/setup_clean_filterbanks_per_epoch.sh

if [ $? -ne 0 ]; then
    echo "Setup failed. Aborting."
    exit 1
fi

sbatch /aphid/temp/fast_crafts/crafts_processing/final_scripts/clean_filterbanks_per_epoch.slurm
