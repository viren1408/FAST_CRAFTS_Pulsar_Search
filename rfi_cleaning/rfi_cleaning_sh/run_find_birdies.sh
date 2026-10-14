#!/bin/bash

bash /aphid/temp/meerkat/crafts_processing/final_scripts/rfi_cleaning/setup_find_birdies.sh

if [ $? -ne 0 ]; then
    echo "Setup failed. Aborting."
    exit 1
fi

sbatch /aphid/temp/meerkat/crafts_processing/final_scripts/find_birdies.slurm
