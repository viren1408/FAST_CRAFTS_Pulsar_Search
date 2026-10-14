#!/bin/bash
CHUNKS_PER_BEAM=76
BEAM_IDX=$(( SLURM_ARRAY_TASK_ID + 1 ))   # 1-19
CHUNK_IDX=$(( SLURM_ARRAY_TASK_ID % CHUNKS_PER_BEAM ))

BEAM=$(printf "%02d" $BEAM_IDX)

# Directories
INDIR="/scratch-meerkat/data/ZD2021_1_1_2bit/Dec+2045_01_05/"
OUTDIR="/scratch-meerkat/crafts_processing/ZD2021_1_1_2bit/Dec+2632_08_05000/20220111/${BEAM}"

mkdir -p "$OUTDIR"

FILES=( $(ls ${INDIR}/Dec+2632_08_05000_arcdrift-M${BEAM}_*_2bit.fits 2>/dev/null | sort) )

if [ ${#FILES[@]} -eq 0 ]; then
    echo "No FITS files found for beam $BEAM"
    exit 0
fi

# Chunking
CHUNK19
STEP=19
IDX=$(( CHUNK_IDX * STEP ))
INPUT_FILES=( "${FILES[@]:IDX:CHUNK}" )

NUM_FITS=${#FILES[@]}
NUM_CHUNKS=$(( (NUM_FITS + STEP - 1) / STEP ))   # ceil(NUM_FITS / STEP)
