#!/bin/bash
#SBATCH --job-name=digifil
#SBATCH --array=0-1443 #chunks_per_beam*number_of_beams

#SBATCH --cpus-per-task=1
#SBATCH --time=00:08:00
#SBATCH --mem=1G
#SBATCH --output=/aphid/temp/fast_crafts/crafts_processing/logs_digifil/digifil_%A_%a.out
#SBATCH --error=/aphid/temp/fast_crafts/crafts_processing/logs_digifil/digifil_%A_%a.err


# beam & chunk
CHUNKS_PER_BEAM=76
BEAM_IDX=$((SLURM_ARRAY_TASK_ID / CHUNKS_PER_BEAM + 1 ))   # 1-19
CHUNK_IDX=$(( SLURM_ARRAY_TASK_ID % CHUNKS_PER_BEAM ))

BEAM=$(printf "%02d" $BEAM_IDX)

# Directories
INDIR="/aphid/temp/fast_crafts/data/ZD2021_1_1_2bit/Dec+3114_06_07/20220602"
OUTDIR="/aphid/temp/fast_crafts/crafts_processing/ZD2021_1_1_2bit/Dec+3114_06_07/20220602/${BEAM}"

mkdir -p "$OUTDIR"

FILES=( $(ls ${INDIR}/Dec+3114_06_07_arcdrift-M${BEAM}_*_2bit.fits 2>/dev/null | sort) )

if [ ${#FILES[@]} -eq 0 ]; then
    echo "No FITS files found for beam $BEAM"
    exit 0
fi

# Chunking
CHUNK=19
STEP=19
IDX=$(( CHUNK_IDX * STEP ))
INPUT_FILES=( "${FILES[@]:IDX:CHUNK}" )

NUM_FITS=${#FILES[@]}
NUM_CHUNKS=$(( (NUM_FITS + STEP - 1) / STEP ))   # ceil(NUM_FITS / STEP)

OUTFIL=$(printf "%s/Dec+3114_06_07_arcdrift-M%s_%04d.fil" "$OUTDIR" "$BEAM" "$CHUNK_IDX")

# Run digifil
digifil "${INPUT_FILES[@]}" -cont -f 4 -o "${OUTFIL}"
DIGIFIL_EXIT=$?

LOG_DIR="/aphid/temp/fast_crafts/crafts_processing/digifil_logs"

mkdir -p "$LOG_DIR"

TASK_LOG="${LOG_DIR}/tasks_${SLURM_ARRAY_JOB_ID}.csv"
SUMMARY_LOG="${LOG_DIR}/summary_${SLURM_ARRAY_JOB_ID}.csv"
LOCK_FILE="${LOG_DIR}/${SLURM_ARRAY_JOB_ID}.lock"

if [ $DIGIFIL_EXIT -eq 0 ] && [ -s "$OUTFIL" ]; then
    TASK_STATUS="OK"
else
    TASK_STATUS="FAILED"
fi

(
    flock -x 200

    if [ ! -f "$TASK_LOG" ]; then
        echo "Timestamp,JobID,TaskID,Beam,ChunkIdx,INDIR,OUTDIR,NumFitsFilesPerBeam,NumChunks,OutputFilterbank,TaskStatus" > "$TASK_LOG"
    fi

    printf "%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s\n" \
        "$(date '+%Y-%m-%dT%H:%M:%S')" \
        "${SLURM_ARRAY_JOB_ID}" \
        "${SLURM_ARRAY_TASK_ID}" \
        "${BEAM}" \
        "${CHUNK_IDX}" \
        "${INDIR}" \
        "${OUTDIR}" \
        "${NUM_FITS}" \
        "${NUM_CHUNKS}" \
        "${OUTFIL}" \
        "${TASK_STATUS}" >> "$TASK_LOG"

    TOTAL_TASKS=$(( SLURM_ARRAY_TASK_MAX - SLURM_ARRAY_TASK_MIN + 1 ))
    COMPLETED=$(( $(wc -l < "$TASK_LOG") - 1 ))   # subtract header

    if [ "$COMPLETED" -eq "$TOTAL_TASKS" ]; then

        NUM_FAILED=$(awk -F',' 'NR>1 && $11=="FAILED"' "$TASK_LOG" | wc -l)
        NUM_OK=$(( TOTAL_TASKS - NUM_FAILED ))
        FAILED_IDS=$(awk -F',' 'NR>1 && $11=="FAILED" {print $3}' "$TASK_LOG" | paste -sd '|' -)

        if [ "$NUM_FAILED" -eq 0 ]; then
            SUMMARY_STATUS="All OK"
            FAILED_IDS="none"
        else
            SUMMARY_STATUS="${NUM_FAILED}/${TOTAL_TASKS} failed"
        fi

        if [ ! -f "$SUMMARY_LOG" ]; then
            echo "Timestamp,JobID,INDIR,OUTDIR,TotalTasks,NumOK,NumFailed,Status,Failed_TaskIDs" > "$SUMMARY_LOG"
        fi

        printf "%s,%s,%s,%s,%s,%s,%s,%s,%s\n" \
            "$(date '+%Y-%m-%dT%H:%M:%S')" \
            "${SLURM_ARRAY_JOB_ID}" \
            "${INDIR}" \
            "${OUTDIR%/${BEAM}}" \
            "${TOTAL_TASKS}" \
            "${NUM_OK}" \
            "${NUM_FAILED}" \
            "${SUMMARY_STATUS}" \
            "${FAILED_IDS}" >> "$SUMMARY_LOG"

        echo "Job ${SLURM_ARRAY_JOB_ID} complete — summary written to ${SUMMARY_LOG}"
    fi

) 200>"$LOCK_FILE"
# ─────────────────────────────────────────────────────────────────────────────

exit $DIGIFIL_EXIT
