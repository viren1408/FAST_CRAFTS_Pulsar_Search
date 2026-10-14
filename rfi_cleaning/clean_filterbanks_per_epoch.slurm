#!/bin/bash
#SBATCH --account=oz002
#SBATCH --cpus-per-task=1
#SBATCH --mem=4G
#SBATCH --time=00:20:00
#SBATCH --partition=${SYS_ARCH}
#SBATCH --array=0-1443
#SBATCH --output=/aphid/temp/fast_crafts/crafts_processing/logs_clean_rfifind/%x_%A_%a.out
#SBATCH --error=/aphid/temp/fast_crafts/crafts_processing/logs_clean_rfifind/%x_%A_%a.err

source /fred/oz002/users/vmandaog/vmandaog_env/bin/activate

INDIR="/aphid/temp/fast_crafts/crafts_processing/ZD2021_1_1_2bit/Dec+2925_06_05/20220510"

LISTDIR="${INDIR}/fil_lists"

EPOCH=$(basename "$(dirname "$INDIR")")

fil=$(sed -n "$((SLURM_ARRAY_TASK_ID+1))p" "${LISTDIR}/${EPOCH}_fil_list.txt")

# Safety check
if [ -z "$fil" ]; then
    echo "No file for task $SLURM_ARRAY_TASK_ID"
    exit 1
fi

echo "Processing $fil"

beam_dir=$(dirname "$fil")
outdir="${beam_dir}/"
RFI_MASK_DIR="${INDIR}/rfi_masks"

# Run cleaning
python /aphid/temp/fast_crafts/crafts_processing/final_scripts/clean_filterbank.py \
    "$fil" "$RFI_MASK_DIR" "$outdir"
CLEAN_EXIT=$?


# ── Per-task logging ──────────────────────────────────────────────────────────
LOG_DIR="/aphid/temp/meerkat/crafts_processing/logs_clean"
TASK_LOG="${LOG_DIR}/tasks_${SLURM_ARRAY_JOB_ID}.csv"
LOCK_FILE="${LOG_DIR}/${SLURM_ARRAY_JOB_ID}.lock"

if [ $CLEAN_EXIT -eq 0 ]; then
    TASK_STATUS="OK"
else
    TASK_STATUS="FAILED"
fi

(
    flock -x 200

    if [ ! -f "$TASK_LOG" ]; then
        echo "Timestamp,JobID,TaskID,Epoch,INDIR,Filterbank,RFIMaskDir,Outdir,Status" > "$TASK_LOG"
    fi

    printf "%s,%s,%s,%s,%s,%s,%s,%s,%s\n" \
        "$(date '+%Y-%m-%dT%H:%M:%S')" \
        "${SLURM_ARRAY_JOB_ID}" \
        "${SLURM_ARRAY_TASK_ID}" \
        "${EPOCH}" \
        "${INDIR}" \
        "${fil}" \
        "${RFI_MASK_DIR}" \
        "${outdir}" \
        "${TASK_STATUS}" >> "$TASK_LOG"

) 200>"$LOCK_FILE"
# ─────────────────────────────────────────────────────────────────────────────

exit $CLEAN_EXIT
