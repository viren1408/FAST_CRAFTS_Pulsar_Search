#!/bin/bash
#SBATCH --job-name=update_pointing_batch
#SBATCH --time=01:00:00
#SBATCH --cpus-per-task=2
#SBATCH --mem=4G
# Step 1: update_pointing_v2.py on the whole raw epoch directory (unchanged script).
# Log goes to <epoch>/pipeline/logs/update_pointing/ instead of the raw data tree.
source "${CRAFTS_EPOCH_ENV:?CRAFTS_EPOCH_ENV not set}"; source "${CRAFTS_STEPS_DIR}/common.sh"
LOG_FILE="${PIPE_DIR}/logs/update_pointing/update_pointing_${SLURM_JOB_ID}.log"
mkdir -p "$(dirname "$LOG_FILE")"
echo "Data dir: $RAW_DIR"; echo "Pointing file: $POS_FILE"; echo "Log file: $LOG_FILE"

echo "Making FITS files writable..."
find "$RAW_DIR" -maxdepth 1 -name "*.fits" -print0 | xargs -0 chmod u+w

TIMEV=""; [ -x /usr/bin/time ] && TIMEV="/usr/bin/time -v"
$TIMEV python "$UPDATE_SCRIPT" \
    --dir "$RAW_DIR" \
    --pointing-file "$POS_FILE" \
    --log-level INFO \
    --log-file "$LOG_FILE"
STATUS=$?

echo "Reverting FITS files to read-only..."
find "$RAW_DIR" -maxdepth 1 -name "*.fits" -print0 | xargs -0 chmod 444
crafts_end $STATUS
