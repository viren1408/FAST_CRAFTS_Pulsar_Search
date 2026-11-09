#!/bin/bash
#SBATCH --account=oz002
#SBATCH --cpus-per-task=1
#SBATCH --mem=1G
#SBATCH --time=00:03:00
#SBATCH --partition=${SYS_ARCH}
#SBATCH --gres=gpu:1
# Step 6: GPU sliding search DM 5-500 with the chunk's zap file.
# Task line: id beam chunk masked_fil outdir zap_file
source "${CRAFTS_EPOCH_ENV:?CRAFTS_EPOCH_ENV not set}"; source "${CRAFTS_STEPS_DIR}/common.sh"
fil=${F[3]}; OUTDIR=${F[4]}; zap_file=${F[5]}; beam_num=${F[1]}
echo "Processing $(basename "$fil" .fil) on task $TASK_ID"
echo "Using zap file: $zap_file"
if [ ! -f "$zap_file" ]; then echo "ERROR zap file missing: $zap_file"; crafts_end 4; fi
cd "$SCRIPTS_DIR"
python "$SCRIPTS_DIR/run_sliding_search_test_z.py" \
    --fil "$fil" \
    --beam "$beam_num" \
    --min_sn "$SEARCH_MIN_SN" \
    --dm_start "$SEARCH_DM_START" \
    --dm_end "$SEARCH_DM_END" \
    --T-window "$T_WINDOW" \
    --T-shift "$T_SHIFT" \
    --zap_file "$zap_file" \
    --outdir "$OUTDIR"
crafts_end $?
