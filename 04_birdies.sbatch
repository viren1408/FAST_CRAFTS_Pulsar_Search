#!/bin/bash
#SBATCH --account=oz002
#SBATCH --cpus-per-task=1
#SBATCH --mem=1G
#SBATCH --time=00:10:00
#SBATCH --tmp=1G
#SBATCH --partition=${SYS_ARCH}
#SBATCH --gres=gpu:1
# Step 4: GPU sliding FFT at DM 0-5 for birdies. Task line: id beam chunk masked_fil outdir
source "${CRAFTS_EPOCH_ENV:?CRAFTS_EPOCH_ENV not set}"; source "${CRAFTS_STEPS_DIR}/common.sh"
fil=${F[3]}; OUTDIR=${F[4]}; beam_num=${F[1]}
echo "Processing $(basename "$fil" .fil) on task $TASK_ID"
cd "$SCRIPTS_DIR"
python "$SCRIPTS_DIR/run_sliding_search_for_birdies.py" \
    --fil "$fil" \
    --beam "$beam_num" \
    --min_sn "$BIRDIE_MIN_SN" \
    --dm_start "$BIRDIE_DM_START" \
    --dm_end "$BIRDIE_DM_END" \
    --T-window "$T_WINDOW" \
    --T-shift "$T_SHIFT" \
    --outdir "$OUTDIR"
crafts_end $?
