#!/bin/bash
#SBATCH --account=oz002
#SBATCH --job-name=make_zaplist
#SBATCH --cpus-per-task=1
#SBATCH --mem=1G
#SBATCH --time=00:10:00
#SBATCH --partition=$USER
# Step 5: make_zap_list.py for the whole epoch -> birdie_lists/birdies_CCCC.zap
source "${CRAFTS_EPOCH_ENV:?CRAFTS_EPOCH_ENV not set}"; source "${CRAFTS_STEPS_DIR}/common.sh"
echo "Base path: $PROC_DIR"; echo "Output dir: ${PROC_DIR}/birdie_lists"
cd "$SCRIPTS_DIR"
python "$SCRIPTS_DIR/make_zap_list.py" \
    --base_path "$PROC_DIR" \
    --outdir    "${PROC_DIR}/birdie_lists" \
    --nbeams    "$NBEAMS" \
    --T         "$ZAP_T"
crafts_end $?
