#!/bin/bash
#SBATCH --account=oz002
#SBATCH --job-name=prepare_fold
#SBATCH --cpus-per-task=1
#SBATCH --mem=8G
#SBATCH --time=01:00:00
#SBATCH --partition=$USER
# Step 7: prepare_final_candidates_to_fold.py -> sifting_output/fold_commands_<epoch>.sh
# An existing commands file is moved aside first (the script appends to it).
source "${CRAFTS_EPOCH_ENV:?CRAFTS_EPOCH_ENV not set}"; source "${CRAFTS_STEPS_DIR}/common.sh"
OUT="${PROC_DIR}/sifting_output"
CMDS="${OUT}/fold_commands_${EPOCH}.sh"
mkdir -p "$OUT"
if [ -f "$CMDS" ]; then mv "$CMDS" "${CMDS}.bak_${SLURM_JOB_ID}"; echo "moved old $CMDS aside"; fi
cd "$SCRIPTS_DIR"
python "$SCRIPTS_DIR/prepare_final_candidates_to_fold.py" \
    --base_path "$PROC_DIR" \
    --outdir    "$OUT" \
    --epoch     "$EPOCH" \
    --nbeams    "$NBEAMS"
crafts_end $?
