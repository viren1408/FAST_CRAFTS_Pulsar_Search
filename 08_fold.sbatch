#!/bin/bash
#SBATCH --account=oz002
#SBATCH --job-name=fold
#SBATCH --cpus-per-task=1
#SBATCH --mem=4G
#SBATCH --time=01:00:00
#SBATCH --partition=${SYS_ARCH}
# Step 8: one task = one line of fold_commands_<epoch>.sh (one beam x one chunk).
# Task line: id n_candidates outdir command. Time/mem are set per size class by crafts.py.
source "${CRAFTS_EPOCH_ENV:?CRAFTS_EPOCH_ENV not set}"; source "${CRAFTS_STEPS_DIR}/common.sh"
NCANDS=${F[1]}; OUTDIR=${F[2]}; CMD=${F[3]}
echo "Running task $TASK_ID ($NCANDS candidates)"
echo "$CMD"
cd "$SCRIPTS_DIR"     # commands call fold_pipeline_v3.py by relative name
bash -c "$CMD"
crafts_end $?
