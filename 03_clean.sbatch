#!/bin/bash
#SBATCH --account=oz002
#SBATCH --cpus-per-task=1
#SBATCH --mem=4G
#SBATCH --time=00:20:00
#SBATCH --partition=${SYS_ARCH}
# Step 3: clean_filterbank.py (rfifind + channel mask). Task line: id beam chunk fil
source "${CRAFTS_EPOCH_ENV:?CRAFTS_EPOCH_ENV not set}"; source "${CRAFTS_STEPS_DIR}/common.sh"
fil=${F[3]}
echo "Processing $fil"
cd "$SCRIPTS_DIR"
python "$SCRIPTS_DIR/clean_filterbank.py" "$fil" "${PROC_DIR}/rfi_masks" "$(dirname "$fil")/"
crafts_end $?
