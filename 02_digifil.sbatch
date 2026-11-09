#!/bin/bash
#SBATCH --job-name=digifil
#SBATCH --cpus-per-task=1
#SBATCH --time=00:08:00
#SBATCH --mem=1G
# Step 2: one task = one beam x one chunk. Task line: id beam chunk outfil "file1 file2 ..."
source "${CRAFTS_EPOCH_ENV:?CRAFTS_EPOCH_ENV not set}"; source "${CRAFTS_STEPS_DIR}/common.sh"
BEAM=${F[1]}; CHUNK=${F[2]}; OUTFIL=${F[3]}
read -r -a INPUT_FILES <<< "${F[4]}"
mkdir -p "$(dirname "$OUTFIL")"
echo "Beam M${BEAM} chunk ${CHUNK}: ${#INPUT_FILES[@]} fits -> $OUTFIL"
rm -f "$OUTFIL"     # never leave a half-written file from an earlier attempt
digifil "${INPUT_FILES[@]}" $DIGIFIL_ARGS -o "$OUTFIL"
EXIT=$?
if [ $EXIT -eq 0 ] && [ ! -s "$OUTFIL" ]; then EXIT=3; echo "digifil returned 0 but $OUTFIL is empty"; fi
crafts_end $EXIT
