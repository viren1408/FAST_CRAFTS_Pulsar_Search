#!/bin/bash
#SBATCH --job-name=crafts_check
#SBATCH --cpus-per-task=1
#SBATCH --mem=2G
#SBATCH --time=00:15:00
# Runs after every step (dependency afterany). Verifies outputs + sacct, writes state,
# and submits the next step only if everything passed.
source "${CRAFTS_EPOCH_ENV:?}"
[ -n "${ENV_ACTIVATE:-}" ] && [ -f "$ENV_ACTIVATE" ] && source "$ENV_ACTIVATE"
"${CRAFTS_PYTHON:-python3}" "$CRAFTS_PY" -c "$CRAFTS_CONFIG" check --epoch-env "$CRAFTS_EPOCH_ENV" --step "$CRAFTS_STEP"
