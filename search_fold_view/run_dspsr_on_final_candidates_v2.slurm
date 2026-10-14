#!/bin/bash
#SBATCH --job-name=fold_array
#SBATCH --output=final_folding_logs/fold_%A_%a.out
#SBATCH --error=final_folding_logs/fold_%A_%a.err
#SBATCH --array=1-1444   # 1-indexed, update upper bound to line count
#SBATCH --time=04:00:00
#SBATCH --cpus-per-task=1
#SBATCH --mem=4G

source /fred/oz002/users/vmandaog/vmandaog_env/bin/activate

CMDDIR="/aphid/temp/fast_crafts/crafts_processing/sifting_output_Dec+2925_06_05/fold_commands_Dec+2925_06_05.sh"

CMD=$(sed -n "${SLURM_ARRAY_TASK_ID}p" "${CMDDIR}")

echo "Running task ${SLURM_ARRAY_TASK_ID}"
echo "$CMD"

eval "$CMD"
