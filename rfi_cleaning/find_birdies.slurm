#!/bin/bash
#SBATCH --account=oz002
#SBATCH --cpus-per-task=1
#SBATCH --mem=1G
#SBATCH --time=00:10:00
#SBATCH --tmp=1G
#SBATCH --partition=${SYS_ARCH}
#SBATCH --gres=gpu:1
#SBATCH --array=0-1443   # adjust based on number of files
#SBATCH --output=/aphid/temp/fast_crafts/crafts_processing/logs_find_birdies/%x_%A_%a.out
#SBATCH --error=/aphid/temp/fast_crafts/crafts_processing/logs_find_birdies/%x_%A_%a.err

source /fred/oz002/users/vmandaog/vmandaog_env/bin/activate


BASEDIR="/aphid/temp/fast_crafts/crafts_processing"
INDIR="${BASEDIR}/ZD2021_1_1_2bit/Dec+2925_06_05/20220510"

LISTDIR="${INDIR}/fil_lists/"
EPOCH=$(basename "$(dirname "$INDIR")")

fil=$(sed -n "$((SLURM_ARRAY_TASK_ID+1))p" "${LISTDIR}/${EPOCH}_masked_fil_list.txt")
fname=$(basename "$fil" .fil)

echo "Processing $fname on task $SLURM_ARRAY_TASK_ID"
beam_num=$(echo "$fname" | grep -oP '(?<=-M)\d+')

python "${BASEDIR}/final_scripts/run_sliding_search_for_birdies.py" \
    --fil "$fil" \
    --beam "$beam_num" \
    --min_sn 10 \
    --dm_start 0 \
    --dm_end 5 \
    --T-window 25.6 \
    --T-shift 12.8 \
    --outdir "${INDIR}/${beam_num}/${fname}_birdies"
