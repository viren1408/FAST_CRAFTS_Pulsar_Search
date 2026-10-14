#!/bin/bash
#SBATCH --account=oz002
#SBATCH --job-name=make_zaplist
#SBATCH --cpus-per-task=1
#SBATCH --mem=1G
#SBATCH --time=00:10:00
#SBATCH --partition=$USER
#SBATCH --output=/aphid/temp/fast_crafts/crafts_processing/logs_make_zap_lists/%x_%j.out
#SBATCH --error=/aphid/temp/fast_crafts/crafts_processing/logs_make_zap_lists/%x_%j.err

source /fred/oz002/users/vmandaog/vmandaog_env/bin/activate

BASEDIR="/aphid/temp/fast_crafts/crafts_processing"
SCRIPT="${BASEDIR}/final_scripts//make_zap_list.py"
INDIR="${BASEDIR}/ZD2021_1_1_2bit/Dec+2925_06_05/20220510"
OUTDIR="${INDIR}/birdie_lists"

mkdir -p "${BASEDIR}/rfi_jobs_logs/logs_make_zap_lists"

NBEAMS=19
T=25.6

echo "=========================================="
echo "Job:        $SLURM_JOB_ID"
echo "Node:       $SLURMD_NODENAME"
echo "Started:    $(date)"
echo "Base path:  $INDIR"
echo "Output dir: $OUTDIR"
echo "=========================================="

python "$SCRIPT" \
    --base_path "$INDIR" \
    --outdir    "$OUTDIR"    \
    --nbeams    "$NBEAMS"   \
    --T         "$T"

EXIT_CODE=$?

echo ""
echo "=========================================="
echo "Finished: $(date)"
echo "Exit code: $EXIT_CODE"
echo "=========================================="

exit $EXIT_CODE
