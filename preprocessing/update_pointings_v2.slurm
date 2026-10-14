#!/bin/bash
#SBATCH --job-name=update_pointing_batch
#SBATCH --output=logs/update_%j.out
#SBATCH --error=logs/update_%j.err
#SBATCH --time=01:00:00
#SBATCH --cpus-per-task=2
#SBATCH --mem=4G

source /fred/oz002/users/vmandaog/vmandaog_env/bin/activate

DATA_DIR="/aphid/temp/fast_crafts/data/ZD2021_1_1_2bit/Dec+3240_01_05/20220329"
POINTING_FILE="/fred/oz002/users/vmandaog/pulsar_search/FAST/crafts_2bit_miaocc01/Dec+3240_01_05_20220329_pos.fits"
SCRIPT="/aphid/temp/fast_crafts/crafts_processing/update_pointing_v2.py"


EPOCH=$(basename "$(dirname "$DATA_DIR")")

LOG_DIR="${DATA_DIR}/update_pointing_logs"
mkdir -p $LOG_DIR

LOG_FILE="${LOG_DIR}/${EPOCH}.log"

echo "========================================"
echo "Epoch: $EPOCH"
echo "Data dir: $DATA_DIR"
echo "Log file: $LOG_FILE"
echo "========================================"


echo "Making FITS files writable..."
find "$DATA_DIR" -maxdepth 1 -name "*.fits" -print0 | xargs -0 chmod u+w

echo "Starting batch processing..."
/usr/bin/time -v python $SCRIPT \
    --dir "$DATA_DIR" \
    --pointing-file "$POINTING_FILE" \
    --log-level INFO \
    --log-file "$LOG_FILE"

STATUS=$?

echo "Reverting FITS files to read-only..."
find "$DATA_DIR" -maxdepth 1 -name "*.fits" -print0 | xargs -0 chmod 444

echo "Finished with status: $STATUS"
exit $STATUS
