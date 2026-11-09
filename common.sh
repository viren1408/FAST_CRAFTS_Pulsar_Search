# Sourced by every step wrapper. Loads the epoch variables and this task's line.
# Exports: TASK_ID and the tab-separated fields F[0..] of the task line.
set -o pipefail
source "${CRAFTS_EPOCH_ENV:?CRAFTS_EPOCH_ENV not set}"
[ -n "${ENV_ACTIVATE:-}" ] && [ -f "$ENV_ACTIVATE" ] && source "$ENV_ACTIVATE"
T0=$(date +%s)
if [ -n "${CRAFTS_TASKS:-}" ]; then
    IDX=$(( ${CRAFTS_OFFSET:-0} + ${SLURM_ARRAY_TASK_ID:-0} ))
    LINE=$(sed -n "$((IDX + 1))p" "$CRAFTS_TASKS")
    if [ -z "$LINE" ]; then echo "CRAFTS: no task line $IDX in $CRAFTS_TASKS"; exit 2; fi
    IFS=$'\t' read -r -a F <<< "$LINE"
    TASK_ID=${F[0]}
fi
echo "CRAFTS_TASK_START step=${CRAFTS_STEP} epoch=${POINTING}/${EPOCH} task=${TASK_ID:-0} job=${SLURM_JOB_ID:-} host=$(hostname) time=$(date -Is)"
crafts_end() {   # crafts_end <exit code>
    echo "CRAFTS_TASK_END step=${CRAFTS_STEP} task=${TASK_ID:-0} exit=$1 elapsed_s=$(( $(date +%s) - T0 ))"
    exit "$1"
}
