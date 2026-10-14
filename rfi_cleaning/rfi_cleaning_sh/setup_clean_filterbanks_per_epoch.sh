#!/bin/bash
# setup_fil_list


BASEDIR="/aphid/temp/fast_crafts/crafts_processing"
INDIR="${BASEDIR}/ZD2021_1_1_2bit/Dec+2925_06_05/20220510"
LISTDIR="${INDIR}/fil_lists"
EPOCH=$(basename "$(dirname "$INDIR")")

mkdir -p "$LISTDIR"
mkdir -p "${BASEDIR}/logs_find_birdies"

ls ${INDIR}/{01..19}/*.fil > "${LISTDIR}/${EPOCH}_fil_list.txt"

echo "Created fil list: ${LISTDIR}/${EPOCH}_fil_list.txt"
echo "Number of files: $(wc -l < "${LISTDIR}/${EPOCH}_fil_list.txt")"
echo "Ready to submit array job."
