#!/bin/bash
set -ex

SCRIPT_DIR=$(dirname $0)
source ${SCRIPT_DIR}/config.sh
#### At least dependent on these configs (config.sh):
####    attribtues
####    NODE_FILE
####    ASSOC_FILE
####    NEO4J_DELIM

#DATASET="higgs-twitter-20attr35each"
#DATASET="higgs-twitter-40attr16each"

RAW_DIR=$HOME/raw
NEO4J_DIR=$HOME/neo4j
#DATASET="liveJournal-40attr16each-minDeg30"
#DATASET="liveJournal-40attr16each-minDeg45"
#DATASET="liveJournal-40attr16each-minDeg60"
#DATASET="liveJournal-40attr16each-minDeg30WithTsAttr"
#DATASET="twitter2010-40attr16each"

mkdir -p ${CSV_DIR}
mkdir -p ${NEO4J_DIR}
if [ -d ${NEO4J_DIR}/${DATASET} ]
then
    echo "Warning: neo4j database ${NEO4J_DIR}/${DATASET} exists, import will \
not force write; if you really want a new database, remove it first then \
re-run this script"
    exit
fi

# NOTE: --id-type ACTUAL supposedly will handle ID mismatch issues
./bin/neo4j-import \
    --into ${NEO4J_DIR}/${DATASET} \
    --nodes:Node "${RAW_DIR}/${DATASET}/nodes.txt" \
    --relationships "${RAW_DIR}/${DATASET}/edges.txt" \
    --id-type ACTUAL \
    --stacktrace --bad-tolerance 0
