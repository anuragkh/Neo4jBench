#!/bin/bash
set -ex

RAW_DIR=$HOME/raw
NEO4J_DIR=$HOME/neo4j
DATASET=social-10M

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
    --id-type string\
    --into ${NEO4J_DIR}/${DATASET}\
    --nodes "nodes.csv,${RAW_DIR}/${DATASET}/nodes.txt"\
    --relationships "edges.csv,${RAW_DIR}/${DATASET}/edges.txt"\
    --stacktrace --bad-tolerance 0
