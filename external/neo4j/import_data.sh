#!/bin/bash
set -ex

RAW_DIR=$HOME/gmark/demo/social
NEO4J_DIR=$HOME/neo4j
DATASETS=("500000" "1000000" "2000000" "4000000" "8000000")

mkdir -p ${NEO4J_DIR}

for DATASET in ${DATASETS[@]}; do
  if [ -d ${NEO4J_DIR}/${DATASET} ]
  then
    echo "Warning: neo4j database ${NEO4J_DIR}/${DATASET} exists, skipping \
import; remove existing db first then retry import"
    continue
  fi

  # NOTE: --id-type ACTUAL supposedly will handle ID mismatch issues
  ./bin/neo4j-import \
    --id-type string\
    --into ${NEO4J_DIR}/${DATASET}\
    --nodes "nodes.csv,${RAW_DIR}/${DATASET}/raw/nodes.txt"\
    --relationships "edges.csv,${RAW_DIR}/${DATASET}/raw/edges.txt"\
    --stacktrace --bad-tolerance 0
done
