#!/bin/bash
set -ex

script_dir="$(dirname $0)"
output_dir="${script_dir}/../results"
mkdir -p "$output_dir"
classpath="${script_dir}/../target/scala-2.10/neo4jbench-assembly-0.1.0-SNAPSHOT.jar"

echo "Performing $bench_type benchmark, script_dir=$script_dir, output_dir=$output_dir"

neo4j_dir="$HOME/neo4j"
datasets=("orkut")

page_cache="10g"
jvm_heap="15360m"
#total_mem=249856 # r3.8xlarge
#avail_mem=$(( $total_mem - 512 )) # reserve 512m for OS

for dataset in ${datasets[@]}; do
  db_path=${neo4j_dir}/${dataset}

  echo "Setting -Xmx to ${jvm_heap}, tuned to $tuned, num_clients to $num_clients, dataset to $dataset..."
  echo "db_path=$db_path, dataset=$dataset, query_path=$query_path"

  for traversal_type in "bfs"; do
    for tuned in "true"; do #"false"; do   
      sync && sudo sh -c 'echo 3 > /proc/sys/vm/drop_caches'
      #if [ "$tuned" = "true" ]; then
        #find ${db_path}/ -name "*store.db*" -type f -exec dd if={} of=/dev/null bs=1M 2>/dev/null \;
      #fi
      java -server -XX:+UseConcMarkSweepGC -Xmx${jvm_heap} -cp ${classpath} \
        edu.berkeley.cs.neo4jbench.traversal.TraversalBench ${db_path}\
        ${output_dir}/${dataset}_${traversal_type}_tuned-${tuned}.txt\
        ${traversal_type} $tuned ${page_cache}
    done
  done
done
