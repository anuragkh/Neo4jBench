#!/bin/bash
set -ex

script_dir="$(dirname $0)"
output_dir="${script_dir}/../results"
mkdir -p "$output_dir"
classpath="${script_dir}/../target/scala-2.10/neo4jbench-assembly-0.1.0-SNAPSHOT.jar"
bench_type="latency"

echo "Performing $bench_type benchmark, script_dir=$script_dir, output_dir=$output_dir"

neo4j_dir="$HOME/neo4j"
datasets=("500000" "1000000" "2000000" "4000000" "8000000")


page_cache="10g"
jvm_heap="15360m"
#total_mem=249856 # r3.8xlarge
#avail_mem=$(( $total_mem - 512 )) # reserve 512m for OS

for dataset in ${datasets[@]}; do
  db_path=${neo4j_dir}/${dataset}
  for q in `seq 0 1 49`; do      
    query_path="$HOME/rpq/queries/$dataset/query-${q}.cypher"

    echo "Setting -Xmx to ${jvm_heap}, tuned to $tuned, num_clients to $num_clients, dataset to $dataset..."
    echo "db_path=$db_path, dataset=$dataset, query_path=$query_path"
      
    sync && sudo sh -c 'echo 3 > /proc/sys/vm/drop_caches'
    find ${db_path}/ -name "*store.db*" -type f -exec dd if={} of=/dev/null bs=1M 2>/dev/null \;
    java -server -XX:+UseConcMarkSweepGC -Xmx${jvm_heap} -cp ${classpath} \
      edu.berkeley.cs.neo4jbench.path.PathBench ${db_path} ${query_path} ${page_cache}
  done
done
