#!/bin/bash
set -ex

script_dir="$(dirname $0)"
output_dir="${script_dir}/../results"
mkdir -p "$output_dir"
classpath="${script_dir}/../target/scala-2.10/neo4jbench-assembly-0.1.0-SNAPSHOT.jar"
bench_type="latency"

echo "Performing $bench_type benchmark, script_dir=$script_dir, output_dir=$output_dir"

neo4j_dir="$HOME/neo4j"
dataset="social-10M"
#dataset="social-large"
query_dir="$HOME/gmark/demo/social/social-translated"
temp=".tmp"
mkdir -p "$temp"
query_path="$temp/queries.cypher"
if [ ! -f $query_path ]; then
  echo "Query file not found!"
  cat $query_dir/query-*.cypher | sed -r 's/UNION//g' > "$query_path"
fi

echo "neo4j_dir=$neo4j_dir, dataset=$dataset, query_dir=$query_dir, query_path=$query_path"

page_cache="10g"
jvm_heap="15360m"
#total_mem=249856 # r3.8xlarge
#avail_mem=$(( $total_mem - 512 )) # reserve 512m for OS

for tuned in true false; do
  for num_clients in 32; do
    # "use more": #`echo "0.75 * ($total_mem - $jvm_heap)" | bc | awk '{printf("%d", $1)}'` #`echo "$avail_mem - $jvm_heap" | bc | awk '{printf("%d", $1)}'`
    # For default neo4j setting, use "Auto"; otherwise support custom value with postfix k/m/g
    echo "Setting -Xmx to ${jvm_heap}, tuned to $tuned, num_clients to $num_clients..."

    sync && sudo sh -c 'echo 3 > /proc/sys/vm/drop_caches'
    if [ "$tuned" = "true" ]; then
      #find ${neo4j_dir}/${dataset}/schema/index -type f -exec dd if={} of=/dev/null bs=1M 2>/dev/null \;
      find ${neo4j_dir}/${dataset}/ -name "*store.db*" -type f -exec dd if={} of=/dev/null bs=1M 2>/dev/null \;
    fi
    java -verbose:gc -server -XX:+UseConcMarkSweepGC -Xmx${jvm_heap} -cp ${classpath} \
      edu.berkeley.cs.succinctgraph.neo4jbench.path.PathBench \
      ${bench_type} \
      ${neo4j_dir}/${dataset} \
      ${query_path} \
      ${output_dir}/neo4j_${dataset}_${bench_type}_jvm${jvm_heap}_pagecache${page_cache}_tuned-${tuned}.txt \
      0 \
      50 \
      ${num_clients} \
      ${tuned} \
      ${page_cache}
  done
done

#rm -rf $temp
