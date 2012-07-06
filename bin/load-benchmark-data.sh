#!/usr/bin/env bash
# Copyright (c) 2012 Cloudera, Inc. All rights reserved.
#
# Script that creates schema and loads data into hive for running benchmarks.
# By default the script will load the base data for the "core" scenario.
# If 'pairwise' is specified as a parameter the pairwise combinations of workload
# + file format + compression will be loaded.
# If 'exhaustive' is passed as an argument the exhaustive set of combinations will
# be executed.

bin=`dirname "$0"`
bin=`cd "$bin"; pwd`
. "$bin"/impala-config.sh

set -e

exploration_strategy=core
if [ $1 ]; then
  exploration_strategy=$1
fi

BENCHMARK_SCRIPT_DIR=$IMPALA_HOME/testdata/bin

function execute_hive_query_from_file {
  hive_args="-hiveconf hive.root.logger=WARN,console -v -f"
  "$HIVE_HOME/bin/hive" $hive_args $1
}

pushd "$IMPALA_HOME/testdata/bin";
./generate_benchmark_statements.py --exploration_strategy $exploration_strategy
popd

if [ "$exploration_strategy" = "exhaustive" ]; then
  execute_hive_query_from_file "$BENCHMARK_SCRIPT_DIR/create-benchmark-exhaustive-generated.sql"
  execute_hive_query_from_file "$BENCHMARK_SCRIPT_DIR/load-benchmark-exhaustive-generated.sql"
elif [ "$exploration_strategy" = "pairwise" ]; then
  execute_hive_query_from_file "$BENCHMARK_SCRIPT_DIR/create-benchmark-pairwise-generated.sql"
  execute_hive_query_from_file "$BENCHMARK_SCRIPT_DIR/load-benchmark-pairwise-generated-sql"
elif [ "$exploration_strategy" = "core" ]; then
  execute_hive_query_from_file "$BENCHMARK_SCRIPT_DIR/create-benchmark-core-generated.sql"
  execute_hive_query_from_file "$BENCHMARK_SCRIPT_DIR/load-benchmark-core-generated.sql"
else
  echo "Invalid exploration strategy: $exploration_strategy"
  exit 1
fi

$IMPALA_HOME/testdata/bin/generate-block-ids.sh
