#!/usr/bin/env bash
# Copyright (c) 2012 Cloudera, Inc. All rights reserved.
#
# This script creates schema and loads data into hive for running benchmarks and
# other tests. Using this script requires passing in two parameters:
# The first is the  data set type (benchmark, tpch). This will load the appropriate
# collection of data sets for the run type.
# The second is the exploration strategy. This determines the different combinations
# of file format, compression, etc that will be created and loaded. 'Core' defines
# a basic set of combinations. If 'pairwise' is specified the pairwise combinations
# of workload # + file format + compression will be loaded. If 'exhaustive' is
# passed as an argument the exhaustive set of combinations will be loaded.

exploration_strategy=core
data_set_type=benchmark

if [ $1 = "hive-benchmark" ]; then
  data_set_type=benchmark
elif [ $1 = "tpch" ]; then
  data_set_type=tpch
elif [ $1 = "all" ]; then
  data_set_type="benchmark tpch"
else
  echo "Invalid run type: $1. Valid values are 'all, tpch, hive-benchmark'"
  exit 1
fi

if [ $2 = "core" -o $2 = "pairwise" -o $2 = "exhaustive" ]; then
 exploration_strategy=$2
else
  echo "Invalid exploration strategy: $2. Valid values are 'core, pairwise, exhaustive'"
  exit 1
fi

bin=`dirname "$0"`
bin=`cd "$bin"; pwd`
. "$bin"/impala-config.sh

set -e

SCRIPT_DIR=$IMPALA_HOME/testdata/bin

function execute_hive_query_from_file {
  hive_args="-hiveconf hive.root.logger=WARN,console -v -f"
  "$HIVE_HOME/bin/hive" $hive_args $1
}

pushd "$IMPALA_HOME/testdata/bin";
for ds in $data_set_type
do
  ./generate_schema_statements.py --exploration_strategy ${exploration_strategy}\
                                  --base_output_file_name=${ds}\
                                  --schema_template=${ds}_schema_template.sql
  execute_hive_query_from_file \
      "$SCRIPT_DIR/create-${ds}-${exploration_strategy}-generated.sql"
  execute_hive_query_from_file \
      "$SCRIPT_DIR/load-${ds}-${exploration_strategy}-generated.sql"
done
popd

# TODO: Temporarily disable block id generation for everything except benchmark runs
# due to IMP-134
if [ $data_set_type = "benchmark" ]; then
  $IMPALA_HOME/testdata/bin/generate-block-ids.sh
fi
