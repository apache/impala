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
# TODO: Rewrite this script in python and detect and load workloads by enumerating
# the workloads directory.

exploration_strategy=
data_set_type=

if [ $1 = "hive-benchmark" ]; then
  data_set_type=$1
elif [ $1 = "functional" ]; then
  data_set_type=$1
elif [ $1 = "tpch" ]; then
  data_set_type=$1
elif [ $1 = "query-test" ]; then
  data_set_type="tpch functional"
elif [ $1 = "all" ]; then
  data_set_type="hive-benchmark tpch functional"
else
  echo "Invalid run type: $1. Valid values are 'all, query-test,"\
       "functional, tpch, hive-benchmark'"
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

WORKLOAD_DIR=$IMPALA_HOME/testdata/workloads
DATASET_DIR=$IMPALA_HOME/testdata/datasets
BIN_DIR=$IMPALA_HOME/testdata/bin

function execute_hive_query_from_file {
  hive_args="-hiveconf hive.root.logger=WARN,console -v -f"
  "$HIVE_HOME/bin/hive" $hive_args $1
  if [ $? != 0 ]; then
    echo LOAD OF $1 FAILED
    exit -1
  fi
}

for ds in $data_set_type
do
  SCRIPT_DIR=$DATASET_DIR/$ds
  pushd $SCRIPT_DIR
  $BIN_DIR/generate_schema_statements.py --exploration_strategy ${exploration_strategy}\
                                  --workload=${ds} --verbose
  execute_hive_query_from_file \
      "$SCRIPT_DIR/load-${ds}-${exploration_strategy}-generated.sql"
  bash $SCRIPT_DIR/load-trevni-${ds}-${exploration_strategy}-generated.sh
  popd
done

# TODO: Temporarily disable block id generation for everything except benchmark runs
# due to IMP-134
if [ $1 = "hive-benchmark" ]; then
  $IMPALA_HOME/testdata/bin/generate-block-ids.sh
fi
