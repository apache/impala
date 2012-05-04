#!/usr/bin/env bash
# Copyright (c) 2011 Cloudera, Inc. All rights reserved.

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

function execute_hive_query_from_file {
    hive_args="-hiveconf hive.root.logger=WARN,console -v -f"
    "$HIVE_HOME/bin/hive" $hive_args $1
}

execute_hive_query_from_file "$IMPALA_HOME/testdata/bin/create-benchmark.sql"
execute_hive_query_from_file "$IMPALA_HOME/testdata/bin/load-benchmark.sql"

if [ "$1" = "exhaustive" ]
then
    execute_hive_query_from_file "$IMPALA_HOME/testdata/bin/create-benchmark-exhaustive-generated.sql"
    execute_hive_query_from_file "$IMPALA_HOME/testdata/bin/load-benchmark-exhaustive-generated.sql"
elif [ "$1" = "pairwise" ]
then
    execute_hive_query_from_file "$IMPALA_HOME/testdata/bin/create-benchmark-pairwise-generated.sql"
    execute_hive_query_from_file "$IMPALA_HOME/testdata/bin/load-benchmark-pairwise-generated-sql"
else
    execute_hive_query_from_file "$IMPALA_HOME/testdata/bin/create-benchmark-core-generated.sql"
    execute_hive_query_from_file "$IMPALA_HOME/testdata/bin/load-benchmark-core-generated.sql"
fi
