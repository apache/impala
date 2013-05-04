#!/usr/bin/env bash
# Copyright 2012 Cloudera Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
# Runs all the tests. Currently includes FE tests, BE unit tests, and the end-to-end
# test suites.

# Exit on reference to uninitialized variables and non-zero exit codes
set -u
set -e
. $IMPALA_HOME/bin/set-pythonpath.sh
EXPLORATION_STRATEGY=core

# parse command line options
while getopts "e:" OPTION
do
  case "$OPTION" in
    e)
      EXPLORATION_STRATEGY=$OPTARG
      ;;
    ?)
      echo "run-all-tests.sh [-e <exploration_strategy>]. The default exploration"\
           "strategy is 'core'."
      exit 1;
      ;;
  esac
done

LOG_DIR=${IMPALA_HOME}/tests/results
FE_LOG_DIR=${IMPALA_HOME}/tests/results/fe
mkdir -p ${LOG_DIR}
mkdir -p ${FE_LOG_DIR}

# Enable core dumps
ulimit -c unlimited

# Start an in-process Impala cluster and run queries against it using run-workload.
# This helps verify mini-impala-cluster and run-workload have not been broken.
# TODO: Disable running in-process cluster until IMPALA-155 is resolved.
#${IMPALA_HOME}/bin/start-impala-cluster.py --log_dir=${LOG_DIR}\
#    --in-process --wait_for_cluster --cluster_size=3

# Run the remaining tests against an external Impala test cluster.
${IMPALA_HOME}/bin/start-impala-cluster.py --log_dir=${LOG_DIR}\
    --wait_for_cluster --cluster_size=3

${IMPALA_HOME}/bin/run-workload.py -w tpch --num_clients=2 --query_names=TPCH-Q1\
    --table_format=text/none

# Run end-to-end tests. The EXPLORATION_STRATEGY parameter should only apply to the
# functional-query workload because the larger datasets (ex. tpch) are not generated
# in all table formats.
${IMPALA_HOME}/tests/run-tests.py -x --exploration_strategy=core \
    --workload_exploration_strategy=functional-query:$EXPLORATION_STRATEGY

# Disable failure tests.
# TODO: Investigate instability to get failure tests back online.
#${IMPALA_HOME}/tests/run-process-failure-tests.sh

# Run JUnit frontend tests
# Requires a running impalad cluster because some tests (such as DataErrorTest and
# JdbcTest) queries against an impala cluster.
# TODO: Currently planner tests require running the end-to-end tests first
# so data is inserted into tables. This will go away once we move the planner
# tests to the new python framework.
${IMPALA_HOME}/bin/start-impala-cluster.py --log_dir=${FE_LOG_DIR}\
    --wait_for_cluster --cluster_size=3

cd $IMPALA_FE_DIR
mvn test

# End-to-end test and frontend tests have completed. Stop the impala test cluster.
${IMPALA_HOME}/bin/start-impala-cluster.py --kill_only

# Run backend tests
${IMPALA_HOME}/bin/run-backend-tests.sh
