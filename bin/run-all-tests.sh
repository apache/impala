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


# Run end-to-end tests using an in-process impala test cluster
LOG_DIR=${IMPALA_HOME}/tests/results
mkdir -p ${LOG_DIR}
${IMPALA_HOME}/bin/start-impala-cluster.py --in-process --log_dir=${LOG_DIR}\
    --wait_for_cluster --cluster_size=3
${IMPALA_HOME}/tests/run-tests.py --exploration_strategy=$EXPLORATION_STRATEGY -x
${IMPALA_HOME}/bin/start-impala-cluster.py --kill_only

# Run JUnit frontend tests
# TODO: Currently planner tests require running the end-to-end tests first
# so data is inserted into tables. This will go away once we move the planner
# tests to the new python framework.
cd $IMPALA_FE_DIR
mvn test

# Run backend tests
${IMPALA_HOME}/bin/run-backend-tests.sh
