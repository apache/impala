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
NUM_ITERATIONS=1

# parse command line options
while getopts "e:n:" OPTION
do
  case "$OPTION" in
    e)
      EXPLORATION_STRATEGY=$OPTARG
      ;;
    n)
      NUM_ITERATIONS=$OPTARG
      ;;
    ?)
      echo "run-all-tests.sh [-e <exploration_strategy>] [-n <num_iters>]"
      echo "[-e] The exploration strategy to use. Default exploration is 'core'."
      echo "[-n] The number of times to run the tests. Default is 1."
      exit 1;
      ;;
  esac
done

LOG_DIR=${IMPALA_TEST_CLUSTER_LOG_DIR}/query_tests
AUTHORIZATION_LOG_DIR=${IMPALA_TEST_CLUSTER_LOG_DIR}/authorization_tests
mkdir -p ${LOG_DIR}
mkdir -p ${AUTHORIZATION_LOG_DIR}


# Enable core dumps
ulimit -c unlimited

echo "Split and assign HBase regions"
# To properly test HBase integeration, HBase regions are split and assigned by this
# script. Restarting HBase will change the region server assignment. Run split-hbase.sh
# before running any test.
${IMPALA_HOME}/testdata/bin/split-hbase.sh

for i in $(seq 1 $NUM_ITERATIONS)
do
  # Preemptively force kill impalads and the statestore to clean up any running instances.
  # The BE unit tests cannot run when impalads are started.
  ${IMPALA_HOME}/bin/start-impala-cluster.py --kill_only --force

  # Run backend tests.
  ${IMPALA_HOME}/bin/run-backend-tests.sh

  # Start up a cluster with authorization enabled.
  ${IMPALA_HOME}/bin/start-impala-cluster.py --log_dir=${AUTHORIZATION_LOG_DIR} \
      --cluster_size=3 --impalad_args="\
      --authorization_policy_file='/test-warehouse/authz-policy.ini'\
      --server_name=server1\
      --authorized_proxy_user_config=hue=$USER"

  # Run authorization tests
  pushd ${IMPALA_HOME}/tests
  py.test authorization/test_authorization.py -k test_impersonation
  popd

  # Run the remaining tests against a cluster with authorization disabled.
  ${IMPALA_HOME}/bin/start-impala-cluster.py --log_dir=${LOG_DIR} --cluster_size=3

  # Run some queries using run-workload to verify run-workload has not been broken.
  ${IMPALA_HOME}/bin/run-workload.py -w tpch --num_clients=2 --query_names=TPCH-Q1\
      --table_format=text/none --exec_options="disable_codegen:False"

  # Run end-to-end tests. The EXPLORATION_STRATEGY parameter should only apply to the
  # functional-query workload because the larger datasets (ex. tpch) are not generated
  # in all table formats.
  ${IMPALA_HOME}/tests/run-tests.py -x --exploration_strategy=core \
      --workload_exploration_strategy=functional-query:$EXPLORATION_STRATEGY

  # Run JUnit frontend tests
  # Requires a running impalad cluster because some tests (such as DataErrorTest and
  # JdbcTest) queries against an impala cluster.
  # TODO: Currently planner tests require running the end-to-end tests first
  # so data is inserted into tables. This will go away once we move the planner
  # tests to the new python framework.
  cd $IMPALA_FE_DIR
  mvn test

  # Run the custom-cluster tests after all other tests, since they will restart the
  # cluster repeatedly and lose state.
  # TODO: Consider moving in to run-tests.py.
  ${IMPALA_HOME}/tests/run-custom-cluster-tests.sh

  # Finally, run the process failure tests.
  # Disabled temporarily until we figure out the proper timeouts required to make the test
  # succeed.
  # ${IMPALA_HOME}/tests/run-process-failure-tests.sh
done
