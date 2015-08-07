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

# Allow picking up strategy from environment
: ${EXPLORATION_STRATEGY:=core}
: ${NUM_TEST_ITERATIONS:=1}
: ${MAX_PYTEST_FAILURES:=10}
KERB_ARGS=""

. ${IMPALA_HOME}/bin/impala-config.sh > /dev/null 2>&1
if ${CLUSTER_DIR}/admin is_kerberized; then
  KERB_ARGS="--use_kerberos"
fi

# Parametrized Test Options
# Run FE Tests
: ${FE_TEST:=true}
# Run Backend Tests
: ${BE_TEST:=true}
# Run End-to-end Tests
: ${EE_TEST:=true}
: ${EE_TEST_FILES:=}
# Run JDBC Test
: ${JDBC_TEST:=true}
# Run Cluster Tests
: ${CLUSTER_TEST:=true}
# Extra arguments passed to start-impala-cluster for tests
: ${TEST_START_CLUSTER_ARGS:=}

# parse command line options
while getopts "e:n:" OPTION
do
  case "$OPTION" in
    e)
      EXPLORATION_STRATEGY=$OPTARG
      ;;
    n)
      NUM_TEST_ITERATIONS=$OPTARG
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
mkdir -p ${LOG_DIR}

# Enable core dumps
ulimit -c unlimited

if [ "${TARGET_FILESYSTEM}" = "hdfs" ]; then
  echo "Split and assign HBase regions"
  # To properly test HBase integeration, HBase regions are split and assigned by this
  # script. Restarting HBase will change the region server assignment. Run split-hbase.sh
  # before running any test.
  ${IMPALA_HOME}/testdata/bin/split-hbase.sh > /dev/null 2>&1
fi

for i in $(seq 1 $NUM_TEST_ITERATIONS)
do
  TEST_RET_CODE=0
  # Preemptively force kill impalads and the statestore to clean up any running instances.
  # The BE unit tests cannot run when impalads are started.
  ${IMPALA_HOME}/bin/start-impala-cluster.py --kill_only --force

  if [[ "$BE_TEST" = true ]]; then
    # Run backend tests.
    if ! ${IMPALA_HOME}/bin/run-backend-tests.sh; then
      TEST_RET_CODE=1
    fi
  fi

  ${IMPALA_HOME}/bin/start-impala-cluster.py --log_dir=${LOG_DIR} --cluster_size=3 \
      ${TEST_START_CLUSTER_ARGS}

  # Run some queries using run-workload to verify run-workload has not been broken.
  if ! ${IMPALA_HOME}/bin/run-workload.py -w tpch --num_clients=2 --query_names=TPCH-Q1 \
       --table_format=text/none --exec_options="disable_codegen:False" ${KERB_ARGS}; then
    TEST_RET_CODE=1
  fi

  if [[ "$FE_TEST" = true ]]; then
    # Run JUnit frontend tests
    # Requires a running impalad cluster because some tests (such as DataErrorTest and
    # JdbcTest) queries against an impala cluster.
    pushd ${IMPALA_FE_DIR}
    MVN_ARGS=""
    if [ "${TARGET_FILESYSTEM}" = "s3" ]; then
      # When running against S3, only run the S3 frontend tests.
      MVN_ARGS="-Dtest=S3*"
    fi
    # quietly resolve dependencies to avoid log spew in jenkins runs
    if [ "${USER}" == "jenkins" ]; then
      echo "Quietly resolving FE dependencies."
      mvn -q dependency:resolve
    fi
    if ! mvn -fae test ${MVN_ARGS}; then
      TEST_RET_CODE=1
    fi
    popd
  fi

  if [[ "$EE_TEST" = true ]]; then
    # Run end-to-end tests. The EXPLORATION_STRATEGY parameter should only apply to the
    # functional-query workload because the larger datasets (ex. tpch) are not generated
    # in all table formats.
    # KERBEROS TODO - this will need to deal with ${KERB_ARGS}
    if ! ${IMPALA_HOME}/tests/run-tests.py --maxfail=${MAX_PYTEST_FAILURES} \
         --exploration_strategy=core \
         --workload_exploration_strategy=functional-query:$EXPLORATION_STRATEGY \
         ${EE_TEST_FILES}; then #${KERB_ARGS};
      TEST_RET_CODE=1
    fi
  fi

  if [[ "$JDBC_TEST" = true ]]; then
    # Run the JDBC tests with background loading disabled. This is interesting because
    # it requires loading missing table metadata.
    ${IMPALA_HOME}/bin/start-impala-cluster.py --log_dir=${LOG_DIR} --cluster_size=3 \
      --catalogd_args=--load_catalog_in_background=false \
      ${TEST_START_CLUSTER_ARGS}
    pushd ${IMPALA_FE_DIR}
    # quietly resolve dependencies to avoid log spew in jenkins runs
    if [ "${USER}" == "jenkins" ]; then
      echo "Quietly resolving FE dependencies."
      mvn -q dependency:resolve
    fi
    if ! mvn test -Dtest=JdbcTest; then
      TEST_RET_CODE=1
    fi
    popd
  fi

  if [[ "$CLUSTER_TEST" = true ]]; then
    # Run the custom-cluster tests after all other tests, since they will restart the
    # cluster repeatedly and lose state.
    # TODO: Consider moving in to run-tests.py.
    if ! ${IMPALA_HOME}/tests/run-custom-cluster-tests.sh \
         --maxfail=${MAX_PYTEST_FAILURES}; then
      TEST_RET_CODE=1
    fi
  fi

  # Finally, run the process failure tests.
  # Disabled temporarily until we figure out the proper timeouts required to make the test
  # succeed.
  # ${IMPALA_HOME}/tests/run-process-failure-tests.sh
  if [ $TEST_RET_CODE == 1 ]; then
    exit $TEST_RET_CODE
  fi
done
