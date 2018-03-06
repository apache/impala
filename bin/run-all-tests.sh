#!/usr/bin/env bash
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#
# Runs all the tests. Currently includes FE tests, BE unit tests, and the end-to-end
# test suites.

# Exit on reference to uninitialized variables and non-zero exit codes
set -euo pipefail
trap 'echo Error in $0 at line $LINENO: $(cd "'$PWD'" && awk "NR == $LINENO" $0)' ERR

. "$IMPALA_HOME/bin/set-pythonpath.sh"

# Allow picking up strategy from environment
: ${EXPLORATION_STRATEGY:=core}
: ${NUM_TEST_ITERATIONS:=1}
: ${MAX_PYTEST_FAILURES:=10}
KERB_ARGS=""

. "${IMPALA_HOME}/bin/impala-config.sh" > /dev/null 2>&1
. "${IMPALA_HOME}/testdata/bin/run-step.sh"
if "${CLUSTER_DIR}/admin" is_kerberized; then
  KERB_ARGS="--use_kerberos"
fi

# Parametrized Test Options
# Disable KRPC for test cluster and test execution
: ${DISABLE_KRPC:=false}
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
# Extra arguments passed to start-impala-cluster for tests. These do not apply to custom
# cluster tests.
: ${TEST_START_CLUSTER_ARGS:=}
if [[ "${TARGET_FILESYSTEM}" == "local" ]]; then
  # TODO: Remove abort_on_config_error flag from here and create-load-data.sh once
  # checkConfiguration() accepts the local filesystem (see IMPALA-1850).
  TEST_START_CLUSTER_ARGS="${TEST_START_CLUSTER_ARGS} --cluster_size=1 "`
      `"--impalad_args=--abort_on_config_error=false"
  FE_TEST=false
else
  TEST_START_CLUSTER_ARGS="${TEST_START_CLUSTER_ARGS} --cluster_size=3"
fi

# If KRPC tests are disabled, pass the flag to disable KRPC during cluster start.
if [[ "${DISABLE_KRPC}" == "true" ]]; then
  TEST_START_CLUSTER_ARGS="${TEST_START_CLUSTER_ARGS} --disable_krpc"
fi

# Indicates whether code coverage reports should be generated.
: ${CODE_COVERAGE:=false}

# parse command line options
while getopts "e:n:c" OPTION
do
  case "$OPTION" in
    e)
      EXPLORATION_STRATEGY="$OPTARG"
      ;;
    n)
      NUM_TEST_ITERATIONS="$OPTARG"
      ;;
    c)
      CODE_COVERAGE=true
      ;;
    ?)
      echo "run-all-tests.sh [-e <exploration_strategy>] [-n <num_iters>]"
      echo "[-e] The exploration strategy to use. Default exploration is 'core'."
      echo "[-n] The number of times to run the tests. Default is 1."
      echo "[-c] Set this option to generate code coverage reports."
      exit 1;
      ;;
  esac
done

# IMPALA-3947: "Exhaustive" tests are actually based on workload. This
# means what we colloquially call "exhaustive" tests are actually
# "exhaustive tests whose workloads are in this set below". Not all
# workloads are able to be exhaustively run through buildall.sh. For
# example, the tpch workload is never run exhaustively, because the
# relatively large size of tpch means data loading in all exhaustive
# formats takes much longer and data load snapshots containing tpch in
# all exhaustive formats are much larger to store and take longer to
# load.
#
# XXX If you change the --workload_exploration_strategy set below,
# please update the buildall.sh help text for -testexhaustive.
COMMON_PYTEST_ARGS="--maxfail=${MAX_PYTEST_FAILURES} --exploration_strategy=core"`
    `" --workload_exploration_strategy="`
        `"functional-query:${EXPLORATION_STRATEGY},"`
        `"targeted-stress:${EXPLORATION_STRATEGY}"

if [[ "${TARGET_FILESYSTEM}" == "local" ]]; then
  # Only one impalad is supported when running against local filesystem.
  COMMON_PYTEST_ARGS+=" --impalad=localhost:21000"
fi

# If KRPC tests are disabled, pass test_no_krpc flag to pytest.
# This includes the end-to-end tests and the custom cluster tests.
if [[ "${DISABLE_KRPC}" == "true" ]]; then
  COMMON_PYTEST_ARGS+=" --test_no_krpc"
fi

# For logging when using run-step.
LOG_DIR="${IMPALA_EE_TEST_LOGS_DIR}"

# Enable core dumps
ulimit -c unlimited || true

if [[ "${TARGET_FILESYSTEM}" == "hdfs" ]]; then
  # To properly test HBase integeration, HBase regions are split and assigned by this
  # script. Restarting HBase will change the region server assignment. Run split-hbase.sh
  # before running any test.
  run-step "Split and assign HBase regions" split-hbase.log \
      "${IMPALA_HOME}/testdata/bin/split-hbase.sh"
fi

for i in $(seq 1 $NUM_TEST_ITERATIONS)
do
  TEST_RET_CODE=0

  run-step "Starting Impala cluster" start-impala-cluster.log \
      "${IMPALA_HOME}/bin/start-impala-cluster.py" --log_dir="${IMPALA_EE_TEST_LOGS_DIR}" \
      ${TEST_START_CLUSTER_ARGS}

  if [[ "$BE_TEST" == true ]]; then
    if [[ "$TARGET_FILESYSTEM" == "local" ]]; then
      # This test will fail the configuration checks on local filesystem.
      # TODO: Don't skip this test once checkConfiguration() accepts the local
      # filesystem (see IMPALA-1850).
      export SKIP_BE_TEST_PATTERN="session*"
    fi
    # Run backend tests.
    if ! "${IMPALA_HOME}/bin/run-backend-tests.sh"; then
      TEST_RET_CODE=1
    fi
  fi

  # Run some queries using run-workload to verify run-workload has not been broken.
  if ! run-step "Run test run-workload" test-run-workload.log \
      "${IMPALA_HOME}/bin/run-workload.py" -w tpch --num_clients=2 --query_names=TPCH-Q1 \
      --table_format=text/none --exec_options="disable_codegen:False" ${KERB_ARGS}; then
    TEST_RET_CODE=1
  fi

  if [[ "$FE_TEST" == true ]]; then
    # Run JUnit frontend tests
    # Requires a running impalad cluster because some tests (such as DataErrorTest and
    # JdbcTest) queries against an impala cluster.
    pushd "${IMPALA_FE_DIR}"
    MVN_ARGS=""
    if [[ "${TARGET_FILESYSTEM}" == "s3" ]]; then
      # When running against S3, only run the S3 frontend tests.
      MVN_ARGS="-Dtest=S3* "
    fi
    if [[ "$CODE_COVERAGE" == true ]]; then
      MVN_ARGS+="-DcodeCoverage"
    fi
    if ! "${IMPALA_HOME}/bin/mvn-quiet.sh" -fae test ${MVN_ARGS}; then
      TEST_RET_CODE=1
    fi
    popd
  fi

  if [[ "$EE_TEST" == true ]]; then
    # Run end-to-end tests.
    # KERBEROS TODO - this will need to deal with ${KERB_ARGS}
    if ! "${IMPALA_HOME}/tests/run-tests.py" ${COMMON_PYTEST_ARGS} ${EE_TEST_FILES}; then
      #${KERB_ARGS};
      TEST_RET_CODE=1
    fi
  fi

  if [[ "$JDBC_TEST" == true ]]; then
    # Run the JDBC tests with background loading disabled. This is interesting because
    # it requires loading missing table metadata.
    "${IMPALA_HOME}/bin/start-impala-cluster.py" --log_dir="${IMPALA_EE_TEST_LOGS_DIR}" \
        --catalogd_args=--load_catalog_in_background=false \
        ${TEST_START_CLUSTER_ARGS}
    pushd "${IMPALA_FE_DIR}"
    if ! "${IMPALA_HOME}/bin/mvn-quiet.sh" test -Dtest=JdbcTest; then
      TEST_RET_CODE=1
    fi
    popd
  fi

  if [[ "$CLUSTER_TEST" == true ]]; then
    # For custom cluster tests only, set an unlimited log rotation
    # policy, for the mini cluster is restarted many times. So as not to
    # pollute the directory with too many files, remove what was there
    # before. Also, save the IMPALA_MAX_LOG_FILES value for re-set
    # later.
    rm -rf "${IMPALA_CUSTOM_CLUSTER_TEST_LOGS_DIR}"
    mkdir -p "${IMPALA_CUSTOM_CLUSTER_TEST_LOGS_DIR}"
    IMPALA_MAX_LOG_FILES_SAVE="${IMPALA_MAX_LOG_FILES:-10}"
    export IMPALA_MAX_LOG_FILES=0
    # Run the custom-cluster tests after all other tests, since they will restart the
    # cluster repeatedly and lose state.
    # TODO: Consider moving in to run-tests.py.
    if ! "${IMPALA_HOME}/tests/run-custom-cluster-tests.sh" ${COMMON_PYTEST_ARGS}; then
      TEST_RET_CODE=1
    fi
    export IMPALA_MAX_LOG_FILES="${IMPALA_MAX_LOG_FILES_SAVE}"
  fi

  # Finally, run the process failure tests.
  # Disabled temporarily until we figure out the proper timeouts required to make the test
  # succeed.
  # ${IMPALA_HOME}/tests/run-process-failure-tests.sh
  if [[ $TEST_RET_CODE == 1 ]]; then
    exit $TEST_RET_CODE
  fi
done
