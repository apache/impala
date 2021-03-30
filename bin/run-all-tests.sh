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
. $IMPALA_HOME/bin/report_build_error.sh
setup_report_build_error

# Allow picking up strategy from environment
: ${EXPLORATION_STRATEGY:=core}
: ${NUM_TEST_ITERATIONS:=1}
: ${MAX_PYTEST_FAILURES:=10}
: ${TIMEOUT_FOR_RUN_ALL_TESTS_MINS:=1200}
KERB_ARGS=""

. "${IMPALA_HOME}/bin/impala-config.sh" > /dev/null 2>&1
. "${IMPALA_HOME}/testdata/bin/run-step.sh"
if "${CLUSTER_DIR}/admin" is_kerberized; then
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
: ${EE_TEST_SHARDS:=1}
# Run JDBC Test
: ${JDBC_TEST:=true}
# Run Cluster Tests
: ${CLUSTER_TEST:=true}
# Extra arguments passed to start-impala-cluster for tests. These do not apply to custom
# cluster tests.
: ${TEST_START_CLUSTER_ARGS:=}
# Extra args to pass to run-tests.py
: ${RUN_TESTS_ARGS:=}
# Extra args to pass to run-custom-cluster-tests.sh
: ${RUN_CUSTOM_CLUSTER_TESTS_ARGS:=}
# Data cache root directory location.
: ${DATA_CACHE_DIR:=}
# Data cache size.
: ${DATA_CACHE_SIZE:=}
# Data cache eviction policy
: ${DATA_CACHE_EVICTION_POLICY:=}
if [[ "${TARGET_FILESYSTEM}" == "local" ]]; then
  # TODO: Remove abort_on_config_error flag from here and create-load-data.sh once
  # checkConfiguration() accepts the local filesystem (see IMPALA-1850).
  TEST_START_CLUSTER_ARGS="${TEST_START_CLUSTER_ARGS} --cluster_size=1 "`
      `"--impalad_args=--abort_on_config_error=false"
  FE_TEST=false
else
  TEST_START_CLUSTER_ARGS="${TEST_START_CLUSTER_ARGS} --cluster_size=3"
fi

# Enable data cache if configured.
if [[ -n "${DATA_CACHE_DIR}" && -n "${DATA_CACHE_SIZE}" ]]; then
   TEST_START_CLUSTER_ARGS="${TEST_START_CLUSTER_ARGS} "`
       `"--data_cache_dir=${DATA_CACHE_DIR} --data_cache_size=${DATA_CACHE_SIZE} "
   if [[ -n "${DATA_CACHE_EVICTION_POLICY}" ]]; then
       TEST_START_CLUSTER_ARGS="${TEST_START_CLUSTER_ARGS} "`
           `"--data_cache_eviction_policy=${DATA_CACHE_EVICTION_POLICY}"
   fi
   # Force use of data cache for HDFS. Data cache is only enabled for remote reads.
   if [[ "${TARGET_FILESYSTEM}" == "hdfs" ]]; then
      TEST_START_CLUSTER_ARGS="${TEST_START_CLUSTER_ARGS} "`
          `"--impalad_args=--always_use_data_cache=true"
   fi
fi

if [[ "${ERASURE_CODING}" = true ]]; then
  # We do not run FE tests when erasure coding is enabled because planner tests
  # would fail.
  FE_TEST=false
  TEST_START_CLUSTER_ARGS="${TEST_START_CLUSTER_ARGS} \
    --impalad_args=--default_query_options=allow_erasure_coded_files=true"
fi

# Indicates whether code coverage reports should be generated.
: ${CODE_COVERAGE:=false}

# parse command line options
# Note: The ":"'s indicate that an option needs an argument.
while getopts "e:n:ct:" OPTION
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
    t)
      TIMEOUT_FOR_RUN_ALL_TESTS_MINS="$OPTARG"
      ;;
    ?)
      echo "run-all-tests.sh [-e <exploration_strategy>] [-n <num_iters>]"
      echo "[-e] The exploration strategy to use. Default exploration is 'core'."
      echo "[-n] The number of times to run the tests. Default is 1."
      echo "[-c] Set this option to generate code coverage reports."
      echo "[-t] The timeout in minutes for running all tests."
      exit 1;
      ;;
  esac
done

"${IMPALA_HOME}/bin/script-timeout-check.sh" -timeout $TIMEOUT_FOR_RUN_ALL_TESTS_MINS \
    -script_name "$(basename $0)" &
TIMEOUT_PID=$!

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
if [[ "${EXPLORATION_STRATEGY}" == "core" ]]; then
  # Skip the stress test in core - all stress tests are in exhaustive and
  # pytest startup takes a significant amount of time.
  RUN_TESTS_ARGS+=" --skip-stress"
fi

if [[ "${TARGET_FILESYSTEM}" == "local" ]]; then
  # Only one impalad is supported when running against local filesystem.
  COMMON_PYTEST_ARGS+=" --impalad=localhost:21000"
fi

# For logging when using run-step.
LOG_DIR="${IMPALA_EE_TEST_LOGS_DIR}"

# Enable core dumps
ulimit -c unlimited || true

TEST_RET_CODE=0

# Helper function to start Impala cluster.
start_impala_cluster() {
  run-step "Starting Impala cluster" start-impala-cluster.log \
      "${IMPALA_HOME}/bin/start-impala-cluster.py" \
      --log_dir="${IMPALA_EE_TEST_LOGS_DIR}" \
      ${TEST_START_CLUSTER_ARGS}
}

run_ee_tests() {
  if [[ $# -gt 0 ]]; then
    EXTRA_ARGS=${1}
  else
    EXTRA_ARGS=""
  fi
  # Run end-to-end tests.
  # KERBEROS TODO - this will need to deal with ${KERB_ARGS}
  if ! "${IMPALA_HOME}/tests/run-tests.py" ${COMMON_PYTEST_ARGS} \
      ${RUN_TESTS_ARGS} ${EXTRA_ARGS} ${EE_TEST_FILES}; then
    #${KERB_ARGS};
    TEST_RET_CODE=1
  fi
}

for i in $(seq 1 $NUM_TEST_ITERATIONS)
do
  TEST_RET_CODE=0

  # Store a list of the files at the beginning of each iteration.
  hdfs dfs -ls -R /test-warehouse > ${IMPALA_LOGS_DIR}/file-list-begin-${i}.log 2>&1

  start_impala_cluster

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
      MVN_ARGS="-Ps3-tests "
    fi
    if [[ "$CODE_COVERAGE" == true ]]; then
      MVN_ARGS+="-DcodeCoverage"
    fi
    # Run the FE tests first. We run the FE custom cluster tests below since they
    # restart Impala.
    MVN_ARGS_TEMP=$MVN_ARGS
    MVN_ARGS+=" -Dtest=!org.apache.impala.custom*.*Test"
    if ! "${IMPALA_HOME}/bin/mvn-quiet.sh" -fae test ${MVN_ARGS}; then
      TEST_RET_CODE=1
    fi

    # Run the FE custom cluster tests only if not running against S3
    if [[ "${TARGET_FILESYSTEM}" != "s3" ]]; then
      MVN_ARGS=$MVN_ARGS_TEMP
      MVN_ARGS+=" -Dtest=org.apache.impala.custom*.*Test"
      if ! "${IMPALA_HOME}/bin/mvn-quiet.sh" -fae test ${MVN_ARGS}; then
        TEST_RET_CODE=1
      fi
      # Restart the minicluster after running the FE custom cluster tests.
      start_impala_cluster
    fi
    popd
  fi

  if [[ "$EE_TEST" == true ]]; then
    if [[ ${EE_TEST_SHARDS} -lt 2 ]]; then
      # For runs without sharding, avoid adding the "--shard_tests" parameter.
      # Some test frameworks (e.g. the docker-based tests) use this.
      run_ee_tests
    else
      # Increase the maximum number of log files so that the logs from the shards
      # don't get aged out. Multiply the default number by the number of shards.
      IMPALA_MAX_LOG_FILES_SAVE="${IMPALA_MAX_LOG_FILES:-10}"
      export IMPALA_MAX_LOG_FILES="$((${EE_TEST_SHARDS} * ${IMPALA_MAX_LOG_FILES_SAVE}))"
      # When the EE tests are sharded, it runs 1/Nth of the tests at a time, restarting
      # Impala between the shards. There are two benefits:
      # 1. It isolates errors so that if Impala crashes, the next shards will still run
      #    with a fresh Impala.
      # 2. For ASAN runs, resources accumulate over test execution, so tests get slower
      #    over time (see IMPALA-9887). Running shards with regular restarts
      #    substantially speeds up execution time.
      #
      # Shards are 1 indexed (i.e. 1/N through N/N). This shards both serial and
      # parallel tests.
      for (( shard_idx=1 ; shard_idx <= ${EE_TEST_SHARDS} ; shard_idx++ )); do
        run_ee_tests "--shard_tests=$shard_idx/${EE_TEST_SHARDS}"
        start_impala_cluster
      done
      export IMPALA_MAX_LOG_FILES="${IMPALA_MAX_LOG_FILES_SAVE}"
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
    if ! "${IMPALA_HOME}/tests/run-custom-cluster-tests.sh" ${COMMON_PYTEST_ARGS} \
        ${RUN_CUSTOM_CLUSTER_TESTS_ARGS}; then
      TEST_RET_CODE=1
    fi
    export IMPALA_MAX_LOG_FILES="${IMPALA_MAX_LOG_FILES_SAVE}"
  fi

  # Run the process failure tests.
  # Disabled temporarily until we figure out the proper timeouts required to make the test
  # succeed.
  # ${IMPALA_HOME}/tests/run-process-failure-tests.sh

  # Store a list of the files at the end of each iteration. This can be compared
  # to the file-list-begin*.log from the beginning of the iteration to see if files
  # are not being cleaned up. This is most useful on the first iteration, when
  # the list of files is from dataload.
  hdfs dfs -ls -R /test-warehouse > ${IMPALA_LOGS_DIR}/file-list-end-${i}.log 2>&1

  # Finally, kill the spawned timeout process and its child sleep process.
  # There may not be a sleep process, so ignore failure.
  pkill -P $TIMEOUT_PID || true
  kill $TIMEOUT_PID

  if [[ $TEST_RET_CODE == 1 ]]; then
    exit $TEST_RET_CODE
  fi
done
