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

set -euo pipefail
trap 'echo Error in $0 at line $LINENO: $(cd "'$PWD'" && awk "NR == $LINENO" $0)' ERR

# run buildall.sh -help to see options

ROOT=`dirname "$0"`
ROOT=`cd "$ROOT" >/dev/null; pwd`

# Grab this *before* we source impala-config.sh to see if the caller has
# kerberized environment variables already or not.
NEEDS_RE_SOURCE_NOTE=1
: ${MINIKDC_REALM=}
if [[ ! -z "${MINIKDC_REALM}" ]]; then
  NEEDS_RE_SOURCE_NOTE=0
fi

export IMPALA_HOME="$ROOT"
if ! . "$ROOT"/bin/impala-config.sh; then
  echo "Bad configuration, aborting buildall."
  exit 1
fi

# Change to IMPALA_HOME so that coredumps, etc end up in IMPALA_HOME.
cd "${IMPALA_HOME}"

# Defaults that are only changable via the commandline.
CLEAN_ACTION=1
TESTDATA_ACTION=0
TESTS_ACTION=1
FORMAT_CLUSTER=0
FORMAT_METASTORE=0
FORMAT_SENTRY_POLICY_DB=0
NEED_MINICLUSTER=0
START_IMPALA_CLUSTER=0
IMPALA_KERBERIZE=0
SNAPSHOT_FILE=
METASTORE_SNAPSHOT_FILE=
MAKE_IMPALA_ARGS=""
CODE_COVERAGE=0
BUILD_ASAN=0
BUILD_FE_ONLY=0
BUILD_TIDY=0
BUILD_UBSAN=0
BUILD_TSAN=0
# Export MAKE_CMD so it is visible in scripts that invoke make, e.g. copy-udfs-udas.sh
export MAKE_CMD=make
LZO_CMAKE_ARGS=

# Defaults that can be picked up from the environment, but are overridable through the
# commandline.
: ${EXPLORATION_STRATEGY:=core}
: ${CMAKE_BUILD_TYPE:=Debug}

# parse command line options
while [ -n "$*" ]
do
  case "$1" in
    -noclean)
      CLEAN_ACTION=0
      ;;
    -testdata)
      TESTDATA_ACTION=1
      ;;
    -skiptests)
      TESTS_ACTION=0
      ;;
    -build_shared_libs|-so)
      MAKE_IMPALA_ARGS="${MAKE_IMPALA_ARGS} -build_shared_libs"
      ;;
    -notests)
      TESTS_ACTION=0
      MAKE_IMPALA_ARGS="${MAKE_IMPALA_ARGS} -notests"
      ;;
    -format)
      FORMAT_CLUSTER=1
      FORMAT_METASTORE=1
      FORMAT_SENTRY_POLICY_DB=1
      ;;
    -format_cluster)
      FORMAT_CLUSTER=1
      ;;
    -format_metastore)
      FORMAT_METASTORE=1
      ;;
    -format_sentry_policy_db)
      FORMAT_SENTRY_POLICY_DB=1
      ;;
    -release)
      CMAKE_BUILD_TYPE=Release
      ;;
    -codecoverage)
      CODE_COVERAGE=1
      ;;
    -asan)
      BUILD_ASAN=1
      ;;
    -tidy)
      BUILD_TIDY=1
      ;;
    -ubsan)
      BUILD_UBSAN=1
      ;;
    -tsan)
      BUILD_TSAN=1
      ;;
    -testpairwise)
      EXPLORATION_STRATEGY=pairwise
      ;;
    -testexhaustive)
      EXPLORATION_STRATEGY=exhaustive
      # See bin/run-all-tests.sh and IMPALA-3947 for more information on
      # what this means.
      ;;
    -snapshot_file)
      SNAPSHOT_FILE="${2-}"
      if [[ ! -f "$SNAPSHOT_FILE" ]]; then
        echo "-snapshot_file does not exist: $SNAPSHOT_FILE"
        exit 1
      fi
      TESTDATA_ACTION=1
      # Get the full path.
      SNAPSHOT_FILE="$(readlink -f "$SNAPSHOT_FILE")"
      shift;
      ;;
    -metastore_snapshot_file)
      METASTORE_SNAPSHOT_FILE="${2-}"
      if [[ ! -f "$METASTORE_SNAPSHOT_FILE" ]]; then
        echo "-metastore_snapshot_file does not exist: $METASTORE_SNAPSHOT_FILE"
        exit 1
      fi
      TESTDATA_ACTION=1
      # Get the full path.
      METASTORE_SNAPSHOT_FILE="$(readlink -f "$METASTORE_SNAPSHOT_FILE")"
      shift;
      ;;
    -start_minicluster)
      NEED_MINICLUSTER=1
      ;;
    -start_impala_cluster)
      START_IMPALA_CLUSTER=1
      ;;
    -k|-kerberize|-kerberos|-kerb)
      # Export to the environment for all child process tools
      export IMPALA_KERBERIZE=1
      set +u
      . ${MINIKDC_ENV}
      set -u
      ;;
    -v|-debug)
      echo "Running in Debug mode"
      set -x
      ;;
    -fe_only)
      BUILD_FE_ONLY=1
      ;;
    -ninja)
      MAKE_IMPALA_ARGS+=" -ninja"
      LZO_CMAKE_ARGS+=" -GNinja"
      MAKE_CMD=ninja
      ;;
    -help|*)
      echo "buildall.sh - Builds Impala and runs all tests."
      echo "[-noclean] : Omits cleaning all packages before building. Will not kill"\
           "running Hadoop services unless any -format* is True"
      echo "[-format] : Format the minicluster, metastore db, and sentry policy db"\
           "[Default: False]"
      echo "[-format_cluster] : Format the minicluster [Default: False]"
      echo "[-format_metastore] : Format the metastore db [Default: False]"
      echo "[-format_sentry_policy_db] : Format the Sentry policy db [Default: False]"
      echo "[-release] : Release build [Default: debug]"
      echo "[-codecoverage] : Build with code coverage [Default: False]"
      echo "[-asan] : Address sanitizer build [Default: False]"
      echo "[-tidy] : clang-tidy build [Default: False]"
      echo "[-ubsan] : Undefined behavior build [Default: False]"
      echo "[-skiptests] : Skips execution of all tests"
      echo "[-notests] : Skips building and execution of all tests"
      echo "[-start_minicluster] : Start test cluster including Impala and all"\
           "its dependencies. If already running, all services are restarted."\
           "Regenerates test cluster config files. [Default: True if running"\
           "tests or loading data, False otherwise]"
      echo "[-start_impala_cluster] : Start Impala minicluster after build"\
           "[Default: False]"
      echo "[-testpairwise] : Run tests in 'pairwise' mode (increases"\
           "test execution time)"
      echo "[-testexhaustive] : Run tests in 'exhaustive' mode, which significantly"\
           "increases test execution time. ONLY APPLIES to suites with workloads:"\
           "functional-query, targeted-stress"
      echo "[-testdata] : Loads test data. Implied as true if -snapshot_file is"\
           "specified. If -snapshot_file is not specified, data will be regenerated."
      echo "[-snapshot_file <file name>] : Load test data from a snapshot file"
      echo "[-metastore_snapshot_file <file_name>]: Load the hive metastore snapshot"
      echo "[-so|-build_shared_libs] : Dynamically link executables (default is static)"
      echo "[-kerberize] : Enable kerberos on the cluster"
      echo "[-fe_only] : Build just the frontend"
      echo "-----------------------------------------------------------------------------
Examples of common tasks:

  # Build and run all tests
  ./buildall.sh

  # Build and skip tests
  ./buildall.sh -skiptests

  # Build, then restart the minicluster and Impala with fresh configs.
  ./buildall.sh -notests -start_minicluster -start_impala_cluster

  # Incrementally rebuild and skip tests. Keeps existing minicluster services running
  # and restart Impala.
  ./buildall.sh -skiptests -noclean -start_impala_cluster

  # Build, load a snapshot file, run tests
  ./buildall.sh -snapshot_file <file>

  # Build, load the hive metastore and the hdfs snapshot, run tests
  ./buildall.sh -snapshot_file <file> -metastore_snapshot_file <file>

  # Build, generate, and incrementally load test data without formatting the mini-cluster
  # (reuses existing data in HDFS if it exists). Can be faster than loading from a
  # snapshot.
  ./buildall.sh -testdata

  # Build, format mini-cluster and metastore, load all test data, run tests
  ./buildall.sh -testdata -format"
      exit 1
      ;;
    esac
  shift;
done

# Adjust CMAKE_BUILD_TYPE for ASAN and code coverage, if necessary.
if [[ ${CODE_COVERAGE} -eq 1 ]]; then
  case ${CMAKE_BUILD_TYPE} in
    Debug)
      CMAKE_BUILD_TYPE=CODE_COVERAGE_DEBUG
      ;;
    Release)
      CMAKE_BUILD_TYPE=CODE_COVERAGE_RELEASE
      ;;
  esac
fi
if [[ ${BUILD_ASAN} -eq 1 ]]; then
  # The next check also catches cases where CODE_COVERAGE=1, which is not supported
  # together with BUILD_ASAN=1.
  if [[ "${CMAKE_BUILD_TYPE}" != "Debug" ]]; then
    echo "Address sanitizer build not supported for build type: ${CMAKE_BUILD_TYPE}"
    exit 1
  fi
  CMAKE_BUILD_TYPE=ADDRESS_SANITIZER
fi
if [[ ${BUILD_TIDY} -eq 1 ]]; then
  CMAKE_BUILD_TYPE=TIDY
fi
if [[ ${BUILD_UBSAN} -eq 1 ]]; then
  CMAKE_BUILD_TYPE=UBSAN
fi
if [[ ${BUILD_TSAN} -eq 1 ]]; then
  CMAKE_BUILD_TYPE=TSAN
fi

MAKE_IMPALA_ARGS+=" -build_type=${CMAKE_BUILD_TYPE}"

# If we aren't kerberized then we certainly don't need to talk about
# re-sourcing impala-config.
if [[ ${IMPALA_KERBERIZE} -eq 0 ]]; then
  NEEDS_RE_SOURCE_NOTE=0
fi

if [[ ${IMPALA_KERBERIZE} -eq 1 &&
  (${TESTDATA_ACTION} -eq 1 || ${TESTS_ACTION} -eq 1) ]]; then
  echo "Running tests or loading test data is not supported for kerberized clusters."
  echo "Please remove the -testdata flag and/or add the -skiptests flag."
  exit 1
fi

# Loading data on a filesystem other than fs.defaultFS is not supported.
if [[ -z "$METASTORE_SNAPSHOT_FILE" && "${TARGET_FILESYSTEM}" != "hdfs" &&
      "$TESTDATA_ACTION" -eq 1 ]]; then
  echo "The metastore snapshot is required for loading data into ${TARGET_FILESYSTEM}"
  echo "Use the -metastore_snapshot_file command line paramater."
  exit 1
fi

if [[ $TESTS_ACTION -eq 1 || $TESTDATA_ACTION -eq 1 || $FORMAT_CLUSTER -eq 1 ||
      $FORMAT_METASTORE -eq 1 || $FORMAT_SENTRY_POLICY_DB -eq 1 || -n "$SNAPSHOT_FILE" ||
      -n "$METASTORE_SNAPSHOT_FILE" ]]; then
  NEED_MINICLUSTER=1
fi

create_log_dirs() {
  # Create all of the log directories.
  mkdir -p $IMPALA_ALL_LOGS_DIRS

  # Create symlinks Testing/Temporary and be/Testing/Temporary that point to the BE test
  # log dir to capture the all logs of BE unit tests. Gtest has Testing/Temporary
  # hardwired in its code, so we cannot change the output dir by configuration.
  # We create two symlinks to capture the logs when running ctest either from
  # ${IMPALA_HOME} or ${IMPALA_HOME}/be.
  rm -rf "${IMPALA_HOME}/Testing"
  mkdir -p "${IMPALA_HOME}/Testing"
  ln -fs "${IMPALA_BE_TEST_LOGS_DIR}" "${IMPALA_HOME}/Testing/Temporary"
  rm -rf "${IMPALA_HOME}/be/Testing"
  mkdir -p "${IMPALA_HOME}/be/Testing"
  ln -fs "${IMPALA_BE_TEST_LOGS_DIR}" "${IMPALA_HOME}/be/Testing/Temporary"
}

bootstrap_dependencies() {
  # Populate necessary thirdparty components unless it's set to be skipped.
  if [[ "${SKIP_TOOLCHAIN_BOOTSTRAP}" = true ]]; then
    echo "SKIP_TOOLCHAIN_BOOTSTRAP is true, skipping download of Python dependencies."
    echo "SKIP_TOOLCHAIN_BOOTSTRAP is true, skipping toolchain bootstrap."
  else
    echo "Downloading Python dependencies"
    # Download all the Python dependencies we need before doing anything
    # of substance. Does not re-download anything that is already present.
    if ! "$IMPALA_HOME/infra/python/deps/download_requirements"; then
      echo "Warning: Unable to download Python requirements."
      echo "Warning: bootstrap_virtualenv or other Python-based tooling may fail."
    else
      echo "Finished downloading Python dependencies"
    fi

    echo "Downloading and extracting toolchain dependencies."
    "$IMPALA_HOME/bin/bootstrap_toolchain.py"
    echo "Toolchain bootstrap complete."
  fi
}

# Build the Impala frontend and its dependencies.
build_fe() {
  "$IMPALA_HOME/bin/make_impala.sh" ${MAKE_IMPALA_ARGS} -fe_only
}

# Build all components.
build_all_components() {
  # Build the Impala frontend, backend and external data source API.
  MAKE_IMPALA_ARGS+=" -fe -cscope -tarballs"
  if [[ -e "$IMPALA_LZO" ]]
  then
    MAKE_IMPALA_ARGS+=" -impala-lzo"
  fi

  echo "Running make_impala.sh ${MAKE_IMPALA_ARGS}"
  "$IMPALA_HOME/bin/make_impala.sh" ${MAKE_IMPALA_ARGS}
}

# Do any configuration of the test cluster required by the script arguments.
# Kills any cluster processes that will need to be restarted to pick up new
# configurations or the new build.
reconfigure_test_cluster() {
  # Stop any running Impala services.
  "${IMPALA_HOME}/bin/start-impala-cluster.py" --kill --force

  if [[ "$FORMAT_METASTORE" -eq 1 || "$FORMAT_CLUSTER" -eq 1 ||
        "$FORMAT_SENTRY_POLICY_DB" -eq 1 || -n "$METASTORE_SNAPSHOT_FILE" ]]
  then
    # Kill any processes that may be accessing postgres metastore. To be safe, this is
    # done before we make any changes to the config files.
    "${IMPALA_HOME}/testdata/bin/kill-all.sh" || true
  fi

  # Stop the minikdc if needed.
  if "${CLUSTER_DIR}/admin" is_kerberized; then
      "${IMPALA_HOME}/testdata/bin/minikdc.sh" stop
  fi

  local CREATE_TEST_CONFIG_ARGS=""
  if [[ "$FORMAT_SENTRY_POLICY_DB" -eq 1 ]]; then
    CREATE_TEST_CONFIG_ARGS+=" -create_sentry_policy_db"
  fi

  if [[ "$FORMAT_METASTORE" -eq 1 && -z "$METASTORE_SNAPSHOT_FILE" ]]; then
    CREATE_TEST_CONFIG_ARGS+=" -create_metastore"
  fi

  # Generate the Hadoop configs needed by Impala
  "${IMPALA_HOME}/bin/create-test-configuration.sh" ${CREATE_TEST_CONFIG_ARGS}

  # Copy Hadoop-lzo dependencies if available (required to generate Lzo data).
  if stat "$HADOOP_LZO"/build/native/Linux-*-*/lib/libgplcompression.* > /dev/null ; then
    cp "$HADOOP_LZO"/build/native/Linux-*-*/lib/libgplcompression.* "$HADOOP_LIB_DIR/native"
  else
    echo "No hadoop-lzo found"
  fi
}

# Starts the test cluster processes except for Impala.
start_test_cluster_dependencies() {
  local RUN_ALL_ARGS=""
  if [[ "$FORMAT_CLUSTER" -eq 1 ]]; then
    RUN_ALL_ARGS+=" -format"
  fi
  "$IMPALA_HOME/testdata/bin/run-all.sh" $RUN_ALL_ARGS
}

# Execute any data loading steps once the cluster dependencies are started.
# This does all data loading, except for the metastore snapshot which must be loaded
# earlier before the cluster is running.
load_test_data() {
  "$IMPALA_HOME/bin/create_testdata.sh"
  # We have 4 cases:
  # - test-warehouse and metastore snapshots exists.
  # - Only the test-warehouse snapshot exists.
  # - Only the metastore snapshot exists.
  # - Neither of them exist.
  local CREATE_LOAD_DATA_ARGS=""
  if [[ "$SNAPSHOT_FILE" && "$METASTORE_SNAPSHOT_FILE" ]]; then
    CREATE_LOAD_DATA_ARGS="-snapshot_file ${SNAPSHOT_FILE} -skip_metadata_load"
  elif [[ "$SNAPSHOT_FILE" && -z "$METASTORE_SNAPSHOT_FILE" ]]; then
    CREATE_LOAD_DATA_ARGS="-snapshot_file ${SNAPSHOT_FILE}"
  elif [[ -z "$SNAPSHOT_FILE" && "$METASTORE_SNAPSHOT_FILE" ]]; then
    CREATE_LOAD_DATA_ARGS="-skip_metadata_load -skip_snapshot_load"
  fi
  "${IMPALA_HOME}/testdata/bin/create-load-data.sh" ${CREATE_LOAD_DATA_ARGS} <<< Y
}

run_all_tests() {
  local RUN_ALL_TESTS_ARGS=
  if [[ $CODE_COVERAGE -eq 1 ]]; then
    RUN_ALL_TESTS_ARGS+=" -c"
  fi
  "${IMPALA_HOME}/bin/run-all-tests.sh" -e $EXPLORATION_STRATEGY $RUN_ALL_TESTS_ARGS
}

# Clean everything first if requested.
if [[ "$CLEAN_ACTION" -eq 1 ]]; then
  "$IMPALA_HOME/bin/clean.sh"
fi

create_log_dirs

bootstrap_dependencies

if [[ "$BUILD_FE_ONLY" -eq 1 ]]; then
  build_fe
  exit 0
fi

build_all_components

if [[ $NEED_MINICLUSTER -eq 1 ]]; then
  reconfigure_test_cluster
fi

# If a metastore snapshot exists, load it while the cluster process are down and not
# accessing the metastore.
if [[ -n "$METASTORE_SNAPSHOT_FILE" ]]; then
  echo "Loading metastore snapshot"
  "${IMPALA_HOME}/testdata/bin/load-metastore-snapshot.sh" "$METASTORE_SNAPSHOT_FILE"
fi

if [[ $NEED_MINICLUSTER -eq 1 ]]; then
  start_test_cluster_dependencies
fi

if [[ $TESTDATA_ACTION -eq 1 ]]; then
  load_test_data
fi

if [[ $TESTS_ACTION -eq 1 ]]; then
  run_all_tests
fi

# Bring up Impala if requested. Tests and data load start their own miniclusters, so we
# should bring up a clean cluster *after* those steps are completed.
if [[ $START_IMPALA_CLUSTER -eq 1 ]]; then
  "${IMPALA_HOME}/bin/start-impala-cluster.py"
fi

if [[ ${NEEDS_RE_SOURCE_NOTE} -eq 1 ]]; then
  echo
  echo "You have just successfully created a kerberized cluster."
  echo "Congratulations!  Communication with this cluster requires"
  echo "the setting of certain environment variables.  These"
  echo "environment variables weren't available before the cluster"
  echo "was created.  To pick them up, please source impala-config.sh:"
  echo
  echo "   . \"${IMPALA_HOME}/bin/impala-config.sh\""
  echo
fi
