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

: ${IMPALA_HOME:=$(cd "$(dirname $0)"; pwd)}
export IMPALA_HOME

. $IMPALA_HOME/bin/report_build_error.sh
setup_report_build_error

# run buildall.sh -help to see options
ROOT=`dirname "$0"`
ROOT=`cd "$ROOT" >/dev/null; pwd`

if [[ "'$ROOT'" =~ [[:blank:]] ]]
then
   echo "IMPALA_HOME cannot have spaces in the path"
   exit 1
fi

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
UPGRADE_METASTORE_SCHEMA=0
FORMAT_RANGER_POLICY_DB=0
NEED_MINICLUSTER=0
START_IMPALA_CLUSTER=0
SNAPSHOT_FILE=
METASTORE_SNAPSHOT_FILE=
CODE_COVERAGE=0
BUILD_ASAN=0
BUILD_FE_ONLY=0
BUILD_TESTS=1
GEN_CMAKE_ONLY=0
GEN_PACKAGE=0
BUILD_RELEASE_AND_DEBUG=0
BUILD_TIDY=0
BUILD_UBSAN=0
BUILD_UBSAN_FULL=0
BUILD_TSAN=0
BUILD_TSAN_FULL=0
BUILD_DEBUG_NOOPT=0
BUILD_SHARED_LIBS=0
# Export MAKE_CMD so it is visible in scripts that invoke make, e.g. copy-udfs-udas.sh
export MAKE_CMD=make

# Defaults that can be picked up from the environment, but are overridable through the
# commandline.
: ${EXPLORATION_STRATEGY:=core}
: ${CMAKE_BUILD_TYPE:=Debug}

# parse command line options
# Note: if you add a new build type, please also add it to 'VALID_BUILD_TYPES' in
# tests/common/environ.py and set correct BUILD_OUTPUT_ROOT_DIRECTORY directory in
# be/CMakeLists.txt.
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
      BUILD_SHARED_LIBS=1
      ;;
    -notests)
      TESTS_ACTION=0
      BUILD_TESTS=0
      ;;
    -format)
      FORMAT_CLUSTER=1
      FORMAT_METASTORE=1
      FORMAT_RANGER_POLICY_DB=1
      ;;
    -format_cluster)
      FORMAT_CLUSTER=1
      ;;
    -format_metastore)
      FORMAT_METASTORE=1
      ;;
    -upgrade_metastore_db)
      UPGRADE_METASTORE_SCHEMA=1
      ;;
    -format_ranger_policy_db)
      FORMAT_RANGER_POLICY_DB=1
      ;;
    -release)
      CMAKE_BUILD_TYPE=Release
      ;;
    -release_and_debug)
      BUILD_RELEASE_AND_DEBUG=1
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
    -full_ubsan)
      BUILD_UBSAN_FULL=1
      ;;
    -tsan)
      BUILD_TSAN=1
      ;;
    -full_tsan)
      BUILD_TSAN_FULL=1
      ;;
    -debug_noopt)
      BUILD_DEBUG_NOOPT=1
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
    -v|-debug)
      echo "Running in Debug mode"
      set -x
      ;;
    -fe_only)
      BUILD_FE_ONLY=1
      ;;
    -ninja)
      MAKE_CMD=ninja
      ;;
    -cmake_only)
      GEN_CMAKE_ONLY=1
      ;;
    -package)
      GEN_PACKAGE=1
      ;;
    -help|*)
      echo "buildall.sh - Builds Impala and runs all tests."
      echo "[-noclean] : Omits cleaning all packages before building. Will not kill"\
           "running Hadoop services unless any -format* is True"
      echo "[-format] : Format the minicluster, metastore db, and ranger policy db"\
           "[Default: False]"
      echo "[-format_cluster] : Format the minicluster [Default: False]"
      echo "[-format_metastore] : Format the metastore db [Default: False]"
      echo "[-upgrade_metastore_db] : Upgrades the schema of metastore db"\
           "[Default: False]"
      echo "[-format_ranger_policy_db] : Format the Ranger policy db [Default: False]"
      echo "[-release_and_debug] : Build both release and debug binaries. Overrides "\
           "other build types [Default: false]"
      echo "[-release] : Release build [Default: debug]"
      echo "[-codecoverage] : Build with code coverage [Default: False]"
      echo "[-asan] : Address sanitizer build [Default: False]"
      echo "[-tidy] : clang-tidy build [Default: False]"
      echo "[-tsan] : Thread sanitizer build, runs with"\
           "ignore_noninstrumented_modules=1. When this flag is true, TSAN ignores"\
           "memory accesses from non-instrumented libraries. This decreases the number"\
           "of false positives, but might miss real issues. -full_tsan disables this"\
           "flag [Default: False]"
      echo "[-full_tsan] : Thread sanitizer build, runs with"\
           "ignore_noninstrumented_modules=0 (see the -tsan description for an"\
           "explanation of what this flag does) [Default: False]"
      echo "[-ubsan] : Undefined behavior sanitizer build [Default: False]"
      echo "[-full_ubsan] : Undefined behavior sanitizer build, including code generated"\
           "by cross-compilation to LLVM IR. Much slower queries than plain -ubsan"\
           "[Default: False]"
      echo "[-debug_noopt] : Debug build without optimizations applied. The regular"\
           "debug build applies basic optimizations, but even these optimizations may"\
           "impact debuggability, so this is an option to omit the optimizations."\
           "[Default: False]"
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
      echo "[-fe_only] : Build just the frontend"
      echo "[-ninja] : Use ninja instead of make"
      echo "[-cmake_only] : Generate makefiles only, instead of doing a full build"
      echo "[-package] : Generate a package for deployment."
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
  ./buildall.sh -testdata -format

  # Build and upgrade metastore schema to latest.
  ./buildall.sh -upgrade_metastore_db"
      exit 1
      ;;
    esac
  shift;
done

declare -a CMAKE_BUILD_TYPE_LIST
# Adjust CMAKE_BUILD_TYPE for code coverage, if necessary.
if [[ ${CODE_COVERAGE} -eq 1 ]]; then
  case ${CMAKE_BUILD_TYPE} in
    Debug)
      CMAKE_BUILD_TYPE_LIST+=(CODE_COVERAGE_DEBUG)
      ;;
    Release)
      CMAKE_BUILD_TYPE_LIST+=(CODE_COVERAGE_RELEASE)
      ;;
  esac
fi

# If the -release flag is specified, add RELEASE to the CMAKE_BUILD_TYPE_LIST so that
# the build exits if both -release and a sanitizer flag are specified. This does not
# apply when -codecoverage is specified because code coverage is not a distinct build
# type, it just controls if additional build flags are added.
if [[ ${CODE_COVERAGE} -ne 1 && ${CMAKE_BUILD_TYPE} = "Release" ]]; then
  CMAKE_BUILD_TYPE_LIST+=(RELEASE)
fi

if [[ ${BUILD_ASAN} -eq 1 ]]; then
  CMAKE_BUILD_TYPE_LIST+=(ADDRESS_SANITIZER)
fi
if [[ ${BUILD_TIDY} -eq 1 ]]; then
  CMAKE_BUILD_TYPE_LIST+=(TIDY)
fi
if [[ ${BUILD_UBSAN} -eq 1 ]]; then
  CMAKE_BUILD_TYPE_LIST+=(UBSAN)
fi
if [[ ${BUILD_UBSAN_FULL} -eq 1 ]]; then
  CMAKE_BUILD_TYPE_LIST+=(UBSAN_FULL)
fi
if [[ ${BUILD_TSAN} -eq 1 ]]; then
  CMAKE_BUILD_TYPE_LIST+=(TSAN)
fi
if [[ ${BUILD_TSAN_FULL} -eq 1 ]]; then
  CMAKE_BUILD_TYPE_LIST+=(TSAN_FULL)
  export TSAN_FULL=1
fi
if [[ ${BUILD_DEBUG_NOOPT} -eq 1 ]]; then
  CMAKE_BUILD_TYPE_LIST+=(DEBUG_NOOPT)
  export DEBUG_NOOPT=1
fi
if [[ -n "${CMAKE_BUILD_TYPE_LIST:+1}" ]]; then
  if [[ ${#CMAKE_BUILD_TYPE_LIST[@]} -gt 1 ]]; then
    echo "ERROR: more than one CMake build type defined: ${CMAKE_BUILD_TYPE_LIST[@]}"
    exit 1
  fi
  CMAKE_BUILD_TYPE=${CMAKE_BUILD_TYPE_LIST[0]}
fi

# If we aren't kerberized then we certainly don't need to talk about
# re-sourcing impala-config.
if [[ ${IMPALA_KERBERIZE} = "true" ]]; then
  . ${MINIKDC_ENV}
else
  NEEDS_RE_SOURCE_NOTE=0
fi

if [[ ${IMPALA_KERBERIZE} = "true" &&
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
      $FORMAT_METASTORE -eq 1 || $FORMAT_RANGER_POLICY_DB -eq 1 || -n "$SNAPSHOT_FILE" ||
      -n "$METASTORE_SNAPSHOT_FILE" || $UPGRADE_METASTORE_SCHEMA -eq 1 ]]; then
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
  if [[ "${SKIP_PYTHON_DOWNLOAD}" = true ]]; then
    echo "SKIP_PYTHON_DOWNLOAD is true, skipping python dependencies download."
  else
    echo ">>> Downloading Python dependencies"
    # Download all the Python dependencies we need before doing anything
    # of substance. Does not re-download anything that is already present.
    if ! "$IMPALA_HOME/infra/python/deps/download_requirements"; then
      echo "Warning: Unable to download Python requirements."
      echo "Warning: bootstrap_virtualenv or other Python-based tooling may fail."
    else
      echo "Finished downloading Python dependencies"
    fi
  fi

  # Populate necessary thirdparty components unless it's set to be skipped.
  if [[ "${SKIP_TOOLCHAIN_BOOTSTRAP}" = true ]]; then
    if ! [ -z "${NATIVE_TOOLCHAIN_HOME}" ]; then
      if ! [ -d "${NATIVE_TOOLCHAIN_HOME}" ]; then
        mkdir -p "${NATIVE_TOOLCHAIN_HOME}"
        pushd "${NATIVE_TOOLCHAIN_HOME}"
        git init
        git remote add toolchain "${IMPALA_TOOLCHAIN_REPO}"
        git fetch toolchain "${IMPALA_TOOLCHAIN_BRANCH}"
        # Specifying a branch avoids a large message from git about detached HEADs.
        git checkout "${IMPALA_TOOLCHAIN_COMMIT_HASH}" -b "${IMPALA_TOOLCHAIN_BRANCH}"
      else
        pushd "${NATIVE_TOOLCHAIN_HOME}"
      fi
      echo "Begin building toolchain, may need several hours, please be patient...."
      ./buildall.sh
      popd
    else
      echo "SKIP_TOOLCHAIN_BOOTSTRAP is true, skipping toolchain bootstrap."
    fi
    if [[ "${DOWNLOAD_CDH_COMPONENTS}" = true ]]; then
      echo ">>> Downloading and extracting cdh components."
      "$IMPALA_HOME/bin/bootstrap_toolchain.py"
    fi
  else
    echo ">>> Downloading and extracting toolchain dependencies."
    "$IMPALA_HOME/bin/bootstrap_toolchain.py"
    echo "Toolchain bootstrap complete."
  fi
  # Use prebuilt Hadoop native binaries for aarch64
  if [[ "$(uname -p)" = "aarch64" ]]; then
    cp $IMPALA_TOOLCHAIN_PACKAGES_HOME/hadoop-client-$IMPALA_HADOOP_CLIENT_VERSION/lib/* \
        $HADOOP_HOME/lib/native/
  fi
  if [[ "${USE_APACHE_HIVE}" = true ]]; then
    rm $HIVE_HOME/lib/guava-*jar
    cp $HADOOP_HOME/share/hadoop/hdfs/lib/guava-*.jar $HIVE_HOME/lib/
  fi
}

# Build the Impala frontend and its dependencies.
build_fe() {
  generate_cmake_files $CMAKE_BUILD_TYPE
  ${MAKE_CMD} ${IMPALA_MAKE_FLAGS} java
}

# Build all components. The build type is specified as the first argument, and the
# second argument is 0 if targets that are independent of the build type (like the
# frontend) should not be built or non-zero otherwise. E.g. to build DEBUG including
# build-type-independent artifacts.
#   build_all_components DEBUG 1
build_all_components() {
  build_type=$1
  build_independent_targets=$2
  echo ">>> Building all components"
  generate_cmake_files $build_type

  # Force regenerating the build version and timestamp (this doesn't happen automatically
  # in incremental builds).
  $IMPALA_HOME/bin/gen_build_version.py

  # If we skip specifying targets, everything we need gets built.
  local MAKE_TARGETS=""
  if [[ $BUILD_TESTS -eq 0 ]]; then
    if (( build_independent_targets )); then
      MAKE_TARGETS="notests_all_targets"
    else
      MAKE_TARGETS="notests_regular_targets"
    fi
  fi
  ${MAKE_CMD} -j${IMPALA_BUILD_THREADS:-4} ${IMPALA_MAKE_FLAGS} ${MAKE_TARGETS}
  save_coverage_data ${build_type}
}

save_coverage_data() {
  local build_type=$1
  local gcov_prefix=''
  case $build_type in
    CODE_COVERAGE_RELEASE)
      gcov_prefix='gcov_release'
      ;;
    CODE_COVERAGE_DEBUG)
      gcov_prefix='gcov_debug'
      ;;
    *)
      # Other build types don't generate coverage data
      return
      ;;
  esac
  # Copy all '.gcno' files to ${gcov_prefix}
  mkdir -p ${IMPALA_HOME}/${gcov_prefix}
  pushd ${IMPALA_HOME}
  find ./be -name '*.gcno' -exec \
       cp --parents \{\} ./${gcov_prefix} \;
  popd
}

# Called with the CMAKE_BUILD_TYPE as the first argument, e.g.
#   generate_cmake_files DEBUG
generate_cmake_files() {
  local build_type=$1
  echo ">>> Generating CMake files" "CMAKE_BUILD_TYPE=$build_type"\
       "BUILD_SHARED_LIBS=$BUILD_SHARED_LIBS" "MAKE_CMD=$MAKE_CMD"
  # Remove cache to ensure that any changes to cmake arguments take effect.
  rm -f ./CMakeCache.txt
  local CMAKE_ARGS=(-DCMAKE_BUILD_TYPE=${build_type})
  if [[ $BUILD_SHARED_LIBS -eq 1 ]]; then
    CMAKE_ARGS+=(-DBUILD_SHARED_LIBS=ON)
  fi
  if [[ "$BUILD_TESTS" -eq 0 && "$GEN_PACKAGE" -eq 1 ]]; then
    CMAKE_ARGS+=(-DBUILD_WITH_NO_TESTS=ON)
  fi
  if [[ "${MAKE_CMD}" = "ninja" ]]; then
    CMAKE_ARGS+=(-GNinja)
  fi
  if [[ ("$build_type" == "ADDRESS_SANITIZER") \
            || ("$build_type" == "TIDY") \
            || ("$build_type" == "UBSAN") \
            || ("$build_type" == "UBSAN_FULL") \
            || ("$build_type" == "TSAN") \
            || ("$build_type" == "TSAN_FULL") ]]; then
    CMAKE_ARGS+=(-DCMAKE_TOOLCHAIN_FILE=$IMPALA_HOME/cmake_modules/clang_toolchain.cmake)
  else
    CMAKE_ARGS+=(-DCMAKE_TOOLCHAIN_FILE=$IMPALA_HOME/cmake_modules/toolchain.cmake)
  fi

  # ARM64's L3 cacheline size is different according to CPU vendor's implementations of
  # architecture. so here we will let user decide this value.
  # If user defined CACHELINESIZE_AARCH64 in impala-config-local.sh, then we will use that
  # value, if user did not define it, then we will get the value from OS, if fail, then
  # we will use the default value 64.
  if [[ "$(uname -p)" = "aarch64" &&  "$(uname -s)" = "Linux" ]]; then
    local cachelinesize=$(cat /sys/devices/system/cpu/cpu0/cache/index3/coherency_line_size)
    if [[ $cachelinesize -gt 0 ]]; then
      CACHELINESIZE_AARCH64=${CACHELINESIZE_AARCH64-$cachelinesize}
    else
      CACHELINESIZE_AARCH64=${CACHELINESIZE_AARCH64-64}
    fi
    echo "CACHELINESIZE_AARCH64:$CACHELINESIZE_AARCH64"
    CMAKE_ARGS+=(-DCACHELINESIZE_AARCH64=$CACHELINESIZE_AARCH64)
  fi
  if [[ "$GEN_PACKAGE" -eq 1 ]]; then
    CMAKE_ARGS+=(-DBUILD_PACKAGES=ON)
  fi

  cmake . ${CMAKE_ARGS[@]}
}

# Do any configuration of the test cluster required by the script arguments.
# Kills any cluster processes that will need to be restarted to pick up new
# configurations or the new build.
reconfigure_test_cluster() {
  # Stop any running Impala services.
  "${IMPALA_HOME}/bin/start-impala-cluster.py" --kill --force

  if [[ "$FORMAT_METASTORE" -eq 1 || "$FORMAT_CLUSTER" -eq 1 ||
        "$FORMAT_RANGER_POLICY_DB" -eq 1 || -n "$METASTORE_SNAPSHOT_FILE" ||
        "$UPGRADE_METASTORE_SCHEMA" -eq 1 ]]
  then
    # Kill any processes that may be accessing postgres metastore. To be safe, this is
    # done before we make any changes to the config files.
    "${IMPALA_HOME}/testdata/bin/kill-all.sh" || true
  fi

  local CREATE_TEST_CONFIG_ARGS=""
  if [[ "$FORMAT_RANGER_POLICY_DB" -eq 1 ]]; then
    CREATE_TEST_CONFIG_ARGS+=" -create_ranger_policy_db"
  fi

  if [[ "$FORMAT_METASTORE" -eq 1 && -z "$METASTORE_SNAPSHOT_FILE" ]]; then
    CREATE_TEST_CONFIG_ARGS+=" -create_metastore"
  fi

  if [[ "$UPGRADE_METASTORE_SCHEMA" -eq 1 ]]; then
    CREATE_TEST_CONFIG_ARGS+=" -upgrade_metastore_db"
  fi

  # Generate the Hadoop configs needed by Impala
  "${IMPALA_HOME}/bin/create-test-configuration.sh" ${CREATE_TEST_CONFIG_ARGS}
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
  echo ">>> Loading test data"
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

# Create .cdp file that contains the CDP_BUILD_NUMBER. If the content of the files
# are different than the ones in the environment variable, append -U into
# IMPALA_MAVEN_OPTION to force Maven to update its local cache.
# TODO: Look into removing this. The CDP components do not use SNAPSHOT versions.
CDP_FILE="${IMPALA_HOME}/.cdp"
if [[ ! -f ${CDP_FILE} || $(cat ${CDP_FILE}) != ${CDP_BUILD_NUMBER} ]]; then
  export IMPALA_MAVEN_OPTIONS="${IMPALA_MAVEN_OPTIONS} -U"
fi
echo "${CDP_BUILD_NUMBER}" > ${CDP_FILE}

if [[ "$BUILD_FE_ONLY" -eq 1 ]]; then
  build_fe
  exit 0
fi

if [[ "$GEN_CMAKE_ONLY" -eq 1 ]]; then
  generate_cmake_files $CMAKE_BUILD_TYPE
  exit 0
fi
if [[ "$BUILD_RELEASE_AND_DEBUG" -eq 1 ]]; then
  # Build the standard release and debug builds. We can't do this for arbitrary build
  # types because many build types reuse the same be/build/debug and be/build/release
  # trees.
  CMAKE_BUILD_TYPE=RELEASE
  if [[ ${CODE_COVERAGE} -eq 1 ]]; then
    CMAKE_BUILD_TYPE=CODE_COVERAGE_RELEASE
  fi
  build_all_components ${CMAKE_BUILD_TYPE} 1
  # Avoid rebuilding targets that are independent of the build type.
  CMAKE_BUILD_TYPE=DEBUG
  if [[ ${CODE_COVERAGE} -eq 1 ]]; then
    CMAKE_BUILD_TYPE=CODE_COVERAGE_DEBUG
  fi
  build_all_components ${CMAKE_BUILD_TYPE} 0
else
  build_all_components $CMAKE_BUILD_TYPE 1
fi

if [[ "$GEN_PACKAGE" -eq 1 ]]; then
  ${MAKE_CMD} -j${IMPALA_BUILD_THREADS:-4} package
fi

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
