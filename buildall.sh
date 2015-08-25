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

# run buildall.sh -help to see options

ROOT=`dirname "$0"`
ROOT=`cd "$ROOT"; pwd`

# Grab this *before* we source impala-config.sh to see if the caller has
# kerberized environment variables already or not.
NEEDS_RE_SOURCE_NOTE=1
if [ ! -z "${MINIKDC_REALM}" ]; then
  NEEDS_RE_SOURCE_NOTE=0
fi

export IMPALA_HOME=$ROOT
. "$ROOT"/bin/impala-config.sh
if [ $? = 1 ]; then
  echo "Bad configuration, aborting buildall."
  exit 1
fi

# Defaults that are only changable via the commandline.
CLEAN_ACTION=1
TESTDATA_ACTION=0
TESTS_ACTION=1
FORMAT_CLUSTER=0
FORMAT_METASTORE=0
IMPALA_KERBERIZE=0
SNAPSHOT_FILE=
METASTORE_SNAPSHOT_FILE=
MAKE_IMPALA_ARGS=""

# Defaults that can be picked up from the environment, but are overridable through the
# commandline.
: ${EXPLORATION_STRATEGY:=core}
: ${TARGET_BUILD_TYPE:=Debug}

# Exit on reference to uninitialized variable
set -u

# Exit on non-zero return value
set -e

# parse command line options
# TODO: We have to change this to use getopts, or something more sensible.
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
    -build_shared_libs)
      ;;
    -so)
      MAKE_IMPALA_ARGS="${MAKE_IMPALA_ARGS} -build_shared_libs"
      ;;
    -notests)
      TESTS_ACTION=0
      MAKE_IMPALA_ARGS="${MAKE_IMPALA_ARGS} -notests"
      ;;
    -format)
      FORMAT_CLUSTER=1
      FORMAT_METASTORE=1
      ;;
    -format_cluster)
      FORMAT_CLUSTER=1
      ;;
    -format_metastore)
      FORMAT_METASTORE=1
      ;;
    -codecoverage_debug)
      TARGET_BUILD_TYPE=CODE_COVERAGE_DEBUG
      ;;
    -codecoverage_release)
      TARGET_BUILD_TYPE=CODE_COVERAGE_RELEASE
      ;;
    -asan)
      TARGET_BUILD_TYPE=ADDRESS_SANITIZER
      ;;
    -release)
      TARGET_BUILD_TYPE=Release
      ;;
    -testpairwise)
      EXPLORATION_STRATEGY=pairwise
      ;;
    -testexhaustive)
      EXPLORATION_STRATEGY=exhaustive
      ;;
    -snapshot_file)
      SNAPSHOT_FILE=${2-}
      if [ ! -f $SNAPSHOT_FILE ]; then
        echo "-snapshot_file does not exist: $SNAPSHOT_FILE"
        exit 1;
      fi
      TESTDATA_ACTION=1
      # Get the full path.
      SNAPSHOT_FILE=$(readlink -f $SNAPSHOT_FILE)
      shift;
      ;;
    -metastore_snapshot_file)
      METASTORE_SNAPSHOT_FILE=${2-}
      if [ ! -f $METASTORE_SNAPSHOT_FILE ]; then
        echo "-metastore_snapshot_file does not exist: $METASTORE_SNAPSHOT_FILE"
        exit 1;
      fi
      TESTDATA_ACTION=1
      # Get the full path.
      METASTORE_SNAPSHOT_FILE=$(readlink -f $METASTORE_SNAPSHOT_FILE)
      shift;
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
    -help|*)
      echo "buildall.sh - Builds Impala and runs all tests."
      echo "[-noclean] : Omits cleaning all packages before building. Will not kill"\
           "running Hadoop services unless any -format* is True"
      echo "[-format] : Format the minicluster and metastore db [Default: False]"
      echo "[-format_cluster] : Format the minicluster [Default: False]"
      echo "[-format_metastore] : Format the metastore db [Default: False]"
      echo "[-codecoverage_release] : Release code coverage build"
      echo "[-codecoverage_debug] : Debug code coverage build"
      echo "[-asan] : Build with address sanitizer"
      echo "[-skiptests] : Skips execution of all tests"
      echo "[-notests] : Skips building and execution of all tests"
      echo "[-testpairwise] : Sun tests in 'pairwise' mode (increases"\
           "test execution time)"
      echo "[-testexhaustive] : Run tests in 'exhaustive' mode (significantly increases"\
           "test execution time)"
      echo "[-testdata] : Loads test data. Implied as true if -snapshot_file is "\
           "specified. If -snapshot_file is not specified, data will be regenerated."
      echo "[-snapshot_file <file name>] : Load test data from a snapshot file"
      echo "[-metastore_snapshot_file <file_name>]: Load the hive metastore snapshot"
      echo "[-so|-build_shared_libs] : Dynamically link executables (default is static)"
      echo "[-kerberize] : Enable kerberos on the cluster"
      echo "-----------------------------------------------------------------------------
Examples of common tasks:

  # Build and run all tests
  ./buildall.sh

  # Build and skip tests
  ./buildall.sh -skiptests

  # Incrementally rebuild and skip tests. Keeps existing Hadoop services running.
  ./buildall.sh -skiptests -noclean

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

# If we aren't kerberized then we certainly don't need to talk about
# re-sourcing impala-config.
if [ ${IMPALA_KERBERIZE} -eq 0 ]; then
  NEEDS_RE_SOURCE_NOTE=0
fi

# Loading data on a filesystem other than fs.defaultFS is not supported.
if [[ -z $METASTORE_SNAPSHOT_FILE && "${TARGET_FILESYSTEM}" != "hdfs" &&
      $TESTDATA_ACTION -eq 1 ]]; then
  echo "The metastore snapshot is required for loading data into ${TARGET_FILESYSTEM}"
  echo "Use the -metastore_snapshot_file command line paramater."
  exit 1
fi

# Only build thirdparty if no toolchain is set
if [[ -z $IMPALA_TOOLCHAIN ]]; then
  # Sanity check that thirdparty is built.
  if [ ! -e $IMPALA_HOME/thirdparty/gflags-${IMPALA_GFLAGS_VERSION}/libgflags.la ]
  then
    echo "Couldn't find thirdparty build files.  Building thirdparty."
    $IMPALA_HOME/bin/build_thirdparty.sh $([ ${CLEAN_ACTION} -eq 0 ] && echo '-noclean')
  fi
fi

if [ -e $HADOOP_LZO/build/native/Linux-*-*/lib/libgplcompression.so ]
then
  cp $HADOOP_LZO/build/native/Linux-*-*/lib/libgplcompression.* \
    $IMPALA_HOME/thirdparty/hadoop-${IMPALA_HADOOP_VERSION}/lib/native/
else
  echo "No hadoop-lzo found"
fi

# Stop any running Impala services.
${IMPALA_HOME}/bin/start-impala-cluster.py --kill --force

if [[ $CLEAN_ACTION -eq 1 || $FORMAT_METASTORE -eq 1 || $FORMAT_CLUSTER -eq 1 ||
      -n $METASTORE_SNAPSHOT_FILE ]]
then
  # Kill any processes that may be accessing postgres metastore. To be safe, this is done
  # before we make any changes to the config files.
  set +e
  ${IMPALA_HOME}/testdata/bin/kill-all.sh
  set -e
fi

# option to clean everything first
if [ $CLEAN_ACTION -eq 1 ]
then
  # Stop the minikdc if needed.
  if ${CLUSTER_DIR}/admin is_kerberized; then
    ${IMPALA_HOME}/testdata/bin/minikdc.sh stop
  fi

  # clean the external data source project
  pushd ${IMPALA_HOME}/ext-data-source
  rm -rf api/generated-sources/*
  mvn clean
  popd

  # clean fe
  # don't use git clean because we need to retain Eclipse conf files
  pushd $IMPALA_FE_DIR
  rm -rf target
  rm -f src/test/resources/{core,hbase,hive}-site.xml
  rm -rf generated-sources/*
  rm -rf ${IMPALA_TEST_CLUSTER_LOG_DIR}/*
  popd

  # clean be
  pushd $IMPALA_HOME/be
  # remove everything listed in .gitignore
  git clean -Xdf
  popd

  # clean shell build artifacts
  pushd $IMPALA_HOME/shell
  # remove everything listed in .gitignore
  git clean -Xdf
  popd

  # Clean stale .pyc, .pyo files and __pycache__ directories.
  pushd ${IMPALA_HOME}
  find . -type f -name "*.py[co]" -delete
  find . -type d -name "__pycache__" -delete
  popd

  # clean llvm
  rm -f $IMPALA_HOME/llvm-ir/impala*.ll
  rm -f $IMPALA_HOME/be/generated-sources/impala-ir/*
fi

# Generate the Hadoop configs needed by Impala
if [[ $FORMAT_METASTORE -eq 1 && -z $METASTORE_SNAPSHOT_FILE ]]; then
  ${IMPALA_HOME}/bin/create-test-configuration.sh -create_metastore
else
  ${IMPALA_HOME}/bin/create-test-configuration.sh
fi

# If a metastore snapshot exists, load it.
if [ $METASTORE_SNAPSHOT_FILE ]; then
  echo "Loading metastore snapshot"
  ${IMPALA_HOME}/testdata/bin/load-metastore-snapshot.sh $METASTORE_SNAPSHOT_FILE
fi

# build common and backend
MAKE_IMPALA_ARGS="${MAKE_IMPALA_ARGS} -build_type=${TARGET_BUILD_TYPE}"
echo "Calling make_impala.sh ${MAKE_IMPALA_ARGS}"
$IMPALA_HOME/bin/make_impala.sh ${MAKE_IMPALA_ARGS}

if [ -e $IMPALA_LZO ]
then
  pushd $IMPALA_LZO
  if [[ ! -z $IMPALA_TOOLCHAIN ]]; then
    cmake -DCMAKE_TOOLCHAIN_FILE=./cmake_modules/toolchain.cmake
  else
    cmake .
  fi
  make
  popd
fi

# build the external data source API
pushd ${IMPALA_HOME}/ext-data-source
mvn install -DskipTests
popd

# build frontend and copy dependencies
pushd ${IMPALA_FE_DIR}
# on jenkins runs, resolve dependencies quietly to avoid log spew
if [ "${USER}" == "jenkins" ]; then
  echo "Quietly resolving FE dependencies."
  mvn -q dependency:resolve
fi
mvn package -DskipTests=true
popd


# Build the shell tarball
echo "Creating shell tarball"
${IMPALA_HOME}/shell/make_shell_tarball.sh

echo "Creating test tarball"
${IMPALA_HOME}/tests/make_test_tarball.sh

# Create subdirectories for the test and data loading impalad logs.
mkdir -p ${IMPALA_TEST_CLUSTER_LOG_DIR}/query_tests
mkdir -p ${IMPALA_TEST_CLUSTER_LOG_DIR}/fe_tests
mkdir -p ${IMPALA_TEST_CLUSTER_LOG_DIR}/data_loading

if [ $FORMAT_CLUSTER -eq 1 ]; then
  $IMPALA_HOME/testdata/bin/run-all.sh -format
elif [ $TESTDATA_ACTION -eq 1 ] || [ $TESTS_ACTION -eq 1 ]; then
  $IMPALA_HOME/testdata/bin/run-all.sh
fi

#
# KERBEROS TODO
# There is still work to be done for kerberos.
# - The hive metastore needs to be kerberized
# - If the user principal is "impala/localhost", MR jobs complain that user
#   "impala" is not user ${USER}.  But if the principal is ${USER}/localhost,
#   the impala daemons change it to impala/localhost in
#   KerberosAuthProvider::RunKinit() - and there may be other difficulties
#   down the road with getting all the permissions correct.
# - Futher Beeline -> HiveServer2 -> HBase|MapReduce combo issues
# - Getting farther down the testing path, it's likely more issues will turn up
# - Further extensive testing
#
if [ ${IMPALA_KERBERIZE} -eq 1 ]; then
  if [ ${TESTDATA_ACTION} -eq 1 -o ${TESTS_ACTION} -eq 1 ]; then
    echo "At this time we only support cluster creation and impala daemon"
    echo "bringup in kerberized mode.  Data won't be loaded, and tests"
    echo "won't be run.  The impala daemons will be started."
    TESTDATA_ACTION=0
    TESTS_ACTION=0
    ${IMPALA_HOME}/bin/start-impala-cluster.py
  fi
fi
# END KERBEROS TODO

#
# Don't try to run tests without data!
#
TESTWH_ITEMS=`hadoop fs -ls ${FILESYSTEM_PREFIX}/test-warehouse 2> /dev/null | \
    grep test-warehouse |wc -l`
if [ ${TESTS_ACTION} -eq 1 -a \
     ${TESTDATA_ACTION} -eq 0 -a \
     ${TESTWH_ITEMS} -lt 5 ]; then
  set +x
  echo "You just asked buildall to run tests, but did not supply any data."
  echo "Running tests without data doesn't work. Exiting now."
  exit 1
fi

if [ $TESTDATA_ACTION -eq 1 ]; then
  # Create testdata.
  $IMPALA_HOME/bin/create_testdata.sh
  cd $ROOT
  # We have 4 cases:
  # - test-warehouse and metastore snapshots exists.
  # - Only the test-warehouse snapshot exists.
  # - Only the metastore snapshot exists.
  # - Neither of them exist.
  CREATE_LOAD_DATA_ARGS=""
  if [[ $SNAPSHOT_FILE  && $METASTORE_SNAPSHOT_FILE ]]; then
    CREATE_LOAD_DATA_ARGS="-snapshot_file ${SNAPSHOT_FILE} -skip_metadata_load"
  elif [[ $SNAPSHOT_FILE && -z $METASTORE_SNAPSHOT_FILE ]]; then
    CREATE_LOAD_DATA_ARGS="-snapshot_file ${SNAPSHOT_FILE}"
  elif [[ -z $SNAPSHOT_FILE && $METASTORE_SNAPSHOT_FILE ]]; then
    CREATE_LOAD_DATA_ARGS="-skip_metadata_load -skip_snapshot_load"
  fi
  yes | ${IMPALA_HOME}/testdata/bin/create-load-data.sh ${CREATE_LOAD_DATA_ARGS}
fi

if [ $TESTS_ACTION -eq 1 ]; then
  ${IMPALA_HOME}/bin/run-all-tests.sh -e $EXPLORATION_STRATEGY
fi

# Generate list of files for Cscope to index
$IMPALA_HOME/bin/gen-cscope.sh

if [ ${NEEDS_RE_SOURCE_NOTE} -eq 1 ]; then
  echo
  echo "You have just successfully created a kerberized cluster."
  echo "Congratulations!  Communication with this cluster requires"
  echo "the setting of certain environment variables.  These"
  echo "environment variables weren't available before the cluster"
  echo "was created.  To pick them up, please source impala-config.sh:"
  echo
  echo "   . ${IMPALA_HOME}/bin/impala-config.sh"
  echo
fi
