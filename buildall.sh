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

export IMPALA_HOME=$ROOT
. "$ROOT"/bin/impala-config.sh

CLEAN_ACTION=1
TESTDATA_ACTION=0
TESTS_ACTION=1
FORMAT_CLUSTER=0
FORMAT_METASTORE=0
TARGET_BUILD_TYPE=Debug
EXPLORATION_STRATEGY=core
SNAPSHOT_FILE=
MAKE_IMPALA_ARGS=""

# Exit on reference to uninitialized variable
set -u

# Exit on non-zero return value
set -e

# Always run in debug mode
set -x

# parse command line options
for ARG in $*
do
  # Interpret this argument as a snapshot file name
  if [ "$SNAPSHOT_FILE" = "UNDEFINED" ]; then
    SNAPSHOT_FILE="$ARG"
    continue;
  fi

  case "$ARG" in
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
      shift
      ;;
    -notests)
      TESTS_ACTION=0
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
    -testpairwise)
      EXPLORATION_STRATEGY=pairwise
      ;;
    -testexhaustive)
      EXPLORATION_STRATEGY=exhaustive
      ;;
    -snapshot_file)
      SNAPSHOT_FILE="UNDEFINED"
      TESTDATA_ACTION=1
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
      echo "[-so|-build_shared_libs] : Dynamically link executables (default is static)"
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

  # Build, generate, and incrementally load test data without formatting the mini-cluster
  # (reuses existing data in HDFS if it exists). Can be faster than loading from a
  # snapshot.
  ./buildall.sh -testdata

  # Build, format mini-cluster and metastore, load all test data, run tests
  ./buildall.sh -testdata -format"
      exit 1
      ;;
  esac
done

if [ "$SNAPSHOT_FILE" = "UNDEFINED" ]; then
  echo "-snapshot_file flag requires a snapshot filename argument"
  exit 1
elif [ "$SNAPSHOT_FILE" != "" ] &&  [ ! -e $SNAPSHOT_FILE ]; then
  echo "Snapshot file: ${SNAPSHOT_FILE} does not exist."
  exit 1
fi

# Sanity check that thirdparty is built.
if [ ! -e $IMPALA_HOME/thirdparty/gflags-${IMPALA_GFLAGS_VERSION}/libgflags.la ]
then
  echo "Couldn't find thirdparty build files.  Building thirdparty."
  $IMPALA_HOME/bin/build_thirdparty.sh $([ ${CLEAN_ACTION} -eq 0 ] && echo '-noclean')
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

if [ $CLEAN_ACTION -eq 1 ] || [ $FORMAT_METASTORE -eq 1 ] || [ $FORMAT_CLUSTER -eq 1 ]
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
  # clean the external data source project
  cd ${IMPALA_HOME}/ext-data-source
  rm -rf api/generated-sources/*
  mvn clean

  # clean fe
  # don't use git clean because we need to retain Eclipse conf files
  cd $IMPALA_FE_DIR
  rm -rf target
  rm -f src/test/resources/{core,hbase,hive}-site.xml
  rm -rf generated-sources/*
  rm -rf ${IMPALA_TEST_CLUSTER_LOG_DIR}/*

  # clean be
  cd $IMPALA_HOME/be
  # remove everything listed in .gitignore
  git clean -Xdf

  # clean shell build artifacts
  cd $IMPALA_HOME/shell
  # remove everything listed in .gitignore
  git clean -Xdf

  # clean llvm
  rm -f $IMPALA_HOME/llvm-ir/impala*.ll
  rm -f $IMPALA_HOME/be/generated-sources/impala-ir/*
fi

# Generate the Hadoop configs needed by Impala
if [ $FORMAT_METASTORE -eq 1 ]; then
  ${IMPALA_HOME}/bin/create-test-configuration.sh -create_metastore
else
  ${IMPALA_HOME}/bin/create-test-configuration.sh
fi

# build common and backend
MAKE_IMPALA_ARGS="${MAKE_IMPALA_ARGS} -build_type=${TARGET_BUILD_TYPE} $*"
echo "Calling make_impala.sh ${MAKE_IMPALA_ARGS}"
$IMPALA_HOME/bin/make_impala.sh ${MAKE_IMPALA_ARGS}

if [ -e $IMPALA_LZO ]
then
  (cd $IMPALA_LZO; cmake .; make)
fi

# build the external data source API
cd ${IMPALA_HOME}/ext-data-source
mvn install -DskipTests

# build frontend and copy dependencies
cd ${IMPALA_FE_DIR}
mvn package -DskipTests=true

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

if [ $TESTDATA_ACTION -eq 1 ]
then
  # create and load test data
  $IMPALA_HOME/bin/create_testdata.sh

  cd $ROOT
  if [ "$SNAPSHOT_FILE" != "" ]
  then
    yes | ${IMPALA_HOME}/testdata/bin/create-load-data.sh $SNAPSHOT_FILE
  else
    ${IMPALA_HOME}/testdata/bin/create-load-data.sh
  fi
fi

if [ $TESTS_ACTION -eq 1 ]
then
    ${IMPALA_HOME}/bin/run-all-tests.sh -e $EXPLORATION_STRATEGY
fi

# Generate list of files for Cscope to index
$IMPALA_HOME/bin/gen-cscope.sh
