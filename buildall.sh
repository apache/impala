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

root=`dirname "$0"`
root=`cd "$root"; pwd`

export IMPALA_HOME=$root
. "$root"/bin/impala-config.sh

clean_action=1
testdata_action=1
tests_action=1

FORMAT_CLUSTER=1
TARGET_BUILD_TYPE=Debug
EXPLORATION_STRATEGY=core

# Exit on reference to uninitialized variable
set -u

# parse command line options
for ARG in $*
do
  case "$ARG" in
    -noclean)
      clean_action=0
      ;;
    -notestdata)
      testdata_action=0
      ;;
    -skiptests)
      tests_action=0
      ;;
    -noformat)
      FORMAT_CLUSTER=0
      ;;
    -codecoverage_debug)
      TARGET_BUILD_TYPE=CODE_COVERAGE_DEBUG
      ;;
    -codecoverage_release)
      TARGET_BUILD_TYPE=CODE_COVERAGE_RELEASE
      ;;
    -testpairwise)
      EXPLORATION_STRATEGY=pairwise
      ;;
    -testexhaustive)
      EXPLORATION_STRATEGY=exhaustive
      ;;
    -help|*)
      echo "buildall.sh [-noclean] [-notestdata] [-noformat] [-codecoverage]"\
           "[-skiptests] [-testexhaustive]"
      echo "[-noclean] : omits cleaning all packages before building"
      echo "[-notestdata] : omits recreating the metastore and loading test data"
      echo "[-noformat] : prevents the minicluster from formatting its data directories,"\
           "and skips the data load step"
      echo "[-codecoverage] : build with 'gcov' code coverage instrumentation at the"\
           "cost of performance"
      echo "[-skiptests] : skips execution of all tests"
      echo "[-testpairwise] : run tests in 'pairwise' mode (increases"\
           "test execution time)"
      echo "[-testexhaustive] : run tests in 'exhaustive' mode (significantly increases"\
           "test execution time)"
      exit 1
      ;;
  esac
done

# Sanity check that thirdparty is built.
if [ ! -e $IMPALA_HOME/thirdparty/gflags-${IMPALA_GFLAGS_VERSION}/libgflags.la ]
then
  echo "Couldn't find thirdparty build files.  Building thirdparty."
  $IMPALA_HOME/bin/build_thirdparty.sh $*
fi

if [ -e $HADOOP_LZO/build/native/Linux-*-*/lib/libgplcompression.so ]
then
  cp $HADOOP_LZO/build/native/Linux-*-*/lib/libgplcompression.* \
    $IMPALA_HOME/thirdparty/hadoop-*/lib/native/
else
  echo "No hadoop-lzo found"
fi

# option to clean everything first
if [ $clean_action -eq 1 ]
then
  # clean selected files from the root
  rm -f CMakeCache.txt

  # clean fe
  # don't use git clean because we need to retain Eclipse conf files
  cd $IMPALA_FE_DIR
  rm -rf target
  rm -f src/test/resources/{core,hbase,hive}-site.xml
  rm -rf src/generated-sources/*

  # clean be
  cd $IMPALA_HOME/be
  # remove everything listed in .gitignore
  git clean -Xdf

  # clean llvm
  rm $IMPALA_HOME/llvm-ir/impala*.ll

  # Cleanup the version.info file so it will be regenerated for the next build.
  rm -f $IMPALA_HOME/bin/version.info
fi

# Generate the Hadoop configs needed by Impala
if [ $FORMAT_CLUSTER -eq 1 ]; then
  ${IMPALA_HOME}/bin/create-test-configuration.sh -create_metastore
else
  ${IMPALA_HOME}/bin/create-test-configuration.sh
fi

# Exit on non-true return value
set -e

# build common and backend
cd $IMPALA_HOME
cmake -DCMAKE_BUILD_TYPE=$TARGET_BUILD_TYPE .
cd $IMPALA_HOME/common/function-registry
make
cd $IMPALA_HOME/common/thrift
make
cd $IMPALA_BE_DIR
make -j4

if [ -e $IMPALA_LZO ]
then
  (cd $IMPALA_LZO; cmake .; make)
fi

# Get Hadoop dependencies onto the classpath
cd $IMPALA_FE_DIR
mvn dependency:copy-dependencies

# build frontend
# Package first since any test failure will prevent the package phase from completing.
# We need to do this before loading data so that hive can see the trevni input/output
# classes.
mvn package -DskipTests=true

if [ $testdata_action -eq 1 ]
then
  # create test data
  cd $IMPALA_HOME/testdata
  $IMPALA_HOME/bin/create_testdata.sh
  cd $IMPALA_FE_DIR
  if [ $FORMAT_CLUSTER -eq 1 ]; then
    mvn -Pload-testdata process-test-resources -Dcluster.format
  else
    mvn -Pload-testdata process-test-resources
  fi
fi

if [ $tests_action -eq 1 ]
then
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
    # tests to the new framework.
    cd $IMPALA_FE_DIR
    mvn test

    # Run backend tests
    ${IMPALA_HOME}/bin/run-backend-tests.sh
fi

# Build the shell tarball
echo "Creating shell tarball"
${IMPALA_HOME}/shell/make_shell_tarball.sh

# Generate list of files for Cscope to index
$IMPALA_HOME/bin/gen-cscope.sh
