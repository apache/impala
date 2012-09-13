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
# Create unique metastore DB name based on the directory we're in.  The result
# must be lower case.
METASTORE_DB=`basename $root | sed -e "s/\\./_/g" | sed -e "s/[.-]/_/g"`
export METASTORE_DB=`echo $METASTORE_DB | tr '[A-Z]' '[a-z]'`
export CURRENT_USER=`whoami`

. "$root"/bin/impala-config.sh

clean_action=1
testdata_action=1
tests_action=1
metastore_is_derby=0

FORMAT_CLUSTER=1
TARGET_BUILD_TYPE=Debug
TEST_EXECUTION_MODE=reduced

if [[ ${METASTORE_IS_DERBY} ]]; then
  metastore_is_derby=1
fi

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
    -testexhaustive)
      TEST_EXECUTION_MODE=exhaustive
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
      echo "[-testexhaustive] : run tests in 'exhaustive' mode (significantly increases"\
           "test execution time)"
      exit 1
      ;;
  esac
done

# Sanity check that thirdparty is built.
if [ ! -e $IMPALA_HOME/thirdparty/gflags-1.5/libgflags.la ]
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
  rm -f src/test/resources/hbase-site.xml
  rm -f src/test/resources/hive-site.xml
  rm -rf src/generated-sources/*
  rm -f derby.log

  # clean be
  cd $IMPALA_HOME/be
  # remove everything listed in .gitignore
  git clean -Xdf

  # clean llvm
  rm $IMPALA_HOME/llvm-ir/impala*.ll

  # Cleanup the version.info file so it will be regenerated for the next build.
  rm -f $IMPALA_HOME/bin/version.info
fi

# Generate hive-site.xml from template via env var substitution
# TODO: Throw an error if the template references an undefined environment variable
cd ${IMPALA_FE_DIR}/src/test/resources
if [ $metastore_is_derby -eq 1 ]
then
  echo "using derby for metastore"
  perl -wpl -e 's/\$\{([^}]+)\}/defined $ENV{$1} ? $ENV{$1} : $&/eg' \
    derby-hive-site.xml.template > hive-site.xml
else
  echo "using postgresql for metastore"
  if [ $FORMAT_CLUSTER -eq 1 ]; then
    echo "Creating postgresql databases"
    dropdb -U hiveuser hive_$METASTORE_DB
    createdb -U hiveuser hive_$METASTORE_DB
    # TODO: Change location of the sql file when Hive release contains this script
    psql -U hiveuser -d hive_$METASTORE_DB \
        -f ${IMPALA_HOME}/bin/hive-schema-0.9.0.postgres.sql
  fi
  perl -wpl -e 's/\$\{([^}]+)\}/defined $ENV{$1} ? $ENV{$1} : $&/eg' \
    postgresql-hive-site.xml.template > hive-site.xml
fi

# Generate hbase-site.xml from template via env var substitution
# TODO: Throw an error if the template references an undefined environment variable
cd ${IMPALA_FE_DIR}/src/test/resources
perl -wpl -e 's/\$\{([^}]+)\}/defined $ENV{$1} ? $ENV{$1} : $&/eg' \
hbase-site.xml.template > hbase-site.xml

# Update dfs.block.local-path-access.user with the current user
perl -wpl -e 's/\$\{([^}]+)\}/defined $ENV{$1} ? $ENV{$1} : $&/eg' \
core-site.xml.template > core-site.xml
# Exit on non-true return value
set -e

# cleanup FE process
$IMPALA_HOME/bin/clean-fe-processes.py

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
    # also run frontend tests
    mvn test -DtestExecutionMode=$TEST_EXECUTION_MODE
fi

if [ $tests_action -eq 1 ]
then
  cd $IMPALA_FE_DIR
  if [ $metastore_is_derby -eq 1 ]
  then
    echo "Cleaning up locks from previous test runs for derby for metastore"
    ls target/test_metastore_db/*.lck
    rm -f target/test_metastore_db/{db,dbex}.lck
  fi
  mvn exec:java -Dexec.mainClass=com.cloudera.impala.testutil.PlanService \
              -Dexec.classpathScope=test &
  PID=$!
  ${IMPALA_HOME}/bin/run-backend-tests.sh
  kill $PID
fi

# Build the shell tarball
echo "Creating shell tarball"
${IMPALA_HOME}/shell/make_shell_tarball.sh

# Generate list of files for Cscope to index
$IMPALA_HOME/bin/gen-cscope.sh


