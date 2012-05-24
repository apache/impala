#!/usr/bin/env bash
# Copyright (c) 2012 Cloudera, Inc. All rights reserved.

# run buildall.sh -help to see options

root=`dirname "$0"`
root=`cd "$root"; pwd`

export IMPALA_HOME=$root
export METASTORE_DB=`basename $root | sed -e "s/\\./_/g" | sed -e "s/[.-]/_/g"`

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
    -codecoverage)
      TARGET_BUILD_TYPE=CODE_COVERAGE
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
  echo "using mysql for metastore"
  if [ $FORMAT_CLUSTER -eq 1 ]; then
    echo "drop database hive_$METASTORE_DB;" | mysql --user=hiveuser --password=password
  fi
  perl -wpl -e 's/\$\{([^}]+)\}/defined $ENV{$1} ? $ENV{$1} : $&/eg' \
    mysql-hive-site.xml.template > hive-site.xml
fi

# Generate hbase-site.xml from template via env var substitution
# TODO: Throw an error if the template references an undefined environment variable
cd ${IMPALA_FE_DIR}/src/test/resources
perl -wpl -e 's/\$\{([^}]+)\}/defined $ENV{$1} ? $ENV{$1} : $&/eg' \
hbase-site.xml.template > hbase-site.xml

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

# Generate list of files for Cscope to index
$IMPALA_HOME/bin/gen-cscope.sh
