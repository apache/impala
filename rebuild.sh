#!/usr/bin/env bash
# Copyright (c) 2011 Cloudera, Inc. All rights reserved.

# run rebuild.sh -help to see options

root=`dirname "$0"`
root=`cd "$root"; pwd`

export IMPALA_HOME=$root
export METASTORE_DB=`basename $root | sed -e "s/\\./_/g" | sed -e "s/[.-]/_/g"`

. "$root"/bin/impala-config.sh

# Exit on non-true return value
set -e
# Exit on reference to unitialized variable
set -u

tests_action=0

# parse command line options
for ARG in $*
do
  case "$ARG" in
    -runtests)
      tests_action=1
      ;;
    -help)
      echo "buildall.sh [-runtests]"
      echo "[-runtests] : run fe and be tests"
      exit
      ;;
  esac
done

# cleanup FE process
$IMPALA_HOME/bin/clean-fe-processes.py

# build common and backend
cd $IMPALA_HOME
cmake -DCMAKE_BUILD_TYPE=Debug .
cd $IMPALA_HOME/common/function-registry
make
cd $IMPALA_HOME/common/thrift
make
cd $IMPALA_BE_DIR
make

# build frontend
# skip tests since any failures will prevent the
# package phase from completing.
cd $IMPALA_FE_DIR
mvn package -DskipTests=true

# run frontend tests
if [ $tests_action -eq 1 ]
then
    mvn test
fi

# run backend tests For some reason this does not work on Jenkins
#if [ $tests_action -eq 1 ] 
#then
#  cd $IMPALA_FE_DIR
#  mvn exec:java -Dexec.mainClass=com.cloudera.impala.testutil.PlanService \
#              -Dexec.classpathScope=test & 
#  PID=$!
#  # Wait for planner to startup TODO: can we do something better than wait arbitrarily for
#  # 3 seconds.  Not a huge deal if it's not long enough, BE tests will just wait a bit
#  sleep 3
#  cd $IMPALA_BE_DIR
#  make test
#  kill $PID
#fi

# Generate list of files for Cscope to index
$IMPALA_HOME/bin/gen-cscope.sh
