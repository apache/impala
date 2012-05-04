#!/usr/bin/env bash
# Copyright (c) 2011 Cloudera, Inc. All rights reserved.

# run buildall.sh -help to see options

root=`dirname "$0"`
root=`cd "$root"; pwd`

export IMPALA_HOME=$root
export METASTORE_DB=`basename $root | sed -e "s/\\./_/g" | sed -e "s/[.-]/_/g"`

. "$root"/bin/impala-config.sh

clean_action=1
testdata_action=1
tests_action=1

FORMAT_CLUSTER=1

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
    -help)
      echo "buildall.sh [-noclean] [-noconfig] [-notestdata] [-noformat]"
      echo "[-noclean] : omits cleaning all packages before building"
      echo "[-notestdata] : omits recreating the metastore and loading test data"
      echo "[-noformat] : prevents the minicluster from formatting its data directories, and skips the data load step"
      exit
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
  cd $IMPALA_HOME/fe
  rm -rf target
  rm -f src/test/resources/hbase-site.xml
  rm -f src/test/resources/hive-site.xml
  rm -f derby.log

  # clean be
  cd $IMPALA_HOME/be
  # remove everything listed in .gitignore
  git clean -Xdf

fi

# Generate hive-site.xml from template via env var substitution
# TODO: Throw an error if the template references an undefined environment variable
cd ${IMPALA_FE_DIR}/src/test/resources
if [[ ${METASTORE_IS_DERBY} ]]
then
  echo "using derby for metastore"
  perl -wpl -e 's/\$\{([^}]+)\}/defined $ENV{$1} ? $ENV{$1} : $&/eg' \
    derby-hive-site.xml.template > hive-site.xml
else
  echo "using mysql for metastore"
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
# Exit on reference to unitialized variable
set -u

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
make -j4

# Get Hadoop dependencies onto the classpath
cd $IMPALA_HOME/fe
mvn dependency:unpack-dependencies

if [ $testdata_action -eq 1 ]
then
  # create test data
  cd $IMPALA_HOME/testdata
  $IMPALA_HOME/bin/create_testdata.sh
  cd $IMPALA_HOME/fe
  if [ $FORMAT_CLUSTER -eq 1 ]; then    
    mvn -Pload-testdata process-test-resources -Dcluster.format
  else
    mvn -Pload-testdata process-test-resources
  fi
fi

# build frontend
# Package first since any test failure will prevent the package phase from completing.
cd $IMPALA_FE_DIR
mvn package -DskipTests=true
if [ $tests_action -eq 1 ]
then
    # also run frontend tests
    mvn test
fi

# run backend tests For some reason this does not work on Jenkins
if [ $tests_action -eq 1 ] 
then
  cd $IMPALA_FE_DIR
  mvn exec:java -Dexec.mainClass=com.cloudera.impala.testutil.PlanService \
              -Dexec.classpathScope=test & 
  PID=$!
  # Wait for planner to startup TODO: can we do something better than wait arbitrarily for
  # 3 seconds.  Not a huge deal if it's not long enough, BE tests will just wait a bit
  sleep 3
  cd $IMPALA_BE_DIR
  make test
  kill $PID
fi

# Generate list of files for Cscope to index
$IMPALA_HOME/bin/gen-cscope.sh
