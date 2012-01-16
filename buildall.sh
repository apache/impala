#!/usr/bin/env bash
# Copyright (c) 2011 Cloudera, Inc. All rights reserved.

# run buildall.sh -help to see options

root=`dirname "$0"`
root=`cd "$root"; pwd`

export IMPALA_HOME=$root

. "$root"/bin/impala-config.sh

# Exit on non-true return value
set -e
# Exit on reference to unitialized variable
set -u

clean_action=1
config_action=1
testdata_action=1
tests_action=1

# parse command line options
for ARG in $*
do
  case "$ARG" in
    -noclean)
      clean_action=0
      ;;
    -noconfig)
      config_action=0
      ;;
    -notestdata)
      testdata_action=0
      ;;
    -skiptests)
      tests_action=0
      ;;
    -help)
      echo "buildall.sh [-noclean] [-noconfig] [-notestdata]"
      echo "[-noclean] : omits cleaning all packages before building"
      echo "[-noconfig] : omits running configure script for third party packages"
      echo "[-testdata] : omits recreating the metastore and loading test data"
      exit
      ;;
  esac
done

# option to clean everything first
if [ $clean_action -eq 1 ]
then
  # clean selected files from the root
  rm -f CMakeCache.txt

  # clean thirdparty
  cd $IMPALA_HOME/thirdparty
  # remove everything that is not checked in
  git clean -dfx

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

# build thirdparty
cd $IMPALA_HOME/thirdparty/gflags-1.5
if [ $config_action -eq 1 ]
then
  ./configure
fi
# add -fPIC to CXXFLAGS by finding and replacing the line that sets the CXXFLAGS in Makefile
OLDCXXFLAGS=$(grep -w "CXXFLAGS =" Makefile)
NEWCXXFLAGS=$OLDCXXFLAGS" -fPIC"
sed -i "s/$OLDCXXFLAGS/$NEWCXXFLAGS/g" Makefile
make

# Build pprof
cd $IMPALA_HOME/thirdparty/google-perftools-1.8.3
if [ $config_action -eq 1 ]
then
# TODO: google perf tools indicates this might be necessary on 64 bit systems.
# we're not compiling the rest of our code to not omit frame pointers but it 
# still seems to generate useful profiling data.
  ./configure --enable-frame-pointers
fi
# add -fPIC to CXXFLAGS by finding and replacing the line that sets the CXXFLAGS in Makefile
OLDCXXFLAGS=$(grep -w "CXXFLAGS =" Makefile)
NEWCXXFLAGS=$OLDCXXFLAGS" -fPIC"
sed -i "s/$OLDCXXFLAGS/$NEWCXXFLAGS/g" Makefile
make

# Build glog
cd $IMPALA_HOME/thirdparty/glog-0.3.1
if [ $config_action -eq 1 ]
then
  ./configure
fi
# add -fPIC to CXXFLAGS by finding and replacing the line that sets the CXXFLAGS in Makefile
OLDCXXFLAGS=$(grep -w "CXXFLAGS =" Makefile)
NEWCXXFLAGS=$OLDCXXFLAGS" -fPIC"
sed -i "s/$OLDCXXFLAGS/$NEWCXXFLAGS/g" Makefile
make

cd $IMPALA_HOME/thirdparty/gtest-1.6.0
cmake .
make

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

# Generate hive-site.xml from template via env var substitution
# TODO: Throw an error if the template references an undefined environment variable
cd ${IMPALA_FE_DIR}/src/test/resources
perl -wpl -e 's/\$\{([^}]+)\}/defined $ENV{$1} ? $ENV{$1} : $&/eg' \
hive-site.xml.template > hive-site.xml

# Generate hbase-site.xml from template via env var substitution
# TODO: Throw an error if the template references an undefined environment variable
cd ${IMPALA_FE_DIR}/src/test/resources
perl -wpl -e 's/\$\{([^}]+)\}/defined $ENV{$1} ? $ENV{$1} : $&/eg' \
hbase-site.xml.template > hbase-site.xml

if [ $testdata_action -eq 1 ]
then
  # create test data
  cd $IMPALA_HOME/testdata
  $IMPALA_HOME/bin/create_testdata.sh
  cd $IMPALA_HOME/fe
  mvn -Pload-testdata process-test-resources
fi

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
