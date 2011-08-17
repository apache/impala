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
skiptests_action=false

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
      skiptests_action=true
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

# Initialize and update submodules
cd $IMPALA_HOME
git submodule init
git submodule update

# option to clean everything first
if [ $clean_action -eq 1 ]
then
  # clean thirdparty
  cd $IMPALA_HOME/thirdparty
  # remove everything that is not checked in
  git clean -dfx

  # clean fe
  cd $IMPALA_HOME/fe
  # remove everything listed in .gitignore
  git clean -Xdf

  # clean be
  cd $IMPALA_HOME/be
  # remove everything listed in .gitignore
  git clean -Xdf

fi

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

# build backend
cd $IMPALA_BE_DIR
cmake -DCMAKE_BUILD_TYPE=Debug . && make -j4

# build frontend and run tests
cd $IMPALA_FE_DIR
mvn package -DskipTests=${skiptests_action}
