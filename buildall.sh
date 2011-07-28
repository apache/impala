#!/bin/bash

# run buildall.sh -help to see options

clean_action=1
config_action=1
testdata_action=1

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
  # clean thirdparty
  cd thirdparty
  cd gflags-1.5
  make clean
  rm Makefile
  cd ..
  cd glog-0.3.1
  make clean
  rm Makefile
  cd ../../

  # clean frontend
  cd fe
  mvn clean
  cd ..

  # clean backend
  cd be
  cmake .
  make clean
  cd ..
fi

# exit script if any following command gives a non-zero return
set -e


if [ $testdata_action -eq 1 ]
then
  # create test data
  cd testdata
  ./recreate_store.sh
  cd ../fe
  mvn -Pload-testdata process-test-resources
  cd ..
fi

# build thirdparty
cd thirdparty
cd gflags-1.5
if [ $config_action -eq 1 ]
then
  ./configure
fi
# add -fPIC to CXXFLAGS by finding and replacing the line that sets the CXXFLAGS in Makefile
OLDCXXFLAGS=$(grep -w "CXXFLAGS =" Makefile)
NEWCXXFLAGS=$OLDCXXFLAGS" -fPIC"
sed -i "s/$OLDCXXFLAGS/$NEWCXXFLAGS/g" Makefile
make
cd ..
cd glog-0.3.1
if [ $config_action -eq 1 ]
then
  ./configure
fi
# add -fPIC to CXXFLAGS by finding and replacing the line that sets the CXXFLAGS in Makefile
OLDCXXFLAGS=$(grep -w "CXXFLAGS =" Makefile)
NEWCXXFLAGS=$OLDCXXFLAGS" -fPIC"
sed -i "s/$OLDCXXFLAGS/$NEWCXXFLAGS/g" Makefile
make
cd ../../

# build backend
cd be
cmake .
make
cd ..

# build frontend and run tests
cd fe
mvn test
cd ..
