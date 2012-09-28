#!/usr/bin/env bash
# Copyright (c) 2012 Cloudera, Inc. All rights reserved.

# cleans and rebuilds thirdparty/

# Exit on non-true return value
set -e
# Exit on reference to unitialized variable
set -u

bin=`dirname "$0"`
bin=`cd "$bin"; pwd`

. "$bin"/impala-config.sh

clean_action=1

for ARG in $*
do
  case "$ARG" in
    -noclean)
      clean_action=0
      ;;
  esac
done

if [ $clean_action -eq 1 ]
then
  # clean thirdparty
  cd $IMPALA_HOME/thirdparty
  # remove everything that is not checked in
  git clean -dfx
fi

# build gflags
cd $IMPALA_HOME/thirdparty/gflags-1.5
./configure --with-pic
make -j4

# Build pprof
cd $IMPALA_HOME/thirdparty/gperftools-2.0
# TODO: google perf tools indicates this might be necessary on 64 bit systems.
# we're not compiling the rest of our code to not omit frame pointers but it 
# still seems to generate useful profiling data.
./configure --enable-frame-pointers --with-pic
make -j4

# Build glog
cd $IMPALA_HOME/thirdparty/glog-0.3.1
./configure --with-pic
make -j4

# Build gtest
cd $IMPALA_HOME/thirdparty/gtest-1.6.0
cmake .
make -j4

# Build Snappy
cd $IMPALA_HOME/thirdparty/snappy-1.0.5
./configure --with-pic --prefix=$IMPALA_HOME/thirdparty/snappy-1.0.5/build
make install

# Build Sasl
# Disable everything except those protocols needed -- currently just Kerberos.
# Sasl does not have a --with-pic configuration.
cd $IMPALA_HOME/thirdparty/cyrus-sasl-2.1.23
CFLAGS="-fPIC -DPIC" CXXFLAGS="-fPIC -DPIC" ./configure \
  --disable-digest --disable-sql --disable-cram --disable-ldap \
  --disable-digest --disable-otp  \
  --prefix=$IMPALA_HOME/thirdparty/cyrus-sasl-2.1.23/build \
  --enable-static --enable-staticdlopen
# the first time you do a make it fails, ignore the error.
(make || true)
make install
