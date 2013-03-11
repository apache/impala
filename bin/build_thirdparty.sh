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

# cleans and rebuilds thirdparty/. The Impala build environment must be set up
# by bin/impala-config.sh before running this script.

# Exit on non-true return value
set -e
# Exit on reference to unitialized variable
set -u

bin=`dirname "$0"`
bin=`cd "$bin"; pwd`

. "$bin"/impala-config.sh

USE_PIC_LIB_PATH=${PIC_LIB_PATH:-}

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

# build thrift
cd ${THRIFT_SRC_DIR}
JAVA_PREFIX=${THRIFT_HOME}/java PY_PREFIX=${THRIFT_HOME}/python \
  ./configure --with-pic --prefix=${THRIFT_HOME} \
  --with-php=no --with-java=no --with-perl=no --with-erlang=no \
  --with-ruby=no --with-haskell=no --with-erlang=no --with-d=no \
  --with-qt4=no
make # Make with -j fails
make install
cd ${THRIFT_SRC_DIR}/contrib/fb303
chmod 755 ./bootstrap.sh
./bootstrap.sh
chmod 755 configure
CPPFLAGS="-I${THRIFT_HOME}/include" PY_PREFIX=${THRIFT_HOME}/python ./configure \
  --prefix=${THRIFT_HOME} --with-thriftpath=${THRIFT_HOME}
make
make install

# build gflags
cd $IMPALA_HOME/thirdparty/gflags-${IMPALA_GFLAGS_VERSION}
GFLAGS_INSTALL=`pwd`/third-party-install
./configure --with-pic --prefix=${GFLAGS_INSTALL}
make -j4 install

# Build pprof
cd $IMPALA_HOME/thirdparty/gperftools-${IMPALA_GPERFTOOLS_VERSION}
# TODO: google perf tools indicates this might be necessary on 64 bit systems.
# we're not compiling the rest of our code to not omit frame pointers but it
# still seems to generate useful profiling data.
./configure --enable-frame-pointers --with-pic
make -j4

# Build glog
cd $IMPALA_HOME/thirdparty/glog-${IMPALA_GLOG_VERSION}
./configure --with-pic --with-gflags=${GFLAGS_INSTALL}

# SLES's gcc45-c++ is required for sse2 support (default is 4.3), but crashes
# when building logging_unittest-logging_unittest.o. Telling it to uses the
# stabs format for debugging symbols instead of dwarf exercises a different
# code path to work around this issue.
cat > Makefile.gcc45sles_workaround <<EOF
logging_unittest-logging_unittest.o : CXXFLAGS= -gstabs -O2
EOF
cat Makefile >> Makefile.gcc45sles_workaround
mv Makefile.gcc45sles_workaround Makefile

make -j4

# Build gtest
cd $IMPALA_HOME/thirdparty/gtest-${IMPALA_GTEST_VERSION}
cmake .
make -j4

# Build Snappy
cd $IMPALA_HOME/thirdparty/snappy-${IMPALA_SNAPPY_VERSION}
./configure --with-pic --prefix=$IMPALA_HOME/thirdparty/snappy-${IMPALA_SNAPPY_VERSION}/build
autoreconf -i
make install

if [ -z "$USE_PIC_LIB_PATH" ]; then
  # Build Sasl
  # Disable everything except those protocols needed -- currently just Kerberos.
  # Sasl does not have a --with-pic configuration.
  cd $IMPALA_HOME/thirdparty/cyrus-sasl-${IMPALA_CYRUS_SASL_VERSION}
  CFLAGS="-fPIC -DPIC" CXXFLAGS="-fPIC -DPIC" ./configure \
    --disable-digest --disable-sql --disable-cram --disable-ldap \
    --disable-digest --disable-otp  \
    --prefix=$IMPALA_HOME/thirdparty/cyrus-sasl-${IMPALA_CYRUS_SASL_VERSION}/build \
    --enable-static --enable-staticdlopen
  # the first time you do a make it fails, ignore the error.
  (make || true)
  make install
fi

# Build Avro
cd $IMPALA_HOME/thirdparty/avro-${IMPALA_AVRO_VERSION}/lang/c++
cmake -G "Unix Makefiles"
make -j4
