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
# Exit on reference to uninitialized variable
set -u

# By default, git clean every library we build
CLEAN_ACTION=1

# By default, build every library
BUILD_ALL=1

# If BUILD_ALL -eq 1, these are ignored, otherwise only build those libraries with
# BUILD_<lib> -eq 1
BUILD_AVRO=0
BUILD_THRIFT=0
BUILD_GLOG=0
BUILD_GFLAGS=0
BUILD_GTEST=0
BUILD_RE2=0
BUILD_SASL=0
BUILD_LDAP=0
BUILD_SNAPPY=0
BUILD_PPROF=0
BUILD_CDH4EXTRAS=0
BUILD_LZ4=0

for ARG in $*
do
  case "$ARG" in
    -noclean)
      CLEAN_ACTION=0
      ;;
    -avro)
      BUILD_ALL=0
      BUILD_AVRO=1
      ;;
    -glog)
      BUILD_ALL=0
      BUILD_GLOG=1
      ;;
    -thrift)
      BUILD_ALL=0
      BUILD_THRIFT=1
      ;;
    -gflags)
      BUILD_ALL=0
      BUILD_GFLAGS=1
      ;;
    -gtest)
      BUILD_ALL=0
      BUILD_GTEST=1
      ;;
    -re2)
      BUILD_ALL=0
      BUILD_RE2=1
      ;;
    -sasl)
      BUILD_ALL=0
      BUILD_SASL=1
      ;;
    -ldap)
      BUILD_ALL=0
      BUILD_LDAP=1
      ;;
    -snappy)
      BUILD_ALL=0
      BUILD_SNAPPY=1
      ;;
    -lz4)
      BUILD_ALL=0
      BUILD_LZ4=1
      ;;
    -pprof)
      BUILD_ALL=0
      BUILD_PPROF=1
      ;;
    -*)
      echo "Usage: build_thirdparty.sh [-noclean] \
[-avro -glog -thrift -gflags -gtest -re2 -sasl -ldap -snappy -pprof]"
      exit 1
  esac
done
bin=`dirname "$0"`
bin=`cd "$bin"; pwd`
. "$bin"/impala-config.sh
USE_PIC_LIB_PATH=${PIC_LIB_PATH:-}

function build_preamble() {
  echo
  echo
  echo "********************************************************************************"
  echo "Building $2 in $1 $([ $CLEAN_ACTION -eq 1 ] && echo '(clean)')"
  echo "********************************************************************************"
  cd $1
  if [ $CLEAN_ACTION -eq 1 ]; then
    # remove everything that is not checked in
    git clean -dfx
  fi
}

# Build Sasl
if [ $BUILD_ALL -eq 1 ] || [ $BUILD_SASL -eq 1 ]; then
  build_preamble $IMPALA_HOME/thirdparty/cyrus-sasl-${IMPALA_CYRUS_SASL_VERSION} Sasl

  # Need to specify which libdb to use on certain OSes
  LIBDB_DIR=""
  if [[ -e "/usr/lib64/libdb4" && -e "/usr/include/libdb4" ]]; then
    LIBDB_DIR="--with-bdb-libdir=/usr/lib64/libdb4 --with-bdb-incdir=/usr/include/libdb4"
  fi
  # Disable everything except those protocols needed -- currently just Kerberos.
  # Sasl does not have a --with-pic configuration.
  CFLAGS="-fPIC -DPIC" CXXFLAGS="-fPIC -DPIC" ./configure \
    --disable-sql --disable-otp --disable-ldap --disable-digest --with-saslauthd=no \
    --prefix=$IMPALA_CYRUS_SASL_INSTALL_DIR --enable-static --enable-staticdlopen \
    $LIBDB_DIR
  # the first time you do a make it fails, build again.
  (make || make)
  make install
fi

set -e
# build thrift
if [ $BUILD_ALL -eq 1 ] || [ $BUILD_THRIFT -eq 1 ]; then
  cd ${THRIFT_SRC_DIR}
  build_preamble ${THRIFT_SRC_DIR} "Thrift"
  if [ -d "${PIC_LIB_PATH:-}" ]; then
    PIC_LIB_OPTIONS="--with-zlib=${PIC_LIB_PATH} "
  fi
  JAVA_PREFIX=${THRIFT_HOME}/java PY_PREFIX=${THRIFT_HOME}/python \
    ./configure --with-pic --prefix=${THRIFT_HOME} \
    --with-php=no --with-java=no --with-perl=no --with-erlang=no \
    --with-ruby=no --with-haskell=no --with-erlang=no --with-d=no \
    --with-go=no --with-qt4=no --with-libevent=no ${PIC_LIB_OPTIONS:-}
  make # Make with -j fails
  make install
  cd ${THRIFT_SRC_DIR}/contrib/fb303
  chmod 755 ./bootstrap.sh
  ./bootstrap.sh
  chmod 755 configure
  CPPFLAGS="-I${THRIFT_HOME}/include" PY_PREFIX=${THRIFT_HOME}/python ./configure \
    --with-java=no --with-php=no --prefix=${THRIFT_HOME} --with-thriftpath=${THRIFT_HOME}
  make
  make install
fi

# build gflags
if [ $BUILD_ALL -eq 1 ] || [ $BUILD_GFLAGS -eq 1 ]; then
  build_preamble $IMPALA_HOME/thirdparty/gflags-${IMPALA_GFLAGS_VERSION} GFlags
  GFLAGS_INSTALL=`pwd`/third-party-install
  ./configure --with-pic --prefix=${GFLAGS_INSTALL}
   make -j${IMPALA_BUILD_THREADS:-4} install
fi

# Build pprof
if [ $BUILD_ALL -eq 1 ] || [ $BUILD_PPROF -eq 1 ]; then
  build_preamble $IMPALA_HOME/thirdparty/gperftools-${IMPALA_GPERFTOOLS_VERSION} \
    GPerftools
  # TODO: google perf tools indicates this might be necessary on 64 bit systems.
  # we're not compiling the rest of our code to not omit frame pointers but it
  # still seems to generate useful profiling data.
  ./configure --enable-frame-pointers --with-pic
   make -j${IMPALA_BUILD_THREADS:-4}
fi

# Build glog
if [ $BUILD_ALL -eq 1 ] || [ $BUILD_GLOG -eq 1 ]; then
  build_preamble  $IMPALA_HOME/thirdparty/glog-${IMPALA_GLOG_VERSION} GLog
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
   make -j${IMPALA_BUILD_THREADS:-4}
fi

# Build gtest
if [ $BUILD_ALL -eq 1 ] || [ $BUILD_GTEST -eq 1 ]; then
  build_preamble $IMPALA_HOME/thirdparty/gtest-${IMPALA_GTEST_VERSION} GTest
  cmake .
   make -j${IMPALA_BUILD_THREADS:-4}
fi

# Build Snappy
if [ $BUILD_ALL -eq 1 ] || [ $BUILD_SNAPPY -eq 1 ]; then
  build_preamble $IMPALA_HOME/thirdparty/snappy-${IMPALA_SNAPPY_VERSION} Snappy
  ./autogen.sh
  ./configure --with-pic --prefix=$IMPALA_HOME/thirdparty/snappy-${IMPALA_SNAPPY_VERSION}/build
  make install
fi

# Build Lz4
if [ $BUILD_ALL -eq 1 ] || [ $BUILD_LZ4 -eq 1 ]; then
   build_preamble $IMPALA_HOME/thirdparty/lz4 Lz4
   cmake .
   make
fi

# Build re2
if [ $BUILD_ALL -eq 1 ] || [ $BUILD_RE2 -eq 1 ]; then
  build_preamble $IMPALA_HOME/thirdparty/re2 RE2
   make -j${IMPALA_BUILD_THREADS:-4}
fi

# Build Ldap
if [ $BUILD_ALL -eq 1 ] || [ $BUILD_LDAP -eq 1 ]; then
    build_preamble $IMPALA_HOME/thirdparty/openldap-${IMPALA_OPENLDAP_VERSION} Openldap
    ./configure --enable-slapd=no --prefix=`pwd`/impala_install --enable-static --with-pic
     make -j${IMPALA_BUILD_THREADS:-4}
     make -j${IMPALA_BUILD_THREADS:-4} depend
    make install
fi

# Build Avro
if [ $BUILD_ALL -eq 1 ] || [ $BUILD_AVRO -eq 1 ]; then
  build_preamble $IMPALA_HOME/thirdparty/avro-c-${IMPALA_AVRO_VERSION} Avro
  cmake .
   make -j${IMPALA_BUILD_THREADS:-4}
fi
