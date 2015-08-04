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

# Incrementally compiles the BE.

# Exit on reference to uninitialized variable
set -u

# Exit on non-zero return value
set -e

BUILD_TESTS=1
CLEAN=0
TARGET_BUILD_TYPE=${TARGET_BUILD_TYPE:-""}
BUILD_SHARED_LIBS=${BUILD_SHARED_LIBS:-""}

# parse command line options
for ARG in $*
do
  case "$ARG" in
    -notests)
      BUILD_TESTS=0
      ;;
    -clean)
      CLEAN=1
      ;;
    -build_type=*)
      TARGET_BUILD_TYPE="${ARG#*=}"
      ;;
    -build_shared_libs)
      BUILD_SHARED_LIBS="ON"
      ;;
    -build_static_libs)
      BUILD_SHARED_LIBS="OFF"
      ;;
    -help)
      echo "make_impala.sh [-build_type=<build type> -notests -clean]"
      echo "[-build_type] : Target build type. Examples: Debug, Release, Address_sanitizer."
      echo "                If omitted, the last build target is built incrementally"
      echo "[-build_shared_libs] : Link all executables dynamically"
      echo "[-build_static_libs] : Link all executables statically (the default)"
      echo "[-notests] : Omits building the tests."
      echo "[-clean] : Cleans previous build artifacts."
      echo ""
      echo "If either -build_type or -build_*_libs is set, cmake will be re-run for the "
      echo "project. Otherwise the last cmake configuration will continue to take effect."
      exit
      ;;
  esac
done

echo "********************************************************************************"
echo " Building Impala "
if [ "x${TARGET_BUILD_TYPE}" != "x" ];
then
  echo " Build type: ${TARGET_BUILD_TYPE} "
  if [ "x${BUILD_SHARED_LIBS}" == "x" ]
  then
      echo " Impala libraries will be STATICALLY linked"
  fi
fi
if [ "x${BUILD_SHARED_LIBS}" == "xOFF" ]
then
  echo " Impala libraries will be STATICALLY linked"
fi
if [ "x${BUILD_SHARED_LIBS}" == "xON" ]
then
  echo " Impala libraries will be DYNAMICALLY linked"
fi
echo "********************************************************************************"

cd ${IMPALA_HOME}

if [ "x${TARGET_BUILD_TYPE}" != "x" ] || [ "x${BUILD_SHARED_LIBS}" != "x" ]
then
    rm -f ./CMakeCache.txt
    CMAKE_ARGS=()
    if [ "x${TARGET_BUILD_TYPE}" != "x" ]; then
      CMAKE_ARGS+=(-DCMAKE_BUILD_TYPE=${TARGET_BUILD_TYPE})
    fi

    if [ "x${BUILD_SHARED_LIBS}" != "x" ]; then
      CMAKE_ARGS+=(-DBUILD_SHARED_LIBS=${BUILD_SHARED_LIBS})
    fi

    if [[ ! -z $IMPALA_TOOLCHAIN ]]; then
      CMAKE_ARGS+=(-DCMAKE_TOOLCHAIN_FILE=$IMPALA_HOME/cmake_modules/toolchain.cmake)
    fi

    cmake . ${CMAKE_ARGS[@]}
fi

if [ $CLEAN -eq 1 ]
then
  make clean
  rm -f $IMPALA_HOME/llvm-ir/impala-nosse.ll
  rm -f $IMPALA_HOME/llvm-ir/impala-sse.ll
fi

$IMPALA_HOME/bin/gen_build_version.py --noclean

cd $IMPALA_HOME/common/function-registry
make

cd $IMPALA_BE_DIR
# TODO: we need to figure out how to use CMake dependencies properly
src/codegen/gen_ir_descriptions.py --noclean

cd $IMPALA_HOME
if [ $BUILD_TESTS -eq 1 ]
then
  make -j${IMPALA_BUILD_THREADS:-4}
else
  # TODO: is there a way to get CMake to do this?
  make -j${IMPALA_BUILD_THREADS:-4} impalad
  make -j${IMPALA_BUILD_THREADS:-4} statestored catalogd fesupport loggingsupport ImpalaUdf
  # Don't execute these two commands in parallel because it might break the build as the
  # dependency resolution of the targets does not happen independently but in parallel as
  # well. If the targets are started sufficiently close together, the common dependencies
  # might not be correctly built, as both targets will believe a target is built as soon
  # as the target file exists. In the worst case, this will lead to corrupt built
  # artifacts caused by missing or truncated object files.
  # http://www.cmake.org/pipermail/cmake/2011-July/045256.html
  make -j${IMPALA_BUILD_THREADS:-4} compile_to_ir_no_sse
  make -j${IMPALA_BUILD_THREADS:-4} compile_to_ir_sse
fi
