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
    -help)
      echo "make_impala.sh [-notests -clean]"
      echo "[-notests] : Omits building the tests."
      echo "[-clean] : Cleans previous build artifacts."
      exit
      ;;
  esac
done

if [ $CLEAN -eq 1 ] 
then
  make clean
  rm -f $IMPALA_HOME/llvm-ir/impala-nosse.ll
  rm -f $IMPALA_HOME/llvm-ir/impala-sse.ll
fi

$IMPALA_HOME/bin/gen_build_version.py --noclean

cd $IMPALA_HOME/common/function-registry
make
cd $IMPALA_HOME/common/thrift
make
cd $IMPALA_BE_DIR
# TODO: we need to figure out how to use CMake dependencies properly
python src/codegen/gen_ir_descriptions.py --noclean

cd $IMPALA_HOME
if [ $BUILD_TESTS -eq 1 ]
then
  make -j${IMPALA_BUILD_THREADS:-4}
else
  # TODO: is there a way to get CMake to do this?
  make compile_to_ir_no_sse compile_to_ir_sse
  make -j${IMPALA_BUILD_THREADS:-4} impalad
  make -j${IMPALA_BUILD_THREADS:-4} statestored catalogd fesupport
fi

