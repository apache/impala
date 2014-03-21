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


TARGET_BUILD_TYPE=Debug

# parse command line options
for ARG in $*
do
  case "$ARG" in
    -codecoverage)
      TARGET_BUILD_TYPE=CODE_COVERAGE_DEBUG
      ;;
    -notests)
      ;;
    -help)
      echo "make_debug.sh [-codecoverage]"
      echo "[-codecoverage] : build with 'gcov' code coverage instrumentation at the cost of performance"
      exit
      ;;
  esac
done

rm -f ./CMakeCache.txt
cmake -DCMAKE_BUILD_TYPE=$TARGET_BUILD_TYPE .
$IMPALA_HOME/bin/make_impala.sh -clean $*
