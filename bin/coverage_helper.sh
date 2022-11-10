#!/bin/bash
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

set -eu -o pipefail

ZERO_COUNTERS_ACTION=0
REPORT_ACTION=0
REPORT_DIRECTORY=${IMPALA_HOME}/logs/coverage

function print_usage {
  echo "coverage_helper.sh - Driver script for coverage"
  echo "[-zerocounters] : Reset coverage counters"
  echo "[-report] : Generate a detailed coverage report"
  echo "[-reportdirectory <directory>] : Output directory for coverage report files"
}

while [ -n "$*" ]
do
  case "$1" in
    -zerocounters)
       ZERO_COUNTERS_ACTION=1
       ;;
    -report)
       REPORT_ACTION=1
       ;;
    -reportdirectory)
       REPORT_DIRECTORY="${2-}"
       shift
       ;;
    -help|*)
       print_usage
       exit 1
       ;;
  esac
  shift
done

if [[ ${ZERO_COUNTERS_ACTION} -eq 0 && ${REPORT_ACTION} -eq 0 ]]; then
  print_usage
  exit 1
fi

if pgrep -U "$USER" impalad; then
  echo "Warning: impalad is running. Coverage counters are only updated when"
  echo "a program exits. Any report will not include the information from"
  echo "the running impalad. Similarly, zeroing the counters will not zero"
  echo "the counters for the running impalad."
fi

if [ ${REPORT_ACTION} -eq 1 ]; then
  mkdir -p "${REPORT_DIRECTORY}"
  rm -f "${REPORT_DIRECTORY}"/index*.html
  if ! which gcov > /dev/null; then
    export PATH="$PATH:$IMPALA_TOOLCHAIN_PACKAGES_HOME/gcc-$IMPALA_GCC_VERSION/bin"
  fi
  echo "Using gcov at `which gcov`"
  # src/util/bit-packing.inline.h gets lots of hits, so generating a detailed report
  # for it takes several minutes. Exclude it to keep the execution time down.
  # gcovr excludes are buggy, so on some environments these excludes won't work.
  echo "Generating report in directory: ${REPORT_DIRECTORY}"
  cd "${IMPALA_HOME}"
  "${IMPALA_HOME}/bin/impala-gcovr" -v -r "${IMPALA_HOME}/be" \
    --exclude=".*src/benchmarks.*" \
    --exclude=".*generated-sources/gen-cpp.*" \
    --exclude=".*src/util/bit-packing.inline.h.*" \
    --html --html-details -o "${REPORT_DIRECTORY}/index.html" > "${REPORT_DIRECTORY}/gcovr.out"
fi

if [ ${ZERO_COUNTERS_ACTION} -eq 1 ]; then
  # The .gcda files contain the counters for coverage. Deleting them zeros the
  # the counters.
  echo "Zeroing Counters"
  find "${IMPALA_HOME}/be" -name "*.gcda" -delete
fi
