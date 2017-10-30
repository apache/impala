#!/usr/bin/env bash
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

set -euo pipefail
trap 'echo Error in $0 at line $LINENO: $(cd "'$PWD'" && awk "NR == $LINENO" $0)' ERR

export GTEST_OUTPUT="xml:$IMPALA_BE_TEST_LOGS_DIR/"

: ${SKIP_BE_TEST_PATTERN:=}

# The backend unit tests currently do not work when HEAPCHECK is enabled.
export HEAPCHECK=

BE_TEST_ARGS=""
if [[ -n "$SKIP_BE_TEST_PATTERN" ]]; then
  BE_TEST_ARGS="-E ${SKIP_BE_TEST_PATTERN}"
fi

cd ${IMPALA_BE_DIR}
. ${IMPALA_HOME}/bin/set-classpath.sh
cd ..

export CTEST_OUTPUT_ON_FAILURE=1
export ASAN_OPTIONS="handle_segv=0 detect_leaks=0 allocator_may_return_null=1"
export UBSAN_OPTIONS="print_stacktrace=1"
UBSAN_OPTIONS="${UBSAN_OPTIONS} suppressions=${IMPALA_HOME}/bin/ubsan-suppressions.txt"
export TSAN_OPTIONS="halt_on_error=1 history_size=7"
export PATH="${IMPALA_TOOLCHAIN}/llvm-${IMPALA_LLVM_VERSION}/bin:${PATH}"
"${MAKE_CMD:-make}" test ARGS="${BE_TEST_ARGS}"
