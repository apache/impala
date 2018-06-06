#!/bin/bash

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

# This script runs clang-tidy on the backend, excluding gutil and kudu/. clang-tidy finds
# bugs and likely bugs.
#
# To use this script, the toolchain must be installed and the toolchain environment
# variables must be set.
#
# The output of this script is quite verbose becuase clang-tidy and the clang-provided
# run-clang-tidy.py have verbose output. The lines indicating likely bugs are the ones
# that end in ']'.

set -euo pipefail

echo "Compiling"
if ! ./buildall.sh -skiptests -tidy -so -noclean 1>/dev/null 2>/dev/null
then
  echo "WARNING: compile failed" >&2
fi

DIRS=$(ls -d "${IMPALA_HOME}/be/src/"*/ | grep -v gutil | grep -v kudu |\
  grep -v thirdparty | tr '\n' ' ')
# Include/exclude select thirdparty dirs.
DIRS=$DIRS$(ls -d "${IMPALA_HOME}/be/src/thirdparty/"*/ | grep -v mpfit | tr '\n' ' ')
PIPE_DIRS=$(echo "${DIRS}" | tr ' ' '|')

# Reduce the concurrency to one less than the number of cores in the system. Note than
# nproc may not be available on older distributions like Centos 5.5.
if type nproc >/dev/null 2>&1; then
  CORES=$(($(nproc) - 1))
else
  # This script has been tested when CORES is actually higher than the number of cores on
  # the system, and the system remained responsive, so it's OK to guess here.
  CORES=7
fi

export PATH="${IMPALA_TOOLCHAIN}/llvm-${IMPALA_LLVM_VERSION}/share/clang\
:${IMPALA_TOOLCHAIN}/llvm-${IMPALA_LLVM_VERSION}/bin/\
:$PATH"
TMP_STDERR=$(mktemp)
trap "rm $TMP_STDERR" EXIT
if ! run-clang-tidy.py -quiet -header-filter "${PIPE_DIRS%?}" \
                       -j"${CORES}" ${DIRS} 2> ${TMP_STDERR};
then
  echo "run-clang-tidy.py hit an error, dumping stderr output"
  cat ${TMP_STDERR} >&2
  exit 1
fi
