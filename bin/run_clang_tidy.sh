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

TMP_BUILDALL_LOG=$(mktemp)
echo "Compiling, for build logs see ${TMP_BUILDALL_LOG}"
if ! ./buildall.sh -skiptests -tidy -so -noclean &> "${TMP_BUILDALL_LOG}"
then
  echo "buildall.sh failed!" >&2
  grep "^make.* Error " ${TMP_BUILDALL_LOG} >&2
  echo "Dumping output of ./buildall.sh -skiptests -tidy -so -noclean" >&2
  cat "${TMP_BUILDALL_LOG}"
  exit 1
fi

DIRS=$(ls -d "${IMPALA_HOME}/be/src/"*/ | grep -v gutil | grep -v kudu |\
    grep -v thirdparty | tr '\n' ' ')
# Include/exclude select thirdparty dirs.
DIRS=$DIRS$(ls -d "${IMPALA_HOME}/be/src/thirdparty/"*/ | grep -v mpfit |\
    grep -v datasketches | grep -v murmurhash | grep -v xxhash | tr '\n' ' ')
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

export PATH="${IMPALA_TOOLCHAIN_PACKAGES_HOME}/llvm-${IMPALA_LLVM_VERSION}/share/clang\
:${IMPALA_TOOLCHAIN_PACKAGES_HOME}/llvm-${IMPALA_LLVM_VERSION}/bin/\
:$PATH"
TMP_STDERR=$(mktemp)
echo; echo "Running clang tidy, for error logs see ${TMP_STDERR}"
STRCAT_MESSAGE="Impala-specific note: This can also be fixed using the StrCat() function \
from be/src/gutil/strings strcat.h)"
CLANG_STRING_CONCAT="performance-inefficient-string-concatenation"
FALLTHROUGH_MESSAGE="Impala-specific note: Impala is a C++ 17 codebase, so the preferred \
way to indicate intended fallthrough is C++ 17's [[fallthrough]]"
CLANG_FALLTHROUGH="clang-diagnostic-implicit-fallthrough"
trap "rm $TMP_STDERR" EXIT
if ! run-clang-tidy.py -quiet -header-filter "${PIPE_DIRS%?}" \
                       -j"${CORES}" ${DIRS} 2> ${TMP_STDERR} | \
   sed "/${CLANG_STRING_CONCAT}/ s#\$# \n${STRCAT_MESSAGE}#" | \
   sed "/${CLANG_FALLTHROUGH}/ s#\$# \n${FALLTHROUGH_MESSAGE}#" | \
   sed 's#FALLTHROUGH_INTENDED#[[fallthrough]]#';
then
  echo "run-clang-tidy.py hit an error, dumping stderr output"
  cat ${TMP_STDERR} >&2
  exit 1
fi
