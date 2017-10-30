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

# Starts up the StateStored with the specified command line arguments.

set -euo pipefail
trap 'echo Error in $0 at line $LINENO: $(cd "'$PWD'" && awk "NR == $LINENO" $0)' ERR

BUILD_TYPE=latest
STATESTORED_ARGS=""
BINARY_BASE_DIR=${IMPALA_HOME}/be/build

# Everything except for -build_type should be passed as a statestored argument
for ARG in $*
do
  case "$ARG" in
    -build_type=debug)
      BUILD_TYPE=debug
      ;;
    -build_type=release)
      BUILD_TYPE=release
      ;;
    -build_type=latest)
      ;;
    -build_type=*)
      echo "Invalid build type. Valid values are: debug, release"
      exit 1
      ;;
    *)
      STATESTORED_ARGS="${STATESTORED_ARGS} ${ARG}"
      ;;
  esac
done

# If Kerberized, source appropriate vars and set startup options
if ${CLUSTER_DIR}/admin is_kerberized; then
  . ${MINIKDC_ENV}
  STATESTORED_ARGS="${STATESTORED_ARGS} -principal=${MINIKDC_PRINC_IMPALA_BE}"
  STATESTORED_ARGS="${STATESTORED_ARGS} -keytab_file=${KRB5_KTNAME}"
  STATESTORED_ARGS="${STATESTORED_ARGS} -krb5_conf=${KRB5_CONFIG}"
  if [ "${MINIKDC_DEBUG}" = "true" ]; then
      STATESTORED_ARGS="${STATESTORED_ARGS} -krb5_debug_file=/tmp/statestored.krb5_debug"
  fi
fi

export ASAN_OPTIONS="handle_segv=0 detect_leaks=0 allocator_may_return_null=1"
export UBSAN_OPTIONS="print_stacktrace=1"
UBSAN_OPTIONS="${UBSAN_OPTIONS} suppressions=${IMPALA_HOME}/bin/ubsan-suppressions.txt"
export TSAN_OPTIONS="halt_on_error=0 history_size=7"
export PATH="${IMPALA_TOOLCHAIN}/llvm-${IMPALA_LLVM_VERSION}/bin:${PATH}"
exec ${BINARY_BASE_DIR}/${BUILD_TYPE}/statestore/statestored ${STATESTORED_ARGS}
