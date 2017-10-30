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

# Starts up an impalad with the specified command line arguments. An optional -build_type
# parameter can be passed to determine the build type to use for the impalad instance.

set -euo pipefail
trap 'echo Error in $0 at line $LINENO: $(cd "'$PWD'" && awk "NR == $LINENO" $0)' ERR

BUILD_TYPE=latest
IMPALAD_ARGS=""
BINARY_BASE_DIR=${IMPALA_HOME}/be/build
TOOL_PREFIX=""
IMPALAD_BINARY=service/impalad
BINARY=${IMPALAD_BINARY}
JVM_DEBUG_PORT=""
JVM_SUSPEND="n"
JVM_ARGS=""
PERF_ARGS=${PERF_ARGS:-"record -F 99"}

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
    -gdb)
      echo "Starting Impala under gdb..."
      TOOL_PREFIX="gdb --args"
      ;;
    -jvm_debug_port=*)
      JVM_DEBUG_PORT="${ARG#*=}"
      ;;
    -jvm_suspend)
      JVM_SUSPEND="y"
      ;;
    -jvm_args=*)
      JVM_ARGS="${ARG#*=}"
      ;;
    -perf)
      echo "Starting Impala with 'perf' tracing. Set \$PERF_ARGS to customize use."
      TOOL_PREFIX="perf ${PERF_ARGS}"
      ;;
    # Pass all other options as an Impalad argument
    *)
      IMPALAD_ARGS="${IMPALAD_ARGS} ${ARG}"
  esac
done

IMPALA_CMD=${BINARY_BASE_DIR}/${BUILD_TYPE}/${BINARY}

# Temporarily disable unbound variable checking in case JAVA_TOOL_OPTIONS is not set.
set +u
# Optionally enable Java debugging.
if [ -n "$JVM_DEBUG_PORT" ]; then
  export JAVA_TOOL_OPTIONS="-agentlib:jdwp=transport=dt_socket,address=${JVM_DEBUG_PORT},server=y,suspend=${JVM_SUSPEND} ${JAVA_TOOL_OPTIONS}"
fi
# Optionally add additional JVM args.
if [ -n "$JVM_ARGS" ]; then
  export JAVA_TOOL_OPTIONS="${JAVA_TOOL_OPTIONS} ${JVM_ARGS}"
fi

# If Kerberized, source appropriate vars and set startup options
if ${CLUSTER_DIR}/admin is_kerberized; then
  . ${MINIKDC_ENV}
  IMPALAD_ARGS="${IMPALAD_ARGS} -principal=${MINIKDC_PRINC_IMPALA}"
  IMPALAD_ARGS="${IMPALAD_ARGS} -be_principal=${MINIKDC_PRINC_IMPALA_BE}"
  IMPALAD_ARGS="${IMPALAD_ARGS} -keytab_file=${KRB5_KTNAME}"
  IMPALAD_ARGS="${IMPALAD_ARGS} -krb5_conf=${KRB5_CONFIG}"
  if [ "${MINIKDC_DEBUG}" = "true" ]; then
      IMPALAD_ARGS="${IMPALAD_ARGS} -krb5_debug_file=/tmp/impalad.krb5_debug"
  fi
fi

. ${IMPALA_HOME}/bin/set-classpath.sh
export ASAN_OPTIONS="handle_segv=0 detect_leaks=0 allocator_may_return_null=1"
export UBSAN_OPTIONS="print_stacktrace=1"
UBSAN_OPTIONS="${UBSAN_OPTIONS} suppressions=${IMPALA_HOME}/bin/ubsan-suppressions.txt"
export TSAN_OPTIONS="halt_on_error=0 history_size=7"
export PATH="${IMPALA_TOOLCHAIN}/llvm-${IMPALA_LLVM_VERSION}/bin:${PATH}"
exec ${TOOL_PREFIX} ${IMPALA_CMD} ${IMPALAD_ARGS}
