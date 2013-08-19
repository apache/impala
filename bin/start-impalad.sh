#!/bin/sh
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

# Starts up an impalad or a mini-impala-cluster with the specified command line
# arguments. An optional -build_type parameter can be passed to determine the build
# type to use for the impalad instance.

set -e
set -u

BUILD_TYPE=debug
IMPALAD_ARGS=""
BINARY_BASE_DIR=${IMPALA_HOME}/be/build
IN_PROCESS=false

# Everything except for -build_type should be passed as an Impalad argument
for ARG in $*
do
  case "$ARG" in
    -build_type=debug)
      BUILD_TYPE=debug
      ;;
    -build_type=release)
      BUILD_TYPE=release
      ;;
    -build_type=*)
      echo "Invalid build type. Valid values are: debug, release"
      exit 1
      ;;
    -in-process)
      IN_PROCESS=true
      ;;
    *)
      IMPALAD_ARGS="${IMPALAD_ARGS} ${ARG}"
  esac
done

. ${IMPALA_HOME}/bin/set-classpath.sh
if $IN_PROCESS; then
  exec ${BINARY_BASE_DIR}/${BUILD_TYPE}/testutil/mini-impala-cluster ${IMPALAD_ARGS}
else
  exec ${BINARY_BASE_DIR}/${BUILD_TYPE}/service/impalad ${IMPALAD_ARGS}
fi
