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

set -euo pipefail

CXX_COMPILER="$1"
shift

if [[ ! -x "$CXX_COMPILER" ]]; then
  echo Configured compiler "$CXX_COMPILER" is not executable.
  exit 1
fi

if [[ -z "$DISTCC_HOSTS" || -z "$CXX_COMPILER" ]]; then
  # This could be sourced here and the build would work but the parallelization (-j)
  # should be wrong at this point and it's too late to fix.
  DIR=$(dirname "$0")
  echo "You must source '$DIR/distcc_env.sh' before attempting to build." 1>&2
  exit 1
fi

TOOLCHAIN_DIR=/opt/Impala-Toolchain
if [[ ! -d "$TOOLCHAIN_DIR" ]]; then
  if [[ -n "$IMPALA_TOOLCHAIN" && -d "$IMPALA_TOOLCHAIN" ]]; then
    if ! sudo -n -- ln -s "$IMPALA_TOOLCHAIN" "$TOOLCHAIN_DIR" &>/dev/null; then
      echo The toolchain must be available at $TOOLCHAIN_DIR for distcc. \
          Try running '"sudo ln -s $IMPALA_TOOLCHAIN $TOOLCHAIN_DIR"'. 1>&2
      exit 1
    fi
  fi
  echo "The toolchain wasn't found at '$TOOLCHAIN_DIR' and IMPALA_TOOLCHAIN is not set." \
      Make sure the toolchain is available at $TOOLCHAIN_DIR and try again. 1>&2
  exit 1
fi

CMD=
CMD_POST_ARGS=
if $IMPALA_DISTCC_LOCAL; then
  CMD="distcc ccache"
fi

CMD+=" $CXX_COMPILER"

exec $CMD "$@" $CMD_POST_ARGS
