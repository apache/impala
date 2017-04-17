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

REMOTE_TOOLCHAIN_DIR=/opt/Impala-Toolchain

if $IMPALA_DISTCC_LOCAL; then
  if [[ "$CXX_COMPILER" == "$IMPALA_TOOLCHAIN"* ]]; then
    # Remap the local toolchain path to the remote toolchain path.
    CXX_COMPILER=$REMOTE_TOOLCHAIN_DIR/${CXX_COMPILER#$IMPALA_TOOLCHAIN}
  fi
  CMD="distcc ccache $CXX_COMPILER"
else
  CMD="$CXX_COMPILER"
fi

exec $CMD "$@"
