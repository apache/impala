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

EXPECTED_VERSION=3
if [[ -z "${IMPALA_DISTCC_ENV_VERSION:-}" || \
      "$IMPALA_DISTCC_ENV_VERSION" -ne $EXPECTED_VERSION ]]; then
  echo "Expected IMPALA_DISTCC_ENV_VERSION=$EXPECTED_VERSION, re-source distcc_env.sh"
  exit 1
fi

REMOTE_TOOLCHAIN_DIR=/opt/Impala-Toolchain

if $IMPALA_DISTCC_LOCAL; then
  # We expect the distcc server to have the same directory structure under its toolchain
  # directory as a regular Impala install. This replaces IMPALA_TOOLCHAIN rather
  # than IMPALA_TOOLCHAIN_PACKAGES_HOME, because IMPALA_TOOLCHAIN_PACKAGES_HOME is a
  # subdirectory of IMPALA_TOOLCHAIN. The same directory should exist on the distcc
  # server, so that part of the path should be preserved.
  if [[ "$CXX_COMPILER" == "$IMPALA_TOOLCHAIN"* ]]; then
    # Remap the local toolchain path to the remote toolchain path.
    CXX_COMPILER=$REMOTE_TOOLCHAIN_DIR/${CXX_COMPILER#$IMPALA_TOOLCHAIN}
  fi
  CMD="distcc ccache $CXX_COMPILER"
else
  CMD="$CXX_COMPILER"
fi

# Disable CCACHE_PREFIX to avoid recursive distcc invocations.
CCACHE_PREFIX="" exec $CMD "$@"
