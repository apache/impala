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
. "$IMPALA_HOME/bin/report_build_error.sh"
setup_report_build_error

: ${ARCH_NAME:=$(uname -p)}
if [[ $ARCH_NAME == aarch64 ]]; then
  NODEJS_DISTRO=linux-arm64
elif [[ $ARCH_NAME == x86_64 ]]; then
  NODEJS_DISTRO=linux-x64
else
  echo "This script supports Intel x86_64 or ARM aarch64 CPU architectures only." >&2
  echo "Current CPU type is reported as '$ARCH_NAME'" >&2
  # report the installation failure as a JUnit symptom
  "${IMPALA_HOME}"/bin/generate_junitxml.py --phase JS_TEST \
      --step "node.js installation" \
      --error "Unknown CPU architecture $ARCH_NAME encountered."
  exit 1
fi

: ${IMPALA_TOOLCHAIN_HOST:=native-toolchain.s3.amazonaws.com}
: ${IMPALA_NODEJS_VERSION:=v16.20.2}
: ${IMPALA_TOOLCHAIN:="$IMPALA_HOME/toolchain"}

IMPALA_NODEJS_LIB="${IMPALA_TOOLCHAIN}/node-${IMPALA_NODEJS_VERSION}"
CACHED_NODEJS_PATH="${HOME}/.cache/impala_nodejs"

# Install nodejs locally, if not installed
if [ ! -r "${IMPALA_NODEJS_LIB}/bin/node" ]; then
  echo "Fetching NodeJS ${IMPALA_NODEJS_VERSION}-${NODEJS_DISTRO} binaries ..."

  NODE_URL_SUFFIX="node-${IMPALA_NODEJS_VERSION}-${NODEJS_DISTRO}.tar.xz"
  CACHED_NODEJS_TARBALL="${CACHED_NODEJS_PATH}/${NODE_URL_SUFFIX}"

  if [ ! -r "${CACHED_NODEJS_TARBALL}" ]; then
    NODE_URL_PREFIX="https://${IMPALA_TOOLCHAIN_HOST}/mirror/nodejs"

    mkdir -p "$CACHED_NODEJS_PATH"

    curl "${NODE_URL_PREFIX}/${NODE_URL_SUFFIX}" -o "${CACHED_NODEJS_TARBALL}"
  fi

  tar -xJf "${CACHED_NODEJS_TARBALL}" -C ./

  mkdir -p "${IMPALA_NODEJS_LIB}"

  mv node-${IMPALA_NODEJS_VERSION}-${NODEJS_DISTRO}/* -t "${IMPALA_NODEJS_LIB}"

  rm -rf node-${IMPALA_NODEJS_VERSION}-${NODEJS_DISTRO}.tar.xz \
      node-${IMPALA_NODEJS_VERSION}-${NODEJS_DISTRO}/
fi;

echo "NodeJS installation found in :"
echo "${IMPALA_NODEJS_LIB}/bin"