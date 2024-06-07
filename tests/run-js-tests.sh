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

: ${IMPALA_JS_TEST_LOGS_DIR:="${IMPALA_LOGS_DIR}/js_tests"}

NODEJS_VERSION=v16.20.2
# ARCH_NAME is set in bin/impala-config.sh
if [[ $ARCH_NAME == aarch64 ]]; then
  NODEJS_DISTRO=linux-arm64
elif [[ $ARCH_NAME == x86_64 ]]; then
  NODEJS_DISTRO=linux-x64
else
  echo "This script supports Intel x86_64 or ARM aarch64 CPU architectures only." >&2
  echo "Current CPU type is reported as $ARCH_NAME" >&2
  # report the installation failure as a JUnit symptom
  "${IMPALA_HOME}"/bin/generate_junitxml.py --phase JS_TEST \
      -- step "node.js installation" \
      --error "Unknown CPU architecture $ARCH_NAME encountered."
  exit 1
fi

NODEJS_LIB_PATH="${IMPALA_TOOLCHAIN}/node-${NODEJS_VERSION}"
export IMPALA_NODEJS="${NODEJS_LIB_PATH}/bin/node"
NPM="${NODEJS_LIB_PATH}/bin/npm"
JS_TESTS_DIR="${IMPALA_HOME}/www/scripts/tests"

export IMPALA_JS_TEST_LOGS_DIR;

# Install nodejs locally, if not installed
if [ -r "$IMPALA_NODEJS" ]; then
  echo "NodeJS ${NODEJS_VERSION} installation found";
else
  echo "Fetching NodeJS ${NODEJS_VERSION}-${NODEJS_DISTRO} binaries ...";
  NODE_URL_PREFIX="https://${IMPALA_TOOLCHAIN_HOST}/mirror/nodejs"
  NODE_URL_SUFFIX="node-${NODEJS_VERSION}-${NODEJS_DISTRO}.tar.xz"
  curl "${NODE_URL_PREFIX}/${NODE_URL_SUFFIX}" -O

  tar -xJf node-${NODEJS_VERSION}-${NODEJS_DISTRO}.tar.xz

  mkdir -p "${NODEJS_LIB_PATH}"

  mv node-${NODEJS_VERSION}-${NODEJS_DISTRO}/* -t "${NODEJS_LIB_PATH}";

  rm -rf node-${NODEJS_VERSION}-${NODEJS_DISTRO}.tar.xz \
      node-${NODEJS_VERSION}-${NODEJS_DISTRO}/
fi;

# Install packages in package.json
"$IMPALA_NODEJS" "$NPM" --prefix "${JS_TESTS_DIR}" install

# Run all JEST testing suites (by default *.test.js)
"$IMPALA_NODEJS" "$NPM" --prefix "${JS_TESTS_DIR}" test
