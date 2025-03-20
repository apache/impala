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

. "$IMPALA_HOME/bin/nodejs/setup_nodejs.sh"

NODEJS_BIN_PATH="${IMPALA_TOOLCHAIN}/node-${IMPALA_NODEJS_VERSION}/bin"
NPM="${NODEJS_BIN_PATH}/npm"
JS_TESTS_DIR="${IMPALA_HOME}/tests/webui/js_tests"
export IMPALA_NODEJS="${NODEJS_BIN_PATH}/node"

# Install packages in package.json
"$IMPALA_NODEJS" "$NPM" --prefix "${JS_TESTS_DIR}" install

# Run all JEST testing suites (by default *.test.js)
NODE_PATH="${IMPALA_HOME}/www/" "$IMPALA_NODEJS" "$NPM" --prefix "${JS_TESTS_DIR}" test