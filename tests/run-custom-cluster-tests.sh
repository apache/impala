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
#
# Runs the custom-cluster tests. Must be run after the query tests because any existing
# clusters will be restarted.

set -euo pipefail
. $IMPALA_HOME/bin/report_build_error.sh
setup_report_build_error

# Disable HEAPCHECK for the process failure tests because they can cause false positives.
# TODO: Combine with run-process-failure-tests.sh
export HEAPCHECK=

export LOG_DIR="${IMPALA_CUSTOM_CLUSTER_TEST_LOGS_DIR}"
RESULTS_DIR="${IMPALA_CUSTOM_CLUSTER_TEST_LOGS_DIR}/results"
mkdir -p "${RESULTS_DIR}"

# KERBEROS TODO We'll want to pass kerberos status in here.
cd "${IMPALA_HOME}/tests"
. "${IMPALA_HOME}/bin/set-classpath.sh" &> /dev/null

: ${CLUSTER_TEST_FILES:=}
if [[ "$CLUSTER_TEST_FILES" != "" ]]; then
  ARGS=($CLUSTER_TEST_FILES)
else
  ARGS=(custom_cluster/ authorization/)
fi
AUX_CUSTOM_DIR="${IMPALA_AUX_TEST_HOME}/tests/aux_custom_cluster_tests/"
if [[ -d "${AUX_CUSTOM_DIR}" ]]
then
  ARGS+=("${AUX_CUSTOM_DIR}")
fi
ARGS+=("--junitxml=${RESULTS_DIR}/TEST-impala-custom-cluster.xml")
ARGS+=("--resultlog=${RESULTS_DIR}/TEST-impala-custom-cluster.log")
ARGS+=("$@")

impala-py.test "${ARGS[@]}"
