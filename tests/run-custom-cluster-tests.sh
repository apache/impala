#!/usr/bin/env bash
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
#
# Runs the custom-cluster tests. Must be run after the query tests because any existing
# clusters will be restarted.

set -euo pipefail
trap 'echo Error in $0 at line $LINENO: $(awk "NR == $LINENO" $0)' ERR

# Disable HEAPCHECK for the process failure tests because they can cause false positives.
# TODO: Combine with run-process-failure-tests.sh
export HEAPCHECK=

RESULTS_DIR=${IMPALA_HOME}/tests/custom_cluster/results
mkdir -p ${RESULTS_DIR}
LOG_DIR=${IMPALA_TEST_CLUSTER_LOG_DIR}/custom_cluster/
mkdir -p ${LOG_DIR}

AUX_CUSTOM_DIR=""
if [ -n ${IMPALA_AUX_TEST_HOME} ]; then
    AUX_CUSTOM_DIR=${IMPALA_AUX_TEST_HOME}/tests/aux_custom_cluster_tests/
fi

export LOG_DIR

# KERBEROS TODO We'll want to pass kerberos status in here.
cd ${IMPALA_HOME}/tests
. ${IMPALA_HOME}/bin/set-classpath.sh &> /dev/null
impala-py.test custom_cluster/ authorization/ ${AUX_CUSTOM_DIR} \
    --junitxml="${RESULTS_DIR}/TEST-impala-custom-cluster.xml" \
    --resultlog="${RESULTS_DIR}/TEST-impala-custom-cluster.log" "$@"
