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
#
# Runs the Impala process failure tests.

# Disable HEAPCHECK for the process failure tests because they can cause false positives.
export HEAPCHECK=
set -u

RESULTS_DIR=${IMPALA_HOME}/tests/results
mkdir -p ${RESULTS_DIR}

pushd ${IMPALA_HOME}/tests
. ${IMPALA_HOME}/bin/set-classpath.sh &> /dev/null
impala-py.test experiments/test_process_failures.py \
    --junitxml="${RESULTS_DIR}/TEST-impala-proc-failure.xml" \
    --resultlog="${RESULTS_DIR}/TEST-impala-proc-failure.log" "$@"
EXIT_CODE=$?
popd
exit $EXIT_CODE
