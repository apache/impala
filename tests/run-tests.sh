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
# Runs the Impala query tests, first executing the tests that cannot be run in parallel
# and then executing the remaining tests in parallel. Additional command line options
# are passed to py.test.

# If the number of concurrent tests to run is specified, use that. Otherwise use the
# number of CPU cores.

if [ -z "${NUM_CONCURRENT_TESTS}" ]; then
  NUM_CONCURRENT_TESTS=`grep "^processor\s\+:" /proc/cpuinfo | sort -u | wc -l`
fi
set -u

RESULTS_DIR=${IMPALA_HOME}/tests/results
mkdir -p ${RESULTS_DIR}

cd ${IMPALA_HOME}/tests
# First run all the tests that need to be executed serially (namely insert tests)
# TODO: Support different log files for each test directory
py.test -v --tb=short -m "execute_serially" --ignore="failure"\
    --junitxml="${RESULTS_DIR}/TEST-impala-serial.xml" \
    --resultlog="${RESULTS_DIR}/TEST-impala-serial.log" "$@"
EXIT_CODE=$?

# Run the remaining tests in parallel
py.test -v --tb=short -m "not execute_serially" --ignore="failure"\
    --junitxml="${RESULTS_DIR}/TEST-impala-parallel.xml" -n ${NUM_CONCURRENT_TESTS}  \
    --resultlog="${RESULTS_DIR}/TEST-impala-parallel.log" "$@"

# The exit code of this script needs to indicated whether either of the py.test
# executions had any failed tests
if [ $? != 0 ]; then
  EXIT_CODE=$?
fi

cd ~
exit $EXIT_CODE
