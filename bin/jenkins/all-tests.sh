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

# Run all Impala tests.

set -euo pipefail
. $IMPALA_HOME/bin/report_build_error.sh
setup_report_build_error

cd "${IMPALA_HOME}"

export IMPALA_MAVEN_OPTIONS="-U"
# Allow unlimited pytest failures
export MAX_PYTEST_FAILURES=0

# When UBSAN_FAIL is "death", the logs are monitored for UBSAN errors. Any errors will
# then cause this script to exit.
#
# When UBSAN_FAIL is "error", monitoring is delayed until tests have finished running.
#
# Any other value ignores UBSAN errors.
: ${UBSAN_FAIL:=error}
export UBSAN_FAIL

if test -v CMAKE_BUILD_TYPE && [[ "${CMAKE_BUILD_TYPE}" =~ 'UBSAN' ]] \
    && [ "${UBSAN_FAIL}" = "death" ]
then
  export PID_TO_KILL="$(echo $$)"
  mkdir -p "${IMPALA_HOME}/logs"

  function killer {
    while ! grep -rI ": runtime error: " "${IMPALA_HOME}/logs"
    do
      sleep 1
      if ! test -e "/proc/$PID_TO_KILL"
      then
        return
      fi
    done
    >&2 echo "Killing process $PID_TO_KILL because it invoked undefined behavior"
    kill -9 $PID_TO_KILL
  }

  killer &
  export KILLER_PID="$(echo $!)"
  disown
  trap "kill -i $KILLER_PID" EXIT
fi

RET_CODE=0
if ! bin/bootstrap_development.sh; then
  RET_CODE=1
fi

source bin/impala-config.sh > /dev/null 2>&1

# Sanity check: bootstrap_development.sh should not have modified any of
# the Impala files. This is important for the continued functioning of
# bin/single_node_perf_run.py.
NUM_MODIFIED_FILES=$(git status --porcelain --untracked-files=no | wc -l)
if [[ "${NUM_MODIFIED_FILES}" -ne 0 ]]; then
  echo "ERROR: Impala source files were modified during bin/bootstrap_development.sh"
  echo "Dumping diff:"
  git status --porcelain --untracked-files=no
  git --no-pager diff
  RET_CODE=1
fi

# Skip tests if build/dataload failed
if [[ $RET_CODE -eq 0 ]]; then
  if ! bin/run-all-tests.sh; then
    RET_CODE=1
  fi
fi

# Always shutdown minicluster at the end and run finalize.sh
testdata/bin/kill-all.sh
bin/jenkins/finalize.sh
exit $RET_CODE
