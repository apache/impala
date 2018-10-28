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

source bin/bootstrap_development.sh

RET_CODE=0
if ! bin/run-all-tests.sh; then
  RET_CODE=1
fi

bin/jenkins/finalize.sh
exit $RET_CODE
