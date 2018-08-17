#!/bin/bash
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

set -euo pipefail
. $IMPALA_HOME/bin/report_build_error.sh
setup_report_build_error

CLASSES=()
EXTRA_SHUTDOWN_TIME_SECS=1

while getopts :c:s: OPTION; do
  case $OPTION in
    c) CLASSES+=($OPTARG);;
    s) EXTRA_SHUTDOWN_TIME_SECS=$OPTARG;;
    *) echo "Usage: $0 -c <java class name> [-c ...] " \
          "[-s <wait time after stopping all processes>]"
      exit 1;;
  esac
done

if [[ ${#CLASSES[@]} -eq 0 ]]; then
  echo At least one class must be given >&2
  exit 1
fi

function pid_is_running {
  kill -0 $1 &>/dev/null
}

# Waits for 3 seconds for a pid to stop. Returns success if the pid is stopped otherwise
# returns failure.
function wait_for_pid_to_stop {
  for I in {1..30}; do
    if ! pid_is_running $1; then
      return
    fi
    sleep 0.1
  done
  return 1
}

NEEDS_EXTRA_WAIT=false
for CLASS in ${CLASSES[@]}; do
  PID=$(jps -m | (grep $CLASS || true) | awk '{print $1}')
  if [[ -z $PID ]]; then
    continue
  fi
  kill $PID || true   # Don't error if the process somehow died on its own.
  NEEDS_EXTRA_WAIT=true
  if wait_for_pid_to_stop $PID; then
    continue
  fi
  kill -9 $PID || true
  if ! wait_for_pid_to_stop $PID; then
    echo Unable to stop process $PID running java class $CLASS >&2
    exit 1
  fi
done
if $NEEDS_EXTRA_WAIT; then
  sleep $EXTRA_SHUTDOWN_TIME_SECS
fi
