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
#
# run-step helper function used by multiple scripts. To use in a bash script, just
# source this file.

# Function to run a build step that logs output to a file and only
# outputs if there is an error.
# Usage: run-step <step description> <log file name> <cmd> <arg1> <arg2> ...
# LOG_DIR must be set to a writable directory for logs.

function run-step {
  local MSG=$1
  shift
  local LOG_FILE_NAME=$1
  shift

  if [ ! -d "${LOG_DIR}" ]; then
    echo "LOG_DIR must be set to a valid directory: ${LOG_DIR}"
    return 1
  fi
  local LOG=${LOG_DIR}/${LOG_FILE_NAME}

  echo "${MSG} (logging to ${LOG})... "
  echo "Log for command '$@'" > ${LOG}
  START_TIME=$SECONDS
  if ! "$@" >> ${LOG} 2>&1 ; then
    ELAPSED_TIME=$(($SECONDS - $START_TIME))
    echo "    FAILED (Took: $(($ELAPSED_TIME/60)) min $(($ELAPSED_TIME%60)) sec)"
    echo "    '$@' failed. Tail of log:"
    tail -n100 ${LOG}

    # Also print the data-loading log files
    if [ -n "$(grep 'Error executing.*.log' ${LOG})" ]; then
      grep "Error executing.*.log" ${LOG} | while read -r ERROR_LINE; do
        SQL_LOG_FILE=$(echo $ERROR_LINE | awk '{print $NF}')
        echo "------------------------------------------------------------------------"
        echo "    Tail of $SQL_LOG_FILE:"
        tail -n100 ${SQL_LOG_FILE}
      done
    fi
    return 1
  fi
  ELAPSED_TIME=$(($SECONDS - $START_TIME))
  echo "  ${MSG} OK (Took: $(($ELAPSED_TIME/60)) min $(($ELAPSED_TIME%60)) sec)"
}

# Array to manage background tasks.
declare -a RUN_STEP_PIDS
declare -a RUN_STEP_MSGS

# Runs the given step in the background. Many tasks may be started in the
# background, and all of them must be joined together with run-step-wait-all.
# No dependency management or maximums on number of tasks are provided.
function run-step-backgroundable {
  MSG="$1"
  run-step "$@" &
  local pid=$!
  echo "Started ${MSG} in background; pid $pid."
  RUN_STEP_PIDS+=($pid)
  RUN_STEP_MSGS+=("${MSG}")
}

# Wait for all tasks that were run with run-step-backgroundable.
# Fails if any of the background tasks has failed. Clears $RUN_STEP_PIDS.
function run-step-wait-all {
  local ret=0
  for idx in "${!RUN_STEP_PIDS[@]}"; do
    pid="${RUN_STEP_PIDS[$idx]}"
    msg="${RUN_STEP_MSGS[$idx]}"

    if ! wait $pid; then
      ret=1
      echo "Background task $msg (pid $pid) failed."
    fi
  done
  RUN_STEP_PIDS=()
  RUN_STEP_MSGS=()
  return $ret
}
