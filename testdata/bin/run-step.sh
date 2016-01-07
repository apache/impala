#!/bin/bash
# Copyright 2015 Cloudera Inc.
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

  echo -n "${MSG} (logging to ${LOG_FILE_NAME})... "
  echo "Log for command '$@'" > ${LOG}
  if ! "$@" >> ${LOG} 2>&1 ; then
    echo "FAILED"
    echo "'$@' failed. Tail of log:"
    tail -n50 ${LOG}
    return 1
  fi
  echo OK
}

