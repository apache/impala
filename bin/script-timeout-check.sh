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
# Helper script that checks every 60 sec if the timeout expired and on timeout prints
# the stacktraces of all impalads (if running) and then finally kills the caller script.
# Takes two required arguments:
# -script_name : name of the caller script (only for debug messages / output filenames)
# -timeout : the timeout in minutes
#
# The way to use it is:
# ${IMPALA_HOME}/bin/script-timeout-check.sh -timeout {TIMEOUT} -script_name {NAME} &
# TIMEOUT_PID=$!
# ... body of script ...
# # Kill the spawned timeout process and its child sleep process. There may not be
# # a sleep process, so ignore failure.
# pkill -P $TIMEOUT_PID || true
# kill $TIMEOUT_PID

SCRIPT_NAME=""
SLEEP_TIMEOUT_MIN=""

# Parse commandline options
while [ -n "$*" ]
do
  case $1 in
    -timeout)
        SLEEP_TIMEOUT_MIN=${2-}
        shift;
        ;;
    -script_name)
        SCRIPT_NAME=${2-}
        shift;
        ;;
    -help|-h|*)
        echo "script-timeout-check.sh : aborts caller script if timeout expires"
        echo "[-timeout] : The timeout in minutes (required)"
        echo "[-script_name] : The name of the caller script (required)"
        exit 1;
        ;;
  esac
  shift;
done

if [ -z "$SLEEP_TIMEOUT_MIN" ]; then
  echo "Must pass a -timeout flag with a valid timeout as an argument"
  exit 1
fi;

if [ -z "$SCRIPT_NAME" ]; then
  echo "Must pass a -script_name flag with an appropriate argument"
  exit 1
fi;

SLEEP_TIMEOUT_S=$((${SLEEP_TIMEOUT_MIN} * 60))

[[ $SLEEP_TIMEOUT_S < 1 ]] && exit

echo
echo
echo "**** Timeout Timer Started (pid $$, ppid $PPID) for $SLEEP_TIMEOUT_S s! ****"
echo
echo

# Check timer every 60 seconds and only proceed if the parent process is still alive.
# Note: $SECONDS is a bash built-in that counts seconds since bash started.
while ((SLEEP_TIMEOUT_S - SECONDS > 0)); do
  sleep 1
  if ! ps $PPID &> /dev/null; then
    echo "Timeout Timer Exited because $SCRIPT_NAME PID $PPID is gone."
    exit
  fi
done

echo
echo
echo "**** ${SCRIPT_NAME} TIMED OUT! ****"
echo
echo

# Impala might have a thread stuck. Dumps the stacktrace for diagnostic.
"${IMPALA_HOME}"/bin/dump-stacktraces.sh

# Now kill the caller
kill $PPID

"${IMPALA_HOME}"/bin/generate_junitxml.py --step "${SCRIPT_NAME}" \
--error "Script ${SCRIPT_NAME} timed out. This probably happened due to a hung
thread which can be confirmed by looking at the stacktrace of running impalad
processes at ${IMPALA_TIMEOUT_LOGS_DIR}"

