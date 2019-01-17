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
# Script run by run-all-tests.sh that checks every 60 sec if the timeout expired and on
# timeout prints the stacktraces of all impalads and then finally kills running tests.
# Takes the timeout in minutes as an argument.

SLEEP_TIMEOUT_S=0
if [ -z "$1" ]; then
  echo "Expected timeout value as an argument"
  exit 1
else
  SLEEP_TIMEOUT_S=$(($1 * 60))
fi

[[ $SLEEP_TIMEOUT_S < 1 ]] && exit

echo
echo
echo "**** Timout Timer Started (pid $$, ppid $PPID) for $SLEEP_TIMEOUT_S s! ****"
echo
echo

# Check timer every 60 seconds and only proceed if the parent process is still alive.
# Note: $SECONDS is a bash built-in that counts seconds since bash started.
while ((SLEEP_TIMEOUT_S - SECONDS > 0)); do
  sleep 1
  if ! ps $PPID &> /dev/null; then
    echo "Timeout Timer Exited because $PPID is gone."
    exit
  fi
done

echo
echo
echo '**** Tests TIMED OUT! ****'
echo
echo

# Impala probably has a thread stuck. Print the stacktrace to the console output.
mkdir -p "$IMPALA_TIMEOUT_LOGS_DIR"
for pid in $(pgrep impalad); do
  echo "**** Generating stacktrace of impalad with process id: $pid ****"
  gdb -ex "thread apply all bt"  --batch -p $pid > "${IMPALA_TIMEOUT_LOGS_DIR}/${pid}.txt"
done

# Now kill any running tests.
kill $PPID

"${IMPALA_HOME}"/bin/generate_junitxml.py --step "test_run" --error "Test run timed out.
This probably happened due to a hung thread which can be confirmed by looking at the
stacktrace of running impalad processes at ${IMPALA_TIMEOUT_LOGS_DIR}"


