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

# This script will send a signal to all impalad processes as identified by their name to
# initiate a graceful shutdown. It will then wait for a configurable amount time to allow
# the daemons to shutdown. The script will not forcefully terminate them, which is left to
# the orchestration framework.

set -euxo pipefail

export IMPALA_HOME=${IMPALA_HOME:-"/opt/impala"}
LOG_FILE="$IMPALA_HOME/logs/shutdown.log"

function LOG() {
  # IMPALA-10006: append to LOG_FILE if possible, but don't fail if the log file isn't
  # writable.
  echo $* | tee -a $LOG_FILE || true
}

# Default grace timeout is the same as the default for Impala (-shutdown_grace_period_s),
# plus some additional safety buffer.
GRACE_TIMEOUT=$((120 + 10))
if [[ $# -ge 1 ]]; then
  GRACE_TIMEOUT=$1
fi

LOG "Initiating graceful shutdown."
for pid in $(pgrep impalad); do
  LOG "Sending signal to daemon with pid $pid"
  kill -SIGRTMIN $pid
done

LOG "Waiting for daemons to exit, up to $GRACE_TIMEOUT s."
for ((i=0; i<$GRACE_TIMEOUT; ++i)); do
  pids=$(pgrep impalad || true)
  if [[ -z "$pids" ]]; then
    LOG "All daemons have exited after $i s."
    break
  else
    LOG "Daemons still running after $i s: $pids"
  fi
  sleep 1
done

LOG "Graceful shutdown process complete."
