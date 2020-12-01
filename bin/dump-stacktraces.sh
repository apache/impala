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
# Helper script that dumps the stacktraces of catalogd, statestored, all impalads
# (if running) and Hive Metastore server. Results files are put in
# $IMPALA_TIMEOUT_LOGS_DIR.
#

function collect_gdb_backtraces() {
  name=$1
  pid=$2
  result="${IMPALA_TIMEOUT_LOGS_DIR}/${name}_${pid}_$(date +%Y%m%d-%H%M%S).txt"
  echo "**** Generating backtrace of $name with process id: $pid to $result ****"
  gdb -ex "thread apply all bt" --batch -p $pid >"$result"
}

function collect_jstacks() {
  name=$1
  pid=$2
  result="${IMPALA_TIMEOUT_LOGS_DIR}/${name}_${pid}_jstack_$(date +%Y%m%d-%H%M%S).txt"
  echo "**** Generating jstack of $name with process id: $pid to $result ****"
  $JAVA_HOME/bin/jstack -F $pid >"$result"
}

# Take stacktraces in parallel to get consistent snapshots
WORKER_PIDS=()
mkdir -p "$IMPALA_TIMEOUT_LOGS_DIR"

for pid in $(pgrep impalad); do
  collect_gdb_backtraces impalad $pid && collect_jstacks impalad $pid &
  WORKER_PIDS+=($!)
done

# Catalogd's process name may change. Use 'ps' directly to search the binary name.
CATALOGD_PID=$(ps aux | grep [c]atalogd | awk '{print $2}')
if [[ ! -z $CATALOGD_PID ]]; then
  collect_gdb_backtraces catalogd $CATALOGD_PID && \
      collect_jstacks catalogd $CATALOGD_PID &
  WORKER_PIDS+=($!)
fi

STATESTORED_PID=$(pgrep statestored)
if [[ ! -z $STATESTORED_PID ]]; then
  collect_gdb_backtraces statestored $STATESTORED_PID &
  WORKER_PIDS+=($!)
fi

HMS_PID=$(ps aux | grep [H]iveMetaStore | awk '{print $2}')
if [[ ! -z $HMS_PID ]]; then
  collect_jstacks hms $HMS_PID &
  WORKER_PIDS+=($!)
fi

NAMENODE_PID=$(ps aux | grep [N]ameNode | awk '{print $2}')
if [[ ! -z $NAMENODE_PID ]]; then
  collect_jstacks namenode $NAMENODE_PID &
  WORKER_PIDS+=($!)
fi

for pid in "${WORKER_PIDS[@]}"; do
  wait $pid
done

