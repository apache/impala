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

if [[ -z "$JAVA_HOME" ]]; then
  echo "JAVA_HOME not set!"
  exit 1
fi

echo "Using JAVA_HOME: $JAVA_HOME"
LIB_JVM_DIR=$(dirname $(find $JAVA_HOME -type f -name libjvm.so))
LIB_JSIG_DIR=$(dirname $(find $JAVA_HOME -type f -name libjsig.so))

export LC_ALL=en_US.utf8
export LD_LIBRARY_PATH="/opt/impala/lib/:$LIB_JVM_DIR:$LIB_JSIG_DIR"
export CLASSPATH="/opt/impala/conf:/opt/impala/jar/*"

#TODO: Add graceful shutdown for impalads
function stop_process {
  name=$1
  pid_file="/tmp/${name}.pid"
  if [[ -f $pid_file ]]; then
    PID=$(cat $pid_file)
    if ps $PID | grep $name; then
      echo "Killing $name with PID=$PID"
      kill $PID
      rm $pid_file
      echo "Killed $name"
    else
      rm $pid_file
      echo "Already stopped: $name is not running with PID=$PID. Removed stale $pid_file"
    fi
  else
    echo "PID file $pid_file not found!"
  fi
}

function wait_for_ready {
  name=$1
  port=$2
  pid=$3

  NUM_WAIT_ITERATIONS=20
  i=0
  while [[ $i -lt $NUM_WAIT_ITERATIONS ]]; do
    if ! ps $pid | grep $name > /dev/null 2>&1; then
      echo "$name with PID $pid doesn't exist"
      break
    fi
    STATUS=$(curl -s http://localhost:$port/healthz)
    if [[ $? != 0 ]]; then
      echo "Waiting for $name. Port $port not ready."
    elif [[ "$STATUS" != "OK" ]]; then
      echo "Waiting for $name to be ready"
    else
      echo "$name is ready"
      break
    fi
    sleep 2
    i=$((i+1))
  done
  if [[ "$STATUS" == "OK" ]]; then
    echo "Launched $name with PID $pid"
  elif [[ $i -eq $NUM_WAIT_ITERATIONS ]]; then
    echo "Timed out waiting for $name to be ready. Check logs for more details."
  else
    echo "Failed to launch $name"
    exit 1
  fi
}
