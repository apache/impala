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

# This script samples the CPU stack traces for the entire system, or
# for a specific PID until it gets an interrupt signal (Ctrl+C).
# It visualizes the stack traces on a flame graph:
# https://www.brendangregg.com/flamegraphs.html
#
# It can be useful if we want to figure out what Impala is doing currently.
#
# PREREQUISITIES:
# * In directory ${IMPALA_TOOLCHAIN}: git clone https://github.com/brendangregg/FlameGraph
# * Install 'perf' on your system (e.g. 'apt install linux-tools-generic')
#
# Usage:
# perf-record.sh # records events for the whole system until Ctrl+C is pressed
# or
# perf-record.sh <pid> # records perf event for specific process until Ctrl+C is pressed


FLAME_GRAPH_DIR=${IMPALA_TOOLCHAIN}/FlameGraph

if [ ! -d ${FLAME_GRAPH_DIR} ]; then
  echo "ERROR: ${FLAME_GRAPH_DIR} does not exist."
  echo "Please use 'git clone https://github.com/brendangregg/FlameGraph' in ${IMPALA_TOOLCHAIN}"
  exit 1
fi

PATH="${FLAME_GRAPH_DIR}:${PATH}"

# We trap the interrupt signal, so only 'perf record' gets interrupted while this script
# continues, so it can create the flame graph.
trap ctrl_c INT
function ctrl_c() {
  echo "Trapped Ctrl+C"
}

# Measure whole system if no args given. Otherwise we expect a pid as argument.
if [ $# -eq 0 ]; then
  echo "This script is going to record perf events for the whole system."
  PERF_ARGS=-a
else
  if ps -p $1 >/dev/null 2>&1; then
    echo "This script is going to record perf events for pid=$1"
    PERF_ARGS="-p $1"
  else
    echo "ERROR: Process with pid=$1 does not exist."
    exit 1
  fi
fi

echo "perf record started... Hit Ctrl+C to stop sampling."
# Sample CPU stack traces at 99 Hertz
sudo perf record -F 99 -g ${PERF_ARGS}

if [ ! -f perf.data ]; then
  echo "ERROR: 'perf.data' has not been generated."
  exit 1
fi

# Create the flame graph
sudo perf script | stackcollapse-perf.pl > out.perf-folded
flamegraph.pl out.perf-folded > perf_record.svg

echo "Flame graph has been written to 'perf_record.svg'."

# Render the flame graph in browser
firefox perf_record.svg 2>/dev/null
