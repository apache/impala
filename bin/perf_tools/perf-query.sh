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

# This script measures the performance of a query, or a sequence of queries.
# It visualizes the stack traces on a flame graph
# (https://www.brendangregg.com/flamegraphs.html).
#
# It uses 'perf' and measures all processes in the system while the query
# is executing, not just Impala.
#
# PREREQUISITIES:
# * In directory ${IMPALA_TOOLCHAIN}: git clone https://github.com/brendangregg/FlameGraph
# * Install 'perf' on your system (e.g. 'apt install linux-tools-generic')
#
# Usage:
# perf-query.sh "<query>"
# E.g.:
# perf-query.sh "select count(*) from tpch.lineitem group by l_returnflag"
# perf-query.sh "set MT_DOP=10; select count(*) from tpch.lineitem group by l_returnflag"
#
# Possible problems:
# Sometimes 'perf record -g' is not able to correctly identify all the stack frames, hence
# we cannot collapse everything together. Instead of '-g' we can use '--call-graph dwarf'
# which does a better job, but later 'perf script' takes much longer time for
# postprocessing the data. Interestingly 'perf-record.sh' doesn't seem to have this
# problem.

FLAME_GRAPH_DIR=${IMPALA_TOOLCHAIN}/FlameGraph

if [ ! -d ${FLAME_GRAPH_DIR} ]; then
  echo "ERROR: ${FLAME_GRAPH_DIR} does not exist."
  echo "Please use 'git clone https://github.com/brendangregg/FlameGraph' in ${IMPALA_TOOLCHAIN}"
  exit 1
fi

PATH="${FLAME_GRAPH_DIR}:${PATH}"

if [ $# -eq 0 ]; then
  echo "Please provide a query as an argument."
  exit 1
fi


# We invoke 'sudo perf' in the background so let's do a blocking sudo now.
sudo echo "test sudo"

# Sample CPU stack traces (-g: via frame pointers) for the entire system, at 99 Hertz.
sudo perf record -F 99 -g -a &
perf_pid=$!

${IMPALA_HOME}/bin/impala-shell.sh -q "$1"

# Send interrupt to 'perf record'. We need to issue 'kill' in a new session/process
# group via 'setsid', otherwise 'perf record' won't get the signal (because it's
# running with sudo).
sudo setsid kill -s INT ${perf_pid}
wait ${perf_pid}

if [ ! -f perf.data ]; then
  echo "ERROR: 'perf.data' has not been generated."
  exit 1
fi

# Create flame graph
sudo perf script | stackcollapse-perf.pl > out.perf-folded
flamegraph.pl out.perf-folded > perf_query.svg

echo "Flame graph has been written to 'perf_query.svg'"

# Open firefox to render the flame graph
firefox perf_query.svg 2>/dev/null

