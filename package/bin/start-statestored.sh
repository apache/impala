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
# Script to launch statestore. Required JAVA_HOME being set.
# Edit conf/statestore_flags to set a correct hostname.
# Example usage:
#   export JAVA_HOME=/usr/java/jdk1.8.0_232-cloudera
#   bin/start-statestored.sh
# To launch statestore using another username (e.g. "impala"):
#   sudo -E -u impala bin/start-statestored.sh

echo "Using IMPALA_HOME: ${IMPALA_HOME:=/opt/impala}"
source $IMPALA_HOME/bin/impala-env.sh
$IMPALA_HOME/bin/statestored --flagfile=$IMPALA_HOME/conf/statestore_flags &
PID=$!
echo $PID > /tmp/statestored.pid

# Sleep 1s so the glog output won't be messed up with waiting messages
sleep 1

wait_for_ready statestored 25010 $PID
