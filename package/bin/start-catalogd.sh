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
# Script to launch catalogd. Required JAVA_HOME being set.
# Edit conf/catalogd_flags to set the correct hostname and state_store_host.
# Edit core-site.xml, hdfs-site.xml, hive-site.xml, etc. in conf based on the cluster.
# Example usage:
#   export JAVA_HOME=/usr/java/jdk1.8.0_232-cloudera
#   bin/start-catalogd.sh
# To launch catalogd using another username (e.g. "impala"):
#   sudo -E -u impala bin/start-catalogd.sh

echo "Using IMPALA_HOME: ${IMPALA_HOME:=/opt/impala}"
source $IMPALA_HOME/bin/impala-env.sh
$IMPALA_HOME/bin/catalogd --flagfile=$IMPALA_HOME/conf/catalogd_flags &
PID=$!
echo $PID > /tmp/catalogd.pid

# Sleep 1s so the glog output won't be messed up with waiting messages
sleep 1

wait_for_ready catalogd 25020 $PID
