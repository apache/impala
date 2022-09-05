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
# Script to launch impalad. Required JAVA_HOME being set.
# Edit conf/impalad_flags to set the correct hostnames.
# Edit core-site.xml, hdfs-site.xml, hive-site.xml, etc. in conf based on the cluster.
# Example usage:
#   export JAVA_HOME=/usr/java/jdk1.8.0_232-cloudera
#   bin/start-impalad.sh
# To launch impalad using another username (e.g. "impala"):
#   sudo -E -u impala bin/start-impalad.sh

echo "Using IMPALA_HOME: ${IMPALA_HOME:=/opt/impala}"
source $IMPALA_HOME/bin/impala-env.sh

if [[ -n "$HADOOP_HOME" ]]; then
  echo "Using HADOOP_HOME: $HADOOP_HOME"
  export HADOOP_LIB_DIR="${HADOOP_HOME}/lib"
  export LIBHDFS_OPTS="${LIBHDFS_OPTS:-} -Djava.library.path=${HADOOP_LIB_DIR}/native/"
  echo "Using hadoop native libs in ${HADOOP_LIB_DIR}/native/"
else
  export LIBHDFS_OPTS="${LIBHDFS_OPTS:-} -Djava.library.path=${IMPALA_HOME}/lib"
  echo "HADOOP_HOME not set. Using hadoop native libs in ${IMPALA_HOME}/lib"
fi

$IMPALA_HOME/bin/impalad --flagfile=$IMPALA_HOME/conf/impalad_flags &
PID=$!
echo $PID > /tmp/impalad.pid

# Sleep 1s so the glog output won't be messed up with waiting messages
sleep 1

wait_for_ready impalad 25000 $PID
