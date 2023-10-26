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

# Wrapper script that runs the command-line provided as its arguments after
# setting up the environment required for utilities like impala-profile-tool
# to run.

export IMPALA_HOME=/opt/impala

# Since the Java location includes the CPU architecture, use a glob to find Java home
export JAVA_HOME=$(compgen -G "/usr/lib/jvm/java-8-openjdk*")

# Add directories containing dynamic libraries required by the daemons that
# are not on the system library paths.
export LD_LIBRARY_PATH=/opt/impala/lib

# Given JAVA_HOME, find libjvm.so and add them to LD_LIBRARY_PATH.
# JAVA_HOME could be a symlink, so follow symlinks when looking for the libraries
LIB_JVM_DIR=$(find -L "${JAVA_HOME}" -name libjvm.so | head -1 | xargs dirname)
if [[ -z $LIB_JVM_DIR ]]; then
  echo "ERROR: Could not find libjvm.so in ${JAVA_HOME}"
  exit 1
fi
LD_LIBRARY_PATH+=:${LIB_JVM_DIR}

echo "LD_LIBRARY_PATH: $LD_LIBRARY_PATH"

# Set ulimit core file size 0.
ulimit -c 0

exec "$@"
