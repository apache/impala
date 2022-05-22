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
# setting up the environment required for the daemon processes to run.
#
# Supported environment variables:
# JAVA_TOOL_OPTIONS: additional options passed to any embedded JVMs. Can be used, e.g.
#                    to set a max heap size with JAVA_TOOL_OPTIONS="-Xmx4g".

export IMPALA_HOME=/opt/impala

# Add directories containing dynamic libraries required by the daemons that
# are not on the system library paths.
export LD_LIBRARY_PATH=/opt/impala/lib

# This can get more detailed if there are specific steps
# for specific versions, but at the moment the distribution
# is all we need.
DISTRIBUTION=Unknown
if [[ -f /etc/redhat-release ]]; then
  echo "Identified Redhat image."
  DISTRIBUTION=Redhat
else
  source /etc/lsb-release
  if [[ $DISTRIB_ID == Ubuntu ]]; then
    echo "Identified Ubuntu image."
    DISTRIBUTION=Ubuntu
  fi
fi

if [[ $DISTRIBUTION == Unknown ]]; then
  echo "ERROR: Did not detect supported distribution."
  echo "Only Ubuntu and Redhat-based distributions are supported."
  exit 1
fi

# Need libjvm.so and libjsig.so on LD_LIBRARY_PATH
# Ubuntu and Redhat use different locations for JAVA_HOME.
JAVA_HOME=Unknown
if [[ $DISTRIBUTION == Ubuntu ]]; then
  JAVA_HOME=$(compgen -G /usr/lib/jvm/java-8-openjdk-*)
elif [[ $DISTRIBUTION == Redhat ]]; then
  JAVA_HOME=/usr/lib/jvm/jre-1.8.0
fi

if [[ $JAVA_HOME == Unknown ]]; then
  echo "ERROR: Did not find Java in any expected location."
  echo "Only Java 8 and Java 11 are supported."
  exit 1
fi

echo "JAVA_HOME: ${JAVA_HOME}"
# Given JAVA_HOME, find libjsig.so and libjvm.so and add them to LD_LIBRARY_PATH.
# JAVA_HOME could be a symlink, so follow symlinks when looking for the libraries
LIB_JSIG_DIR=$(find -L "${JAVA_HOME}" -name libjsig.so | head -1 | xargs dirname)
if [[ -z $LIB_JSIG_DIR ]]; then
  echo "ERROR: Could not find libjsig.so in ${JAVA_HOME}"
  exit 1
fi
LIB_JVM_DIR=$(find -L "${JAVA_HOME}" -name libjvm.so |head -1 | xargs dirname)
if [[ -z $LIB_JVM_DIR ]]; then
  echo "ERROR: Could not find libjvm.so in ${JAVA_HOME}"
  exit 1
fi
LD_LIBRARY_PATH+=:${LIB_JSIG_DIR}:${LIB_JVM_DIR}

# Add directory with optional plugins that can be mounted for the container.
LD_LIBRARY_PATH+=:/opt/impala/lib/plugins

# Configs should be first on classpath
export CLASSPATH=/opt/impala/conf
# Append all of the jars in /opt/impala/lib to the classpath.
for jar in /opt/impala/lib/*.jar
do
  CLASSPATH+=:$jar
done
echo "CLASSPATH: $CLASSPATH"
echo "LD_LIBRARY_PATH: $LD_LIBRARY_PATH"

# Default to 2GB heap. Allow overriding by externally-set JAVA_TOOL_OPTIONS.
export JAVA_TOOL_OPTIONS="-Xmx2g $JAVA_TOOL_OPTIONS"

# Various Hadoop libraries depend on having a username. If we're running under
# an unknown username, create an entry in the password file for this user.
if ! whoami ; then
  export USER=${HADOOP_USER_NAME:-dummyuser}
  echo "${USER}:x:$(id -u):$(id -g):,,,:/opt/impala:/bin/bash" >> /etc/passwd
  whoami
  cat /etc/passwd
fi

# IMPALA-10006: avoid cryptic failures if log dir isn't writable.
LOG_DIR=$IMPALA_HOME/logs
if [[ ! -w "$LOG_DIR" ]]; then
  echo "$LOG_DIR is not writable"
  exit 1
fi

# Set ulimit core file size 0.
ulimit -c 0

# Set a UTF-8 locale to enable upper/lower/initcap functions with UTF-8 mode.
export LC_ALL=C.UTF-8

exec "$@"
