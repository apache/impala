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
# This detection logic handles Java 8, 11, and 17. Prefers the newest version.
JAVA_HOME=Unknown
if [[ $DISTRIBUTION == Ubuntu ]]; then
  # Since the Java location includes the CPU architecture, use a glob
  # to find Java home
  if compgen -G "/usr/lib/jvm/java-17-openjdk*" ; then
    echo "Detected Java 17"
    JAVA_HOME=$(compgen -G "/usr/lib/jvm/java-17-openjdk*")
  elif compgen -G "/usr/lib/jvm/java-11-openjdk*" ; then
    echo "Detected Java 11"
    JAVA_HOME=$(compgen -G "/usr/lib/jvm/java-11-openjdk*")
  elif compgen -G "/usr/lib/jvm/java-8-openjdk*" ; then
    echo "Detected Java 8"
    JAVA_HOME=$(compgen -G "/usr/lib/jvm/java-8-openjdk*")
  fi
elif [[ $DISTRIBUTION == Redhat ]]; then
  if [[ -d /usr/lib/jvm/jre-17 ]]; then
    echo "Detected Java 17"
    JAVA_HOME=/usr/lib/jvm/jre-17
  elif [[ -d /usr/lib/jvm/jre-11 ]]; then
    echo "Detected Java 11"
    JAVA_HOME=/usr/lib/jvm/jre-11
  elif [[ -d /usr/lib/jvm/jre-1.8.0 ]]; then
    echo "Detected Java 8"
    JAVA_HOME=/usr/lib/jvm/jre-1.8.0
  fi
fi

if [[ $JAVA_HOME == Unknown ]]; then
  echo "ERROR: Did not find Java in any expected location."
  echo "Only Java 8, 11, and 17 are supported."
  exit 1
fi

echo "JAVA_HOME: ${JAVA_HOME}"
export JAVA_HOME
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

echo "JAVA_TOOL_OPTIONS: $JAVA_TOOL_OPTIONS"

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

# graceful_shutdown_backends.sh requires pgrep, so verify it is present
# at startup.
if ! command -v pgrep ; then
  echo "ERROR: 'pgrep' is not present."
  exit 1
fi

# The UTF-8 masking functions rely on the presence of en_US.utf8. Make sure
# it is present.
if locale -a | grep en_US.utf8 ; then
  echo "en_US.utf8 is present"
else
  echo "ERROR: en_US.utf8 locale is not present."
  exit 1
fi

# Set a UTF-8 locale to enable upper/lower/initcap functions with UTF-8 mode.
# Use C.UTF-8 (aka C.utf8) if it is available, and fall back to en_US.utf8 if not
#
# Distributions can show either C.UTF-8 or C.utf8 in "locale -a", match either one
if locale -a | grep -e "^C.UTF-8" -e "^C.utf8" ; then
  # C.UTF-8 and C.utf8 are interchangeable as a setting for LC_ALL.
  export LC_ALL=C.UTF-8
else
  # Presence of en_US.utf8 was verified above
  export LC_ALL=en_US.utf8
fi
echo "LC_ALL: ${LC_ALL}"

exec "$@"
