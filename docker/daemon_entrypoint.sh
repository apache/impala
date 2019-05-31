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
LD_LIBRARY_PATH+=:/usr/lib/jvm/java-8-openjdk-amd64/jre/lib/amd64/
LD_LIBRARY_PATH+=:/usr/lib/jvm/java-8-openjdk-amd64/jre/lib/amd64/server/

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

"$@"
EXIT_CODE=$?

# Print out any INFO logs to help with debugging container startup failures.
# TODO: remove once we have proper logging
cat /tmp/*.INFO
exit $EXIT_CODE
