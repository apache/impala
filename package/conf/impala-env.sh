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
# Specify default value for user-specific environment variables.
# It's recommended to use a form that can be overridden from the command line, like
# ': ${FOO:="bar"}', or 'export FOO=${FOO:-"bar"}' when the variable need to be exported.

# Uncomment and modify next line to specify a valid JDK location.
# : ${JAVA_HOME:="/usr/lib/jvm/java"}

# Specify extra CLASSPATH.
: ${CLASSPATH:=}

# Specify extra LD_LIBRARY_PATH.
: ${LD_LIBRARY_PATH:=}

# Uncomment next line to enable coredump.
# ulimit -c unlimited

# Specify JVM options.
export JAVA_TOOL_OPTIONS=${JAVA_TOOL_OPTIONS:-}

# Specify default pidfile directories.
: ${IMPALAD_PIDFILE:="/tmp/impalad.pid"}
: ${CATALOGD_PIDFILE:="/tmp/catalogd.pid"}
: ${ADMISSIOND_PIDFILE:="/tmp/admissiond.pid"}
: ${STATESTORED_PIDFILE:="/tmp/statestored.pid"}
