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

# This script explicitly sets the CLASSPATH for embedded JVMs (e.g. in
# Impalad or in runquery) Because embedded JVMs do not honour
# CLASSPATH wildcard expansion, we have to add every dependency jar
# explicitly to the CLASSPATH.

CLASSPATH=\
"$IMPALA_HOME"/fe/src/test/resources:\
"$IMPALA_HOME"/fe/target/classes:\
"$IMPALA_HOME"/fe/target/dependency:\
"$IMPALA_HOME"/fe/target/test-classes:\
"${HIVE_HOME}"/lib/datanucleus-api-jdo-3.2.1.jar:\
"${HIVE_HOME}"/lib/datanucleus-core-3.2.2.jar:\
"${HIVE_HOME}"/lib/datanucleus-rdbms-3.2.1.jar:

FE_CP_FILE="$IMPALA_HOME/fe/target/build-classpath.txt"

if [ ! -s "$FE_CP_FILE" ]; then
  >&2 echo FE classpath file $FE_CP_FILE missing.
  >&2 echo Build the front-end first.
  exit 1
fi

CLASSPATH=$(cat "$IMPALA_HOME"/fe/target/build-classpath.txt):"$CLASSPATH"

export CLASSPATH
