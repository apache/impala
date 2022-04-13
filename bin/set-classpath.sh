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

if [ "$0" = "$BASH_SOURCE" ]; then
  >&2 echo "error: $BASH_SOURCE should be sourced, not executed. e.g:"
  >&2 echo "   source $BASH_SOURCE"
  exit 1
fi

# IMPALA-10057: The fe/target/classes and fe/target/test-classes
# directory can get cleaned out / rebuilt as part of running the
# frontend tests. This uses jars to avoid issues related to that
# inconsistency.
CLASSPATH=\
"$IMPALA_HOME"/fe/src/test/resources:\
"$IMPALA_HOME"/fe/target/impala-frontend-${IMPALA_VERSION}.jar:\
"$IMPALA_HOME"/fe/target/dependency:\
"$IMPALA_HOME"/fe/target/impala-frontend-${IMPALA_VERSION}-tests.jar:

FE_CP_FILE="$IMPALA_HOME/fe/target/build-classpath.txt"

if [ ! -s "$FE_CP_FILE" ]; then
  >&2 echo FE classpath file $FE_CP_FILE missing.
  >&2 echo Build the front-end first.
  return 1
fi

CLASSPATH=$(cat "$IMPALA_HOME"/fe/target/build-classpath.txt):"$CLASSPATH"

: ${CUSTOM_CLASSPATH=}
CLASSPATH="$CUSTOM_CLASSPATH:$CLASSPATH"

export CLASSPATH
