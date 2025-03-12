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

# Set JAVA_TOOL_OPTIONS needed by some Hadoop dependencies when running with JDK>=9
# TODO: check if this is still needed once these deps are build with JDK17 (IMPALA-13922)

export IMPALA_JAVA_TOOL_OPTIONS="${IMPALA_JAVA_TOOL_OPTIONS:-}"

if (( IMPALA_JDK_VERSION_NUM > 8 )); then
  echo "JDK >= 9 detected, adding --add-opens to IMPALA_JAVA_TOOL_OPTIONS"
  ADD_OPENS_OPTS=" --add-opens=java.base/java.io=ALL-UNNAMED"
  ADD_OPENS_OPTS+=" --add-opens=java.base/java.lang.invoke=ALL-UNNAMED"
  ADD_OPENS_OPTS+=" --add-opens=java.base/java.lang.module=ALL-UNNAMED"
  ADD_OPENS_OPTS+=" --add-opens=java.base/java.lang.ref=ALL-UNNAMED"
  ADD_OPENS_OPTS+=" --add-opens=java.base/java.lang.reflect=ALL-UNNAMED"
  ADD_OPENS_OPTS+=" --add-opens=java.base/java.lang=ALL-UNNAMED"
  ADD_OPENS_OPTS+=" --add-opens=java.base/java.net=ALL-UNNAMED"
  ADD_OPENS_OPTS+=" --add-opens=java.base/java.nio.charset=ALL-UNNAMED"
  ADD_OPENS_OPTS+=" --add-opens=java.base/java.nio.file.attribute=ALL-UNNAMED"
  ADD_OPENS_OPTS+=" --add-opens=java.base/java.nio=ALL-UNNAMED"
  ADD_OPENS_OPTS+=" --add-opens=java.base/java.security=ALL-UNNAMED"
  ADD_OPENS_OPTS+=" --add-opens=java.base/java.util.concurrent.locks=ALL-UNNAMED"
  ADD_OPENS_OPTS+=" --add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED"
  ADD_OPENS_OPTS+=" --add-opens=java.base/java.util.concurrent=ALL-UNNAMED"
  ADD_OPENS_OPTS+=" --add-opens=java.base/java.util.jar=ALL-UNNAMED"
  ADD_OPENS_OPTS+=" --add-opens=java.base/java.util.regex=ALL-UNNAMED"
  ADD_OPENS_OPTS+=" --add-opens=java.base/java.util.zip=ALL-UNNAMED"
  ADD_OPENS_OPTS+=" --add-opens=java.base/java.util=ALL-UNNAMED"
  ADD_OPENS_OPTS+=" --add-opens=java.base/jdk.internal.loader=ALL-UNNAMED"
  ADD_OPENS_OPTS+=" --add-opens=java.base/jdk.internal.math=ALL-UNNAMED"
  ADD_OPENS_OPTS+=" --add-opens=java.base/jdk.internal.module=ALL-UNNAMED"
  ADD_OPENS_OPTS+=" --add-opens=java.base/jdk.internal.perf=ALL-UNNAMED"
  ADD_OPENS_OPTS+=" --add-opens=java.base/jdk.internal.platform=ALL-UNNAMED"
  ADD_OPENS_OPTS+=" --add-opens=java.base/jdk.internal.platform.cgroupv1=ALL-UNNAMED"
  ADD_OPENS_OPTS+=" --add-opens=java.base/jdk.internal.ref=ALL-UNNAMED"
  ADD_OPENS_OPTS+=" --add-opens=java.base/jdk.internal.reflect=ALL-UNNAMED"
  ADD_OPENS_OPTS+=" --add-opens=java.base/jdk.internal.util.jar=ALL-UNNAMED"
  ADD_OPENS_OPTS+=" --add-opens=java.base/sun.net.www.protocol.jar=ALL-UNNAMED"
  ADD_OPENS_OPTS+=" --add-opens=java.base/sun.nio.ch=ALL-UNNAMED"
  ADD_OPENS_OPTS+=" --add-opens=java.base/sun.nio.fs=ALL-UNNAMED"
  ADD_OPENS_OPTS+=" --add-opens=jdk.dynalink/jdk.dynalink.beans=ALL-UNNAMED"
  ADD_OPENS_OPTS+=" --add-opens=jdk.dynalink/jdk.dynalink.linker.support=ALL-UNNAMED"
  ADD_OPENS_OPTS+=" --add-opens=jdk.dynalink/jdk.dynalink.linker=ALL-UNNAMED"
  ADD_OPENS_OPTS+=" --add-opens=jdk.dynalink/jdk.dynalink.support=ALL-UNNAMED"
  ADD_OPENS_OPTS+=" --add-opens=jdk.dynalink/jdk.dynalink=ALL-UNNAMED"
  ADD_OPENS_OPTS+=" --add-opens=jdk.management.jfr/jdk.management.jfr=ALL-UNNAMED"
  ADD_OPENS_OPTS+=" --add-opens=jdk.management/com.sun.management.internal=ALL-UNNAMED"
  export IMPALA_JAVA_TOOL_OPTIONS="$IMPALA_JAVA_TOOL_OPTIONS $ADD_OPENS_OPTS"
fi

