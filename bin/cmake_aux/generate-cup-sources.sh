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
# Utility script to generate the CUP parsing sources. Since there is
# only one CUP file, this hard codes all the files and locations
# rather than taking them in as arguments.

set -euo pipefail

# Dependencies for running the CUP code generation tool. The
# impala-frontend.jar still needs a Maven dependency separately.
JAVA_CUP_JAR=java-cup-0.11-a-czt02-cdh.jar
JAVA_CUP_RUNTIME_JAR=java-cup-runtime-0.11-a-czt01-cdh.jar

# The java CUP jars are mirrored to the native-toolchain to make
# them easy to download.
TOOLCHAIN_URL="https://${IMPALA_TOOLCHAIN_HOST}/mirror/java_cup"
CUP_DEFINITION_FILE="${IMPALA_HOME}/common/cup/sql-parser.cup"

# The output directory matches the added source directory in
# fe/pom.xml.
BASE_OUTPUT_DIRECTORY="${IMPALA_HOME}/fe/generated-sources/cup/"

# To do the generation, we need the java cup jars. If we don't have
# them, download them.
mkdir -p ${IMPALA_TOOLCHAIN}/java_cup/
pushd ${IMPALA_TOOLCHAIN}/java_cup/ > /dev/null
for jar in $JAVA_CUP_JAR $JAVA_CUP_RUNTIME_JAR ; do
  if [[ ! -f ${jar} ]]; then
    echo "Downloading ${jar} from toolchain"
    wget --quiet "${TOOLCHAIN_URL}/${jar}"
  fi
done

# Create the output directory if it doesn't exist. Java CUP doesn't
# create the package directories, so we need to include them here.
OUTPUT_DIRECTORY=${BASE_OUTPUT_DIRECTORY}/org/apache/impala/analysis/
mkdir -p ${OUTPUT_DIRECTORY}

# This command generates the same files that the maven-cup-plugin
# would have generated before.
java -cp ${JAVA_CUP_RUNTIME_JAR}:${JAVA_CUP_JAR} java_cup.Main \
     -package org.apache.impala.analysis \
     -parser SqlParser \
     -symbols SqlParserSymbols \
     -destdir ${OUTPUT_DIRECTORY} \
     -expect 0 \
     ${CUP_DEFINITION_FILE}

popd > /dev/null
