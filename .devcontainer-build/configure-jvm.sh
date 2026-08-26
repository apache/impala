#!/usr/bin/env bash
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

# Configures the VSCode settings.

BASE_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

if [[ -z "${JAVA_VERSION}" ]]
then
  echo "JAVA_VERSION is not set"
  exit 1
fi

if [[ $(grep -cE '"name":\s+"JavaSE' "${BASE_DIR}/.vscode/settings.json") -ne 1 ]]
then
  echo "Cannot find a unique JavaSE entry in settings.json"
  exit 1
fi

if [[ $(grep -cE '"path":\s+"/usr/lib/jvm' "${BASE_DIR}/.vscode/settings.json") -ne 1 ]]
then
  echo "Cannot find a unique JavaSE entry in settings.json"
  exit 1
fi

echo "Determine JVM name and path"
JVM_PATH=$(update-java-alternatives -l | rev | cut -d" " -f1 | rev)
JVM_NAME="JavaSE-"
if [[ "${JAVA_VERSION}" == "8" ]]
then
  JVM_NAME="${JVM_NAME}1.8"
else
  JVM_NAME="${JVM_NAME}${JAVA_VERSION}"
fi

echo "Using JVM Name: ${JVM_NAME} in VSCode settings."
echo "Using JVM Path: ${JVM_PATH} in VSCode settings."

echo "Updating VSCode settings with JVM name and path"
sed -i "s|\"name\": \"JavaSE-.*\"|\"name\": \"${JVM_NAME}\"|" \
  "${BASE_DIR}/.vscode/settings.json"
sed -i "s|\"path\": \"/usr/lib/jvm/.*\"|\"path\": \"${JVM_PATH}\"|" \
  "${BASE_DIR}/.vscode/settings.json"
