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

# Expect maven log as the first argument.
# There are two requirements for the maven log:
# 1. It needs to be produced by a recent maven version (such as 3.5.4 installed by
# bin/bootstrap_system.sh). This is required because recent maven outputs line like:
# [INFO] Downloading from {repo}: {url}
# [INFO] Downloaded from {repo}: {url}
# Older maven (e.g. 3.3.9) omits the "from {repo}" part.
# 2. Maven needs to run in batch mode (-B). This keeps the output from using special
# characters to format things on the console (e.g. carriage return ^M).
set -euo pipefail

MVN_LOG=$1

# Dump how many artifacts were downloaded from each repo
echo "Number of artifacts downloaded from each repo:"
if grep -q "Downloaded from" "${MVN_LOG}"; then
  cat "${MVN_LOG}" | grep "Downloaded from" | sed 's|.* Downloaded from ||' \
      | cut -d: -f1 | sort | uniq -c
else
  echo "No artifacts downloaded"
fi

# Dump how many artifacts we tried to download from each repo
echo
echo "Number of download attempts (successful or unsuccessful) per repo:"
if grep -q "Downloading from" "${MVN_LOG}"; then
  cat "${MVN_LOG}" | grep "Downloading from" | sed 's|.* Downloading from ||' \
      | cut -d: -f1 | sort | uniq -c
else
  echo "No downloads attempted"
fi
