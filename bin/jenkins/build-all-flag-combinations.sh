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

# Build Impala with the most common build configurations and check that the build
# succeeds.a Intended for use as a precommit test to make sure nothing got broken.
#
# Assumes that ninja and ccache are installed.

set -euo pipefail
trap 'echo Error in $0 at line $LINENO: $(cd "'$PWD'" && awk "NR == $LINENO" $0)' ERR

. bin/impala-config.sh
OPTIONS=("-skiptests" "-noclean")
FAILED_OPTIONS=""
for BUILD_TYPE in "" -asan -release -ubsan -tsan
do
  OPTIONS[2]=$BUILD_TYPE
  for NINJA in "" -ninja
  do
    OPTIONS[3]=$NINJA
    for BUILD_SHARED_LIBS in "" -so
    do
      OPTIONS[4]=$BUILD_SHARED_LIBS
      if ! ./bin/clean.sh
      then
        echo "Clean failed"
        exit 1
      fi
      echo "Building with OPTIONS: ${OPTIONS[@]}"
      if ! time -p ./buildall.sh ${OPTIONS[@]}
      then
        echo "Build failed with OPTIONS: ${OPTIONS[@]}"
        FAILED_OPTIONS="${FAILED_OPTIONS}:${OPTIONS[@]}"
      fi
      ccache -s
    done
  done
done

if [[ "$FAILED_OPTIONS" != "" ]]
then
  echo "Builds with the following options failed:"
  echo "$FAILED_OPTIONS"
  exit 1
fi
