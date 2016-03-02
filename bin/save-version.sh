#!/usr/bin/env bash
# Copyright 2012 Cloudera Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Generates the impala version and build information.
# Note: for internal (aka pre-release) versions, the version should have
# "-INTERNAL" appended. Parts of the code will look for this to distinguish
# between released and internal versions.
VERSION=2.6.0-cdh5-INTERNAL
GIT_HASH=$(git rev-parse HEAD)
BUILD_TIME=`date`
HEADER="# Generated version information from save-version.sh"
echo -e \
"${HEADER}\nVERSION: ${VERSION}\nGIT_HASH: ${GIT_HASH}\nBUILD_TIME: ${BUILD_TIME}"\
> $IMPALA_HOME/bin/version.info

cat $IMPALA_HOME/bin/version.info
