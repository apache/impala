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

# Removes compiled or generated artifacts. Should be used before switching branches
# between releases. In addition must be used before switching from a toolchain enabled
# branch to a non-toolchain branch due to caching in CMake generated files.

set -euo pipefail
. $IMPALA_HOME/bin/report_build_error.sh
setup_report_build_error

# If the project was never build, no Makefile will exist and thus make clean will fail.
# Combine the make command with the bash noop to always return true.
"${MAKE_CMD:-make}" clean || :

# clean the external data source project
pushd "${IMPALA_HOME}/java/ext-data-source"
rm -rf api/generated-sources/*
${IMPALA_HOME}/bin/mvn-quiet.sh clean
popd

# clean fe
# don't use git clean because we need to retain Eclipse conf files
pushd "${IMPALA_FE_DIR}"
rm -rf target
rm -f src/test/resources/{core,hbase,hive,ozone}-site.xml
rm -rf generated-sources/*
[ -z "${IMPALA_LOGS_DIR}" ] || rm -rf "${IMPALA_LOGS_DIR}"/*
mkdir -p ${IMPALA_ALL_LOGS_DIRS}
popd

# clean be
pushd "${IMPALA_HOME}/be"
# remove everything listed in .gitignore
git rev-parse 2>/dev/null && git clean -Xdfq
popd

# clean shell build artifacts
pushd "${IMPALA_HOME}/shell"
# remove everything listed in .gitignore
git rev-parse 2>/dev/null && git clean -Xdfq
popd

# Clean stale .pyc, .pyo files and __pycache__ directories.
pushd "${IMPALA_HOME}"
find . -type f -name "*.py[co]" -delete
find . -type d -name "__pycache__" -delete
popd

# clean llvm
rm -f "${IMPALA_HOME}/llvm-ir/"impala*.ll
rm -f "${IMPALA_HOME}/be/generated-sources/impala-ir/"*

# When switching to and from toolchain, make sure to remove all CMake generated files
"${IMPALA_HOME}/bin/clean-cmake.sh"
