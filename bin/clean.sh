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

# Removes compiled or generated artifacts. Should be used before switching branches
# between releases. In addition must be used before switching from a toolchain enabled
# branch to a non-toolchain branch due to caching in CMake generated files.

# Exit on non-true return value
set -e
# Exit on reference to uninitialized variable
set -u

# If the project was never build, no Makefile will exist and thus make clean will fail.
# Combine the make command with the bash noop to always return true.
make clean || :

# Stop the minikdc if needed.
if ${CLUSTER_DIR}/admin is_kerberized; then
    ${IMPALA_HOME}/testdata/bin/minikdc.sh stop
fi

# clean the external data source project
pushd ${IMPALA_HOME}/ext-data-source
rm -rf api/generated-sources/*
mvn clean
popd

# clean fe
# don't use git clean because we need to retain Eclipse conf files
pushd $IMPALA_FE_DIR
rm -rf target
rm -f src/test/resources/{core,hbase,hive}-site.xml
rm -rf generated-sources/*
rm -rf ${IMPALA_TEST_CLUSTER_LOG_DIR}/*
popd

# clean be
pushd $IMPALA_HOME/be
# remove everything listed in .gitignore
git clean -Xdf
popd

# clean shell build artifacts
pushd $IMPALA_HOME/shell
# remove everything listed in .gitignore
git clean -Xdf
popd

# Clean stale .pyc, .pyo files and __pycache__ directories.
pushd ${IMPALA_HOME}
find . -type f -name "*.py[co]" -delete
find . -type d -name "__pycache__" -delete
popd

# clean llvm
rm -f $IMPALA_HOME/llvm-ir/impala*.ll
rm -f $IMPALA_HOME/be/generated-sources/impala-ir/*

# Cleanup Impala-lzo
if [ -e $IMPALA_LZO ]; then
  pushd $IMPALA_LZO; git clean -fdx .; popd
fi

# When switching to and from toolchain, make sure to remove all CMake generated files
find -iname '*cmake*' -not -name CMakeLists.txt | grep -v -e cmake_module | grep -v -e thirdparty | xargs rm -Rf
