#!/bin/bash
#
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
#
# Runs cppclean on the BE code and prints the results to stdout.
#
# cppclean is a tool for detecting various issues with C++ code.
# https://github.com/myint/cppclean
#
# Requires cppclean:
# sudo pip install cppclean

. $IMPALA_HOME/bin/impala-config.sh &> /dev/null

# Use CMake to dump the BE include paths.
INCLUDE_PATHS_FILE="/tmp/be_include_paths.txt"
pushd $IMPALA_HOME
cmake -DDUMP_INCLUDE_PATHS=$INCLUDE_PATHS_FILE &> /dev/null
popd

# Generate include path arguments for cppclean.
INCLUDE_PATH_ARGS=" --include-path=$IMPALA_HOME/be/src"
INCLUDE_PATH_ARGS+=" --include-path=$IMPALA_HOME/be/generated-sources"
for dir in $(cat $INCLUDE_PATHS_FILE)
do
  INCLUDE_PATH_ARGS+=" --include-path=$dir"
done

cppclean ${INCLUDE_PATH_ARGS} $@ $IMPALA_HOME/be
