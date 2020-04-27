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

# This file should be sourced to set up LD_LIBRARY_PATH and LD_PRELOAD to
# run Impala binaries in the context of a dev environment.
export LD_LIBRARY_PATH="${LD_LIBRARY_PATH:+${LD_LIBRARY_PATH}:}"

# We built against toolchain GCC so we need to dynamically link against matching
# library versions. (the rpath isn't baked into the binaries)
IMPALA_TOOLCHAIN_GCC_LIB=\
"${IMPALA_TOOLCHAIN_PACKAGES_HOME}/gcc-${IMPALA_GCC_VERSION}/lib64"
LD_LIBRARY_PATH="${LD_LIBRARY_PATH}:${IMPALA_TOOLCHAIN_GCC_LIB}"

export LD_PRELOAD="${LD_PRELOAD:+${LD_PRELOAD}:}${LIB_JSIG}"

