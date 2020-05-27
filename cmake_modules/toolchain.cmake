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

if($ENV{USE_SYSTEM_GCC} EQUAL "0")
  # If Impala is built with the GCC from the toolchain, change compiler and link paths
  set(GCC_ROOT $ENV{IMPALA_TOOLCHAIN_PACKAGES_HOME}/gcc-$ENV{IMPALA_GCC_VERSION})
  set(CMAKE_C_COMPILER ${GCC_ROOT}/bin/gcc)
  set(CMAKE_CXX_COMPILER ${GCC_ROOT}/bin/g++)

  # The rpath is needed to be able to run the binaries produced by the toolchain without
  # specifying an LD_LIBRARY_PATH
  set(TOOLCHAIN_LINK_FLAGS "-Wl,-rpath,${GCC_ROOT}/lib64")
  set(TOOLCHAIN_LINK_FLAGS "${TOOLCHAIN_LINK_FLAGS} -L${GCC_ROOT}/lib64")
endif()

if(NOT $ENV{IMPALA_CXX_COMPILER} STREQUAL "default")
  set(CMAKE_CXX_COMPILER $ENV{IMPALA_CXX_COMPILER})
endif()

message(STATUS "Setup toolchain link flags ${TOOLCHAIN_LINK_FLAGS}")
