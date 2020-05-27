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

# If Impala is built with the toolchain, change compiler and link paths
set(GCC_ROOT $ENV{IMPALA_TOOLCHAIN_PACKAGES_HOME}/gcc-$ENV{IMPALA_GCC_VERSION})

# Use the appropriate LLVM version to build ASAN.
set(LLVM_ASAN_ROOT
    $ENV{IMPALA_TOOLCHAIN_PACKAGES_HOME}/llvm-$ENV{IMPALA_LLVM_ASAN_VERSION})

set(LLVM_ROOT $ENV{IMPALA_TOOLCHAIN_PACKAGES_HOME}/llvm-$ENV{IMPALA_LLVM_VERSION})

if ("${CMAKE_BUILD_TYPE}" STREQUAL "ADDRESS_SANITIZER")
  set(CMAKE_C_COMPILER ${LLVM_ASAN_ROOT}/bin/clang)
else()
  set(CMAKE_C_COMPILER ${LLVM_ROOT}/bin/clang)
endif()

# Use clang to build unless overridden by environment.
if($ENV{IMPALA_CXX_COMPILER} STREQUAL "default")
  if ("${CMAKE_BUILD_TYPE}" STREQUAL "ADDRESS_SANITIZER")
    set(CMAKE_CXX_COMPILER ${LLVM_ASAN_ROOT}/bin/clang++)
  else()
    set(CMAKE_CXX_COMPILER ${LLVM_ROOT}/bin/clang++)
  endif()
else()
  set(CMAKE_CXX_COMPILER $ENV{IMPALA_CXX_COMPILER})
endif()

# Add the GCC root location to the compiler flags
set(CXX_COMMON_FLAGS "--gcc-toolchain=${GCC_ROOT}")

# The rpath is needed to be able to run the binaries produced by the toolchain without
# specifying an LD_LIBRARY_PATH
set(TOOLCHAIN_LINK_FLAGS "-Wl,-rpath,${GCC_ROOT}/lib64")
set(TOOLCHAIN_LINK_FLAGS "${TOOLCHAIN_LINK_FLAGS} -L${GCC_ROOT}/lib64")

message(STATUS "Setup toolchain link flags ${TOOLCHAIN_LINK_FLAGS}")
