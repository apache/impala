# Copyright 2015 Cloudera Inc.
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

set(IMPALA_TOOLCHAIN ON)

# Set the root directory for the toolchain
set(TOOLCHAIN_ROOT $ENV{IMPALA_TOOLCHAIN})

# If Impala is built with the toolchain, change compiler and link paths
set(GCC_ROOT $ENV{IMPALA_TOOLCHAIN}/gcc-$ENV{IMPALA_GCC_VERSION})

# Use LLVM Trunk to build asan
set(LLVM_TRUNK_ROOT $ENV{IMPALA_TOOLCHAIN}/llvm-trunk)

set(CMAKE_C_COMPILER ${LLVM_TRUNK_ROOT}/bin/clang)
set(CMAKE_CXX_COMPILER ${LLVM_TRUNK_ROOT}/bin/clang++)

# Add the GCC root location to the compiler flags
# TODO: remove no-c++11-extensions when we enable c++11
set(CXX_COMMON_FLAGS "-Wno-unused-local-typedef -Wno-c++11-extensions --gcc-toolchain=${GCC_ROOT}")

# The rpath is needed to be able to run the binaries produced by the toolchain without
# specifying an LD_LIBRARY_PATH
set(TOOLCHAIN_LINK_FLAGS "-Wl,-rpath,${GCC_ROOT}/lib64")
set(TOOLCHAIN_LINK_FLAGS "${TOOLCHAIN_LINK_FLAGS} -L${GCC_ROOT}/lib64")

message(STATUS "Setup toolchain link flags ${TOOLCHAIN_LINK_FLAGS}")
