##############################################################################
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
##############################################################################

# Find the LLVM binaries: clang and opt
#  LLVM_BINARIES_ROOT - hints the search path
#  LLVM_CLANG_EXECUTABLE - set to path to clang
#  LLVM_OPT_EXECUTABLE - set to path to opt

find_program(LLVM_BINARIES_CONFIG_EXECUTABLE llvm-config
  PATHS
  ${LLVM_BINARIES_ROOT}/bin
  $ENV{LLVM_HOME}
  NO_DEFAULT_PATH
)

if (LLVM_BINARIES_CONFIG_EXECUTABLE STREQUAL "LLVM_BINARIES_CONFIG_EXECUTABLE-NOTFOUND")
  message(FATAL_ERROR "Could not find llvm-config")
endif ()

# Check LLVM Version to be compatible
execute_process(
  COMMAND ${LLVM_BINARIES_CONFIG_EXECUTABLE} --version
  OUTPUT_VARIABLE LLVM_VERSION
  OUTPUT_STRIP_TRAILING_WHITESPACE
)

if (NOT "${LLVM_VERSION}" VERSION_EQUAL "$ENV{IMPALA_LLVM_VERSION}")
  message(FATAL_ERROR
      "LLVM version must be $ENV{IMPALA_LLVM_VERSION}. Found version: ${LLVM_VERSION}")
endif()

# get the location of the binaries
execute_process(
  COMMAND ${LLVM_BINARIES_CONFIG_EXECUTABLE} --bindir
  OUTPUT_VARIABLE LLVM_BIN_DIR
  OUTPUT_STRIP_TRAILING_WHITESPACE
)

set(LLVM_CLANG_EXECUTABLE "${LLVM_BIN_DIR}/clang++")
set(LLVM_OPT_EXECUTABLE "${LLVM_BIN_DIR}/opt")

message(STATUS "LLVM llvm-config found at: ${LLVM_BINARIES_CONFIG_EXECUTABLE}")
message(STATUS "LLVM clang++ found at: ${LLVM_CLANG_EXECUTABLE}")
message(STATUS "LLVM opt found at: ${LLVM_OPT_EXECUTABLE}")

