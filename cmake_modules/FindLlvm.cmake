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

# Find the native LLVM includes and library
#
#  LLVM_ROOT        - hints the search path
#  LLVM_INCLUDE_DIR - where to find llvm include files
#  LLVM_LIBRARY_DIR - where to find llvm libs
#  LLVM_LFLAGS      - llvm linker flags
#  LLVM_MODULE_LIBS - list of llvm libs for working with modules.

# First look in LLVM_ROOT then ENV{LLVM_HOME} then system path.
find_program(LLVM_CONFIG_EXECUTABLE llvm-config
  PATHS
  ${LLVM_ROOT}/bin
  $ENV{LLVM_HOME}
  NO_DEFAULT_PATH
)

if (LLVM_CONFIG_EXECUTABLE STREQUAL "LLVM_CONFIG_EXECUTABLE-NOTFOUND")
  message(FATAL_ERROR "Could not find llvm-config")
endif ()

# Check LLVM Version to be compatible
execute_process(
  COMMAND ${LLVM_CONFIG_EXECUTABLE} --version
  OUTPUT_VARIABLE LLVM_VERSION
  OUTPUT_STRIP_TRAILING_WHITESPACE
)

if (NOT "${LLVM_VERSION}" VERSION_EQUAL "$ENV{IMPALA_LLVM_VERSION}")
  message(FATAL_ERROR
      "LLVM version must be $ENV{IMPALA_LLVM_VERSION}. Found version: ${LLVM_VERSION}")
endif()

message(STATUS "LLVM llvm-config found at: ${LLVM_CONFIG_EXECUTABLE}")

execute_process(
  COMMAND ${LLVM_CONFIG_EXECUTABLE} --includedir
  OUTPUT_VARIABLE LLVM_INCLUDE_DIR
  OUTPUT_STRIP_TRAILING_WHITESPACE
)

execute_process(
  COMMAND ${LLVM_CONFIG_EXECUTABLE} --libdir
  OUTPUT_VARIABLE LLVM_LIBRARY_DIR
  OUTPUT_STRIP_TRAILING_WHITESPACE
)

execute_process(
  COMMAND ${LLVM_CONFIG_EXECUTABLE} --ldflags
  OUTPUT_VARIABLE LLVM_LFLAGS
  OUTPUT_STRIP_TRAILING_WHITESPACE
)

 # TODO: this does not work well.  the config file will output -I/<include path> and
 # also -DNDEBUG.  I've hard coded the #define that are necessary but we should make
 # this better.  The necesesary flags are only #defines so maybe just def/undef those
 # around #include to llvm headers?
 #execute_process(
 #  COMMAND ${LLVM_CONFIG_EXECUTABLE} --cxxflags
 #  OUTPUT_VARIABLE LLVM_CFLAGS
 #  OUTPUT_STRIP_TRAILING_WHITESPACE
 #)
 set(LLVM_CFLAGS
   "-D_GNU_SOURCE -D__STDC_CONSTANT_MACROS -D__STDC_FORMAT_MACROS -D__STDC_LIMIT_MACROS")


# Get the link libs we need.  llvm has many and we don't want to link all of the libs
# if we don't need them.
execute_process(
  COMMAND ${LLVM_CONFIG_EXECUTABLE} --libnames core mcjit native ipo bitreader target linker analysis debuginfodwarf passes
  OUTPUT_VARIABLE LLVM_MODULE_LIBS
  OUTPUT_STRIP_TRAILING_WHITESPACE
)

# CMake really doesn't like adding link directories and wants absolute paths
# Reconstruct it with LLVM_MODULE_LIBS and LLVM_LIBRARY_DIR
string(REPLACE " " ";" LIBS_LIST ${LLVM_MODULE_LIBS})
set (LLVM_MODULE_LIBS "")
foreach (LIB ${LIBS_LIST})
  set(LLVM_MODULE_LIBS ${LLVM_MODULE_LIBS} "${LLVM_LIBRARY_DIR}/${LIB}")
endforeach(LIB)

message(STATUS "LLVM include dir: ${LLVM_INCLUDE_DIR}")
message(STATUS "LLVM lib dir: ${LLVM_LIBRARY_DIR}")

if (CMAKE_DEBUG)
  message(STATUS "LLVM libs: ${LLVM_MODULE_LIBS}")
endif()
