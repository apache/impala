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

# - Find Thrift (a cross platform RPC lib/tool)
# THRIFT_ROOT hints the location
#
# This module defines
#  THRIFT_VERSION, version string of ant if found
#  THRIFT_INCLUDE_DIR, where to find THRIFT headers
#  THRIFT_CONTRIB_DIR, where contrib thrift files (e.g. fb303.thrift) are installed
#  THRIFT_LIBS, THRIFT libraries
#  thriftstatic - imported static library

# prefer the thrift version supplied in THRIFT_HOME
if (NOT THRIFT_FIND_QUIETLY)
  message(STATUS "THRIFT_HOME: $ENV{THRIFT_HOME}")
endif()
find_path(THRIFT_INCLUDE_DIR thrift/Thrift.h HINTS
  ${THRIFT_ROOT}/include
  $ENV{THRIFT_HOME}/include/
  /usr/local/include/
  /opt/local/include/
)

find_path(THRIFT_CONTRIB_DIR share/fb303/if/fb303.thrift HINTS
  ${THRIFT_ROOT}/include
  $ENV{THRIFT_HOME}
  /usr/local/
)

set(THRIFT_LIB_PATHS
  ${THRIFT_ROOT}/lib
  $ENV{THRIFT_HOME}/lib
  /usr/local/lib
  /opt/local/lib)

find_path(THRIFT_STATIC_LIB_PATH libthrift.a PATHS ${THRIFT_LIB_PATHS})

# prefer the thrift version supplied in THRIFT_HOME
find_library(THRIFT_LIB NAMES thrift HINTS ${THRIFT_LIB_PATHS})

find_program(THRIFT_COMPILER thrift
  ${THRIFT_ROOT}/bin
  $ENV{THRIFT_HOME}/bin
  /usr/local/bin
  /usr/bin
  NO_DEFAULT_PATH
)

if (THRIFT_LIB)
  set(THRIFT_LIBS ${THRIFT_LIB})
  set(THRIFT_STATIC_LIB ${THRIFT_STATIC_LIB_PATH}/libthrift.a)
  exec_program(${THRIFT_COMPILER}
    ARGS -version OUTPUT_VARIABLE THRIFT_VERSION RETURN_VALUE THRIFT_RETURN)
  if (NOT THRIFT_FIND_QUIETLY)
    message(STATUS "Thrift version: ${THRIFT_VERSION}")
  endif ()
  set(THRIFT_FOUND TRUE)
  # for static linking with Thrift, THRIFT_STATIC_LIB is set in FindThrift.cmake
  add_library(thriftstatic STATIC IMPORTED)
  set_target_properties(thriftstatic PROPERTIES IMPORTED_LOCATION ${THRIFT_STATIC_LIB})

else ()
  set(THRIFT_FOUND FALSE)
  message(FATAL_ERROR "Thrift compiler/libraries NOT found. "
          "Thrift support will be disabled (${THRIFT_RETURN}, "
          "${THRIFT_INCLUDE_DIR}, ${THRIFT_LIB})")
endif ()


mark_as_advanced(
  THRIFT_LIB
  THRIFT_COMPILER
  THRIFT_INCLUDE_DIR
  thriftstatic
)
