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
# THRIFT_CPP_ROOT hints the location
#
# This module defines
#  THRIFT_CPP_INCLUDE_DIR, where to find THRIFT headers
#  THRIFT_CPP_CONTRIB_DIR, where contrib thrift files (e.g. fb303.thrift) are installed
#  THRIFT_CPP_LIBS, THRIFT libraries
#  THRIFT_CPP_COMPILER, where to find thrift compiler
#  thriftstatic_cpp - imported static library

# prefer the thrift version supplied in THRIFT_CPP_HOME
if (NOT THRIFT_CPP_FIND_QUIETLY)
  message(STATUS "THRIFT_CPP_HOME: $ENV{THRIFT_CPP_HOME}")
endif()
find_path(THRIFT_CPP_INCLUDE_DIR thrift/Thrift.h HINTS
  ${THRIFT_CPP_ROOT}/include
  $ENV{THRIFT_CPP_HOME}/include/
  /usr/local/include/
  /opt/local/include/
)

find_path(THRIFT_CPP_CONTRIB_DIR share/fb303/if/fb303.thrift HINTS
  ${THRIFT_CPP_ROOT}/include
  $ENV{THRIFT_CPP_HOME}
  /usr/local/
)

set(THRIFT_CPP_LIB_PATHS
  ${THRIFT_CPP_ROOT}/lib
  $ENV{THRIFT_CPP_HOME}/lib
  /usr/local/lib
  /opt/local/lib)

find_path(THRIFT_CPP_STATIC_LIB_PATH libthrift.a PATHS ${THRIFT_CPP_LIB_PATHS})

# prefer the thrift version supplied in THRIFT_CPP_HOME
find_library(THRIFT_CPP_LIB NAMES thrift HINTS ${THRIFT_CPP_LIB_PATHS})

find_program(THRIFT_CPP_COMPILER thrift
  ${THRIFT_CPP_ROOT}/bin
  $ENV{THRIFT_CPP_HOME}/bin
  /usr/local/bin
  /usr/bin
  NO_DEFAULT_PATH
)

if (THRIFT_CPP_LIB)
  set(THRIFT_CPP_LIBS ${THRIFT_CPP_LIB})
  set(THRIFT_CPP_STATIC_LIB ${THRIFT_CPP_STATIC_LIB_PATH}/libthrift.a)
  exec_program(${THRIFT_CPP_COMPILER}
    ARGS -version OUTPUT_VARIABLE THRIFT_CPP_VERSION RETURN_VALUE THRIFT_CPP_RETURN)
  if (NOT THRIFT_CPP_FIND_QUIETLY)
    message(STATUS "Thrift version: ${THRIFT_CPP_VERSION}")
  endif ()
  set(THRIFT_CPP_FOUND TRUE)
  # for static linking with Thrift, THRIFT_CPP_STATIC_LIB is set in FindThrift.cmake
  add_library(thriftstatic_cpp STATIC IMPORTED)
  set_target_properties(thriftstatic_cpp PROPERTIES IMPORTED_LOCATION ${THRIFT_CPP_STATIC_LIB})

else ()
  set(THRIFT_CPP_FOUND FALSE)
  message(FATAL_ERROR "Thrift compiler/libraries NOT found. "
          "Thrift support will be disabled (${THRIFT_CPP_RETURN}, "
          "${THRIFT_CPP_INCLUDE_DIR}, ${THRIFT_CPP_LIB})")
endif ()


mark_as_advanced(
  THRIFT_CPP_LIB
  THRIFT_CPP_COMPILER
  THRIFT_CPP_INCLUDE_DIR
  thriftstatic_cpp
)
