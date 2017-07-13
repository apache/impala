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

# ZLIB_ROOT hints the location
# Provides
# - ZLIB,
# - ZLIB_LIBRARIES,
# - ZLIB_STATIC,
# - ZLIB_SHARED
# - ZLIB_FOUND

set(_ZLIB_SEARCH_DIR)
if (ZLIB_ROOT)
set(_ZLIB_SEARCH_DIR PATHS ${ZLIB_ROOT} NO_DEFAULT_PATH)
endif()

find_path(ZLIB_INCLUDE_DIR zlib.h ${_ZLIB_SEARCH_DIR}
  PATH_SUFFIXES include)

find_library(ZLIB_STATIC_LIBRARIES libz.a
  ${_ZLIB_SEARCH_DIR} PATH_SUFFIXES lib lib64)
find_library(ZLIB_SHARED_LIBRARIES libz.so
  ${_ZLIB_SEARCH_DIR} PATH_SUFFIXES lib lib64)

if (ZLIB_STATIC_LIBRARIES AND ZLIB_SHARED_LIBRARIES)
  set(ZLIB_FOUND ON)
else()
  message(FATAL_ERROR "zlib headers and libraries NOT found. "
    "Looked for both ${_ZLIB_SEARCH_DIR}.")
  set(ZLIB_FOUND OFF)
endif()

set(ZLIB_NAMES z zlib zdll zlib1 zlibd zlibd1)
find_library(ZLIB_LIBRARIES ${ZLIB_NAMES}
  ${_ZLIB_SEARCH_DIR} PATH_SUFFIXES lib lib64)

if (NOT ZLIB_LIBRARIES AND NOT ZLIB_STATIC_LIBRARIES)
  message(FATAL_ERROR "zlib not found in ${ZLIB_ROOT}")
  set(ZLIB_FOUND FALSE)
else()
  message(STATUS "Zlib: ${ZLIB_INCLUDE_DIR}")
  set(ZLIB_FOUND TRUE)
endif()

mark_as_advanced(
  ZLIB_INCLUDE_DIR
  ZLIB_LIBRARIES
  ZLIB_STATIC
  ZLIB_STATIC_FOUND
)
