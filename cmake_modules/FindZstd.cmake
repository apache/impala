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

# - Find ZSTD (zstd.h, libzstd.a, libzstd.so, and libzstd.so.1)
# ZSTD_ROOT hints the location
#
# This module defines
# ZSTD_INCLUDE_DIR, directory containing headers
# ZSTD_LIBS, directory containing zstd libraries
# ZSTD_STATIC_LIB, path to libzstd.a

set(ZSTD_SEARCH_HEADER_PATHS ${ZSTD_ROOT}/include)

set(ZSTD_SEARCH_LIB_PATH ${ZSTD_ROOT}/lib)

find_path(ZSTD_INCLUDE_DIR
  NAMES zstd.h
  PATHS ${ZSTD_SEARCH_HEADER_PATHS}
        NO_DEFAULT_PATH
  DOC   "Path to ZSTD headers"
)

find_library(ZSTD_LIBS NAMES zstd
  PATHS ${ZSTD_SEARCH_LIB_PATH}
        NO_DEFAULT_PATH
  DOC   "Path to ZSTD library"
)

find_library(ZSTD_STATIC_LIB NAMES libzstd.a
  PATHS ${ZSTD_SEARCH_LIB_PATH}
        NO_DEFAULT_PATH
  DOC   "Path to ZSTD static library"
)

if (NOT ZSTD_LIBS OR NOT ZSTD_STATIC_LIB)
  message(FATAL_ERROR "Zstd includes and libraries NOT found. "
    "Looked for headers in ${ZSTD_SEARCH_HEADER_PATHS}, "
    "and for libs in ${ZSTD_SEARCH_LIB_PATH}")
  set(ZSTD_FOUND FALSE)
else()
  set(ZSTD_FOUND TRUE)
endif ()

mark_as_advanced(
  ZSTD_INCLUDE_DIR
  ZSTD_LIBS
  ZSTD_STATIC_LIB
)


