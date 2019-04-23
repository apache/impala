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

# - Find LZ4 (lz4.h, liblz4.a, liblz4.so, and liblz4.so.1)
# LZ4_ROOT hints the location
#
# This module defines
# LZ4_INCLUDE_DIR, directory containing headers
# LZ4_LIBS, directory containing lz4 libraries
# LZ4_STATIC_LIB, path to liblz4.a

set(LZ4_SEARCH_LIB_PATH ${LZ4_ROOT}/lib)

set(LZ4_SEARCH_INCLUDE_DIR ${LZ4_ROOT}/include)

find_path(LZ4_INCLUDE_DIR lz4.h
  PATHS ${LZ4_SEARCH_INCLUDE_DIR}
  NO_DEFAULT_PATH
  DOC "Path to LZ4 headers"
  )

find_library(LZ4_LIBS NAMES lz4
  PATHS ${LZ4_SEARCH_LIB_PATH}
        NO_DEFAULT_PATH
  DOC "Path to LZ4 library"
)

find_library(LZ4_STATIC_LIB NAMES liblz4.a
  PATHS ${LZ4_SEARCH_LIB_PATH}
        NO_DEFAULT_PATH
  DOC "Path to LZ4 static library"
)

if (NOT LZ4_LIBS OR NOT LZ4_STATIC_LIB)
  message(FATAL_ERROR "Lz4 includes and libraries NOT found. "
    "Looked for headers in ${LZ4_SEARCH_INCLUDE_DIR}, "
    "and for libs in ${LZ4_SEARCH_LIB_PATH}")
  set(LZ4_FOUND FALSE)
else()
  set(LZ4_FOUND TRUE)
endif ()

mark_as_advanced(
  LZ4_INCLUDE_DIR
  LZ4_LIBS
  LZ4_STATIC_LIB
)
