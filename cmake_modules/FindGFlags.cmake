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

# - Find GFLAGS (gflags.h, libgflags.a, libgflags.so, and libgflags.so.0) with
# GFLAGS_ROOT hinting a location
#
# This module defines
#  GFLAGS_INCLUDE_DIR, directory containing headers
#  GFLAGS_LIBS, directory containing gflag libraries
#  GFLAGS_STATIC_LIB, path to libgflags.a
#  GFLAGS_SHARED_LIB, path to libgflags.so

set(GFLAGS_SEARCH_HEADER_PATHS ${GFLAGS_ROOT}/include)

set(GFLAGS_SEARCH_LIB_PATH ${GFLAGS_ROOT}/lib)

find_path(GFLAGS_INCLUDE_DIR gflags/gflags.h PATHS
  ${GFLAGS_SEARCH_HEADER_PATHS}
  # make sure we don't accidentally pick up a different version
  NO_DEFAULT_PATH
)

find_library(GFLAGS_LIBS NAMES gflags PATHS ${GFLAGS_SEARCH_LIB_PATH})
find_library(GFLAGS_STATIC_LIB NAMES libgflags.a PATHS ${GFLAGS_SEARCH_LIB_PATH})
find_library(GFLAGS_SHARED_LIB NAMES libgflags.so PATHS ${GFLAGS_SEARCH_LIB_PATH})

if (NOT GFLAGS_LIBS OR NOT GFLAGS_STATIC_LIB OR NOT GFLAGS_SHARED_LIB)
  message(FATAL_ERROR "GFlags includes and libraries NOT found. "
    "Looked for headers in ${GFLAGS_SEARCH_HEADER_PATHS}, "
    "and for libs in ${GFLAGS_SEARCH_LIB_PATH}")
  set(GFLAGS_FOUND FALSE)
else()
  set(GFLAGS_FOUND TRUE)
endif ()

mark_as_advanced(
  GFLAGS_INCLUDE_DIR
  GFLAGS_LIBS
  GFLAGS_STATIC_LIB
  GFLAGS_SHARED_LIB
)
