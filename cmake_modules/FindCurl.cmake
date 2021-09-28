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

# - Find CURL (curl.h, libcurl.a, libcurl.so, and libglog.so.4) with CURL_ROOT
# hinting a location
#
# This module defines
#  CURL_INCLUDE_DIR, directory containing curl headers
#  CURL_LIBS, directory containing curl libraries
#  CURL_STATIC_LIB, path to libcurl.a

set(CURL_SEARCH_HEADER_PATHS ${CURL_ROOT}/include)
set(CURL_SEARCH_LIB_PATH ${CURL_ROOT}/lib)

find_path(CURL_INCLUDE_DIR NAMES curl/curl.h PATHS
  ${CURL_SEARCH_HEADER_PATHS}
  # make sure we don't accidentally pick up a different version
  NO_DEFAULT_PATH
)

find_library(CURL_STATIC_LIB NAMES libcurl.a PATHS
  ${CURL_SEARCH_LIB_PATH}
  # make sure we don't accidentally pick up a different version
  NO_DEFAULT_PATH
)

find_library(CURL_SHARED_LIB NAMES libcurl.so PATHS
  ${CURL_SEARCH_LIB_PATH}
  # make sure we don't accidentally pick up a different version
  NO_DEFAULT_PATH
)

if (NOT CURL_INCLUDE_DIR OR NOT CURL_STATIC_LIB)
  message(FATAL_ERROR "Curl includes and libraries NOT found. "
    "Looked for headers in ${CURL_SEARCH_HEADER_PATH}, "
    "and for libs in ${CURL_SEARCH_LIB_PATH}")
  set(CURL_FOUND FALSE)
else()
  set(CURL_FOUND TRUE)
endif ()

mark_as_advanced(
  CURL_INCLUDE_DIR
  CURL_STATIC_LIB
  CURL_SHARED_LIB
  CURL_FOUND
)
