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

# - Find Cctz (headers and libcctz.a) with CCTZ_ROOT hinting a location
# This module defines
#  CCTZ_INCLUDE_DIR, directory containing headers
#  CCTZ_STATIC_LIB, path to libcctz.a
#  CCTZ_FOUND
set(CCTZ_SEARCH_HEADER_PATHS ${CCTZ_ROOT}/include)

set(CCTZ_SEARCH_LIB_PATH ${CCTZ_ROOT}/lib)

find_path(CCTZ_INCLUDE_DIR NAMES cctz/civil_time.h civil_time.h PATHS
  ${CCTZ_SEARCH_HEADER_PATHS}
  # make sure we don't accidentally pick up a different version
  NO_DEFAULT_PATH)

find_library(CCTZ_STATIC_LIB NAMES libcctz.a PATHS ${CCTZ_SEARCH_LIB_PATH})

if(NOT CCTZ_STATIC_LIB)
  message(FATAL_ERROR "Cctz includes and libraries NOT found. "
    "Looked for headers in ${CCTZ_SEARCH_HEADER_PATHS}, "
    "and for libs in ${CCTZ_SEARCH_LIB_PATH}")
  set(CCTZ_FOUND FALSE)
else()
  set(CCTZ_FOUND TRUE)
endif ()

set(CCTZ_FOUND ${CCTZ_STATIC_LIB_FOUND})

mark_as_advanced(
  CCTZ_INCLUDE_DIR
  CCTZ_STATIC_LIB
  CCTZ_FOUND
)
