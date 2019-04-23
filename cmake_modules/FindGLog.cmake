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

# - Find GLOG (logging.h, libglog.a, libglog.so, and libglog.so.0) with GLOG_ROOT
# hinting a location
#
# This module defines
#  GLOG_INCLUDE_DIR, directory containing headers
#  GLOG_LIBS, directory containing glog libraries
#  GLOG_STATIC_LIB, path to libglog.a

set(GLOG_SEARCH_HEADER_PATHS ${GLOG_ROOT}/include)
set(GLOG_SEARCH_LIB_PATH ${GLOG_ROOT}/lib)

find_path(GLOG_INCLUDE_DIR glog/logging.h PATHS
  ${GLOG_SEARCH_HEADER_PATHS}
  # make sure we don't accidentally pick up a different version
  NO_DEFAULT_PATH
)

find_library(GLOG_LIBS NAMES glog PATHS ${GLOG_SEARCH_LIB_PATH})
find_library(GLOG_STATIC_LIB NAMES libglog.a PATHS ${GLOG_SEARCH_LIB_PATH})
find_library(GLOG_SHARED_LIB NAMES libglog.so PATHS ${GLOG_SEARCH_LIB_PATH})

if (NOT GLOG_LIBS OR NOT GLOG_STATIC_LIB)
  message(FATAL_ERROR "GLog includes and libraries NOT found. "
    "Looked for headers in ${GLOG_SEARCH_HEADER_PATH}, "
    "and for libs in ${GLOG_SEARCH_LIB_PATH}")
  set(GLOG_FOUND FALSE)
else()
  set(GLOG_FOUND TRUE)
endif ()

mark_as_advanced(
  GLOG_INCLUDE_DIR
  GLOG_LIBS
  GLOG_STATIC_LIB
  GLOG_SHARED_LIB
)
