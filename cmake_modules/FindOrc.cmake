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

# - Find Orc (headers and liborc.a) with ORC_ROOT hinting a location
# This module defines
#  ORC_INCLUDE_DIR, directory containing headers
#  ORC_STATIC_LIB, path to liborc.a
#  ORC_FOUND
set(ORC_SEARCH_HEADER_PATHS ${ORC_ROOT}/include)

set(ORC_SEARCH_LIB_PATH ${ORC_ROOT}/lib)

find_path(ORC_INCLUDE_DIR NAMES orc/OrcFile.hh OrcFile.hh PATHS
  ${ORC_SEARCH_HEADER_PATHS}
  # make sure we don't accidentally pick up a different version
  NO_DEFAULT_PATH)

find_library(ORC_STATIC_LIB NAMES liborc.a PATHS ${ORC_SEARCH_LIB_PATH})

if(NOT ORC_STATIC_LIB)
  message(FATAL_ERROR "ORC includes and libraries NOT found. "
    "Looked for headers in ${ORC_SEARCH_HEADER_PATHS}, "
    "and for libs in ${ORC_SEARCH_LIB_PATH}")
  set(ORC_FOUND FALSE)
else()
  set(ORC_FOUND TRUE)
endif ()

set(ORC_FOUND ${ORC_STATIC_LIB_FOUND})

mark_as_advanced(
  ORC_INCLUDE_DIR
  ORC_STATIC_LIB
  ORC_FOUND
)
