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

# - Find Arrow (headers and libarrow.a) with ARROW_ROOT hinting a location
# This module defines
#  ARROW_INCLUDE_DIR, directory containing headers
#  ARROW_STATIC_LIB, path to libarrow.a
#  ARROW_FOUND
set(ARROW_ROOT $ENV{IMPALA_TOOLCHAIN_PACKAGES_HOME}/arrow-$ENV{IMPALA_ARROW_VERSION})

set(ARROW_SEARCH_HEADER_PATHS ${ARROW_ROOT}/include)

set(ARROW_SEARCH_LIB_PATH ${ARROW_ROOT}/lib)

find_path(ARROW_INCLUDE_DIR NAMES arrow/api.h arrow/c/bridge.h PATHS
  ${ARROW_SEARCH_HEADER_PATHS}
  # make sure we don't accidentally pick up a different version
  NO_DEFAULT_PATH)

find_library(ARROW_STATIC_LIB NAMES libarrow.a libarrow_bundled_dependencies.a PATHS
  ${ARROW_SEARCH_LIB_PATH})

if(NOT ARROW_STATIC_LIB)
  message(FATAL_ERROR "Arrow includes and libraries NOT found. "
    "Looked for headers in ${ARROW_SEARCH_HEADER_PATHS}, "
    "and for libs in ${ARROW_SEARCH_LIB_PATH}")
  set(ARROW_FOUND FALSE)
else()
  set(ARROW_FOUND TRUE)
endif ()

set(ARROW_FOUND ${ARROW_STATIC_LIB_FOUND})

mark_as_advanced(
  ARROW_INCLUDE_DIR
  ARROW_STATIC_LIB
  ARROW_FOUND
)
