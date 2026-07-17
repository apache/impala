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

# - Find simdutf headers and lib.
# SIMDUTF_ROOT hints the location
# This module defines
#  SIMDUTF_INCLUDE_DIR, directory containing headers
#  SIMDUTF_STATIC_LIB, path to libsimdutf.a

set(SIMDUTF_SEARCH_HEADER_PATHS ${SIMDUTF_ROOT}/include)

set(SIMDUTF_SEARCH_LIB_PATHS ${SIMDUTF_ROOT}/lib)

find_path(SIMDUTF_INCLUDE_DIR simdutf.h
  PATHS ${SIMDUTF_SEARCH_HEADER_PATHS}
        NO_DEFAULT_PATH
  DOC  "simdutf Unicode validation/transcoding header path"
)

find_library(SIMDUTF_LIBS NAMES simdutf
  PATHS ${SIMDUTF_SEARCH_LIB_PATHS}
        NO_DEFAULT_PATH
  DOC   "simdutf library"
)

find_library(SIMDUTF_STATIC_LIB NAMES libsimdutf.a
  PATHS ${SIMDUTF_SEARCH_LIB_PATHS}
        NO_DEFAULT_PATH
  DOC   "simdutf static library"
)

message(STATUS ${SIMDUTF_INCLUDE_DIR})

if (NOT SIMDUTF_INCLUDE_DIR OR NOT SIMDUTF_LIBS OR
    NOT SIMDUTF_STATIC_LIB)
  set(SIMDUTF_FOUND FALSE)
  message(FATAL_ERROR "simdutf includes and/or libraries NOT found. "
    "Looked for headers in ${SIMDUTF_SEARCH_HEADER_PATHS}, "
    "and for libs in ${SIMDUTF_SEARCH_LIB_PATHS}")
else()
  set(SIMDUTF_FOUND TRUE)
endif ()

mark_as_advanced(
  SIMDUTF_INCLUDE_DIR
  SIMDUTF_LIBS
  SIMDUTF_STATIC_LIB
)
