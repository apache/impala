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

# - Find Avro (headers and libavrocpp_s.a) with AVRO_ROOT hinting a location
# This module defines
#  AVRO_INCLUDE_DIR, directory containing headers
#  AVRO_STATIC_LIB, path to libavrocpp_s.a
#  AVRO_FOUND
set(AVRO_SEARCH_HEADER_PATHS ${AVRO_ROOT}/include)

set(AVRO_SEARCH_LIB_PATH ${AVRO_ROOT}/lib)

set(AVRO_LIB_NAME "libavro.a")
set(AVRO_PROBE_INCLUDE_NAME_1 "avro/schema.h")
set(AVRO_PROBE_INCLUDE_NAME_2 "schema.h")

# Search for the AVRO C++ library when USE_AVRO_CPP environment variable is true.
string(TOUPPER $ENV{USE_AVRO_CPP} USE_AVRO_CPP)
if (USE_AVRO_CPP)
  set(AVRO_LIB_NAME "libavrocpp_s.a")
  set(AVRO_PROBE_INCLUDE_NAME_1 "avro/Schema.hh")
  set(AVRO_PROBE_INCLUDE_NAME_2 "Schema.hh")
endif()

find_path(AVRO_INCLUDE_DIR NAMES ${AVRO_PROBE_INCLUDE_NAME_1} ${AVRO_PROBE_INCLUDE_NAME_2}
  PATHS ${AVRO_SEARCH_HEADER_PATHS}
  # make sure we don't accidentally pick up a different version
  NO_DEFAULT_PATH)

find_library(AVRO_STATIC_LIB NAMES ${AVRO_LIB_NAME} PATHS ${AVRO_SEARCH_LIB_PATH})

if(NOT AVRO_STATIC_LIB)
  message(FATAL_ERROR "Avro includes and libraries NOT found. "
    "Looked for headers in ${AVRO_SEARCH_HEADER_PATHS}, "
    "and for libs in ${AVRO_SEARCH_LIB_PATH}")
  set(AVRO_FOUND FALSE)
else()
  set(AVRO_FOUND TRUE)
endif ()

set(AVRO_FOUND ${AVRO_STATIC_LIB_FOUND})

mark_as_advanced(
  AVRO_INCLUDE_DIR
  AVRO_STATIC_LIB
  AVRO_FOUND
)
