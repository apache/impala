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

# - Find OpentelemetryCpp static libraries for the OpenTelemetry C++ SDK.
# OPENTELEMETRY_CPP_ROOT hints the location
#
# This module defines
# OPENTELEMETRY_CPP_INCLUDE_DIR, directory containing headers
#
# See https://github.com/open-telemetry/opentelemetry-cpp/blob/main/INSTALL.md for
# documentation on the OpenTelemetry C++ SDK installation.

set(OPENTELEMETRY_CPP_SEARCH_HEADER_PATHS ${OPENTELEMETRY_CPP_ROOT}/include)

set(OPENTELEMETRY_CPP_SEARCH_LIB_PATH ${OPENTELEMETRY_CPP_ROOT}/lib)

find_path(OPENTELEMETRY_CPP_INCLUDE_DIR
  NAMES opentelemetry/sdk/resource/resource.h
  PATHS ${OPENTELEMETRY_CPP_SEARCH_HEADER_PATHS}
  NO_DEFAULT_PATH)

# The following libraries are the minimum set of libraries required to use the OTLP HTTP
# and file exporters. Order is important, as some libraries depend on others.
set(LIB_LIST
  libopentelemetry_exporter_otlp_file
  libopentelemetry_exporter_otlp_file_client
  libopentelemetry_exporter_otlp_http
  libopentelemetry_exporter_otlp_http_client
  libopentelemetry_http_client_curl
  libopentelemetry_otlp_recordable
  libopentelemetry_logs
  libopentelemetry_proto
  libopentelemetry_resources
  libopentelemetry_trace
  libopentelemetry_version
  libopentelemetry_common
)

set(OPENTELEMETRY_CPP_LIBS "")

set(OPENTELEMETRY_CPP_FOUND TRUE)

foreach(LIB ${LIB_LIST})
  find_library(OPENTELEMETRY_CPP_${LIB} ${LIB}.a
    PATHS ${OPENTELEMETRY_CPP_SEARCH_LIB_PATH}
    NO_DEFAULT_PATH
    DOC "OpenTelemetry ${LIB}"
    )
  if (NOT OPENTELEMETRY_CPP_${LIB})
    message(FATAL_ERROR "OpenTelemetry includes and libraries NOT found.")
    set(OPENTELEMETRY_CPP_FOUND FALSE)
  endif()
  set(OPENTELEMETRY_CPP_LIBS ${OPENTELEMETRY_CPP_LIBS} "${OPENTELEMETRY_CPP_${LIB}}")
endforeach(LIB)

message(STATUS "OpenTelemetry libs: ${OPENTELEMETRY_CPP_LIBS}")

mark_as_advanced(
  OPENTELEMETRY_CPP_LIBS
  OPENTELEMETRY_CPP_INCLUDE_DIR
)
