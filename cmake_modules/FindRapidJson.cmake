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

# - Find rapidjson headers and lib.
# RAPIDJSON_ROOT hints the location
# This module defines RAPIDJSON_INCLUDE_DIR, the directory containing headers

set(RAPIDJSON_SEARCH_HEADER_PATHS ${RAPIDJSON_ROOT}/include)

find_path(RAPIDJSON_INCLUDE_DIR rapidjson/rapidjson.h HINTS
  ${RAPIDJSON_SEARCH_HEADER_PATHS})

if (NOT RAPIDJSON_INCLUDE_DIR)
  message(FATA_ERROR "RapidJson headers NOT found.")
  set(RAPIDJSON_FOUND FALSE)
else()
  set(RAPIDJSON_FOUND TRUE)
endif ()

mark_as_advanced(
  RAPIDJSON_INCLUDE_DIR
)
