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

# CALLONCEHACK_ROOT hints the location
# Provides
#  - CALLONCEHACK_SHARED_LIB,
#  - CALLONCEHACK_INCLUDE_DIR,
#  - CALLONCEHACK_FOUND
set(_CALLONCEHACK_SEARCH_DIR)

if (CALLONCEHACK_ROOT)
set(_CALLONCEHACK_SEARCH_DIR PATHS ${CALLONCEHACK_ROOT} NO_DEFAULT_PATH)
endif()

find_path(CALLONCEHACK_INCLUDE_DIR calloncehack.h
  ${_CALLONCEHACK_SEARCH_DIR} PATH_SUFFIXES include)

# Calloncehack is always a shared library
find_library(CALLONCEHACK_SHARED_LIB libcalloncehack.so ${_CALLONCEHACK_SEARCH_DIR} PATH_SUFFIXES lib)

if (CALLONCEHACK_INCLUDE_DIR AND CALLONCEHACK_SHARED_LIB)
  set(CALLONCEHACK_FOUND TRUE)
  message(STATUS "Found the calloncehack library: ${CALLONCEHACK_SHARED_LIB}")
else()
  set(CALLONCEHACK_FOUND FALSE)
  message(FATAL_ERROR "calloncehack not found in ${CALLONCEHACK_ROOT}")
endif()

mark_as_advanced(
  CALLONCEHACK_INCLUDE_DIR
  CALLONCEHACK_SHARED_LIB
)
