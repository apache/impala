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

# BZIP2_ROOT hints the location
# Provides
#  - BZIP2_LIBRARIES,
#  - BZIP2_STATIC,
#  - BZIP2_INCLUDE_DIR,
#  - BZIP2_FOUND
set(_BZIP2_SEARCH_DIR)

if (BZIP2_ROOT)
set(_BZIP2_SEARCH_DIR PATHS ${BZIP2_ROOT} NO_DEFAULT_PATH)
endif()

find_path(BZIP2_INCLUDE_DIR bzlib.h
  ${_BZIP2_SEARCH_DIR} PATH_SUFFIXES include)

# Add dynamic and static libraries
find_library(BZIP2_LIBRARIES bz2 ${_BZIP2_SEARCH_DIR} PATH_SUFFIXES lib lib64)
find_library(BZIP2_STATIC_LIBRARIES libbz2.a ${_BZIP2_SEARCH_DIR} PATH_SUFFIXES lib lib64)

if (BZIP2_STATIC_LIBRARIES)
  add_library(BZIP2_STATIC STATIC IMPORTED)
  set_target_properties(BZIP2_STATIC PROPERTIES
    IMPORTED_LOCATION ${BZIP2_STATIC_LIBRARIES})
  set(BZIP2_STATIC_FOUND ON)
else()
  set(BZIP2_STATIC_FOUND OFF)
  set(BZIP2_STATIC ${BZIP2_STATIC_LIBRARIES})
endif()

if (NOT BZIP2_STATIC_LIBRARIES AND
    NOT BZIP2_LIBRARIES)
  message(FATAL_ERROR "bzip2 not found in ${BZIP2_ROOT}")
  set(BZIP2_FOUND FALSE)
else()
  set(BZIP2_FOUND TRUE)
  message(STATUS "Bzip2: ${BZIP2_INCLUDE_DIR}")
endif()

mark_as_advanced(
  BZIP2_INCLUDE_DIR
  BZIP2_STATIC
  BZIP2_LIBRARIES
)
