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

# - Find CRoaring headers and lib.
# CROARING_ROOT hints the location
# This module defines
#  CROARING_INCLUDE_DIR, directory containing headers
#  CROARING_STATIC_LIB, path to libroaring.a

set(CROARING_SEARCH_HEADER_PATHS ${CROARING_ROOT}/include)

set(CROARING_SEARCH_LIB_PATHS ${CROARING_ROOT}/lib ${CROARING_ROOT}/lib64)

find_path(CROARING_INCLUDE_DIR roaring/roaring64.h
  PATHS ${CROARING_SEARCH_HEADER_PATHS}
        NO_DEFAULT_PATH
  DOC  "CRoaring compressed bitmap header path"
)

find_library(CROARING_STATIC_LIB NAMES libroaring.a
  PATHS ${CROARING_SEARCH_LIB_PATHS}
        NO_DEFAULT_PATH
  DOC   "CRoaring static library"
)

if (NOT CROARING_INCLUDE_DIR OR NOT CROARING_STATIC_LIB)
  set(CROARING_FOUND FALSE)
  message(FATAL_ERROR "CRoaring includes and/or libraries NOT found. "
    "Looked for headers in ${CROARING_SEARCH_HEADER_PATHS}, "
    "and for libs in ${CROARING_SEARCH_LIB_PATHS}")
else()
  set(CROARING_FOUND TRUE)
endif ()

mark_as_advanced(
  CROARING_INCLUDE_DIR
  CROARING_STATIC_LIB
)
