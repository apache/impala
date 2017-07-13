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

# - Find breakpad headers and lib.
# This module defines
#  BREAKPAD_INCLUDE_DIR, directory containing headers
#  BREAKPAD_STATIC_LIB, path to libbreakpad_client.a

set(BREAKPAD_SEARCH_LIB_PATH
  ${BREAKPAD_ROOT}/lib
)

set(BREAKPAD_INCLUDE_DIR
  ${BREAKPAD_ROOT}/include/breakpad
)

find_library(BREAKPAD_LIB_PATH NAMES breakpad_client
  PATHS ${BREAKPAD_SEARCH_LIB_PATH}
        NO_DEFAULT_PATH
  DOC   "Breakpad library"
)

if (BREAKPAD_LIB_PATH)
  set(BREAKPAD_LIBS ${BREAKPAD_SEARCH_LIB_PATH})
  set(BREAKPAD_STATIC_LIB ${BREAKPAD_SEARCH_LIB_PATH}/libbreakpad_client.a)
  set(BREAKPAD_FOUND TRUE)
else ()
  message(FATAL_ERROR "Breakpad library NOT found. "
    "in ${BREAKPAD_SEARCH_LIB_PATH}")
  set(BREAKPAD_FOUND FALSE)
endif ()

mark_as_advanced(
  BREAKPAD_INCLUDE_DIR
  BREAKPAD_LIBS
  BREAKPAD_STATIC_LIB
)
