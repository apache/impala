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

# OPENSSL_ROOT hints the location
# Provides
#  - OPENSSL_LIBRARIES,
#  - OPENSSL_STATIC,
#  - OPENSSL_INCLUDE_DIR,
#  - OPENSSL_FOUND
set(_OPENSSL_SEARCH_DIR)

if (OPENSSL_ROOT)
set(_OPENSSL_SEARCH_DIR PATHS ${OPENSSL_ROOT} NO_DEFAULT_PATH)
endif()

find_path(OPENSSL_INCLUDE_DIR openssl/opensslconf.h
  ${_OPENSSL_SEARCH_DIR} PATH_SUFFIXES include)

# Add dynamic and static libraries
find_library(OPENSSL_SSL ssl  ${_OPENSSL_SEARCH_DIR} PATH_SUFFIXES lib lib64)
find_library(OPENSSL_CRYPTO crypto  ${_OPENSSL_SEARCH_DIR} PATH_SUFFIXES lib lib64)

if (NOT OPENSSL_SSL AND
    NOT OPENSSL_CRYPTO)
  message(FATAL_ERROR "OpenSSL not found in ${OPENSSL_ROOT}")
  set(OPENSSL_FOUND FALSE)
else()
  set(OPENSSL_FOUND TRUE)
  message(STATUS "OpenSSL: ${OPENSSL_INCLUDE_DIR}")
  set(OPENSSL_LIBRARIES ${OPENSSL_SSL} ${OPENSSL_CRYPTO})
endif()

mark_as_advanced(
  OPENSSL_INCLUDE_DIR
  OPENSSL_LIBRARIES
  OPENSSL_CRYPTO
  OPENSSL_SSL
)
