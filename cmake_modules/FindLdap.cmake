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

# - Find Openldap
# LDAP_ROOT hints a location
#
# This module defines
#  LDAP_INCLUDE_DIR, where to find LDAP headers
#  LDAP_STATIC_LIBRARY, the LDAP library to use.
#  LBER_STATIC_LIBRARY, a support library for LDAP.

set(LDAP_SEARCH_LIB_PATH ${OPENLDAP_ROOT}/lib)

find_path(LDAP_INCLUDE_DIR ldap.h PATHS
  ${OPENLDAP_ROOT}/include
  NO_DEFAULT_PATH)

find_library(LDAP_STATIC_LIBRARY libldap.a
  PATHS ${LDAP_SEARCH_LIB_PATH}
        NO_DEFAULT_PATH
        DOC   "Static Openldap library"
)

find_library(LBER_STATIC_LIBRARY liblber.a
  PATHS ${LDAP_SEARCH_LIB_PATH}
        NO_DEFAULT_PATH
        DOC   "Static Openldap lber library"
)

if (NOT LDAP_STATIC_LIBRARY OR NOT LBER_STATIC_LIBRARY OR
    NOT LDAP_INCLUDE_DIR)
  message(FATAL_ERROR "LDAP includes and libraries NOT found.")
  set(LDAP_FOUND TRUE)
else()
  set(LDAP_FOUND FALSE)
endif ()


mark_as_advanced(
  LDAP_STATIC_LIBRARY
  LBER_STATIC_LIBRARY
  LDAP_INCLUDE_DIR)
