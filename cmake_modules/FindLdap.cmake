# - Find Openldap
# LDAP_ROOT hints a location
#
# This module defines
#  LDAP_INCLUDE_DIR, where to find LDAP headers
#  LDAP_STATIC_LIBRARY, the LDAP library to use.
#  LBER_STATIC_LIBRARY, a support library for LDAP.
#  ldapstatic, lberstatic, imported libraries

set(THIRDPARTY_LDAP thirdparty/openldap-$ENV{IMPALA_OPENLDAP_VERSION})

set (THIRDPARTY ${CMAKE_SOURCE_DIR}/thirdparty)
set(LDAP_SEARCH_LIB_PATH
  ${OPENLDAP_ROOT}/lib
  ${THIRDPARTY}/openldap-$ENV{IMPALA_OPENLDAP_VERSION}/impala_install/lib
)

find_path(LDAP_INCLUDE_DIR ldap.h PATHS
  ${OPENLDAP_ROOT}/include
  ${THIRDPARTY_LDAP}/impala_install/include
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
  add_library(ldapstatic STATIC IMPORTED)
  set_target_properties(ldapstatic PROPERTIES IMPORTED_LOCATION ${LDAP_STATIC_LIBRARY})
  add_library(lberstatic STATIC IMPORTED)
  set_target_properties(lberstatic PROPERTIES IMPORTED_LOCATION ${LBER_STATIC_LIBRARY})
endif ()


mark_as_advanced(
  LDAP_STATIC_LIBRARY
  LBER_STATIC_LIBRARY
  LDAP_INCLUDE_DIR
  ldapstatic
  lberstatic
)
