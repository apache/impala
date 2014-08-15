# - Find Openldap
# This module defines
#  LDAP_INCLUDE_DIR, where to find LDAP headers
#  LDAP_STATIC_LIBRARY, the LDAP library to use.
#  LBER_STATIC_LIBRARY, a support library for LDAP.
#  LDAP_FOUND, If false, do not try to use.

set(THIRDPARTY_LDAP thirdparty/openldap-$ENV{IMPALA_OPENLDAP_VERSION})

set (THIRDPARTY ${CMAKE_SOURCE_DIR}/thirdparty)
set(LDAP_SEARCH_LIB_PATH
  ${THIRDPARTY}/openldap-$ENV{IMPALA_OPENLDAP_VERSION}/impala_install/lib
)

set(LDAP_INCLUDE_DIR
  ${THIRDPARTY_LDAP}/impala_install/include
)

find_library(LDAP_LIB_PATH NAMES ldap
  PATHS ${LDAP_SEARCH_LIB_PATH}
        NO_DEFAULT_PATH
        DOC   "Openldap library"
)

if (LDAP_LIB_PATH)
  set(LDAP_FOUND TRUE)
  set(LDAP_STATIC_LIBRARY ${LDAP_SEARCH_LIB_PATH}/libldap.a)
  SET(LBER_STATIC_LIBRARY ${LDAP_SEARCH_LIB_PATH}/liblber.a)
else ()
  set(LDAP_FOUND FALSE)
endif ()

if (LDAP_FOUND)
  if (NOT LDAP_FIND_QUIETLY)
    message(STATUS "Found LDAP ${LDAP_STATIC_LIBRARY}")
  endif ()
else ()
  message(STATUS "LDAP includes and libraries NOT found.")
endif ()


mark_as_advanced(
  LDAP_STATIC_LIBRARY
  LDAP_INCLUDE_DIR
)
