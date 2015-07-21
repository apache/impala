# - Find SASL security library.
# CYRUS_SASL_ROOT hints the location
# This module defines
#  SASL_INCLUDE_DIR, where to find SASL headers
#  SASL_STATIC_LIBRARY, the library to use
#  saslstatic - imported static library

set(THIRDPARTY_SASL $ENV{IMPALA_HOME}/thirdparty/cyrus-sasl-$ENV{IMPALA_CYRUS_SASL_VERSION})

set(THIRDPARTY $ENV{IMPALA_HOME}/thirdparty)

set(SASL_SEARCH_LIB_PATH
  ${CYRUS_SASL_ROOT}/lib
  $ENV{IMPALA_CYRUS_SASL_INSTALL_DIR}/lib)
set(SASL_SEARCH_INCLUDE_DIR
  ${CYRUS_SASL_ROOT}/include
  $ENV{IMPALA_CYRUS_SASL_INSTALL_DIR}/include)


find_path(SASL_INCLUDE_DIR NAMES sasl/sasl.h
  PATHS ${SASL_SEARCH_INCLUDE_DIR}
  NO_DEFAULT_PATH)

find_library(SASL_STATIC_LIBRARY NAMES libsasl2.a
  PATHS ${SASL_SEARCH_LIB_PATH}
        NO_DEFAULT_PATH
        DOC   "Cyrus-sasl library"
)

if (NOT SASL_STATIC_LIBRARY OR NOT SASL_INCLUDE_DIR)
  set(SASL_FOUND FALSE)
  message(FATAL_ERROR "SASL includes and libraries NOT found.")
else()
  set(SASL_FOUND TRUE)
  add_library(saslstatic STATIC IMPORTED)
  set_target_properties(saslstatic PROPERTIES IMPORTED_LOCATION ${SASL_STATIC_LIBRARY})
endif ()


mark_as_advanced(
  SASL_STATIC_LIBRARY
  SASL_INCLUDE_DIR
  saslstatic
)
