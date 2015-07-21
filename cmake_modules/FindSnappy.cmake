# - Find SNAPPY (snappy.h, libsnappy.a, libsnappy.so, and libsnappy.so.1)
# SNAPPY_ROOT hints the location
#
# This module defines
#  SNAPPY_INCLUDE_DIR, directory containing headers
#  SNAPPY_LIBS, directory containing gflag libraries
#  SNAPPY_STATIC_LIB, path to libsnappy.a
#  SNAPPY_FOUND, whether gflags has been found
#  snappy - imported static library

set(SNAPPY_SEARCH_HEADER_PATHS
  ${SNAPPY_ROOT}/include
  $ENV{IMPALA_HOME}/thirdparty/snappy-1.0.5/build/include
)

set(SNAPPY_SEARCH_LIB_PATH
  ${SNAPPY_ROOT}/lib
  $ENV{IMPALA_HOME}/thirdparty/snappy-1.0.5/build/lib
)

find_path(SNAPPY_INCLUDE_DIR
  NAMES snappy.h
  PATHS ${SNAPPY_SEARCH_HEADER_PATHS}
  NO_DEFAULT_PATH)

find_library(SNAPPY_LIBS NAMES snappy
  PATHS ${SNAPPY_SEARCH_LIB_PATH}
        NO_DEFAULT_PATH
  DOC   "Google's snappy compression library"
)

find_library(SNAPPY_STATIC_LIB NAMES libsnappy.a
  PATHS ${SNAPPY_SEARCH_LIB_PATH}
        NO_DEFAULT_PATH
  DOC   "Google's snappy compression static library"
)

if (NOT SNAPPY_LIBS OR NOT SNAPPY_STATIC_LIB)
  message(FATAL_ERROR "Snappy includes and libraries NOT found. "
    "Looked for headers in ${SNAPPY_SEARCH_HEADER_PATH}, "
    "and for libs in ${SNAPPY_SEARCH_LIB_PATH}")
  set(SNAPPY_FOUND FALSE)
else()
  set(SNAPPY_FOUND TRUE)
  add_library(snappy STATIC IMPORTED)
  set_target_properties(snappy PROPERTIES IMPORTED_LOCATION "${SNAPPY_STATIC_LIB}")
endif ()

mark_as_advanced(
  SNAPPY_INCLUDE_DIR
  SNAPPY_LIBS
  SNAPPY_STATIC_LIB
  snappy
)
