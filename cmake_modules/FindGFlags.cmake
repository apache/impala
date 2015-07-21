# - Find GFLAGS (gflags.h, libgflags.a, libgflags.so, and libgflags.so.0) with
# GFLAGS_ROOT hinting a location
#
# This module defines
#  GFLAGS_INCLUDE_DIR, directory containing headers
#  GFLAGS_LIBS, directory containing gflag libraries
#  GFLAGS_STATIC_LIB, path to libgflags.a
#  gflagsstatic

set(GFLAGS_SEARCH_HEADER_PATHS
  ${GFLAGS_ROOT}/include
  $ENV{IMPALA_HOME}/thirdparty/gflags-$ENV{IMPALA_GFLAGS_VERSION}/src
)

set(GFLAGS_SEARCH_LIB_PATH
  ${GFLAGS_ROOT}/lib
  $ENV{IMPALA_HOME}/thirdparty/gflags-$ENV{IMPALA_GFLAGS_VERSION}/.libs
)

find_path(GFLAGS_INCLUDE_DIR gflags/gflags.h PATHS
  ${GFLAGS_SEARCH_HEADER_PATHS}
  # make sure we don't accidentally pick up a different version
  NO_DEFAULT_PATH
)

find_library(GFLAGS_LIBS NAMES gflags PATHS ${GFLAGS_SEARCH_LIB_PATH})
find_library(GFLAGS_STATIC_LIB NAMES libgflags.a PATHS ${GFLAGS_SEARCH_LIB_PATH})

if (NOT GFLAGS_LIBS OR NOT GFLAGS_STATIC_LIB)
  message(FATAL_ERROR "GFlags includes and libraries NOT found. "
    "Looked for headers in ${GFLAGS_SEARCH_HEADER_PATHS}, "
    "and for libs in ${GFLAGS_SEARCH_LIB_PATH}")
  set(GFLAGS_FOUND FALSE)
else()
  set(GFLAGS_FOUND TRUE)
  # for static linking with GFLAGS, GFLAGS_STATIC_LIB is set in GFLAGS' find module
  add_library(gflagsstatic STATIC IMPORTED)
  set_target_properties(gflagsstatic PROPERTIES IMPORTED_LOCATION ${GFLAGS_STATIC_LIB})

endif ()

mark_as_advanced(
  GFLAGS_INCLUDE_DIR
  GFLAGS_LIBS
  GFLAGS_STATIC_LIB
  gflagsstatic
)
