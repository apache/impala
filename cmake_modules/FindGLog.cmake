# - Find GLOG (logging.h, libglog.a, libglog.so, and libglog.so.0) with GLOG_ROOT
# hinting a location
#
# This module defines
#  GLOG_INCLUDE_DIR, directory containing headers
#  GLOG_LIBS, directory containing glog libraries
#  GLOG_STATIC_LIB, path to libglog.a
#  glogstatic

set(THIRDPARTY $ENV{IMPALA_HOME}/thirdparty)

set(GLOG_SEARCH_HEADER_PATHS
  ${GLOG_ROOT}/include
  ${THIRDPARTY}/glog-$ENV{IMPALA_GLOG_VERSION}/src
)
set(GLOG_SEARCH_LIB_PATH
  ${GLOG_ROOT}/lib
  ${THIRDPARTY}/glog-$ENV{IMPALA_GLOG_VERSION}/.libs
)

find_path(GLOG_INCLUDE_DIR glog/logging.h PATHS
  ${GLOG_SEARCH_HEADER_PATHS}
  # make sure we don't accidentally pick up a different version
  NO_DEFAULT_PATH
)

find_library(GLOG_LIBS NAMES glog PATHS ${GLOG_SEARCH_LIB_PATH})
find_library(GLOG_STATIC_LIB NAMES libglog.a PATHS ${GLOG_SEARCH_LIB_PATH})

if (NOT GLOG_LIBS OR NOT GLOG_STATIC_LIB)
  message(FATAL_ERROR "GLog includes and libraries NOT found. "
    "Looked for headers in ${GLOG_SEARCH_HEADER_PATH}, "
    "and for libs in ${GLOG_SEARCH_LIB_PATH}")
  set(GLOG_FOUND FALSE)
else()
  set(GLOG_FOUND TRUE)
  # for static linking with GLOG, GLOG_STATIC_LIB is set in GLOG's find module
  add_library(glogstatic STATIC IMPORTED)
  # TODO: Is this directive required for all libraries? Seems to make no difference.
  set_target_properties(glogstatic PROPERTIES IMPORTED_LOCATION ${GLOG_STATIC_LIB})

endif ()

mark_as_advanced(
  GLOG_INCLUDE_DIR
  GLOG_LIBS
  GLOG_STATIC_LIB
  glogstatic
)
