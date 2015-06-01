# - Find pprof (libprofiler.a)
# PPROF_ROOT hints the location
#
# This module defines
#  PPROF_INCLUDE_DIR, directory containing headers
#  PPROF_LIBS, directory containing pprof libraries
#  PPROF_STATIC_LIB, path to libprofiler.a
#  tcmallocstatic, pprofstatic

set(PPROF_SEARCH_HEADER_PATHS
  ${GPERFTOOLS_ROOT}/include
  ${CMAKE_SOURCE_DIR}/thirdparty/gperftools-2.0/src
)

set(PPROF_SEARCH_LIB_PATH
  ${GPERFTOOLS_ROOT}/lib
  ${CMAKE_SOURCE_DIR}/thirdparty/gperftools-2.0/.libs
)

find_path(PPROF_INCLUDE_DIR google/profiler.h PATHS
  ${PPROF_SEARCH_HEADER_PATHS}
  # make sure we don't accidentally pick up a different version
  NO_DEFAULT_PATH
)

find_library(PPROF_LIB_PATH profiler ${PPROF_SEARCH_LIB_PATH})
find_library(PPROF_STATIC_LIB libprofiler.a ${PPROF_SEARCH_LIB_PATH})
find_library(HEAPPROF_STATIC_LIB libtcmalloc.a ${PPROF_SEARCH_LIB_PATH})

if (NOT PPROF_LIB_PATH OR NOT PPROF_STATIC_LIB OR
    NOT HEAPPROF_STATIC_LIB)
  message(FATAL_ERROR "gperftools libraries NOT found. "
    "Looked for libs in ${PPROF_SEARCH_LIB_PATH}")
  set(PPROF_FOUND FALSE)
else()
  set(PPROF_FOUND TRUE)
  add_library(pprofstatic STATIC IMPORTED)
  set_target_properties(pprofstatic PROPERTIES IMPORTED_LOCATION "${PPROF_STATIC_LIB}")
  add_library(tcmallocstatic STATIC IMPORTED)
  set_target_properties(tcmallocstatic PROPERTIES IMPORTED_LOCATION "${HEAPPROF_STATIC_LIB}")
endif ()

mark_as_advanced(
  PPROF_INCLUDE_DIR
  PPROF_LIBS
  PPROF_STATIC_LIB
  pprofstatic
  tcmallocstatic
)
