# - Find pprof (libprofiler.a)
# This module defines
#  PPROF_INCLUDE_DIR, directory containing headers
#  PPROF_LIBS, directory containing pprof libraries
#  PPROF_STATIC_LIB, path to libprofiler.a 
#  PPROF_FOUND, whether pprof has been found

set(PPROF_SEARCH_HEADER_PATHS
  ${CMAKE_SOURCE_DIR}/thirdparty/gperftools-2.0/src
)

set(PPROF_SEARCH_LIB_PATH
  ${CMAKE_SOURCE_DIR}/thirdparty/gperftools-2.0/.libs
)

find_path(PPROF_INCLUDE_DIR google/profiler.h PATHS
  ${PPROF_SEARCH_HEADER_PATHS}
  # make sure we don't accidentally pick up a different version
  NO_DEFAULT_PATH
)

find_library(PPROF_LIB_PATH NAMES profiler PATHS ${PPROF_SEARCH_LIB_PATH})

if (PPROF_LIB_PATH)
  set(PPROF_FOUND TRUE)
  set(PPROF_LIBS ${PPROF_SEARCH_LIB_PATH})
  set(PPROF_STATIC_LIB ${PPROF_SEARCH_LIB_PATH}/libprofiler.a)
  set(HEAPPROF_STATIC_LIB ${PPROF_SEARCH_LIB_PATH}/libtcmalloc.a)
else ()
  set(PPROF_FOUND FALSE)
endif ()

if (PPROF_FOUND)
  if (NOT PPROF_FIND_QUIETLY)
    message(STATUS "PProf Found in ${PPROF_SEARCH_LIB_PATH}")
  endif ()
else ()
  message(STATUS "PProf libraries NOT found. "
    "Looked for libs in ${PPROF_SEARCH_LIB_PATH}")
endif ()

mark_as_advanced(
  PPROF_INCLUDE_DIR
  PPROF_LIBS
  PPROF_STATIC_LIB
)
