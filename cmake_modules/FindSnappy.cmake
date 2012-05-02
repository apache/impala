# - Find SNAPPY (snappy.h, libsnappy.a, libsnappy.so, and libsnappy.so.1)
# This module defines
#  SNAPPY_INCLUDE_DIR, directory containing headers
#  SNAPPY_LIBS, directory containing gflag libraries
#  SNAPPY_STATIC_LIB, path to libsnappy.a
#  SNAPPY_FOUND, whether gflags has been found

set(SNAPPY_SEARCH_HEADER_PATHS  
  ${CMAKE_SOURCE_DIR}/thirdparty/snappy-1.0.5/build/include
)

set(SNAPPY_SEARCH_LIB_PATH
  ${CMAKE_SOURCE_DIR}/thirdparty/snappy-1.0.5/build/lib
)

set(SNAPPY_INCLUDE_DIR 
  ${CMAKE_SOURCE_DIR}/thirdparty/snappy-1.0.5/build/include
)

find_library(SNAPPY_LIB_PATH NAMES snappy
  PATHS ${SNAPPY_SEARCH_LIB_PATH}
        NO_DEFAULT_PATH
  DOC   "Google's snappy compression library"
)

if (SNAPPY_LIB_PATH)
  set(SNAPPY_FOUND TRUE)
  set(SNAPPY_LIBS ${SNAPPY_SEARCH_LIB_PATH})
  set(SNAPPY_STATIC_LIB ${SNAPPY_SEARCH_LIB_PATH}/libsnappy.a)
else ()
  set(SNAPPY_FOUND FALSE)
endif ()

if (SNAPPY_FOUND)
  if (NOT SNAPPY_FIND_QUIETLY)
    message(STATUS "Snappy Found in ${SNAPPY_SEARCH_LIB_PATH}")
  endif ()
else ()
  message(STATUS "Snappy includes and libraries NOT found. "
    "Looked for headers in ${SNAPPY_SEARCH_HEADER_PATH}, "
    "and for libs in ${SNAPPY_SEARCH_LIB_PATH}")
endif ()

mark_as_advanced(
  SNAPPY_INCLUDE_DIR
  SNAPPY_LIBS
  SNAPPY_STATIC_LIB
)
