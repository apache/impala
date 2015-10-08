# - Find LZ4 (lz4.h, liblz4.a, liblz4.so, and liblz4.so.1)
# LZ4_ROOT hints the location
#
# This module defines
# LZ4_INCLUDE_DIR, directory containing headers
# LZ4_LIBS, directory containing lz4 libraries
# LZ4_STATIC_LIB, path to liblz4.a
# lz4 - static library

set(LZ4_SEARCH_LIB_PATH
  ${LZ4_ROOT}/lib
  $ENV{IMPALA_HOME}/thirdparty/lz4
)

set(LZ4_SEARCH_INCLUDE_DIR
  ${LZ4_ROOT}/include
  $ENV{IMPALA_HOME}/thirdparty/lz4
)

find_path(LZ4_INCLUDE_DIR lz4.h
  PATHS ${LZ4_SEARCH_INCLUDE_DIR}
  NO_DEFAULT_PATH
  DOC "Path to LZ4 headers"
  )

find_library(LZ4_LIBS NAMES lz4
  PATHS ${LZ4_SEARCH_LIB_PATH}
        NO_DEFAULT_PATH
  DOC "Path to LZ4 library"
)

find_library(LZ4_STATIC_LIB NAMES liblz4.a
  PATHS ${LZ4_SEARCH_LIB_PATH}
        NO_DEFAULT_PATH
  DOC "Path to LZ4 static library"
)

if (NOT LZ4_LIBS OR NOT LZ4_STATIC_LIB)
  message(FATAL_ERROR "Lz4 includes and libraries NOT found. "
    "Looked for headers in ${LZ4_SEARCH_INCLUDE_DIR}, "
    "and for libs in ${LZ4_SEARCH_LIB_PATH}")
  set(LZ4_FOUND FALSE)
else()
  set(LZ4_FOUND TRUE)
  add_library(lz4 STATIC IMPORTED)
  set_target_properties(lz4 PROPERTIES IMPORTED_LOCATION "${LZ4_STATIC_LIB}")
endif ()

mark_as_advanced(
  LZ4_INCLUDE_DIR
  LZ4_LIBS
  LZ4_STATIC_LIB
  lz4
)
