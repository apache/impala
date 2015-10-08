# - Find Mustache (mustache.h and mustache.c)
# This module defines
#  MUSTACHE_INCLUDE_DIR, directory containing headers
#  MUSTACHE_SRC_DIR, directory containing source
#  MUSTACHE_FOUND, whether the Mustache library has been located

find_path(MUSTACHE_INCLUDE_DIR mustache/mustache.h HINTS $ENV{IMPALA_HOME}/thirdparty)
find_path(MUSTACHE_SRC_DIR mustache.cc HINTS $ENV{IMPALA_HOME}/thirdparty/mustache)

if (MUSTACHE_INCLUDE_DIR)
  set(MUSTACHE_FOUND TRUE)
else ()
  set(MUSTACHE_FOUND FALSE)
endif ()

if (MUSTACHE_FOUND)
  if (NOT MUSTACHE_FIND_QUIETLY)
    message(STATUS "Mustache template library found in ${MUSTACHE_INCLUDE_DIR}")
  endif ()
else ()
  message(STATUS "Mustache template library includes NOT found. ")
endif ()

mark_as_advanced(
  MUSTACHE_INCLUDE_DIR
)
