# - Find Squeasel (squeasel.h and squeasel.c)
# This module defines
#  SQUEASEL_INCLUDE_DIR, directory containing headers
#  SQUEASEL_SRC_DIR, directory containing source

find_path(SQUEASEL_INCLUDE_DIR squeasel/squeasel.h HINTS $ENV{IMPALA_HOME}/thirdparty)
find_path(SQUEASEL_SRC_DIR squeasel.c HINTS $ENV{IMPALA_HOME}/thirdparty/squeasel)

if (SQUEASEL_INCLUDE_DIR)
  set(SQUEASEL_FOUND TRUE)
else ()
  set(SQUEASEL_FOUND FALSE)
endif ()

if (SQUEASEL_FOUND)
  if (NOT SQUEASEL_FIND_QUIETLY)
    message(STATUS "Squeasel web server library found in ${SQUEASEL_INCLUDE_DIR}")
  endif ()
else ()
  message(FATAL_ERROR "Squeasel web server library includes NOT found. ")
endif ()

mark_as_advanced(
  SQUEASEL_INCLUDE_DIR
)
