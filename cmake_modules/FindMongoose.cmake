# - Find Mongoose (mongoose.h and mongoose.c)
# This module defines
#  MONGOOSE_INCLUDE_DIR, directory containing headers
#  MONGOOSE_SRC_DIR, directory containing source
#  MONGOOSE_FOUND, whether the Mongoose library has been located

find_path(MONGOOSE_INCLUDE_DIR mongoose/mongoose.h HINTS ${CMAKE_SOURCE_DIR}/thirdparty)
find_path(MONGOOSE_SRC_DIR mongoose.c HINTS ${CMAKE_SOURCE_DIR}/thirdparty/mongoose)

if (MONGOOSE_INCLUDE_DIR)
  set(MONGOOSE_FOUND TRUE)
else ()
  set(MONGOOSE_FOUND FALSE)
endif ()

if (MONGOOSE_FOUND)
  if (NOT MONGOOSE_FIND_QUIETLY)
    message(STATUS "Mongoose web server library found in ${MONGOOSE_INCLUDE_DIR}")
  endif ()
else ()
  message(STATUS "Mongoose web server library includes NOT found. ")
endif ()

mark_as_advanced(
  MONGOOSE_INCLUDE_DIR
)
