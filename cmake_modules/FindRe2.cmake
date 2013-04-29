# - Find re2 headers and lib.
# This module defines
#  RE2_INCLUDE_DIR, directory containing headers
#  RE2_STATIC_LIB, path to libsnappy.a
#  RE2_FOUND, whether gflags has been found

set(RE2_SEARCH_HEADER_PATHS  
  ${CMAKE_SOURCE_DIR}/thirdparty/re2/re2
)

set(RE2_SEARCH_LIB_PATH
  ${CMAKE_SOURCE_DIR}/thirdparty/re2/obj
)

set(RE2_INCLUDE_DIR 
  ${CMAKE_SOURCE_DIR}/thirdparty/re2
)

find_library(RE2_LIB_PATH NAMES re2
  PATHS ${RE2_SEARCH_LIB_PATH}
        NO_DEFAULT_PATH
  DOC   "Google's re2 regex library"
)

if (RE2_LIB_PATH)
  set(RE2_FOUND TRUE)
  set(RE2_LIBS ${RE2_SEARCH_LIB_PATH})
  set(RE2_STATIC_LIB ${RE2_SEARCH_LIB_PATH}/libre2.a)
else ()
  set(RE2_FOUND FALSE)
endif ()

if (RE2_FOUND)
  if (NOT RE2_FIND_QUIETLY)
    message(STATUS "Re2 Found in ${RE2_SEARCH_LIB_PATH}")
  endif ()
else ()
  message(STATUS "Re2 includes and libraries NOT found. "
    "Looked for headers in ${RE2_SEARCH_HEADER_PATH}, "
    "and for libs in ${RE2_SEARCH_LIB_PATH}")
endif ()

mark_as_advanced(
  RE2_INCLUDE_DIR
  RE2_LIBS
  RE2_STATIC_LIB
)
