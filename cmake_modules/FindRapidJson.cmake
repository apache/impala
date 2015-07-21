# - Find rapidjson headers and lib.
# RAPIDJSON_ROOT hints the location
# This module defines RAPIDJSON_INCLUDE_DIR, the directory containing headers

set(RAPIDJSON_SEARCH_HEADER_PATHS
  ${RAPIDJSON_ROOT}/include
  $ENV{IMPALA_HOME}/thirdparty/rapidjson/include/
)

find_path(RAPIDJSON_INCLUDE_DIR rapidjson/rapidjson.h HINTS
  ${RAPIDJSON_SEARCH_HEADER_PATHS})

if (NOT RAPIDJSON_INCLUDE_DIR)
  message(FATA_ERROR "RapidJson headers NOT found.")
  set(RAPIDJSON_FOUND FALSE)
else()
  set(RAPIDJSON_FOUND TRUE)
endif ()

mark_as_advanced(
  RAPIDJSON_INCLUDE_DIR
)
