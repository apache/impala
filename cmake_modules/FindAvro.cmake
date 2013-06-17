# - Find Avro (headers and libavrocpp_s.a)
# This module defines
#  AVRO_INCLUDE_DIR, directory containing headers
#  AVRO_LIBS, directory containing Avro libraries
#  AVRO_STATIC_LIB, path to libavrocpp_s.a
#  AVRO_FOUND, whether Avro has been found

set(AVRO_SEARCH_HEADER_PATHS
  ${CMAKE_SOURCE_DIR}/thirdparty/avro-cpp-$ENV{IMPALA_AVRO_VERSION}/api
)

set(AVRO_SEARCH_LIB_PATH
  ${CMAKE_SOURCE_DIR}/thirdparty/avro-cpp-$ENV{IMPALA_AVRO_VERSION}
)

find_path(AVRO_INCLUDE_DIR AvroSerialize.hh PATHS
  ${AVRO_SEARCH_HEADER_PATHS}
  # make sure we don't accidentally pick up a different version
  NO_DEFAULT_PATH
)

find_library(AVRO_LIB_PATH NAMES avrocpp PATHS ${AVRO_SEARCH_LIB_PATH})

if (AVRO_LIB_PATH)
  set(AVRO_FOUND TRUE)
  set(AVRO_LIBS ${AVRO_SEARCH_LIB_PATH})
  set(AVRO_STATIC_LIB ${AVRO_SEARCH_LIB_PATH}/libavrocpp_s.a)
else ()
  set(AVRO_FOUND FALSE)
endif ()

if (AVRO_FOUND)
  if (NOT AVRO_FIND_QUIETLY)
    message(STATUS "Avro found in ${AVRO_SEARCH_LIB_PATH}")
  endif ()
else ()
  message(STATUS "Avro includes and libraries NOT found. "
    "Looked for headers in ${AVRO_SEARCH_HEADER_PATHS}, "
    "and for libs in ${AVRO_SEARCH_LIB_PATH}")
endif ()

mark_as_advanced(
  AVRO_INCLUDE_DIR
  AVRO_LIBS
  AVRO_STATIC_LIB
)
