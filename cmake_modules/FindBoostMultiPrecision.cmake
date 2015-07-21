# - Find Boost Multi Precision library (cpp_int.h)
# This is a header only library so we just need to set the include dir

set(BOOST_MULTI_PRECISION_SEARCH_HEADER_PATH
  $ENV{IMPALA_HOME}/thirdparty/boost_multiprecision
)

find_file(BOOST_MULTI_PRECISION_HEADER NAMES cpp_int.hpp
  PATHS ${BOOST_MULTI_PRECISION_SEARCH_HEADER_PATH}
        NO_DEFAULT_PATH
  DOC   "Boost Multi Precision Library"
)

if (BOOST_MULTI_PRECISION_HEADER)
  set(BOOST_MULTI_PRECISION_FOUND TRUE)
  set(BOOST_MULTI_PRECISION_INCLUDE_DIR ${BOOST_MULTI_PRECISION_SEARCH_HEADER_PATH})
else ()
  set(BOOST_MULTI_PRECISION_FOUND FALSE)
endif ()

if (BOOST_MULTI_PRECISION_FOUND)
  message(STATUS "Boost Multi Precision found in ${BOOST_MULTI_PRECISION_INCLUDE_DIR}")
else ()
  message(STATUS "Boost Multip Precision NOT found. "
    "Header should be in ${BOOST_MULTI_PRECISION_SEARCH_HEADER_PATH}")
endif ()

mark_as_advanced(
  BOOST_MULTI_PRECISION_INCLUDE_DIR
)
