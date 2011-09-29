# Copyright (c) 2011 Cloudera, Inc. All rights reserved.

# where to put generated libraries
set(LIBRARY_OUTPUT_PATH "${CMAKE_SOURCE_DIR}/build/runtime")

# where to put generated binaries
set(EXECUTABLE_OUTPUT_PATH "${CMAKE_SOURCE_DIR}/build/runtime")

# add .cc files and uncomment line
add_library(Runtime STATIC
  descriptors.cc
  mem-pool.cc
  raw-value.cc
  runtime-state.cc
  string-value.cc
  tuple.cc
)

add_executable(mem-pool-test
  mem-pool-test.cc
)

# TODO: w/o the thrift dependency, we're getting a bunch of 'pthread_<> missing' linker
# errors; figure out what we actually need to link against here
target_link_libraries(mem-pool-test
  Runtime
  thrift
  glogstatic
  gflagsstatic
  gtest
  ${Boost_LIBRARIES}
)

target_link_libraries(Runtime
  Common
)

add_test(mem-pool-test ${CMAKE_SOURCE_DIR}/build/runtime/mem-pool-test)
