# Copyright (c) 2011 Cloudera, Inc. All rights reserved.

# where to put generated libraries
set(LIBRARY_OUTPUT_PATH "${CMAKE_SOURCE_DIR}/build/runtime")

# where to put generated binaries
set(EXECUTABLE_OUTPUT_PATH "${CMAKE_SOURCE_DIR}/build/runtime")

# add .cc files and uncomment line
add_library(Runtime STATIC
  data-stream-mgr.cc
  data-stream-sender.cc
  descriptors.cc
  mem-pool.cc
  raw-value.cc
  row-batch.cc
  runtime-state.cc
  string-value.cc
  tuple.cc
)

target_link_libraries(Runtime
  Common
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

add_executable(free-list-test
  free-list-test.cc
)

target_link_libraries(free-list-test
  Runtime
  thrift
  glogstatic
  gflagsstatic
  gtest
  ${Boost_LIBRARIES}
)

add_executable(data-stream-test
  data-stream-test.cc
)

target_link_libraries(data-stream-test
  TestUtil
  Service
  Exec
  Exprs
  Opcode
  Exprs
  Runtime
  Common
  Util
  thrift
  ImpalaThrift
  gtest
  ${Boost_LIBRARIES}
)

add_test(mem-pool-test ${CMAKE_SOURCE_DIR}/build/runtime/mem-pool-test)
add_test(free-list-test ${CMAKE_SOURCE_DIR}/build/runtime/free-list-test)
add_test(data-stream-test ${CMAKE_SOURCE_DIR}/build/runtime/data-stream-test)
