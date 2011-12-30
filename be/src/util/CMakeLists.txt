# Copyright (c) 2011 Cloudera, Inc. All rights reserved.

# where to put generated libraries
set(LIBRARY_OUTPUT_PATH "${CMAKE_SOURCE_DIR}/be/build/util")

# where to put generated binaries
set(EXECUTABLE_OUTPUT_PATH "${CMAKE_SOURCE_DIR}/be/build/util")

add_library(Util
  cpu-info.cc
  debug-util.cc
  jni-util.cc
  perf-counters.cc
)

add_executable(perf-counters-test
  perf-counters-test.cc
)

target_link_libraries(perf-counters-test
  TestUtil
  Util
  Runtime
  Util
  Common
  thrift
  ImpalaThrift
  glogstatic
  gflagsstatic
  gtest
  ${Boost_LIBRARIES}
)

add_test(perf-counters-test ${CMAKE_SOURCE_DIR}/be/build/util/perf-counters-test)

