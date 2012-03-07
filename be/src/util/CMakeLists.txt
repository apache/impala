# Copyright (c) 2011 Cloudera, Inc. All rights reserved.

# where to put generated libraries
set(LIBRARY_OUTPUT_PATH "${BUILD_OUTPUT_ROOT_DIRECTORY}/util")

# where to put generated binaries
set(EXECUTABLE_OUTPUT_PATH "${BUILD_OUTPUT_ROOT_DIRECTORY}/util")

add_library(Util
  cpu-info.cc
  debug-util.cc
  jni-util.cc
  perf-counters.cc
  runtime-profile.cc
)

add_executable(perf-counters-test
  perf-counters-test.cc
)

add_executable(runtime-profile-test
  runtime-profile-test.cc
)

target_link_libraries(perf-counters-test
  TestUtil
  Util
  Runtime
  Util
  Common
  thriftstatic
  ImpalaThrift
  glogstatic
  gflagsstatic
  gtest
  ${Boost_LIBRARIES}
)

target_link_libraries(runtime-profile-test
  TestUtil
  Util
  Runtime
  Util
  Common
  thriftstatic
  ImpalaThrift
  glogstatic
  gflagsstatic
  gtest
  ${Boost_LIBRARIES}
)

add_test(perf-counters-test ${BUILD_OUTPUT_ROOT_DIRECTORY}/util/perf-counters-test)
add_test(runtime-profile-test ${BUILD_OUTPUT_ROOT_DIRECTORY}/util/runtime-profile-test)

