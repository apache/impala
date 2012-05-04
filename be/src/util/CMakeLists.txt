# Copyright (c) 2011 Cloudera, Inc. All rights reserved.

# where to put generated libraries
set(LIBRARY_OUTPUT_PATH "${BUILD_OUTPUT_ROOT_DIRECTORY}/util")

# where to put generated binaries
set(EXECUTABLE_OUTPUT_PATH "${BUILD_OUTPUT_ROOT_DIRECTORY}/util")

add_library(Util
  benchmark.cc
  cpu-info.cc
  debug-util.cc
  jni-util.cc
  perf-counters.cc
  runtime-profile.cc
  url-parser.cc
)

add_executable(perf-counters-test perf-counters-test.cc)
add_executable(runtime-profile-test runtime-profile-test.cc)
add_executable(benchmark-test benchmark-test.cc)

target_link_libraries(perf-counters-test ${IMPALA_TEST_LINK_LIBS})
target_link_libraries(runtime-profile-test ${IMPALA_TEST_LINK_LIBS})
target_link_libraries(benchmark-test ${IMPALA_TEST_LINK_LIBS})

add_test(perf-counters-test ${BUILD_OUTPUT_ROOT_DIRECTORY}/util/perf-counters-test)
add_test(runtime-profile-test ${BUILD_OUTPUT_ROOT_DIRECTORY}/util/runtime-profile-test)
add_test(benchmark-test ${BUILD_OUTPUT_ROOT_DIRECTORY}/util/benchmark-test)

