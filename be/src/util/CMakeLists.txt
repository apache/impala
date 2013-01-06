# Copyright 2012 Cloudera Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


# where to put generated libraries
set(LIBRARY_OUTPUT_PATH "${BUILD_OUTPUT_ROOT_DIRECTORY}/util")

# where to put generated binaries
set(EXECUTABLE_OUTPUT_PATH "${BUILD_OUTPUT_ROOT_DIRECTORY}/util")

add_library(Util
  authorization.cc
  benchmark.cc
  codec.cc
  compress.cc
  cpu-info.cc
  dynamic-util.cc
  debug-util.cc
  decompress.cc
  default-path-handlers.cc
  disk-info.cc
  hdfs-util.cc
  impalad-metrics.cc
  integer-array.cc
  jni-util.cc
  logging.cc
  metrics.cc
  path-builder.cc
# TODO: not supported on RHEL 5
#  perf-counters.cc
  progress-updater.cc
  runtime-profile.cc
  thrift-util.cc
  thrift-client.cc
  thrift-server.cc
  static-asserts.cc
  stopwatch.cc
  url-parser.cc
  )

add_library(Webserver STATIC
  ${MONGOOSE_SRC_DIR}/mongoose.c
  webserver.cc
)

target_link_libraries(Webserver pthread dl)

add_executable(integer-array-test integer-array-test.cc)
#add_executable(perf-counters-test perf-counters-test.cc)
add_executable(runtime-profile-test runtime-profile-test.cc)
add_executable(benchmark-test benchmark-test.cc)
add_executable(decompress-test decompress-test.cc)
add_executable(metrics-test metrics-test.cc)
add_executable(debug-util-test debug-util-test.cc)
add_executable(refresh-catalog refresh-catalog.cc)

target_link_libraries(integer-array-test ${IMPALA_TEST_LINK_LIBS})
#target_link_libraries(perf-counters-test ${IMPALA_TEST_LINK_LIBS})
target_link_libraries(runtime-profile-test ${IMPALA_TEST_LINK_LIBS})
target_link_libraries(benchmark-test ${IMPALA_TEST_LINK_LIBS})
target_link_libraries(decompress-test ${IMPALA_TEST_LINK_LIBS})
target_link_libraries(metrics-test ${IMPALA_TEST_LINK_LIBS})
target_link_libraries(debug-util-test ${IMPALA_TEST_LINK_LIBS})
target_link_libraries(refresh-catalog ${IMPALA_LINK_LIBS})

add_test(integer-array-test ${BUILD_OUTPUT_ROOT_DIRECTORY}/util/integer-array-test)
#add_test(perf-counters-test ${BUILD_OUTPUT_ROOT_DIRECTORY}/util/perf-counters-test)
add_test(runtime-profile-test ${BUILD_OUTPUT_ROOT_DIRECTORY}/util/runtime-profile-test)
add_test(benchmark-test ${BUILD_OUTPUT_ROOT_DIRECTORY}/util/benchmark-test)
add_test(decompress-test ${BUILD_OUTPUT_ROOT_DIRECTORY}/util/decompress-test)
add_test(metrics-test ${BUILD_OUTPUT_ROOT_DIRECTORY}/util/metrics-test)
add_test(debug-util-test ${BUILD_OUTPUT_ROOT_DIRECTORY}/util/debug-util-test)

