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

add_executable(refresh-catalog refresh-catalog.cc)
target_link_libraries(refresh-catalog ${IMPALA_LINK_LIBS})

ADD_BE_TEST(integer-array-test)
ADD_BE_TEST(runtime-profile-test)
ADD_BE_TEST(benchmark-test)
ADD_BE_TEST(decompress-test)
ADD_BE_TEST(metrics-test)
ADD_BE_TEST(debug-util-test)
#ADD_BE_TEST(perf-counters-test)

