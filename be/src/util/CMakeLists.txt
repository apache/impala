# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

set(SQUEASEL_SRC_DIR "${CMAKE_SOURCE_DIR}/be/src/thirdparty/squeasel")
set(MUSTACHE_SRC_DIR "${CMAKE_SOURCE_DIR}/be/src/thirdparty/mustache")

# where to put generated libraries
set(LIBRARY_OUTPUT_PATH "${BUILD_OUTPUT_ROOT_DIRECTORY}/util")

# where to put generated binaries
set(EXECUTABLE_OUTPUT_PATH "${BUILD_OUTPUT_ROOT_DIRECTORY}/util")

add_library(Util
  auth-util.cc
  avro-util.cc
  backend-gflag-util.cc
  benchmark.cc
  bitmap.cc
  bit-util.cc
  bloom-filter.cc
  coding-util.cc
  codec.cc
  common-metrics.cc
  compress.cc
  cpu-info.cc
  decimal-util.cc
  dynamic-util.cc
  debug-util.cc
  decompress.cc
  default-path-handlers.cc
  disk-info.cc
  error-util.cc
  filesystem-util.cc
  hdfs-util.cc
  hdfs-bulk-ops.cc
  hdr-histogram.cc
  impalad-metrics.cc
  jni-util.cc
  logging-support.cc
  mem-info.cc
  memory-metrics.cc
  metrics.cc
  minidump.cc
  network-util.cc
  openssl-util.cc
  os-info.cc
  os-util.cc
  parse-util.cc
  path-builder.cc
  periodic-counter-updater
  pprof-path-handlers.cc
# TODO: not supported on RHEL 5
#  perf-counters.cc
  progress-updater.cc
  process-state-info.cc
  redactor.cc
  runtime-profile.cc
  simple-logger.cc
  symbols-util.cc
  static-asserts.cc
  summary-util.cc
  table-printer.cc
  test-info.cc
  thread.cc
  time.cc
  tuple-row-compare.cc
  url-parser.cc
  ${SQUEASEL_SRC_DIR}/squeasel.c
  webserver.cc
  ${MUSTACHE_SRC_DIR}/mustache.cc
)
add_dependencies(Util thrift-deps gen_ir_descriptions)

# Squeasel requires C99 compatibility to build.
SET_SOURCE_FILES_PROPERTIES(${SQUEASEL_SRC_DIR}/squeasel.c
  PROPERTIES COMPILE_FLAGS -std=c99)

# shared library which provides native logging support to JVMs over JNI.
add_library(loggingsupport SHARED
  logging-support.cc
)

add_executable(parquet-reader parquet-reader.cc)

target_link_libraries(parquet-reader ${IMPALA_LINK_LIBS})

target_link_libraries(loggingsupport ${IMPALA_LINK_LIBS_DYNAMIC_TARGETS})

ADD_BE_TEST(benchmark-test)
ADD_BE_TEST(bitmap-test)
ADD_BE_TEST(bit-packing-test)
ADD_BE_TEST(bit-util-test)
ADD_BE_TEST(blocking-queue-test)
ADD_BE_TEST(bloom-filter-test)
ADD_BE_TEST(coding-util-test)
ADD_BE_TEST(debug-util-test)
ADD_BE_TEST(decompress-test)
ADD_BE_TEST(dict-test)
ADD_BE_TEST(error-util-test)
ADD_BE_TEST(filesystem-util-test)
ADD_BE_TEST(fixed-size-hash-table-test)
ADD_BE_TEST(hdfs-util-test)
ADD_BE_TEST(internal-queue-test)
ADD_BE_TEST(logging-support-test)
ADD_BE_TEST(lru-cache-test)
ADD_BE_TEST(metrics-test)
ADD_BE_TEST(openssl-util-test)
ADD_BE_TEST(parse-util-test)
#ADD_BE_TEST(perf-counters-test)
ADD_BE_TEST(pretty-printer-test)
ADD_BE_TEST(proc-info-test)
ADD_BE_TEST(promise-test)
ADD_BE_TEST(redactor-config-parser-test)
ADD_BE_TEST(redactor-test)
ADD_BE_TEST(redactor-unconfigured-test)
ADD_BE_TEST(rle-test)
ADD_BE_TEST(runtime-profile-test)
ADD_BE_TEST(string-parser-test)
ADD_BE_TEST(symbols-util-test)
ADD_BE_TEST(thread-pool-test)
ADD_BE_TEST(uid-util-test)
ADD_BE_TEST(webserver-test)
