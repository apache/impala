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

# Without this option Squeasel looks up the SSL library at run-time
# and may not guess the correct name on some distributions
SET_SOURCE_FILES_PROPERTIES(${SQUEASEL_SRC_DIR}/squeasel.c PROPERTIES
  COMPILE_FLAGS -DNO_SSL_DL)

# where to put generated libraries
set(LIBRARY_OUTPUT_PATH "${BUILD_OUTPUT_ROOT_DIRECTORY}/util")

# where to put generated binaries
set(EXECUTABLE_OUTPUT_PATH "${BUILD_OUTPUT_ROOT_DIRECTORY}/util")

add_library(Util
  avro-util.cc
  benchmark.cc
  bitmap.cc
  cgroups-mgr.cc
  codec.cc
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
  llama-util.cc
  logging-support.cc
  mem-info.cc
  memory-metrics.cc
  metrics.cc
  network-util.cc
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
  spinlock.cc
  symbols-util.cc
  static-asserts.cc
  summary-util.cc
  table-printer.cc
  test-info.cc
  thread.cc
  time.cc
  tuple-row-compare.cc
  url-parser.cc
  url-coding.cc
)
add_dependencies(Util thrift-deps)

add_library(Webserver STATIC
  ${SQUEASEL_SRC_DIR}/squeasel.c
  webserver.cc
  ${MUSTACHE_SRC_DIR}/mustache.cc
)

# Squeasel requires C99 compatibility to build.
SET_SOURCE_FILES_PROPERTIES(${SQUEASEL_SRC_DIR}/squeasel.c
  PROPERTIES COMPILE_FLAGS -std=c99)

# shared library which provides native logging support to JVMs over JNI.
add_library(loggingsupport SHARED
  logging-support.cc
)

target_link_libraries(Webserver pthread dl Util ${OPENSSL_LIBS})

add_executable(network-perf-benchmark network-perf-benchmark.cc)
add_executable(parquet-reader parquet-reader.cc)

target_link_libraries(network-perf-benchmark ${IMPALA_LINK_LIBS})
target_link_libraries(parquet-reader ${IMPALA_LINK_LIBS})

target_link_libraries(loggingsupport ${IMPALA_LINK_LIBS_DYNAMIC_TARGETS})

ADD_BE_TEST(runtime-profile-test)
ADD_BE_TEST(benchmark-test)
ADD_BE_TEST(decompress-test)
ADD_BE_TEST(metrics-test)
ADD_BE_TEST(debug-util-test)
ADD_BE_TEST(url-coding-test)
ADD_BE_TEST(bit-util-test)
ADD_BE_TEST(rle-test)
ADD_BE_TEST(blocking-queue-test)
ADD_BE_TEST(dict-test)
ADD_BE_TEST(thread-pool-test)
ADD_BE_TEST(internal-queue-test)
ADD_BE_TEST(string-parser-test)
ADD_BE_TEST(parse-util-test)
ADD_BE_TEST(promise-test)
ADD_BE_TEST(symbols-util-test)
#ADD_BE_TEST(perf-counters-test)
ADD_BE_TEST(webserver-test)
ADD_BE_TEST(pretty-printer-test)
ADD_BE_TEST(redactor-config-parser-test)
ADD_BE_TEST(redactor-test)
ADD_BE_TEST(redactor-unconfigured-test)
ADD_BE_TEST(error-util-test)
target_link_libraries(error-util-test Util)
ADD_BE_TEST(proc-info-test)
ADD_BE_TEST(lru-cache-test)
ADD_BE_TEST(filesystem-util-test)
ADD_BE_TEST(bitmap-test)
ADD_BE_TEST(fixed-size-hash-table-test)
