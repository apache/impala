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
set(MPFIT_SRC_DIR "${CMAKE_SOURCE_DIR}/be/src/thirdparty/mpfit")

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
  bit-packing.cc
  bit-util.cc
  bloom-filter.cc
  bloom-filter-ir.cc
  cgroup-util.cc
  coding-util.cc
  codec.cc
  collection-metrics.cc
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
  event-metrics.cc
  filesystem-util.cc
  flat_buffer.cc
  hdfs-util.cc
  hdfs-bulk-ops.cc
  hdr-histogram.cc
  histogram-metric.cc
  impalad-metrics.cc
  jni-util.cc
  logging-support.cc
  mem-info.cc
  memory-metrics.cc
  metrics.cc
  min-max-filter.cc
  min-max-filter-ir.cc
  minidump.cc
  mpfit-util.cc
  network-util.cc
  openssl-util.cc
  os-info.cc
  os-util.cc
  parse-util.cc
  path-builder.cc
  periodic-counter-updater
  pprof-path-handlers.cc
  progress-updater.cc
  process-state-info.cc
  redactor.cc
  runtime-profile.cc
  simple-logger.cc
  string-parser.cc
  string-util.cc
  symbols-util.cc
  system-state-info.cc
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
  zip-util.cc
  ${MUSTACHE_SRC_DIR}/mustache.cc
  ${MPFIT_SRC_DIR}/mpfit.c)
add_dependencies(Util gen-deps gen_ir_descriptions)

add_library(UtilTests
  benchmark-test.cc
  bitmap-test.cc
  bit-packing-test.cc
  bit-stream-utils-test.cc
  bit-util-test.cc
  blocking-queue-test.cc
  bloom-filter-test.cc
  coding-util-test.cc
  debug-util-test.cc
  dict-test.cc
  error-util-test.cc
  filesystem-util-test.cc
  fixed-size-hash-table-test.cc
  hdfs-util-test.cc
  logging-support-test.cc
  lru-cache-test.cc
  metrics-test.cc
  min-max-filter-test.cc
  openssl-util-test.cc
  os-util-test.cc
  parse-util-test.cc
  pretty-printer-test.cc
  proc-info-test.cc
  redactor-config-parser-test.cc
  redactor-test.cc
  redactor-test-utils.cc
  redactor-unconfigured-test.cc
  rle-test.cc
  runtime-profile-test.cc
  string-parser-test.cc
  string-util-test.cc
  symbols-util-test.cc
  sys-info-test.cc
  system-state-info-test.cc
  thread-pool-test.cc
  time-test.cc
  uid-util-test.cc
  zip-util-test.cc
)
add_dependencies(UtilTests gen-deps gen_ir_descriptions)

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

ADD_UNIFIED_BE_LSAN_TEST(benchmark-test "BenchmarkTest.*")
ADD_UNIFIED_BE_LSAN_TEST(bitmap-test "Bitmap.*")
ADD_UNIFIED_BE_LSAN_TEST(bit-packing-test "BitPackingTest.*")
ADD_UNIFIED_BE_LSAN_TEST(bit-stream-utils-test "BitArray.*")
ADD_UNIFIED_BE_LSAN_TEST(bit-util-test "BitUtil.*")
ADD_UNIFIED_BE_LSAN_TEST(blocking-queue-test "BlockingQueueTest.*")
ADD_UNIFIED_BE_LSAN_TEST(bloom-filter-test "BloomFilter.*:BloomFilterTest.*")
ADD_UNIFIED_BE_LSAN_TEST(coding-util-test "UrlCodingTest.*:Base64Test.*:HtmlEscapingTest.*")
ADD_UNIFIED_BE_LSAN_TEST(debug-util-test "DebugUtil.*")
# Decompress-test fails in unified mode (possibly due to missing libs)
ADD_BE_LSAN_TEST(decompress-test)
ADD_UNIFIED_BE_LSAN_TEST(dict-test "DictTest.*")
ADD_UNIFIED_BE_LSAN_TEST(error-util-test "ErrorMsg.*")
ADD_UNIFIED_BE_LSAN_TEST(filesystem-util-test "FilesystemUtil.*")
ADD_UNIFIED_BE_LSAN_TEST(fixed-size-hash-table-test "FixedSizeHash.*")
ADD_UNIFIED_BE_LSAN_TEST(hdfs-util-test HdfsUtilTest.*)
# internal-queue-test has a non-standard main(), so it needs a small amount of thought
# to use a unified executable
ADD_BE_LSAN_TEST(internal-queue-test)
ADD_UNIFIED_BE_LSAN_TEST(logging-support-test "LoggingSupport.*")
ADD_UNIFIED_BE_LSAN_TEST(lru-cache-test "FifoMultimap.*")
ADD_UNIFIED_BE_LSAN_TEST(metrics-test "MetricsTest.*")
ADD_UNIFIED_BE_LSAN_TEST(min-max-filter-test "MinMaxFilterTest.*")
ADD_UNIFIED_BE_LSAN_TEST(openssl-util-test "OpenSSLUtilTest.*")
ADD_UNIFIED_BE_LSAN_TEST(os-util-test "OsUtil.*")
ADD_UNIFIED_BE_LSAN_TEST(parse-util-test "ParseMemSpecs.*")
ADD_UNIFIED_BE_LSAN_TEST(pretty-printer-test "PrettyPrinterTest.*")
ADD_UNIFIED_BE_LSAN_TEST(proc-info-test "MemInfo.*:ProcessStateInfo.*:MappedMapInfo.*:CGroupInfo.*")
# IMPALA-4128: promise-test has a non-standard main(), so it can't be unified yet
ADD_BE_LSAN_TEST(promise-test)
ADD_UNIFIED_BE_LSAN_TEST(redactor-config-parser-test ParserTest.*)
ADD_UNIFIED_BE_LSAN_TEST(redactor-test "RedactorTest.*")
ADD_UNIFIED_BE_LSAN_TEST(redactor-unconfigured-test "RedactorUnconfigTest.*")
ADD_UNIFIED_BE_LSAN_TEST(rle-test "BitArray.*:RleTest.*")
ADD_UNIFIED_BE_LSAN_TEST(runtime-profile-test "CountersTest.*:TimerCounterTest.*:TimeSeriesCounterTest.*:VariousNumbers/TimeSeriesCounterResampleTest.*:ToThrift.*")
ADD_UNIFIED_BE_LSAN_TEST(string-parser-test "StringToInt.*:StringToIntWithBase.*:StringToFloat.*:StringToBool.*:StringToDate.*")
ADD_UNIFIED_BE_LSAN_TEST(string-util-test "TruncateDownTest.*:TruncateUpTest.*:CommaSeparatedContainsTest.*")
ADD_UNIFIED_BE_LSAN_TEST(symbols-util-test "SymbolsUtil.*")
ADD_UNIFIED_BE_LSAN_TEST(system-state-info-test "SystemStateInfoTest.*")
ADD_UNIFIED_BE_LSAN_TEST(sys-info-test "CpuInfoTest.*:DiskInfoTest.*")
ADD_UNIFIED_BE_LSAN_TEST(thread-pool-test "ThreadPoolTest.*")
ADD_UNIFIED_BE_LSAN_TEST(time-test "TimeTest.*")
ADD_UNIFIED_BE_LSAN_TEST(uid-util-test "UidUtil.*")
# Using standalone webserver-test for now, nonstandard main() passes in a port.
ADD_BE_LSAN_TEST(webserver-test)
ADD_UNIFIED_BE_LSAN_TEST(zip-util-test "ZipUtilTest.*")
