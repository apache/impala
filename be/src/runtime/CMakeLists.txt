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

add_subdirectory(bufferpool)
add_subdirectory(io)

# where to put generated libraries
set(LIBRARY_OUTPUT_PATH "${BUILD_OUTPUT_ROOT_DIRECTORY}/runtime")

# where to put generated binaries
set(EXECUTABLE_OUTPUT_PATH "${BUILD_OUTPUT_ROOT_DIRECTORY}/runtime")

# Mark the protobuf file as generated
set_source_files_properties(${ROW_BATCH_PROTO_SRCS} PROPERTIES GENERATED TRUE)

add_library(RuntimeIr
  krpc-data-stream-sender-ir.cc
  raw-value-ir.cc
  runtime-filter-ir.cc
  sorter-ir.cc
  sorted-run-merger-ir.cc
  string-value-ir.cc
  tuple-ir.cc
)
add_dependencies(RuntimeIr gen-deps)

add_library(Runtime
  blocking-row-batch-queue.cc
  buffered-tuple-stream.cc
  client-cache.cc
  collection-value.cc
  coordinator.cc
  coordinator-backend-state.cc
  coordinator-backend-resource-state.cc
  datetime-iso-sql-format-parser.cc
  datetime-iso-sql-format-tokenizer.cc
  datetime-parser-common.cc
  datetime-simple-date-format-parser.cc
  date-parse-util.cc
  date-value.cc
  debug-options.cc
  descriptors.cc
  dml-exec-state.cc
  exec-env.cc
  fragment-state.cc
  fragment-instance-state.cc
  hbase-table.cc
  hbase-table-factory.cc
  hdfs-fs-cache.cc
  initial-reservations.cc
  krpc-data-stream-mgr.cc
  krpc-data-stream-recvr.cc
  krpc-data-stream-sender.cc
  lib-cache.cc
  mem-tracker.cc
  mem-pool.cc
  query-driver.cc
  query-exec-mgr.cc
  query-exec-params.cc
  query-state.cc
  test-env.cc
  types.cc
  raw-value.cc
  reservation-manager.cc
  row-batch.cc
  ${ROW_BATCH_PROTO_SRCS}
  runtime-filter.cc
  runtime-filter-bank.cc
  runtime-state.cc
  scanner-mem-limiter.cc
  sorted-run-merger.cc
  sorter.cc
  spillable-row-batch-queue.cc
  string-value.cc
  thread-resource-mgr.cc
  timestamp-parse-util.cc
  timestamp-value.cc
  tuple.cc
  tuple-cache-mgr.cc
  tuple-row.cc
  tmp-file-mgr.cc
)
add_dependencies(Runtime gen-deps)

if (BUILD_WITH_NO_TESTS)
  return()
endif()

add_library(RuntimeTests STATIC
  coordinator-backend-state-test.cc
  date-test.cc
  decimal-test.cc
  free-pool-test.cc
  hdfs-fs-cache-test.cc
  mem-pool-test.cc
  mem-tracker-test.cc
  multi-precision-test.cc
  raw-value-test.cc
  row-batch-serialize-test.cc
  runtime-filter-test.cc
  string-buffer-test.cc
  string-compare-test.cc
  string-search-test.cc
  string-value-test.cc
  thread-resource-mgr-test.cc
  timestamp-test.cc
  tmp-file-mgr-test.cc
  tuple-cache-mgr-test.cc
)
add_dependencies(RuntimeTests gen-deps)

ADD_UNIFIED_BE_LSAN_TEST(coordinator-backend-state-test CoordinatorBackendStateTest.*)
ADD_UNIFIED_BE_LSAN_TEST(mem-pool-test MemPoolTest.*)
ADD_BE_TEST(client-cache-test)
ADD_UNIFIED_BE_LSAN_TEST(free-pool-test FreePoolTest.*)
ADD_UNIFIED_BE_LSAN_TEST(string-buffer-test StringBufferTest.*)
# Exception to unified be tests: Custom main function (initializes LLVM)
ADD_BE_TEST(data-stream-test) # TODO: this test leaks
ADD_UNIFIED_BE_LSAN_TEST(date-test DateTest.*)
ADD_UNIFIED_BE_LSAN_TEST(timestamp-test TimestampTest.*)
ADD_UNIFIED_BE_LSAN_TEST(raw-value-test "RawValueTest.*:HashUtil.*")
ADD_UNIFIED_BE_LSAN_TEST(string-compare-test StringCompareTest.*)
ADD_UNIFIED_BE_LSAN_TEST(string-search-test StringSearchTest.*)
ADD_UNIFIED_BE_LSAN_TEST(string-value-test StringValueTest.*)
ADD_UNIFIED_BE_LSAN_TEST(thread-resource-mgr-test ThreadResourceMgr.*)
ADD_UNIFIED_BE_LSAN_TEST(mem-tracker-test MemTestTest.*)
ADD_UNIFIED_BE_LSAN_TEST(multi-precision-test
    "MultiPrecisionIntTest.*:MultiPrecisionFloatTest.*")
ADD_UNIFIED_BE_LSAN_TEST(decimal-test DecimalTest.*)
# Exception to unified be tests: Custom main function (initializes LLVM)
ADD_BE_LSAN_TEST(buffered-tuple-stream-test)
ADD_UNIFIED_BE_LSAN_TEST(hdfs-fs-cache-test "HdfsFsCacheTest.*")
ADD_UNIFIED_BE_LSAN_TEST(tmp-file-mgr-test "TmpFileMgrTest.*")
ADD_UNIFIED_BE_LSAN_TEST(tuple-cache-mgr-test "TupleCacheMgrTest.*")
ADD_UNIFIED_BE_LSAN_TEST(row-batch-serialize-test "RowBatchSerializeTest.*")
# Exception to unified be tests: Custom main function with global Frontend object
ADD_UNIFIED_BE_LSAN_TEST(runtime-filter-test "RuntimeFilterTest.*")
ADD_BE_LSAN_TEST(row-batch-test)
# Exception to unified be tests: Custom main function with global Frontend object
ADD_BE_LSAN_TEST(collection-value-builder-test)
# Exception to unified be tests: Custom main function (initializes LLVM)
ADD_BE_LSAN_TEST(runtime-state-test)
