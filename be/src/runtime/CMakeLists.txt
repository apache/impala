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

add_library(Runtime
  buffered-tuple-stream.cc
  client-cache.cc
  collection-value.cc
  coordinator.cc
  coordinator-backend-state.cc
  datetime-parse-util.cc
  date-parse-util.cc
  date-value.cc
  debug-options.cc
  descriptors.cc
  dml-exec-state.cc
  exec-env.cc
  fragment-instance-state.cc
  hbase-table.cc
  hbase-table-factory.cc
  hdfs-fs-cache.cc
  initial-reservations.cc
  krpc-data-stream-mgr.cc
  krpc-data-stream-recvr.cc
  krpc-data-stream-sender.cc
  krpc-data-stream-sender-ir.cc
  lib-cache.cc
  mem-tracker.cc
  mem-pool.cc
  multi-precision.cc
  query-exec-mgr.cc
  query-state.cc
  test-env.cc
  types.cc
  raw-value.cc
  raw-value-ir.cc
  reservation-manager.cc
  row-batch.cc
  ${ROW_BATCH_PROTO_SRCS}
  row-batch-queue.cc
  runtime-filter.cc
  runtime-filter-bank.cc
  runtime-filter-ir.cc
  runtime-state.cc
  scanner-mem-limiter.cc
  sorted-run-merger.cc
  sorter.cc
  sorter-ir.cc
  string-value.cc
  thread-resource-mgr.cc
  timestamp-parse-util.cc
  timestamp-value.cc
  tuple.cc
  tuple-ir.cc
  tuple-row.cc
  tmp-file-mgr.cc
)
add_dependencies(Runtime gen-deps)

ADD_BE_LSAN_TEST(mem-pool-test)
ADD_BE_LSAN_TEST(free-pool-test)
ADD_BE_LSAN_TEST(string-buffer-test)
ADD_BE_TEST(data-stream-test) # TODO: this test leaks
ADD_BE_LSAN_TEST(date-test)
ADD_BE_LSAN_TEST(timestamp-test)
ADD_BE_LSAN_TEST(raw-value-test)
ADD_BE_LSAN_TEST(string-compare-test)
ADD_BE_LSAN_TEST(string-search-test)
ADD_BE_LSAN_TEST(string-value-test)
ADD_BE_LSAN_TEST(thread-resource-mgr-test)
ADD_BE_LSAN_TEST(mem-tracker-test)
ADD_BE_LSAN_TEST(multi-precision-test)
ADD_BE_LSAN_TEST(decimal-test)
ADD_BE_LSAN_TEST(buffered-tuple-stream-test)
ADD_BE_LSAN_TEST(hdfs-fs-cache-test)
ADD_BE_LSAN_TEST(tmp-file-mgr-test)
ADD_BE_LSAN_TEST(row-batch-serialize-test)
ADD_BE_LSAN_TEST(row-batch-test)
ADD_BE_LSAN_TEST(collection-value-builder-test)
ADD_BE_LSAN_TEST(runtime-state-test)
