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

# where to put generated libraries
set(LIBRARY_OUTPUT_PATH "${BUILD_OUTPUT_ROOT_DIRECTORY}/runtime")

# where to put generated binaries
set(EXECUTABLE_OUTPUT_PATH "${BUILD_OUTPUT_ROOT_DIRECTORY}/runtime")

add_library(Runtime
  buffered-tuple-stream.cc
  client-cache.cc
  coordinator.cc
  coordinator-backend-state.cc
  data-stream-mgr.cc
  data-stream-recvr.cc
  data-stream-sender.cc
  debug-options.cc
  descriptors.cc
  disk-io-mgr.cc
  disk-io-mgr-reader-context.cc
  disk-io-mgr-scan-range.cc
  disk-io-mgr-stress.cc
  exec-env.cc
  fragment-instance-state.cc
  hbase-table.cc
  hbase-table-factory.cc
  hdfs-fs-cache.cc
  initial-reservations.cc
  krpc-data-stream-mgr.cc
  krpc-data-stream-recvr.cc
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
  row-batch.cc
  runtime-filter.cc
  runtime-filter-bank.cc
  runtime-filter-ir.cc
  runtime-state.cc
  sorted-run-merger.cc
  sorter.cc
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

# This test runs forever so should not be part of 'make test'
add_executable(disk-io-mgr-stress-test disk-io-mgr-stress-test.cc)
target_link_libraries(disk-io-mgr-stress-test ${IMPALA_TEST_LINK_LIBS})

ADD_BE_TEST(mem-pool-test)
ADD_BE_TEST(free-pool-test)
ADD_BE_TEST(string-buffer-test)
ADD_BE_TEST(data-stream-test)
ADD_BE_TEST(timestamp-test)
ADD_BE_TEST(disk-io-mgr-test)
ADD_BE_TEST(raw-value-test)
ADD_BE_TEST(string-compare-test)
ADD_BE_TEST(string-search-test)
ADD_BE_TEST(string-value-test)
ADD_BE_TEST(thread-resource-mgr-test)
ADD_BE_TEST(mem-tracker-test)
ADD_BE_TEST(multi-precision-test)
ADD_BE_TEST(decimal-test)
ADD_BE_TEST(buffered-tuple-stream-test)
ADD_BE_TEST(hdfs-fs-cache-test)
ADD_BE_TEST(tmp-file-mgr-test)
ADD_BE_TEST(row-batch-serialize-test)
ADD_BE_TEST(row-batch-test)
ADD_BE_TEST(collection-value-builder-test)
