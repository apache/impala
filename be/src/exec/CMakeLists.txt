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

add_subdirectory(parquet)

cmake_minimum_required(VERSION 2.6)

# where to put generated libraries
set(LIBRARY_OUTPUT_PATH "${BUILD_OUTPUT_ROOT_DIRECTORY}/exec")

# where to put generated binaries
set(EXECUTABLE_OUTPUT_PATH "${BUILD_OUTPUT_ROOT_DIRECTORY}/exec")

add_library(Exec
  aggregation-node.cc
  aggregation-node-base.cc
  aggregator.cc
  analytic-eval-node.cc
  base-sequence-scanner.cc
  blocking-join-node.cc
  cardinality-check-node.cc
  catalog-op-executor.cc
  data-sink.cc
  data-source-scan-node.cc
  delimited-text-parser.cc
  empty-set-node.cc
  exec-node.cc
  exchange-node.cc
  external-data-source-executor.cc
  filter-context.cc
  grouping-aggregator.cc
  grouping-aggregator-ir.cc
  grouping-aggregator-partition.cc
  hash-table.cc
  hbase-table-sink.cc
  hbase-table-writer.cc
  hdfs-scan-node.cc
  hdfs-scan-node-base.cc
  hdfs-scan-node-mt.cc
  hdfs-scanner.cc
  hdfs-scanner-ir.cc
  hdfs-table-sink.cc
  hdfs-table-writer.cc
  hdfs-rcfile-scanner.cc
  hdfs-sequence-scanner.cc
  hdfs-avro-scanner.cc
  hdfs-avro-scanner-ir.cc
  hdfs-plugin-text-scanner.cc
  hdfs-text-scanner.cc
  hdfs-text-table-writer.cc
  hdfs-orc-scanner.cc
  hbase-scan-node.cc
  hbase-table-scanner.cc
  incr-stats-util.cc
  nested-loop-join-builder.cc
  nested-loop-join-node.cc
  non-grouping-aggregator.cc
  non-grouping-aggregator-ir.cc
  orc-column-readers.cc
  orc-metadata-utils.cc
  partial-sort-node.cc
  partitioned-hash-join-builder.cc
  partitioned-hash-join-builder-ir.cc
  partitioned-hash-join-node.cc
  partitioned-hash-join-node-ir.cc
  plan-root-sink.cc
  kudu-scanner.cc
  kudu-scan-node.cc
  kudu-scan-node-base.cc
  kudu-scan-node-mt.cc
  kudu-table-sink.cc
  kudu-util.cc
  read-write-util.cc
  scan-node.cc
  scanner-context.cc
  select-node.cc
  select-node-ir.cc
  singular-row-src-node.cc
  sort-node.cc
  streaming-aggregation-node.cc
  subplan-node.cc
  text-converter.cc
  topn-node.cc
  topn-node-ir.cc
  union-node.cc
  union-node-ir.cc
  unnest-node.cc
)

add_dependencies(Exec gen-deps)

ADD_BE_LSAN_TEST(zigzag-test)
ADD_BE_LSAN_TEST(hash-table-test)
ADD_BE_LSAN_TEST(delimited-text-parser-test)
ADD_BE_LSAN_TEST(read-write-util-test)
ADD_BE_LSAN_TEST(row-batch-list-test)
ADD_BE_LSAN_TEST(incr-stats-util-test)
ADD_BE_LSAN_TEST(hdfs-avro-scanner-test)
