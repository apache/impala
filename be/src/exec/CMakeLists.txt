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


cmake_minimum_required(VERSION 2.6)

# where to put generated libraries
set(LIBRARY_OUTPUT_PATH "${BUILD_OUTPUT_ROOT_DIRECTORY}/exec")

# where to put generated binaries
set(EXECUTABLE_OUTPUT_PATH "${BUILD_OUTPUT_ROOT_DIRECTORY}/exec")

add_library(Exec STATIC
  aggregation-node.cc
  aggregation-node-ir.cc
  base-sequence-scanner.cc
  buffered-byte-stream.cc
  data-sink.cc
  ddl-executor.cc
  delimited-text-parser.cc
  exec-node.cc
  exchange-node.cc
  hash-join-node.cc
  hash-join-node-ir.cc
  hash-table.cc
  hdfs-byte-stream.cc
  hdfs-scan-node.cc
  hdfs-scanner.cc
  hdfs-scanner-ir.cc
  hdfs-table-sink.cc
  hdfs-table-writer.cc
  hdfs-rcfile-scanner.cc
  hdfs-sequence-scanner.cc
  hdfs-avro-scanner.cc
  hdfs-text-scanner.cc
  hdfs-lzo-text-scanner.cc
  hdfs-text-table-writer.cc
  merge-node.cc
#  hdfs-trevni-scanner.cc
  hdfs-trevni-table-writer.cc
  hbase-scan-node.cc
  hbase-table-scanner.cc
  read-write-util.cc
  scan-range-context.cc
  serde-utils.cc
  scan-node.cc
  text-converter.cc
  topn-node.cc
)

# TODO: why is this test disabled?
#ADD_BE_TEST(sequence-file-recovery-test)

ADD_BE_TEST(zigzag-test)
ADD_BE_TEST(hash-table-test)
ADD_BE_TEST(delimited-text-parser-test)
