# Copyright (c) 2011 Cloudera, Inc. All rights reserved.

cmake_minimum_required(VERSION 2.6)

# where to put generated libraries
set(LIBRARY_OUTPUT_PATH "${BUILD_OUTPUT_ROOT_DIRECTORY}/exec")

# where to put generated binaries
set(EXECUTABLE_OUTPUT_PATH "${BUILD_OUTPUT_ROOT_DIRECTORY}/exec")

add_library(Exec STATIC
  aggregation-node.cc
  aggregation-node-ir.cc
  buffered-byte-stream.cc
  data-sink.cc
  delimited-text-parser.cc
  exec-node.cc
  exchange-node.cc
  hash-join-node.cc
  hdfs-byte-stream.cc
  hdfs-scan-node.cc
  hdfs-scanner.cc
  hash-table.cc
  hdfs-rcfile-scanner.cc
  hdfs-sequence-scanner.cc
  hdfs-text-scanner.cc
  hdfs-text-table-sink.cc
  hbase-scan-node.cc
  hbase-table-scanner.cc
  text-converter.cc
  topn-node.cc
  rcfile-reader.cc
  serde-utils.cc
)

target_link_libraries(Exec
  Runtime
  ${JAVA_JVM_LIBRARY}
  ${HDFS_LIBS}
)

