# Copyright (c) 2011 Cloudera, Inc. All rights reserved.

cmake_minimum_required(VERSION 2.6)

# where to put generated libraries
set(LIBRARY_OUTPUT_PATH "${BUILD_OUTPUT_ROOT_DIRECTORY}/exec")

# where to put generated binaries
set(EXECUTABLE_OUTPUT_PATH "${BUILD_OUTPUT_ROOT_DIRECTORY}/exec")

add_library(Exec STATIC
  aggregation-node.cc
  exec-node.cc
  exchange-node.cc
  hash-join-node.cc
  hash-table.cc
  hdfs-text-scan-node.cc
  hdfs-rcfile-scan-node.cc
  hbase-scan-node.cc
  hbase-table-scanner.cc
  text-converter.cc
  topn-node.cc
  rcfile-reader.cc
  serde-utils.cc
)

target_link_libraries(Exec
  Runtime
)

target_link_libraries(Exec 
  ${JAVA_JVM_LIBRARY}
  ${HDFS_LIBS}
)
