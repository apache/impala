# Copyright (c) 2011 Cloudera, Inc. All rights reserved.

cmake_minimum_required(VERSION 2.6)

# where to put generated libraries
set(LIBRARY_OUTPUT_PATH "${CMAKE_SOURCE_DIR}/build/exec")

# where to put generated binaries
set(EXECUTABLE_OUTPUT_PATH "${CMAKE_SOURCE_DIR}/build/exec")

add_library(Exec STATIC
  aggregation-node.cc
  exec-node.cc
  hdfs-text-scan-node.cc
  hbase-scan-node.cc
  hbase-table-scanner.cc
  text-converter.cc
)

target_link_libraries(Exec 
  ${JAVA_JVM_LIBRARY}
  ${HDFS_LIBS}
)
