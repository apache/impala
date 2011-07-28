# Copyright (c) 2011 Cloudera, Inc. All rights reserved.

cmake_minimum_required(VERSION 2.6)

# where to put generated libraries
set(LIBRARY_OUTPUT_PATH "${CMAKE_SOURCE_DIR}/build/testutil")

# where to put generated binaries
set(EXECUTABLE_OUTPUT_PATH "${CMAKE_SOURCE_DIR}/build/testutil")

add_library(MockHdfs STATIC
  mock-hdfs.cc
)

add_library(TestUtil STATIC
  query-executor.cc  
)

target_link_libraries(TestUtil
  glogstatic
  gflagsstatic  
)