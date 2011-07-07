# Copyright (c) 2011 Cloudera, Inc. All rights reserved.

cmake_minimum_required(VERSION 2.6)

#add_executable(text-scan-node-test text-scan-node-test.cc)
#target_link_libraries(text-scan-node-test exec-lib)

# where to put generated libraries
set(LIBRARY_OUTPUT_PATH "${CMAKE_SOURCE_DIR}/build/exec")

# where to put generated binaries
set(EXECUTABLE_OUTPUT_PATH "${CMAKE_SOURCE_DIR}/build/exec")

add_library(Exec STATIC
  exec-node.cc
  text-scan-node.cc
)
