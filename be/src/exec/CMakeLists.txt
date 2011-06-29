# Copyright (c) 2011 Cloudera, Inc. All rights reserved.

cmake_minimum_required(VERSION 2.6)

#add_library(exec-lib STATIC text-scan-node.cc)

#add_executable(text-scan-node-test text-scan-node-test.cc)
#target_link_libraries(text-scan-node-test exec-lib)

# where to put generated libraries
set(LIBRARY_OUTPUT_PATH "${CMAKE_SOURCE_DIR}/build/")

# where to put generated binaries
set(EXECUTABLE_OUTPUT_PATH "${CMAKE_SOURCE_DIR}/build/")
