# Copyright (c) 2011 Cloudera, Inc. All rights reserved.

add_library(Common
  status.cc
)

# where to put generated libraries
set(LIBRARY_OUTPUT_PATH "${CMAKE_SOURCE_DIR}/build/common")

# where to put generated binaries
set(EXECUTABLE_OUTPUT_PATH "${CMAKE_SOURCE_DIR}/build/common")
