# Copyright (c) 2011 Cloudera, Inc. All rights reserved.

# where to put generated libraries
set(LIBRARY_OUTPUT_PATH "${CMAKE_SOURCE_DIR}/build/util")

# where to put generated binaries
set(EXECUTABLE_OUTPUT_PATH "${CMAKE_SOURCE_DIR}/build/util")

add_library(Util
  debug-string-util.cc
  jni-util.cc
)
