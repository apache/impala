# Copyright (c) 2011 Cloudera, Inc. All rights reserved.

# where to put generated libraries
set(LIBRARY_OUTPUT_PATH "${CMAKE_SOURCE_DIR}/build/runtime")

# where to put generated binaries
set(EXECUTABLE_OUTPUT_PATH "${CMAKE_SOURCE_DIR}/build/runtime")

# add .cc files and uncomment line
add_library(Runtime STATIC
  descriptors.cc
  mem-pool.cc
  runtime-state.cc
  string-value.cc
)
target_link_libraries(Runtime
  Common
)
