# Copyright (c) 2011 Cloudera, Inc. All rights reserved.

cmake_minimum_required(VERSION 2.6)

# where to put generated libraries
set(LIBRARY_OUTPUT_PATH "${BUILD_OUTPUT_ROOT_DIRECTORY}/testutil")

# where to put generated binaries
set(EXECUTABLE_OUTPUT_PATH "${BUILD_OUTPUT_ROOT_DIRECTORY}/testutil")

add_library(TestUtil STATIC
  query-executor-if.cc
  impalad-query-executor.cc  
  test-exec-env.cc
)

# TODO why do we need this
target_link_libraries(TestUtil
  ImpalaThrift
)
