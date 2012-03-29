# Copyright (c) 2011 Cloudera, Inc. All rights reserved.

cmake_minimum_required(VERSION 2.6)

# where to put generated libraries
set(LIBRARY_OUTPUT_PATH "${BUILD_OUTPUT_ROOT_DIRECTORY}/testutil")

# where to put generated binaries
set(EXECUTABLE_OUTPUT_PATH "${BUILD_OUTPUT_ROOT_DIRECTORY}/testutil")

add_library(TestUtil STATIC
  query-executor.cc  
  test-exec-env.cc
)

target_link_libraries(TestUtil
  Service
  Runtime
  ImpalaThrift
  glogstatic
  gflagsstatic  
)

add_executable(query-jitter
  query-jitter.cc
)

target_link_libraries(query-jitter
  TestUtil
  CodeGen
  Exprs
  Opcode
  Exprs
  Util
  Runtime
  Util
  Common
  ImpalaThrift
  glogstatic
  gflagsstatic
  gtest
  ${Boost_LIBRARIES}
  ${LLVM_MODULE_LIBS}
)

  

