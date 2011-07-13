# Copyright (c) 2011 Cloudera, Inc. All rights reserved.

# where to put generated libraries
set(LIBRARY_OUTPUT_PATH "${CMAKE_SOURCE_DIR}/build/service")

# where to put generated binaries
set(EXECUTABLE_OUTPUT_PATH "${CMAKE_SOURCE_DIR}/build/service")

add_library(Service
  plan-executor.cc
)

add_library(planexec SHARED
  dummy-plan-executor.cc
)
target_link_libraries(planexec
  Common
  Exec
  Exprs
  Runtime
  thrift
  ThriftExprs
  ThriftRuntime
  ThriftService
)
