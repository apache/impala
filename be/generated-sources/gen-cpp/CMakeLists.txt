# Copyright (c) 2011 Cloudera, Inc. All rights reserved.

# where to put generated libraries
set(LIBRARY_OUTPUT_PATH "${CMAKE_SOURCE_DIR}/build/thrift")

# where to put generated binaries
set(EXECUTABLE_OUTPUT_PATH "${CMAKE_SOURCE_DIR}/build/thrift")

add_library(ThriftService
  ImpalaService.cpp
  ImpalaService_constants.cpp
  ImpalaService_types.cpp
  # into separate library?
  LocalExecutor_constants.cpp
  LocalExecutor_types.cpp
)

add_library(ThriftRuntime
  Descriptors_constants.cpp
  Descriptors_types.cpp
  PlanNodes_constants.cpp
  PlanNodes_types.cpp
  Types_constants.cpp
  Types_types.cpp
)

add_library(ThriftExprs
  Exprs_constants.cpp
  Exprs_types.cpp
)

add_library(thrift STATIC IMPORTED)
