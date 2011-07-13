# Copyright (c) 2011 Cloudera, Inc. All rights reserved.

# where to put generated libraries
set(LIBRARY_OUTPUT_PATH "${CMAKE_SOURCE_DIR}/build/thrift")

# where to put generated binaries
set(EXECUTABLE_OUTPUT_PATH "${CMAKE_SOURCE_DIR}/build/thrift")

# keep everything in one library, the object files reference 
# each other
add_library(ImpalaThrift
  ImpalaService.cpp
  ImpalaService_constants.cpp
  ImpalaService_types.cpp
  LocalExecutor_constants.cpp
  LocalExecutor_types.cpp
  Descriptors_constants.cpp
  Descriptors_types.cpp
  PlanNodes_constants.cpp
  PlanNodes_types.cpp
  Types_constants.cpp
  Types_types.cpp
  Exprs_constants.cpp
  Exprs_types.cpp
)

add_library(thrift STATIC IMPORTED)
