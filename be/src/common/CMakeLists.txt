# Copyright (c) 2011 Cloudera, Inc. All rights reserved.

message (STATUS "Common ${BUILD_OUTPUT_ROOT_DIRECTORY}")
# where to put generated libraries
set(LIBRARY_OUTPUT_PATH "${BUILD_OUTPUT_ROOT_DIRECTORY}/common")

# where to put generated binaries
set(EXECUTABLE_OUTPUT_PATH "${BUILD_OUTPUT_ROOT_DIRECTORY}/common")

add_library(Common STATIC
  service-ids.cc
  status.cc
)

add_library(GlobalFlags STATIC
  global-flags.cc
)

add_library(glog STATIC IMPORTED)
add_library(gflags STATIC IMPORTED)
