# Copyright (c) 2011 Cloudera, Inc. All rights reserved.

# where to put generated libraries
set(LIBRARY_OUTPUT_PATH "${CMAKE_SOURCE_DIR}/build/opcode")

# where to put generated binaries
set(EXECUTABLE_OUTPUT_PATH "${CMAKE_SOURCE_DIR}/build/opcode")

# keep everything in one library, the object files reference 
# each other
add_library(Opcode
  functions.cc
  opcode-registry-init.cc
)

