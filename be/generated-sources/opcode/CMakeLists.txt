# Copyright (c) 2011 Cloudera, Inc. All rights reserved.

# where to put generated libraries
set(LIBRARY_OUTPUT_PATH "${CMAKE_CURRENT_SOURCE_DIR}/build/opcode")

# where to put generated binaries
set(EXECUTABLE_OUTPUT_PATH "${CMAKE_CURRENT_SOURCE_DIR}/build/opcode")

set(SRC_FILES
  functions.cc
  opcode-registry-init.cc
  )

# keep everything in one library, the object files reference 
# each other
add_library(Opcode ${SRC_FILES})

# Setting these files as code-generated lets make clean and incremental builds work correctly
set_source_files_properties(${SRC_FILES} PROPERTIES GENERATED TRUE)

add_dependencies(Opcode function-registry)
