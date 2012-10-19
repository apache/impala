# Copyright (c) 2012 Cloudera, Inc. All rights reserved.

# where to put generated libraries
set(LIBRARY_OUTPUT_PATH "${BUILD_OUTPUT_ROOT_DIRECTORY}/opcode")

# where to put generated binaries
set(EXECUTABLE_OUTPUT_PATH "${BUILD_OUTPUT_ROOT_DIRECTORY}/opcode")

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
