# Copyright 2012 Cloudera Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


# where to put generated libraries
set(LIBRARY_OUTPUT_PATH "${BUILD_OUTPUT_ROOT_DIRECTORY}/opcode")

# where to put generated binaries
set(EXECUTABLE_OUTPUT_PATH "${BUILD_OUTPUT_ROOT_DIRECTORY}/opcode")

set(SRC_FILES
  functions.cc
  )

# keep everything in one library, the object files reference
# each other
add_library(Opcode ${SRC_FILES})

# Setting these files as code-generated lets make clean and incremental builds work correctly
set_source_files_properties(${SRC_FILES} PROPERTIES GENERATED TRUE)

add_dependencies(Opcode function-registry)
