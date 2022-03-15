# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

# Helper function to generate build rules. For each input flatbuffer file (*.fbs), this
# function will generate a rule that maps the input file to an output c++ file.
# The flatbuffers compiler will generate multiple output files for each input file. In
# particular, it will generate a '_generated.h' for c++ and one .java file per flatbuffer
# table definition.
#
# To call this function, pass it the output file list followed by the input flatbuffers
# files: e.g. FB_GEN(OUTPUT_FILES, ${FLATBUFFER_FILES})
#
function(FB_GEN VAR)
  IF (NOT ARGN)
    MESSAGE(SEND_ERROR "Error: FB_GEN called without any src files")
    RETURN()
  ENDIF(NOT ARGN)

  set(${VAR})
  foreach(FIL ${ARGN})
    # Get full path
    get_filename_component(ABS_FIL ${FIL} ABSOLUTE)
    # Get basename
    get_filename_component(FIL_WE ${FIL} NAME_WE)

    set(OUTPUT_BE_FILE "${BE_OUTPUT_DIR}/${FIL_WE}_generated.h")
    list(APPEND ${VAR} ${OUTPUT_BE_FILE})

    add_custom_command(
      OUTPUT ${OUTPUT_BE_FILE}
      COMMAND ${FLATBUFFERS_COMPILER} ${CPP_ARGS} ${FIL}
      COMMAND ${FLATBUFFERS_COMPILER} ${JAVA_FE_ARGS} ${FIL}
      DEPENDS ${ABS_FIL}
      COMMENT "Running FlatBuffers compiler on ${FIL}"
      VERBATIM
    )
  endforeach(FIL)

  set(${VAR} ${${VAR}} PARENT_SCOPE)
endfunction(FB_GEN)

message("Using FlatBuffers compiler: ${FLATBUFFERS_COMPILER}")
set(BE_OUTPUT_DIR ${CMAKE_SOURCE_DIR}/be/generated-sources/gen-cpp)
set(FE_OUTPUT_DIR ${CMAKE_SOURCE_DIR}/fe/generated-sources/gen-java)
file(MAKE_DIRECTORY ${FE_OUTPUT_DIR})
file(MAKE_DIRECTORY ${BE_OUTPUT_DIR})
set(JAVA_FE_ARGS --gen-mutable --java -o ${FE_OUTPUT_DIR} -b)
message(${JAVA_FE_ARGS})
set(CPP_ARGS --cpp -o ${BE_OUTPUT_DIR} -b)
message(${CPP_ARGS})

# Add new FlatBuffer schema files here.
set (SRC_FILES
  CatalogObjects.fbs
  IcebergObjects.fbs
)

FB_GEN(FB_ALL_FILES ${SRC_FILES})
add_custom_target(fb-deps ALL DEPENDS ${FB_ALL_FILES})

