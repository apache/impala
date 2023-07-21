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

# where to put generated libraries
set(LIBRARY_OUTPUT_PATH "${BUILD_OUTPUT_ROOT_DIRECTORY}/udf_samples")
# where to put generated binaries
set(EXECUTABLE_OUTPUT_PATH "${BUILD_OUTPUT_ROOT_DIRECTORY}/udf_samples")

set(HIDE_SYMBOLS "-fvisibility=hidden -fvisibility-inlines-hidden")
set(HIDE_SYMBOLS_ARGS "${HIDE_SYMBOLS}")
separate_arguments(HIDE_SYMBOLS_ARGS)

# Function to generate rule to cross compile a source file to an IR module.
# This should be called with the .cc src file and it will generate a
# src-file-ir target that can be built.
# e.g. COMPILE_TO_IR(test.cc) generates the "test-ir" make target.
set(IR_COMPILE_FLAGS "-emit-llvm" "-O3" "-std=c++17" "-c" "-I../" ${HIDE_SYMBOLS_ARGS})
set(IR_COMPILE_FLAGS ${IR_COMPILE_FLAGS} ${CLANG_BASE_FLAGS})
function(COMPILE_TO_IR SRC_FILE)
  get_filename_component(BASE_NAME ${SRC_FILE} NAME_WE)
  set(OUTPUT_FILE "${LIBRARY_OUTPUT_PATH}/${BASE_NAME}.ll")
  add_custom_command(
    OUTPUT ${OUTPUT_FILE}
    COMMAND ${LLVM_CLANG_EXECUTABLE} ${IR_COMPILE_FLAGS} ${CLANG_INCLUDE_FLAGS} ${SRC_FILE} -o ${OUTPUT_FILE}
    DEPENDS ${SRC_FILE})
  add_custom_target(${BASE_NAME}-ir ALL DEPENDS ${OUTPUT_FILE})
endfunction(COMPILE_TO_IR)

# Build the UDA/UDFs into a shared library.
add_library(udfsample SHARED udf-sample.cc)
set_target_properties(udfsample PROPERTIES COMPILE_FLAGS "${HIDE_SYMBOLS}")
add_dependencies(udfsample gen-deps)
add_library(udasample SHARED uda-sample.cc hyperloglog-uda.cc)
set_target_properties(udasample PROPERTIES COMPILE_FLAGS "${HIDE_SYMBOLS}")
add_dependencies(udasample gen-deps)

# Custom targest to cross compile UDA/UDF to ir
COMPILE_TO_IR(udf-sample.cc )
add_dependencies(udf-sample-ir gen-deps)
COMPILE_TO_IR(uda-sample.cc )
add_dependencies(uda-sample-ir gen-deps)

if (BUILD_WITH_NO_TESTS)
  return()
endif()

# This is an example of how to use the test harness to help develop UDF and UDAs.
add_executable(udf-sample-test udf-sample-test.cc)
target_link_libraries(udf-sample-test ImpalaUdf udfsample)
add_executable(uda-sample-test uda-sample-test.cc)
target_link_libraries(uda-sample-test ImpalaUdf udasample)
