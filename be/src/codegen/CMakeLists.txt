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
set(LIBRARY_OUTPUT_PATH "${BUILD_OUTPUT_ROOT_DIRECTORY}/codegen")

# where to put generated binaries
set(EXECUTABLE_OUTPUT_PATH "${BUILD_OUTPUT_ROOT_DIRECTORY}/codegen")

# Generated C files for IR
set(IR_C_FILE $ENV{IMPALA_HOME}/be/generated-sources/impala-ir/impala-ir.cc)

add_library(CodeGen
  codegen-anyval.cc
  codegen-callgraph.cc
  codegen-symbol-emitter.cc
  codegen-util.cc
  llvm-codegen.cc
  instruction-counter.cc
  ${IR_C_FILE}
)
add_dependencies(CodeGen gen-deps gen_ir_descriptions)

add_library(CodeGenTests STATIC
  instruction-counter-test.cc
)
add_dependencies(CodeGenTests gen-deps)

# output cross compile to ir metadata
set(IR_DESC_GEN_OUTPUT
  $ENV{IMPALA_HOME}/be/generated-sources/impala-ir/impala-ir-names.h
  $ENV{IMPALA_HOME}/be/generated-sources/impala-ir/impala-ir-functions.h
)
add_custom_command(
  OUTPUT ${IR_DESC_GEN_OUTPUT}
  COMMAND ./gen_ir_descriptions.py
  DEPENDS ./gen_ir_descriptions.py
  COMMENT "Generating ir cross compile metadata."
  VERBATIM
)
add_custom_target(gen_ir_descriptions ALL DEPENDS ${IR_DESC_GEN_OUTPUT})

set(IR_INPUT_FILES impala-ir.cc)
set(IR_TMP_OUTPUT_FILE "${LLVM_IR_OUTPUT_DIRECTORY}/impala-tmp.bc")
set(IR_OUTPUT_FILE "${LLVM_IR_OUTPUT_DIRECTORY}/impala.bc")
set(IR_TMP_C_FILE ${IR_C_FILE}.tmp)

# Run the clang compiler to generate IR. Then run the LLVM opt tool to apply specific
# optimisations. We need to compile to IR twice, once with sse enabled and one without.
# At runtime impala will pick the correct file to load.

if (NOT CMAKE_SYSTEM_PROCESSOR STREQUAL "aarch64")
  set(PLATFORM_SPECIFIC_FLAGS "-mavx2")
endif()
add_custom_command(
  OUTPUT ${IR_OUTPUT_FILE}
  COMMAND ${LLVM_CLANG_EXECUTABLE} ${CLANG_IR_CXX_FLAGS} ${PLATFORM_SPECIFIC_FLAGS}
          ${CLANG_INCLUDE_FLAGS} ${IR_INPUT_FILES} -o ${IR_TMP_OUTPUT_FILE}
  COMMAND ${LLVM_OPT_EXECUTABLE} ${LLVM_OPT_IR_FLAGS} < ${IR_TMP_OUTPUT_FILE} > ${IR_OUTPUT_FILE}
  COMMAND rm ${IR_TMP_OUTPUT_FILE}
  DEPENDS Exec Exprs Runtime Udf Util ${IR_INPUT_FILES}
)

add_custom_target(compile_to_ir DEPENDS ${IR_OUTPUT_FILE})

# Convert LLVM bytecode to C array.
add_custom_command(
  OUTPUT ${IR_C_FILE}
  COMMAND $ENV{IMPALA_HOME}/bin/file2array.sh -n -v impala_llvm_ir ${IR_OUTPUT_FILE} > ${IR_TMP_C_FILE}
  COMMAND mv ${IR_TMP_C_FILE} ${IR_C_FILE}
  DEPENDS $ENV{IMPALA_HOME}/bin/file2array.sh
  DEPENDS ${IR_OUTPUT_FILE}
)

# Run the clang compiler to generate BC for llvm-codegen-test
add_custom_target(test-loop.bc
  COMMAND ${LLVM_CLANG_EXECUTABLE} ${CLANG_IR_CXX_FLAGS} ${CLANG_INCLUDE_FLAGS} ${CMAKE_SOURCE_DIR}/testdata/llvm/test-loop.cc -o ${CMAKE_SOURCE_DIR}/llvm-ir/test-loop.bc
  SOURCES ${CMAKE_SOURCE_DIR}/testdata/llvm/test-loop.cc
)

# Exception to unified be tests: custom main initializes LLVM
ADD_BE_LSAN_TEST(llvm-codegen-test)
add_dependencies(llvm-codegen-test test-loop.bc)

ADD_UNIFIED_BE_LSAN_TEST(instruction-counter-test InstructionCounterTest.*)
