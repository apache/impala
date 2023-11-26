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


set(THIRDPARTY_LLVM_SRC_DIR "${CMAKE_SOURCE_DIR}/be/src/thirdparty/llvm")

# where to put generated libraries
set(LIBRARY_OUTPUT_PATH "${BUILD_OUTPUT_ROOT_DIRECTORY}/codegen")

# where to put generated binaries
set(EXECUTABLE_OUTPUT_PATH "${BUILD_OUTPUT_ROOT_DIRECTORY}/codegen")

# Generated C files for IR
set(IR_O1_C_FILE $ENV{IMPALA_HOME}/be/generated-sources/impala-ir/impala-ir-o1.cc)
set(IR_O2_C_FILE $ENV{IMPALA_HOME}/be/generated-sources/impala-ir/impala-ir-o2.cc)
set(IR_Os_C_FILE $ENV{IMPALA_HOME}/be/generated-sources/impala-ir/impala-ir-os.cc)
set(LEGACY_AVX_IR_C_FILE $ENV{IMPALA_HOME}/be/generated-sources/impala-ir/impala-ir-legacy-avx.cc)

add_library(CodeGen
  codegen-anyval.cc
  codegen-anyval-read-write-info.cc
  codegen-callgraph.cc
  codegen-symbol-emitter.cc
  codegen-util.cc
  llvm-codegen.cc
  llvm-codegen-cache.cc
  llvm-codegen-object-cache.cc
  instruction-counter.cc
  ${THIRDPARTY_LLVM_SRC_DIR}/SectionMemoryManager.cpp
  ${IR_O1_C_FILE}
  ${IR_O2_C_FILE}
  ${IR_Os_C_FILE}
  ${LEGACY_AVX_IR_C_FILE}
)
add_dependencies(CodeGen gen-deps)

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

function(COMPILE_TO_IR_C_ARRAY IR_C_FILE VARNAME)
  # Run the clang compiler to generate IR. Then run the LLVM opt tool to apply specific
  # optimisations. We need to compile to IR several times for different optimization settings
  # and legacy AVX support. Additional args (ARGN) are passed to clang as compiler flags.
  get_filename_component(BASE_NAME ${IR_C_FILE} NAME_WE)
  set(IR_OUTPUT_FILE "${LLVM_IR_OUTPUT_DIRECTORY}/${BASE_NAME}.bc")
  set(IR_TMP_OUTPUT_FILE "${LLVM_IR_OUTPUT_DIRECTORY}/${BASE_NAME}-tmp.bc")
  add_custom_command(
    OUTPUT ${IR_OUTPUT_FILE}
    COMMAND ${LLVM_CLANG_EXECUTABLE} ${CLANG_IR_CXX_FLAGS} ${ARGN}
            ${CLANG_INCLUDE_FLAGS} ${IR_INPUT_FILES} -o ${IR_TMP_OUTPUT_FILE}
    COMMAND ${LLVM_OPT_EXECUTABLE} ${LLVM_OPT_IR_FLAGS} < ${IR_TMP_OUTPUT_FILE} > ${IR_OUTPUT_FILE}
    COMMAND rm ${IR_TMP_OUTPUT_FILE}
    DEPENDS ExecIr ExecAvroIr ExecKuduIr ExprsIr RuntimeIr UdfIr UtilIr ${IR_INPUT_FILES}
  )

  # Convert LLVM bytecode to C array.
  set(IR_TMP_C_FILE ${IR_C_FILE}.tmp)
  add_custom_command(
    OUTPUT ${IR_C_FILE}
    COMMAND $ENV{IMPALA_HOME}/bin/file2array.sh -n -v ${VARNAME} ${IR_OUTPUT_FILE} > ${IR_TMP_C_FILE}
    COMMAND mv ${IR_TMP_C_FILE} ${IR_C_FILE}
    DEPENDS $ENV{IMPALA_HOME}/bin/file2array.sh
    DEPENDS ${IR_OUTPUT_FILE}
  )
endfunction(COMPILE_TO_IR_C_ARRAY)

if (NOT CMAKE_SYSTEM_PROCESSOR STREQUAL "aarch64")
  set(PLATFORM_SPECIFIC_FLAGS "-mavx2")
  set(LEGACY_AVX_SPECIFIC_FLAGS "-mavx")
endif()
# At runtime impala will pick the file to load based on a startup parameter.
COMPILE_TO_IR_C_ARRAY(${IR_O1_C_FILE} impala_llvm_o1_ir -O1 ${PLATFORM_SPECIFIC_FLAGS})
COMPILE_TO_IR_C_ARRAY(${IR_O2_C_FILE} impala_llvm_o2_ir -O2 ${PLATFORM_SPECIFIC_FLAGS})
COMPILE_TO_IR_C_ARRAY(${IR_Os_C_FILE} impala_llvm_os_ir -Os ${PLATFORM_SPECIFIC_FLAGS})
COMPILE_TO_IR_C_ARRAY(${LEGACY_AVX_IR_C_FILE} impala_legacy_avx_llvm_ir -O1 ${LEGACY_AVX_SPECIFIC_FLAGS})

if (BUILD_WITH_NO_TESTS)
  return()
endif()

add_library(CodeGenTests STATIC
  instruction-counter-test.cc
)
add_dependencies(CodeGenTests gen-deps)

# Run the clang compiler to generate BC for llvm-codegen-test
add_custom_target(test-loop.bc
  COMMAND ${LLVM_CLANG_EXECUTABLE} ${CLANG_IR_CXX_FLAGS} -O2 ${CLANG_INCLUDE_FLAGS} ${CMAKE_SOURCE_DIR}/testdata/llvm/test-loop.cc -o ${CMAKE_SOURCE_DIR}/llvm-ir/test-loop.bc
  SOURCES ${CMAKE_SOURCE_DIR}/testdata/llvm/test-loop.cc
)

# Provide "unoptimized" output. Clang O0 adds attributes to everything which prevents us
# doing optimizations in the test, so use O1 instead.
add_custom_target(test-opt.bc
  COMMAND ${LLVM_CLANG_EXECUTABLE} ${CLANG_IR_CXX_FLAGS} -O1 ${CLANG_INCLUDE_FLAGS} ${CMAKE_SOURCE_DIR}/testdata/llvm/test-opt.cc -o ${CMAKE_SOURCE_DIR}/llvm-ir/test-opt.bc
  SOURCES ${CMAKE_SOURCE_DIR}/testdata/llvm/test-opt.cc
)

# Exception to unified be tests: custom main initializes LLVM
ADD_BE_LSAN_TEST(llvm-codegen-test)
add_dependencies(llvm-codegen-test test-loop.bc test-opt.bc)
ADD_BE_LSAN_TEST(llvm-codegen-cache-test LlvmCodegenCacheTest.*)

ADD_UNIFIED_BE_LSAN_TEST(instruction-counter-test InstructionCounterTest.*)
