# Copyright (c) 2012 Cloudera, Inc. All rights reserved.

# where to put generated libraries
set(LIBRARY_OUTPUT_PATH "${BUILD_OUTPUT_ROOT_DIRECTORY}/codegen")

# where to put generated binaries
set(EXECUTABLE_OUTPUT_PATH "${BUILD_OUTPUT_ROOT_DIRECTORY}/codegen")

add_library(CodeGen
  llvm-codegen.cc
)

# output cross compile to ir metadata
set(IR_DESC_GEN_OUTPUT
  ${CMAKE_SOURCE_DIR}/be/generated-sources/impala-ir/impala-ir-names.h
  ${CMAKE_SOURCE_DIR}/be/generated-sources/impala-ir/impala-ir-functions.h
)
add_custom_command(
  OUTPUT ${IR_DESC_GEN_OUTPUT}
  COMMAND ./gen_ir_descriptions.py
  DEPENDS ./gen_ir_descriptions.py
  COMMENT "Generating ir cross compile metadata."
  VERBATIM
)
add_custom_target(gen_ir_functions ALL DEPENDS ${IR_DESC_GEN_OUTPUT})


# impala-ir.cc is the central place for all cross-compiled code.  All of
# this code gets cross compiled to a single IR module, impala.ll.  
set(IR_INPUT_FILES impala-ir.cc)
set(IR_TMP_OUTPUT_FILE "${LLVM_IR_OUTPUT_DIRECTORY}/impala-tmp.ll")
set(IR_OUTPUT_FILE "${LLVM_IR_OUTPUT_DIRECTORY}/impala.ll")

# Run the clang compiler to generate IR.  Then run their opt tool to remove
# unnamed instr.  This makes the IR verifiable and more readable.
add_custom_command(
  OUTPUT ${IR_OUTPUT_FILE}
  COMMAND ${LLVM_CLANG_EXECUTABLE} ${CLANG_IR_CXX_FLAGS} ${CLANG_INCLUDE_FLAGS} ${IR_INPUT_FILES} -o ${IR_TMP_OUTPUT_FILE}
  COMMAND ${LLVM_OPT_EXECUTABLE} --instnamer < ${IR_TMP_OUTPUT_FILE} > ${IR_OUTPUT_FILE}
  DEPENDS ${IR_INPUT_FILES}
)

add_custom_target(compile_to_ir ALL DEPENDS ${IR_OUTPUT_FILE})


add_executable(llvm-codegen-test llvm-codegen-test.cc)
target_link_libraries(llvm-codegen-test ${IMPALA_TEST_LINK_LIBS})
add_test(llvm-codegen-test ${BUILD_OUTPUT_ROOT_DIRECTORY}/codegen/llvm-codegen-test)

