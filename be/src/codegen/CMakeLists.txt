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
set(LIBRARY_OUTPUT_PATH "${BUILD_OUTPUT_ROOT_DIRECTORY}/codegen")

# where to put generated binaries
set(EXECUTABLE_OUTPUT_PATH "${BUILD_OUTPUT_ROOT_DIRECTORY}/codegen")

add_library(CodeGen
  codegen-anyval.cc
  llvm-codegen.cc
  subexpr-elimination.cc
  instruction-counter.cc
)

# output cross compile to ir metadata
# TODO: this doesn't track dependences right.  We need to specify that
# the files in impala-ir must exist before building.
#set(IR_DESC_GEN_OUTPUT
#  ${CMAKE_SOURCE_DIR}/be/generated-sources/impala-ir/impala-ir-names.h
#  ${CMAKE_SOURCE_DIR}/be/generated-sources/impala-ir/impala-ir-functions.h
#)
#add_custom_command(
#  OUTPUT ${IR_DESC_GEN_OUTPUT}
#  COMMAND ./gen_ir_descriptions.py
#  DEPENDS ./gen_ir_descriptions.py
#  COMMENT "Generating ir cross compile metadata."
#  VERBATIM
#)
#add_custom_target(gen_ir_functions ALL DEPENDS ${IR_DESC_GEN_OUTPUT})


# impala-ir.cc is the central place for all cross-compiled code.  All of
# this code gets cross compiled to a single IR module, impala.ll.  
# Custom build commands don't seem to propagate dependencies correctly
# list all the ir files 
# Don't include any .cc files that contain the initialize for complex (i.e. boost)
# static objects. These cannot be trimmed for the module and greatly increases
# our runtime compile time.
set(IR_DEPENDENT_FILES
  impala-ir.cc
  codegen-anyval-ir.cc
  ../exec/aggregation-node-ir.cc
  ../exec/hash-join-node-ir.cc
  ../exec/hash-table-ir.cc
  ../exec/hdfs-avro-scanner-ir.cc
  ../exec/hdfs-scanner-ir.cc
  ../exec/partitioned-aggregation-node-ir.cc
  ../exec/partitioned-hash-join-node-ir.cc
  ../exprs/aggregate-functions.cc
  ../exprs/cast-functions.cc
  ../exprs/compound-predicates-ir.cc
  ../exprs/conditional-functions-ir.cc
  ../exprs/decimal-functions.cc
  ../exprs/decimal-operators.cc
  ../exprs/expr-ir.cc
  ../exprs/in-predicate-ir.cc
  ../exprs/is-null-predicate.cc
  ../exprs/like-predicate.cc
  ../exprs/math-functions.cc
  ../exprs/operators.cc
  ../exprs/string-functions.cc
  ../exprs/timestamp-functions.cc
  ../exprs/udf-builtins.cc
  ../exprs/utility-functions.cc
  ../udf/udf-ir.cc
  ../util/hash-util-ir.cc
)

set(IR_INPUT_FILES impala-ir.cc)
set(IR_SSE_TMP_OUTPUT_FILE "${LLVM_IR_OUTPUT_DIRECTORY}/impala-sse-tmp.ll")
set(IR_NO_SSE_TMP_OUTPUT_FILE "${LLVM_IR_OUTPUT_DIRECTORY}/impala-no-sse-tmp.ll")
set(IR_SSE_OUTPUT_FILE "${LLVM_IR_OUTPUT_DIRECTORY}/impala-sse.ll")
set(IR_NO_SSE_OUTPUT_FILE "${LLVM_IR_OUTPUT_DIRECTORY}/impala-no-sse.ll")

# Run the clang compiler to generate IR.  Then run their opt tool to remove
# unnamed instr.  This makes the IR verifiable and more readable.
# We need to compile to IR twice, once with sse enabled and one without.  At runtime
# impala will pick the correct file to load.
add_custom_command(
  OUTPUT ${IR_SSE_OUTPUT_FILE}
  COMMAND ${LLVM_CLANG_EXECUTABLE} ${CLANG_IR_CXX_FLAGS} "-msse4.2" ${CLANG_INCLUDE_FLAGS} ${IR_INPUT_FILES} -o ${IR_SSE_TMP_OUTPUT_FILE}
  COMMAND ${LLVM_OPT_EXECUTABLE} --instnamer < ${IR_SSE_TMP_OUTPUT_FILE} > ${IR_SSE_OUTPUT_FILE}
  COMMAND rm ${IR_SSE_TMP_OUTPUT_FILE}
  DEPENDS ${IR_DEPENDENT_FILES}
)

# Compile without sse enabled.
add_custom_command(
  OUTPUT ${IR_NO_SSE_OUTPUT_FILE}
  COMMAND ${LLVM_CLANG_EXECUTABLE} ${CLANG_IR_CXX_FLAGS} ${CLANG_INCLUDE_FLAGS} ${IR_INPUT_FILES} -o ${IR_NO_SSE_TMP_OUTPUT_FILE}
  COMMAND ${LLVM_OPT_EXECUTABLE} --instnamer < ${IR_NO_SSE_TMP_OUTPUT_FILE} > ${IR_NO_SSE_OUTPUT_FILE}
  COMMAND rm ${IR_NO_SSE_TMP_OUTPUT_FILE}
  DEPENDS ${IR_DEPENDENT_FILES}
)

add_custom_target(compile_to_ir_sse ALL DEPENDS ${IR_SSE_OUTPUT_FILE})
add_custom_target(compile_to_ir_no_sse ALL DEPENDS ${IR_NO_SSE_OUTPUT_FILE})

ADD_BE_TEST(llvm-codegen-test)
ADD_BE_TEST(instruction-counter-test)

