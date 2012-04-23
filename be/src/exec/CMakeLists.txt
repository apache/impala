# Copyright (c) 2011 Cloudera, Inc. All rights reserved.

cmake_minimum_required(VERSION 2.6)

# where to put generated libraries
set(LIBRARY_OUTPUT_PATH "${BUILD_OUTPUT_ROOT_DIRECTORY}/exec")

# where to put generated binaries
set(EXECUTABLE_OUTPUT_PATH "${BUILD_OUTPUT_ROOT_DIRECTORY}/exec")

add_library(Exec STATIC
  aggregation-node.cc
  aggregation-node-ir.cc
  buffered-byte-stream.cc
  data-sink.cc
  delimited-text-parser.cc
  exec-node.cc
  exchange-node.cc
  hash-join-node.cc
  hdfs-byte-stream.cc
  hdfs-scan-node.cc
  hdfs-scanner.cc
  hash-table.cc
  hdfs-rcfile-scanner.cc
  hdfs-sequence-scanner.cc
  hdfs-text-scanner.cc
  hdfs-text-table-sink.cc
  hbase-scan-node.cc
  hbase-table-scanner.cc
  text-converter.cc
  topn-node.cc
  rcfile-reader.cc
  serde-utils.cc
)

target_link_libraries(Exec
  Runtime
  ${JAVA_JVM_LIBRARY}
  ${HDFS_LIBS}
)

set(IR_INPUT_FILES
  aggregation-node-ir.cc
)
set(IR_TMP_OUTPUT_FILE "${LLVM_IR_OUTPUT_DIRECTORY}/exec-tmp.ll")
set(IR_OUTPUT_FILE "${LLVM_IR_OUTPUT_DIRECTORY}/exec.ll")

# Run the clang compiler to generate IR.  Then run their opt tool to remove
# unnamed instr.  This makes the IR verifiable and more readable.
add_custom_command(
  OUTPUT ${IR_OUTPUT_FILE}
  COMMAND ${LLVM_CLANG_EXECUTABLE} ${CLANG_IR_CXX_FLAGS} ${CLANG_INCLUDE_FLAGS} ${IR_INPUT_FILES} -o ${IR_TMP_OUTPUT_FILE}
  COMMAND ${LLVM_OPT_EXECUTABLE} --instnamer < ${IR_TMP_OUTPUT_FILE} > ${IR_OUTPUT_FILE}
  DEPENDS ${IR_INPUT_FILES}
)

add_custom_target(compile_to_ir ALL DEPENDS ${IR_OUTPUT_FILE})
