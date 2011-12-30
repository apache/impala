# Copyright (c) 2011 Cloudera, Inc. All rights reserved.

cmake_minimum_required(VERSION 2.6)

set(BE_OUTPUT_DIR ${CMAKE_SOURCE_DIR}/be/generated-sources)
set(FE_OUTPUT_DIR ${CMAKE_SOURCE_DIR}/fe/generated-sources/gen-java/com/cloudera/impala/)

# Set output files for dependency tracking
set(CODE_GEN_OUTPUT
  ${BE_OUTPUT_DIR}/opcode/functions.cc
  ${BE_OUTPUT_DIR}/opcode/functions.h
  ${BE_OUTPUT_DIR}/opcode/opcode-registry-init.cc
  ${FE_OUTPUT_DIR}/opcode/FunctionOperator.java
  ${FE_OUTPUT_DIR}/opcode/FunctionRegistry.java
  ${CMAKE_CURRENT_SOURCE_DIR}/../thrift/Opcodes.thrift
)

# Source python files
set(FUNCTION_REGISTRY_INPUT
  gen_functions.py
  gen_opcodes.py
  impala_functions.py
)

# Run the python scripts
add_custom_command(
  OUTPUT ${CODE_GEN_OUTPUT}
  COMMAND ./gen_functions.py
  COMMAND ./gen_opcodes.py
  DEPENDS ${FUNCTION_REGISTRY_INPUT}
  COMMENT "Generating Opcode Registry files."
  VERBATIM
)

add_custom_target(function-registry ALL DEPENDS ${CODE_GEN_OUTPUT})

