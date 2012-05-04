# Copyright (c) 2012 Cloudera, Inc. All rights reserved.

# where to put generated libraries
set(LIBRARY_OUTPUT_PATH "${BUILD_OUTPUT_ROOT_DIRECTORY}/codegen")

# where to put generated binaries
set(EXECUTABLE_OUTPUT_PATH "${BUILD_OUTPUT_ROOT_DIRECTORY}/codegen")

add_library(CodeGen
  llvm-codegen.cc
)

add_executable(llvm-codegen-test llvm-codegen-test.cc)
target_link_libraries(llvm-codegen-test ${IMPALA_TEST_LINK_LIBS})
add_test(llvm-codegen-test ${BUILD_OUTPUT_ROOT_DIRECTORY}/codegen/llvm-codegen-test)

