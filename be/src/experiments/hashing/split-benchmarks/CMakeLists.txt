# Copyright (c) 2012 Cloudera, Inc. All rights reserved.

# where to put generated libraries
set(LIBRARY_OUTPUT_PATH "${BUILD_OUTPUT_ROOT_DIRECTORY}/experiments/hashing")

# where to put generated binaries
set(EXECUTABLE_OUTPUT_PATH "${BUILD_OUTPUT_ROOT_DIRECTORY}/experiments/hashing")

add_library(Hashing
  standard-hash-table.cc
)

add_executable(cache-hash-test cache-hash-test.cc)
add_executable(growing-test growing-test.cc)
add_executable(partitioning-throughput-test partitioning-throughput-test.cc)

target_link_libraries(cache-hash-test Hashing ${IMPALA_TEST_LINK_LIBS})
target_link_libraries(growing-test Hashing ${IMPALA_TEST_LINK_LIBS})
target_link_libraries(partitioning-throughput-test Hashing ${IMPALA_TEST_LINK_LIBS})
