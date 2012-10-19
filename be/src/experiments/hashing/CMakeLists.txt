# Copyright (c) 2012 Cloudera, Inc. All rights reserved.

# where to put generated libraries
set(LIBRARY_OUTPUT_PATH "${BUILD_OUTPUT_ROOT_DIRECTORY}/experiments/hashing")

# where to put generated binaries
set(EXECUTABLE_OUTPUT_PATH "${BUILD_OUTPUT_ROOT_DIRECTORY}/experiments/hashing")

add_library(Hashing
  standard-hash-table.cc
)

add_executable(multilevel-cache-hash-test cache-hash-test.cc)
add_executable(multilevel-growing-test growing-test.cc)

target_link_libraries(multilevel-cache-hash-test Hashing ${IMPALA_TEST_LINK_LIBS})
target_link_libraries(multilevel-growing-test Hashing ${IMPALA_TEST_LINK_LIBS})

add_subdirectory(multilevel)
add_subdirectory(streaming)
add_subdirectory(interface)
