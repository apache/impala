# Copyright (c) 2012 Cloudera, Inc. All rights reserved.

# where to put generated libraries
set(LIBRARY_OUTPUT_PATH "${BUILD_OUTPUT_ROOT_DIRECTORY}/experiments/hashing/streaming")

# where to put generated binaries
set(EXECUTABLE_OUTPUT_PATH "${BUILD_OUTPUT_ROOT_DIRECTORY}/experiments/hashing/streaming")

add_library(StreamingHashing
  standard-hash-table.cc
)

add_executable(streaming-cache-hash-test cache-hash-test.cc)
add_executable(streaming-growing-test growing-test.cc)

target_link_libraries(streaming-cache-hash-test StreamingHashing ${IMPALA_TEST_LINK_LIBS})
target_link_libraries(streaming-growing-test StreamingHashing ${IMPALA_TEST_LINK_LIBS})
