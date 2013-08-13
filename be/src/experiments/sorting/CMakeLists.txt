# Copyright (c) 2013 Cloudera, Inc. All rights reserved.

# where to put generated libraries
set(LIBRARY_OUTPUT_PATH "${BUILD_OUTPUT_ROOT_DIRECTORY}/experiments/sorting")

# where to put generated binaries
set(EXECUTABLE_OUTPUT_PATH "${BUILD_OUTPUT_ROOT_DIRECTORY}/experiments/sorting")

add_library(Sorting
  buffer-pool.cc
  disk-writer.cc
  sorted-merger.cc
  sorter.cc
)

add_executable(sorting-benchmark sorting-benchmark.cc)
add_executable(blocked-vector-test blocked-vector-test.cc)
add_executable(sorter-test sorter-test.cc)
add_executable(buffer-pool-test buffer-pool-test.cc)

target_link_libraries(sorting-benchmark Sorting ${IMPALA_TEST_LINK_LIBS})
target_link_libraries(blocked-vector-test Sorting ${IMPALA_TEST_LINK_LIBS})
target_link_libraries(sorter-test Sorting ${IMPALA_TEST_LINK_LIBS})
target_link_libraries(buffer-pool-test Sorting ${IMPALA_TEST_LINK_LIBS})
