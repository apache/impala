# Copyright (c) 2011 Cloudera, Inc. All rights reserved.

# where to put generated libraries
set(LIBRARY_OUTPUT_PATH "${BUILD_OUTPUT_ROOT_DIRECTORY}/runtime")

# where to put generated binaries
set(EXECUTABLE_OUTPUT_PATH "${BUILD_OUTPUT_ROOT_DIRECTORY}/runtime")

add_library(Runtime STATIC
  client-cache.cc
  coordinator.cc
  data-stream-mgr.cc
  data-stream-sender.cc
  descriptors.cc
  exec-env.cc
  hbase-table-cache.cc
  hdfs-fs-cache.cc
  mem-pool.cc
  plan-executor.cc
  primitive-type.cc
  raw-value.cc
  row-batch.cc
  runtime-state.cc
  string-value.cc
  timestamp-value.cc
  tuple.cc
  tuple-row.cc
)

add_executable(mem-pool-test mem-pool-test.cc)
add_executable(free-list-test  free-list-test.cc)
add_executable(string-buffer-test  string-buffer-test.cc)
add_executable(data-stream-test data-stream-test.cc)

target_link_libraries(mem-pool-test ${IMPALA_TEST_LINK_LIBS})
target_link_libraries(free-list-test ${IMPALA_TEST_LINK_LIBS})
target_link_libraries(string-buffer-test ${IMPALA_TEST_LINK_LIBS})
target_link_libraries(data-stream-test ${IMPALA_TEST_LINK_LIBS})

add_test(mem-pool-test ${BUILD_OUTPUT_ROOT_DIRECTORY}/runtime/mem-pool-test)
add_test(free-list-test ${BUILD_OUTPUT_ROOT_DIRECTORY}/runtime/free-list-test)
add_test(string-buffer-test ${BUILD_OUTPUT_ROOT_DIRECTORY}/runtime/string-buffer-test)
add_test(data-stream-test ${BUILD_OUTPUT_ROOT_DIRECTORY}/runtime/data-stream-test)
