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
  disk-io-mgr.cc
  disk-io-mgr-stress.cc
  exec-env.cc
  hbase-table-cache.cc
  hdfs-fs-cache.cc
  mem-pool.cc
  parallel-executor.cc
  plan-fragment-executor.cc
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
add_executable(timestamp-test timestamp-test.cc)
add_executable(disk-io-mgr-test disk-io-mgr-test.cc)
add_executable(disk-io-mgr-stress-test disk-io-mgr-stress-test.cc)
add_executable(parallel-executor-test parallel-executor-test.cc)

target_link_libraries(mem-pool-test ${IMPALA_TEST_LINK_LIBS})
target_link_libraries(free-list-test ${IMPALA_TEST_LINK_LIBS})
target_link_libraries(string-buffer-test ${IMPALA_TEST_LINK_LIBS})
target_link_libraries(data-stream-test ${IMPALA_TEST_LINK_LIBS})
target_link_libraries(timestamp-test ${IMPALA_TEST_LINK_LIBS})
target_link_libraries(disk-io-mgr-test ${IMPALA_TEST_LINK_LIBS})
target_link_libraries(disk-io-mgr-stress-test ${IMPALA_TEST_LINK_LIBS})
target_link_libraries(parallel-executor-test ${IMPALA_TEST_LINK_LIBS})

add_test(mem-pool-test ${BUILD_OUTPUT_ROOT_DIRECTORY}/runtime/mem-pool-test)
add_test(free-list-test ${BUILD_OUTPUT_ROOT_DIRECTORY}/runtime/free-list-test)
add_test(string-buffer-test ${BUILD_OUTPUT_ROOT_DIRECTORY}/runtime/string-buffer-test)
# IMP-409: disable data-stream-test until it's fixed
#add_test(data-stream-test ${BUILD_OUTPUT_ROOT_DIRECTORY}/runtime/data-stream-test)
add_test(timestamp-test ${BUILD_OUTPUT_ROOT_DIRECTORY}/runtime/timestamp-test)
add_test(disk-io-mgr-test ${BUILD_OUTPUT_ROOT_DIRECTORY}/runtime/disk-io-mgr-test)
add_test(parallel-executor-test ${BUILD_OUTPUT_ROOT_DIRECTORY}/runtime/parallel-executor-test)
