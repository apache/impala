# Copyright (c) 2011 Cloudera, Inc. All rights reserved.

# where to put generated libraries
set(LIBRARY_OUTPUT_PATH "${BUILD_OUTPUT_ROOT_DIRECTORY}/runtime")

# where to put generated binaries
set(EXECUTABLE_OUTPUT_PATH "${BUILD_OUTPUT_ROOT_DIRECTORY}/runtime")

# add .cc files and uncomment line
add_library(Runtime STATIC
  client-cache.cc
  coordinator.cc
  data-stream-mgr.cc
  data-stream-sender.cc
  descriptors.cc
  exec-env.cc
  hdfs-fs-cache.cc
  mem-pool.cc
  plan-executor.cc
  raw-value.cc
  row-batch.cc
  runtime-state.cc
  simple-scheduler.cc
  string-value.cc
  timestamp-value.cc
  tuple.cc
)

# TODO: remove TestUtil dependency as soon as we have our scheduler library in place
target_link_libraries(Runtime
  Common
  Exec
  TestUtil
  ${Boost_LIBRARIES}
  snappy
  -lz -lbz2
)

add_executable(mem-pool-test
  mem-pool-test.cc
)

# TODO: w/o the thrift dependency, we're getting a bunch of 'pthread_<> missing' linker
# errors; figure out what we actually need to link against here
target_link_libraries(mem-pool-test
  Runtime
  thriftstatic
  glogstatic
  gflagsstatic
  gtest
  ${Boost_LIBRARIES}
)

add_executable(free-list-test
  free-list-test.cc
)

target_link_libraries(free-list-test
  Runtime
  thriftstatic
  glogstatic
  gflagsstatic
  gtest
  ${Boost_LIBRARIES}
)

add_executable(string-buffer-test
  string-buffer-test.cc
)

target_link_libraries(string-buffer-test
  Runtime
  thriftstatic
  glogstatic
  gflagsstatic
  gtest
  ${Boost_LIBRARIES}
)

add_executable(data-stream-test
  data-stream-test.cc
)
target_link_libraries(data-stream-test
  TestUtil
  CodeGen
  Service
  Exec
  Exprs
  Opcode
  Exprs
  Runtime
  Common
  Util
  thriftstatic
  ImpalaThrift
  Exec
  Exprs
  Opcode
  Exprs
  gtest
  ${Boost_LIBRARIES}
  ${LLVM_MODULE_LIBS}
  -lz -lbz2
)

add_test(mem-pool-test ${BUILD_OUTPUT_ROOT_DIRECTORY}/runtime/mem-pool-test)
add_test(free-list-test ${BUILD_OUTPUT_ROOT_DIRECTORY}/runtime/free-list-test)
add_test(string-buffer-test ${BUILD_OUTPUT_ROOT_DIRECTORY}/runtime/string-buffer-test)
add_test(data-stream-test ${BUILD_OUTPUT_ROOT_DIRECTORY}/runtime/data-stream-test)
