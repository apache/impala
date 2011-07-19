# Copyright (c) 2011 Cloudera, Inc. All rights reserved.

# where to put generated libraries
set(LIBRARY_OUTPUT_PATH "${CMAKE_SOURCE_DIR}/build/service")

# where to put generated binaries
set(EXECUTABLE_OUTPUT_PATH "${CMAKE_SOURCE_DIR}/build/service")

add_library(Service
  plan-executor.cc
)

target_link_libraries(Service
  Exec
  Runtime
  thrift  
  ImpalaThrift
)

add_library(planexec SHARED
  plan-executor.cc  
)

target_link_libraries(planexec
  Common
  Exec
  Exprs
  Runtime
  thrift
  ImpalaThrift
  ${HDFS_LIBS}
)

add_executable(runquery 
  run-query.cc
)
# don't link this against hdfs_libs or java_jvm_library;
# we need to avoid jni calls in order to be able to run under gdb,
# and TestUtil contains a mock libhdfs implementation
target_link_libraries(runquery
  Common
  Exec
  Exprs
  Runtime
  thrift
  ImpalaThrift
  Service
  pthread
  TestUtil
)
