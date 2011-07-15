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

add_executable(planexecmain 
  plan-executor-main.cc
)

target_link_libraries(planexecmain
  Common
  Exec
  Exprs
  Runtime
  thrift
  ImpalaThrift
  Service
  pthread
  ${HDFS_LIBS}
  ${JAVA_JVM_LIBRARY}    
)