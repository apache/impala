# Copyright (c) 2011 Cloudera, Inc. All rights reserved.

# where to put generated libraries
set(LIBRARY_OUTPUT_PATH "${CMAKE_SOURCE_DIR}/build/service")

# where to put generated binaries
set(EXECUTABLE_OUTPUT_PATH "${CMAKE_SOURCE_DIR}/build/service")

add_library(Service
  plan-executor.cc
  plan-executor-adaptor.cc
)

add_library(backend SHARED
  backend.cc
  plan-executor.cc  
  plan-executor-adaptor.cc
)

target_link_libraries(backend
  Common
  Exec
  Exprs
  Runtime
  thrift
  ImpalaThrift
  ${HDFS_LIBS}
  glogstatic
  gflagsstatic
)

add_executable(runquery 
  run-query.cc
)
# Don't link this against hdfs_libs or java_jvm_library;
# we need to avoid jni calls in order to be able to run under gdb,
# and MockHdfs contains a mock libhdfs implementation.
# Also, make sure to specify link libraries in reverse order of dependency
# (ie, if libA uses symbols from libB, libA must precede libB), otherwise
# when linking statically the linker won't look for the needed symbols.
# TODO: is there a better way to specify link dependencies without having to
# list all included libs for each specific binary? any gcc flags to help with this?
target_link_libraries(runquery
  TestUtil
  Service
  Exec
  Exprs
  Runtime
  Common
  thrift
  ImpalaThrift
  MockHdfs
  ${Boost_LIBRARIES}  
)
