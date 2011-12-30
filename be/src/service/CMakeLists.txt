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

# The order of link libararies matter.  
#  - Util must come before ImpalaThrift
#  - Exprs & Opcode have a circular dependency (Exprs is included twice)
target_link_libraries(backend
  Common
  Exec
  Exprs
  Opcode
  Exprs
  Runtime
  Util
  thrift
  ImpalaThrift
  ${HDFS_LIBS}
  gflagsstatic
  glogstatic
  tcmallocstatic
  pprofstatic
)

add_executable(runquery 
  run-query.cc
)
# Make sure to specify link libraries in reverse order of dependency
# (ie, if libA uses symbols from libB, libA must precede libB), otherwise
# when linking statically the linker won't look for the needed symbols.
# TODO: is there a better way to specify link dependencies without having to
# list all included libs for each specific binary? any gcc flags to help with this?
#
# The order of link libararies matter.  
#  - Util must come before ImpalaThrift
#  - Exprs & Opcode have a circular dependency (Exprs is included twice)
target_link_libraries(runquery
  TestUtil
  Service
  Exec
  Exprs
  Opcode
  Exprs
  Runtime
  Common
  Util
  thrift
  ImpalaThrift
  ${Boost_LIBRARIES}  
  gflagsstatic
  tcmallocstatic
  pprofstatic
)
