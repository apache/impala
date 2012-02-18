# Copyright (c) 2011 Cloudera, Inc. All rights reserved.

# where to put generated libraries
set(LIBRARY_OUTPUT_PATH "${BUILD_OUTPUT_ROOT_DIRECTORY}/service")

# where to put generated binaries
set(EXECUTABLE_OUTPUT_PATH "${BUILD_OUTPUT_ROOT_DIRECTORY}/service")

add_library(Service
  backend-service.cc
  jni-coordinator.cc
)

target_link_libraries(Service
  Runtime
)

add_library(backend SHARED
  backend.cc
  backend-service.cc
  jni-coordinator.cc
)

# The order of link libararies matter.  
#  - Util must come before ImpalaThrift
#  - Exprs & Opcode have a circular dependency (Exprs is included twice)
# Commenting out tcmalloc for now in order to get more meaningful stack
# dumps in response to test failures on Jenkins.
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
  gflagsstatic
  #tcmallocstatic
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
  Service
  ${Boost_LIBRARIES}  
  gflagsstatic
  tcmallocstatic
  pprofstatic
)

add_executable(impalad
  impalad-main.cc
)

# multiple alternating includes of Opcode and Exprs libs necessary to get symbols resolve (why?)
target_link_libraries(impalad
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
  Service
  Exec
  Exprs
  Opcode
  Exprs
  ${Boost_LIBRARIES}  
  gflagsstatic
  tcmallocstatic
  pprofstatic
)

