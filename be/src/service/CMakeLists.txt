# Copyright (c) 2012 Cloudera, Inc. All rights reserved.

# where to put generated libraries
set(LIBRARY_OUTPUT_PATH "${BUILD_OUTPUT_ROOT_DIRECTORY}/service")

# where to put generated binaries
set(EXECUTABLE_OUTPUT_PATH "${BUILD_OUTPUT_ROOT_DIRECTORY}/service")

add_library(Service
  fe-support.cc
  impala-server.cc
)

# fe-support.cc uses TestExecEnv from TestUtil
target_link_libraries(Service
  TestUtil
)

# this shared library provides Impala executor functionality to FE test and planservice
add_library(fesupport SHARED
  fe-support.cc
)

target_link_libraries(fesupport ${IMPALA_LINK_LIBS})

add_executable(runquery
run-query.cc
)

message(STATUS, "IMPALA_LINK_LIBS: ${IMPALA_LINK_LIBS}")

target_link_libraries(runquery
  # Library intercepts signal handlers and sets them properly for running the JVM.
  # Must be before other libraries.
  ${JAVA_JSIG_LIBRARY}
  ${IMPALA_LINK_LIBS}
  # this is commented out from IMPALA_LINK_LIBS because it causes crashes in
  # libfesupport.so
  # see Imp-39
  tcmallocstatic
)

add_executable(impalad
  impalad-main.cc
)

target_link_libraries(impalad
  ${JAVA_JSIG_LIBRARY}
  ${IMPALA_LINK_LIBS}
  tcmallocstatic
)
