# Copyright (c) 2011 Cloudera, Inc. All rights reserved.

# where to put generated libraries
set(LIBRARY_OUTPUT_PATH "${BUILD_OUTPUT_ROOT_DIRECTORY}/service")

# where to put generated binaries
set(EXECUTABLE_OUTPUT_PATH "${BUILD_OUTPUT_ROOT_DIRECTORY}/service")

add_library(Service
  backend-service.cc
  jni-coordinator.cc
)

# TODO: why do we need this
target_link_libraries(Service 
  TestUtil
)

add_library(backend SHARED
  backend.cc
  backend-service.cc
  jni-coordinator.cc
)

target_link_libraries(backend ${IMPALA_LINK_LIBS})

add_executable(runquery 
  run-query.cc 
)

target_link_libraries(runquery 
  ${IMPALA_LINK_LIBS}
  # this is commented out from IMPALA_LINK_LIBS because it causes crashes in libbackend.so
  # see Imp-39
  tcmallocstatic      
)

add_executable(impalad
  impalad-main.cc
)

target_link_libraries(impalad ${IMPALA_LINK_LIBS})
