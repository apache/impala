# Copyright (c) 2012 Cloudera, Inc. All rights reserved.

# where to put generated libraries
set(LIBRARY_OUTPUT_PATH "${BUILD_OUTPUT_ROOT_DIRECTORY}/sparrow")

# where to put generated binaries
set(EXECUTABLE_OUTPUT_PATH "${BUILD_OUTPUT_ROOT_DIRECTORY}/sparrow")

add_library(Sparrow STATIC
  simple-scheduler.cc
)

add_executable(simple-scheduler-test simple-scheduler-test.cc)

target_link_libraries(simple-scheduler-test ${IMPALA_TEST_LINK_LIBS})

add_test(simple-scheduler-test ${BUILD_OUTPUT_ROOT_DIRECTORY}/sparrow/simple-scheduler-test)
