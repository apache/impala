# Copyright (c) 2012 Cloudera, Inc. All rights reserved.

# where to put generated libraries
set(LIBRARY_OUTPUT_PATH "${BUILD_OUTPUT_ROOT_DIRECTORY}/experiments/hashing/interface")

# where to put generated binaries
set(EXECUTABLE_OUTPUT_PATH "${BUILD_OUTPUT_ROOT_DIRECTORY}/experiments/hashing/interface")

add_library(InterfaceHashing
  standard-hash-table.cc
)

#cache-hash-test and table written against old code, not worth fixing.
# Uncomment if you want to compile them.
#add_executable(cache-hash-test cache-hash-test.cc)
add_executable(interface-growing-test growing-test.cc)

#target_link_libraries(cache-hash-test Hashing ${IMPALA_TEST_LINK_LIBS})
target_link_libraries(interface-growing-test InterfaceHashing ${IMPALA_TEST_LINK_LIBS})
