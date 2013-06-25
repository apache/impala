# Copyright (c) 2012 Cloudera, Inc. All rights reserved.

# where to put generated libraries
set(LIBRARY_OUTPUT_PATH "${BUILD_OUTPUT_ROOT_DIRECTORY}/experiments")

# where to put generated binaries
set(EXECUTABLE_OUTPUT_PATH "${BUILD_OUTPUT_ROOT_DIRECTORY}/experiments")

add_library(Experiments
  data-provider.cc
)

add_executable(data-provider-test data-provider-test.cc)
add_executable(tuple-splitter-test tuple-splitter-test.cc)
add_executable(hash-partition-test hash-partition-test.cc)
add_executable(compression-test compression-test.cc)

# Add Experiments lib explicitly here.  It is not used by any other part
# of impala so don't include it in link_libs
target_link_libraries(data-provider-test Experiments ${IMPALA_LINK_LIBS})
target_link_libraries(tuple-splitter-test Experiments ${IMPALA_LINK_LIBS})
target_link_libraries(hash-partition-test ${IMPALA_LINK_LIBS})
target_link_libraries(compression-test ${IMPALA_LINK_LIBS})

ADD_BE_TEST(string-search-test)

add_subdirectory(hashing)
