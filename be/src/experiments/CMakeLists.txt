# Copyright (c) 2011 Cloudera, Inc. All rights reserved.

# where to put generated libraries
set(LIBRARY_OUTPUT_PATH "${BUILD_OUTPUT_ROOT_DIRECTORY}/experiments")

# where to put generated binaries
set(EXECUTABLE_OUTPUT_PATH "${BUILD_OUTPUT_ROOT_DIRECTORY}/experiments")

add_library(Experiments
  data-provider.cc
)

add_executable(data-provider-test
  data-provider-test.cc
)

add_executable(tuple-splitter-test
  tuple-splitter-test.cc
)

set (LINK_LIBS
  Experiments
  Util
  Runtime
  Util
  Common
  ImpalaThrift
  glogstatic
  gflagsstatic
  gtest
  ${Boost_LIBRARIES}
  ${LLVM_MODULE_LIBS}
)
  
target_link_libraries(data-provider-test ${LINK_LIBS})
target_link_libraries(tuple-splitter-test ${LINK_LIBS})

