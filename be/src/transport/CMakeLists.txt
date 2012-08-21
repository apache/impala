# Copyright (c) 2012 Cloudera, Inc. All rights reserved.
# This will move from the Impala source to the Thrift source repository. 

# where to put generated libraries
set(LIBRARY_OUTPUT_PATH "${BUILD_OUTPUT_ROOT_DIRECTORY}/transport")

# where to put generated binaries
set(EXECUTABLE_OUTPUT_PATH "${BUILD_OUTPUT_ROOT_DIRECTORY}/transport")

add_library(ThriftSaslTransport
    TSaslClientTransport.cpp
    TSasl.cpp
    TSaslServerTransport.cpp
    TSaslTransport.cpp
    undef.cpp
  )

