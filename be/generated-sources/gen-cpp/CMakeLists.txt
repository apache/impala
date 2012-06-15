# Copyright (c) 2011 Cloudera, Inc. All rights reserved.

# where to put generated libraries
set(LIBRARY_OUTPUT_PATH "${BUILD_OUTPUT_ROOT_DIRECTORY}/thrift")

# where to put generated binaries
set(EXECUTABLE_OUTPUT_PATH "${BUILD_OUTPUT_ROOT_DIRECTORY}/thrift")

set(SRC_FILES
  ImpalaBackendService.cpp
  ImpalaBackendService_constants.cpp
  ImpalaBackendService_types.cpp
  ImpalaPlanService.cpp
  ImpalaPlanService_constants.cpp
  ImpalaPlanService_types.cpp
  ImpalaService.cpp
  ImpalaService_constants.cpp
  ImpalaService_types.cpp
  beeswax_constants.cpp
  beeswax_types.cpp
  BeeswaxService.cpp
  Data_constants.cpp
  Data_types.cpp
  DataSinks_constants.cpp
  DataSinks_types.cpp
  Descriptors_constants.cpp
  Descriptors_types.cpp
  Exprs_constants.cpp
  Exprs_types.cpp
  Frontend_types.cpp
  Frontend_constants.cpp
  hive_metastore_constants.cpp
  hive_metastore_types.cpp
  Opcodes_constants.cpp
  Opcodes_types.cpp
  PlanNodes_constants.cpp
  PlanNodes_types.cpp
  RuntimeProfile_constants.cpp
  RuntimeProfile_types.cpp
  SparrowTypes_constants.cpp
  SparrowTypes_types.cpp
  StateStoreService.cpp
  StateStoreService_constants.cpp
  StateStoreService_types.cpp
  StateStoreSubscriberService.cpp
  StateStoreSubscriberService_constants.cpp
  StateStoreSubscriberService_types.cpp
  Types_constants.cpp
  Types_types.cpp
  JavaConstants_constants.cpp
  )

# keep everything in one library, the object files reference 
# each other
add_library(ImpalaThrift ${SRC_FILES})

# Setting these files as code-generated lets make clean and incremental builds work
# correctly
set_source_files_properties(${SRC_FILES} PROPERTIES GENERATED TRUE)

add_dependencies(ImpalaThrift thrift-cpp)

target_link_libraries(ImpalaThrift
  thriftstatic
)
