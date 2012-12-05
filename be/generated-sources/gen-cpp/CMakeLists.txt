# Copyright 2012 Cloudera Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


# where to put generated libraries
set(LIBRARY_OUTPUT_PATH "${BUILD_OUTPUT_ROOT_DIRECTORY}/thrift")

# where to put generated binaries
set(EXECUTABLE_OUTPUT_PATH "${BUILD_OUTPUT_ROOT_DIRECTORY}/thrift")

set(SRC_FILES
  ImpalaInternalService.cpp
  ImpalaInternalService_constants.cpp
  ImpalaInternalService_types.cpp
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
  Partitions_constants.cpp
  Partitions_types.cpp
  Planner_constants.cpp
  Planner_types.cpp
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
  Status_constants.cpp
  Status_types.cpp
  Types_constants.cpp
  Types_types.cpp
  JavaConstants_constants.cpp
)

# keep everything in one library, the object files reference 
# each other
add_library(ImpalaThrift STATIC ${SRC_FILES})

# Setting these files as code-generated lets make clean and incremental builds work
# correctly
set_source_files_properties(${SRC_FILES} PROPERTIES GENERATED TRUE)

add_dependencies(ImpalaThrift thrift-cpp)

