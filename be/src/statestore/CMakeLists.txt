# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.


# where to put generated libraries
set(LIBRARY_OUTPUT_PATH "${BUILD_OUTPUT_ROOT_DIRECTORY}/statestore")

# where to put generated binaries
set(EXECUTABLE_OUTPUT_PATH "${BUILD_OUTPUT_ROOT_DIRECTORY}/statestore")

# Mark the protobuf file as generated
set_source_files_properties(${STATESTORE_SERVICE_PROTO_SRCS} PROPERTIES GENERATED TRUE)

add_library(Statestore
  failure-detector.cc
  ${STATESTORE_SERVICE_PROTO_SRCS}
  statestore.cc
  statestore-catalogd-mgr.cc
  statestore-subscriber-catalog.cc
  statestore-subscriber.cc
  statestored-main.cc
)
add_dependencies(Statestore gen-deps)

if (BUILD_WITH_NO_TESTS)
  return()
endif()

ADD_BE_LSAN_TEST(statestore-test)
