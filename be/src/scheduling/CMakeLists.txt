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
set(LIBRARY_OUTPUT_PATH "${BUILD_OUTPUT_ROOT_DIRECTORY}/scheduling")

# where to put generated binaries
set(EXECUTABLE_OUTPUT_PATH "${BUILD_OUTPUT_ROOT_DIRECTORY}/scheduling")

# Mark the protobuf file as generated
set_source_files_properties(${ADMISSION_CONTROL_SERVICE_PROTO_SRCS} PROPERTIES GENERATED TRUE)

# TODO: Move other scheduling-related classes here
add_library(Scheduling STATIC
  admissiond-env.cc
  ${ADMISSION_CONTROL_SERVICE_PROTO_SRCS}
  admission-controller.cc
  admission-control-client.cc
  admission-control-service.cc
  admissiond-main.cc
  executor-blacklist.cc
  cluster-membership-mgr.cc
  cluster-membership-test-util.cc
  executor-group.cc
  hash-ring.cc
  local-admission-control-client.cc
  remote-admission-control-client.cc
  request-pool-service.cc
  scheduler-test-util.cc
  schedule-state.cc
  scheduler.cc
)
add_dependencies(Scheduling gen-deps)

if (BUILD_WITH_NO_TESTS)
  return()
endif()

add_library(SchedulingTests STATIC
  admission-controller-test.cc
  cluster-membership-mgr-test.cc
  executor-group-test.cc
  hash-ring-test.cc
  scheduler-test.cc
)
add_dependencies(SchedulingTests gen-deps)

ADD_UNIFIED_BE_LSAN_TEST(admission-controller-test AdmissionControllerTest.*)
ADD_UNIFIED_BE_LSAN_TEST(cluster-membership-mgr-test "ClusterMembershipMgrTest.*:ClusterMembershipMgrUnitTest.*")
ADD_UNIFIED_BE_LSAN_TEST(executor-group-test ExecutorGroupTest.*)
ADD_UNIFIED_BE_LSAN_TEST(hash-ring-test HashRingTest.*)
ADD_UNIFIED_BE_LSAN_TEST(scheduler-test SchedulerTest.*)
