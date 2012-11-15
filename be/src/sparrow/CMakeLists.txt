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
set(LIBRARY_OUTPUT_PATH "${BUILD_OUTPUT_ROOT_DIRECTORY}/sparrow")

# where to put generated binaries
set(EXECUTABLE_OUTPUT_PATH "${BUILD_OUTPUT_ROOT_DIRECTORY}/sparrow")

add_library(Sparrow STATIC
  util.cc
  failure-detector.cc
  simple-scheduler.cc
  state-store.cc
  state-store-subscriber.cc
  subscription-manager.cc
)

add_executable(statestored statestored-main.cc)

target_link_libraries(statestored ${IMPALA_LINK_LIBS})

add_executable(state-store-test state-store-test.cc)

target_link_libraries(state-store-test ${IMPALA_TEST_LINK_LIBS})

add_test(state-store-test ${BUILD_OUTPUT_ROOT_DIRECTORY}/sparrow/state-store-test)

add_executable(simple-scheduler-test simple-scheduler-test.cc)

target_link_libraries(simple-scheduler-test
  Common
  Runtime
  Common
  Runtime
  ${IMPALA_TEST_LINK_LIBS}
)

add_test(simple-scheduler-test ${BUILD_OUTPUT_ROOT_DIRECTORY}/sparrow/simple-scheduler-test)
