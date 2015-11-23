# Copyright 2014 Cloudera Inc.
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
set(LIBRARY_OUTPUT_PATH "${BUILD_OUTPUT_ROOT_DIRECTORY}/scheduling")

# where to put generated binaries
set(EXECUTABLE_OUTPUT_PATH "${BUILD_OUTPUT_ROOT_DIRECTORY}/scheduling")

# TODO: Move other scheduling-related classes here
add_library(Scheduling STATIC
  admission-controller.cc
  query-resource-mgr.cc
  query-schedule.cc
  request-pool-service.cc
  simple-scheduler.cc
)
add_dependencies(Scheduling thrift-deps)

ADD_BE_TEST(simple-scheduler-test)
# TODO: Add BE test
# ADD_BE_TEST(admission-controller-test)
