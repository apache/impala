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
set(LIBRARY_OUTPUT_PATH "${BUILD_OUTPUT_ROOT_DIRECTORY}/experiments")

# where to put generated binaries
set(EXECUTABLE_OUTPUT_PATH "${BUILD_OUTPUT_ROOT_DIRECTORY}/experiments")

add_library(Experiments
  data-provider.cc
)
add_dependencies(Experiments gen-deps)

if (BUILD_WITH_NO_TESTS)
  return()
endif()

add_executable(data-provider-test data-provider-test.cc)
add_executable(tuple-splitter-test tuple-splitter-test.cc)
add_executable(hash-partition-test hash-partition-test.cc)
add_executable(compression-test compression-test.cc)
add_executable(hash-ring-util hash-ring-util.cc)

# Add Experiments lib explicitly here.  It is not used by any other part
# of impala so don't include it in link_libs
target_link_libraries(data-provider-test Experiments ${IMPALA_LINK_LIBS})
target_link_libraries(tuple-splitter-test Experiments ${IMPALA_LINK_LIBS})
target_link_libraries(hash-partition-test ${IMPALA_LINK_LIBS})
target_link_libraries(compression-test ${IMPALA_LINK_LIBS})
target_link_libraries(hash-ring-util ${IMPALA_LINK_LIBS})

ADD_BE_LSAN_TEST(string-search-sse-test)
