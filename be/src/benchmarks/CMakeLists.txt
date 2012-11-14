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
set(LIBRARY_OUTPUT_PATH "${BUILD_OUTPUT_ROOT_DIRECTORY}/benchmarks")

# where to put generated binaries
set(EXECUTABLE_OUTPUT_PATH "${BUILD_OUTPUT_ROOT_DIRECTORY}/benchmarks")

add_executable(parse-timestamp-benchmark parse-timestamp-benchmark.cc)
add_executable(string-search-benchmark string-search-benchmark.cc)
add_executable(atof-benchmark atof-benchmark.cc)
add_executable(atoi-benchmark atoi-benchmark.cc)
add_executable(hash-benchmark hash-benchmark.cc)
add_executable(thread-create-benchmark thread-create-benchmark.cc)
add_executable(tuple-layout-benchmark tuple-layout-benchmark.cc)

target_link_libraries(parse-timestamp-benchmark ${IMPALA_LINK_LIBS})
target_link_libraries(string-search-benchmark ${IMPALA_LINK_LIBS})
target_link_libraries(atof-benchmark ${IMPALA_LINK_LIBS})
target_link_libraries(atoi-benchmark ${IMPALA_LINK_LIBS})
target_link_libraries(hash-benchmark Experiments ${IMPALA_LINK_LIBS})
target_link_libraries(thread-create-benchmark ${IMPALA_LINK_LIBS})
target_link_libraries(tuple-layout-benchmark ${IMPALA_LINK_LIBS})

