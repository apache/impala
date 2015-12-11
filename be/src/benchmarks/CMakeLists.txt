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

ADD_BE_BENCHMARK(parse-timestamp-benchmark)
ADD_BE_BENCHMARK(string-search-benchmark)
ADD_BE_BENCHMARK(atod-benchmark)
ADD_BE_BENCHMARK(atof-benchmark)
ADD_BE_BENCHMARK(atoi-benchmark)
ADD_BE_BENCHMARK(lock-benchmark)
ADD_BE_BENCHMARK(thread-create-benchmark)
ADD_BE_BENCHMARK(tuple-layout-benchmark)
ADD_BE_BENCHMARK(string-benchmark)
ADD_BE_BENCHMARK(rle-benchmark)
ADD_BE_BENCHMARK(string-compare-benchmark)
ADD_BE_BENCHMARK(multiint-benchmark)
ADD_BE_BENCHMARK(status-benchmark)
ADD_BE_BENCHMARK(row-batch-serialize-benchmark)
ADD_BE_BENCHMARK(overflow-benchmark)

add_executable(hash-benchmark hash-benchmark.cc)
target_link_libraries(hash-benchmark Experiments ${IMPALA_LINK_LIBS})
