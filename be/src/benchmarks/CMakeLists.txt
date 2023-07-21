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

if (BUILD_WITH_NO_TESTS)
  return()
endif()

# where to put generated libraries
set(LIBRARY_OUTPUT_PATH "${BUILD_OUTPUT_ROOT_DIRECTORY}/benchmarks")

# where to put generated binaries
set(EXECUTABLE_OUTPUT_PATH "${BUILD_OUTPUT_ROOT_DIRECTORY}/benchmarks")

# Add custom target to only build the backend benchmarks
add_custom_target(be-benchmarks)

FUNCTION(ADD_BE_BENCHMARK BENCHMARK_NAME)
  ADD_EXECUTABLE(${BENCHMARK_NAME} ${BENCHMARK_NAME}.cc)
  TARGET_LINK_LIBRARIES(${BENCHMARK_NAME} ${IMPALA_LINK_LIBS})
  ADD_DEPENDENCIES(be-benchmarks ${BENCHMARK_NAME})
ENDFUNCTION()

ADD_BE_BENCHMARK(atod-benchmark)
ADD_BE_BENCHMARK(atof-benchmark)
ADD_BE_BENCHMARK(atoi-benchmark)
ADD_BE_BENCHMARK(bitmap-benchmark)
ADD_BE_BENCHMARK(bit-packing-benchmark)
ADD_BE_BENCHMARK(bloom-filter-benchmark)
ADD_BE_BENCHMARK(bswap-benchmark)
ADD_BE_BENCHMARK(decimal-util-benchmark)
ADD_BE_BENCHMARK(expr-benchmark)
ADD_BE_BENCHMARK(free-lists-benchmark)
ADD_BE_BENCHMARK(hash-benchmark)
ADD_BE_BENCHMARK(in-predicate-benchmark)
ADD_BE_BENCHMARK(int-hash-benchmark)
ADD_BE_BENCHMARK(lock-benchmark)
ADD_BE_BENCHMARK(multiint-benchmark)
ADD_BE_BENCHMARK(network-perf-benchmark)
ADD_BE_BENCHMARK(overflow-benchmark)
ADD_BE_BENCHMARK(parse-timestamp-benchmark)
ADD_BE_BENCHMARK(parquet-delta-benchmark)
ADD_BE_BENCHMARK(process-wide-locks-benchmark)
ADD_BE_BENCHMARK(rle-benchmark)
ADD_BE_BENCHMARK(row-batch-serialize-benchmark)
ADD_BE_BENCHMARK(runtime-profile-benchmark)
ADD_BE_BENCHMARK(scheduler-benchmark)
ADD_BE_BENCHMARK(status-benchmark)
ADD_BE_BENCHMARK(string-benchmark)
ADD_BE_BENCHMARK(string-compare-benchmark)
ADD_BE_BENCHMARK(string-search-benchmark)
ADD_BE_BENCHMARK(thread-create-benchmark)
ADD_BE_BENCHMARK(tuple-layout-benchmark)
ADD_BE_BENCHMARK(convert-timestamp-benchmark)
ADD_BE_BENCHMARK(date-benchmark)
ADD_BE_BENCHMARK(hash-table-benchmark)

target_link_libraries(hash-benchmark Experiments)
