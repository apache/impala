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
set(LIBRARY_OUTPUT_PATH "${BUILD_OUTPUT_ROOT_DIRECTORY}/util/cache")

# where to put generated binaries
set(EXECUTABLE_OUTPUT_PATH "${BUILD_OUTPUT_ROOT_DIRECTORY}/util/cache")

add_library(UtilCache
  cache.cc
  lirs-cache.cc
  rl-cache.cc
)
add_dependencies(UtilCache gen-deps)

if (BUILD_WITH_NO_TESTS)
  return()
endif()

add_library(UtilCacheTests STATIC
  cache-test.cc
  lirs-cache-test.cc
  rl-cache-test.cc
)
add_dependencies(UtilCacheTests gen-deps)

add_executable(cache-bench cache-bench.cc)
target_link_libraries(cache-bench ${IMPALA_TEST_LINK_LIBS})

ADD_UNIFIED_BE_LSAN_TEST(cache-test "CacheTypes/CacheTest.*")
ADD_UNIFIED_BE_LSAN_TEST(lirs-cache-test "LIRSCacheTest.*")
ADD_UNIFIED_BE_LSAN_TEST(rl-cache-test "CacheTypes/CacheInvalidationTest.*:CacheTypes/LRUCacheTest.*:FIFOCacheTest.*")
