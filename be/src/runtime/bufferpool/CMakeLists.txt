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
set(LIBRARY_OUTPUT_PATH "${BUILD_OUTPUT_ROOT_DIRECTORY}/runtime/bufferpool")

# where to put generated binaries
set(EXECUTABLE_OUTPUT_PATH "${BUILD_OUTPUT_ROOT_DIRECTORY}/runtime/bufferpool")

add_library(BufferPool
  buffer-allocator.cc
  buffer-pool.cc
  reservation-tracker.cc
  reservation-util.cc
  suballocator.cc
  system-allocator.cc
)
add_dependencies(BufferPool gen-deps)

if (BUILD_WITH_NO_TESTS)
  return()
endif()

add_library(BufferPoolTests STATIC
  free-list-test.cc
  suballocator-test.cc
)
add_dependencies(BufferPoolTests gen-deps)

# Exception to unified be tests: Custom main() with test repeats, messages, etc.
ADD_BE_LSAN_TEST(buffer-allocator-test)
ADD_BE_LSAN_TEST(buffer-pool-test)

ADD_UNIFIED_BE_LSAN_TEST(free-list-test FreeListTest.*)
# Exception to unified be tests: Custom main() with InitFeSupport(disable_codegen=false)
ADD_BE_LSAN_TEST(reservation-tracker-test)
ADD_UNIFIED_BE_LSAN_TEST(suballocator-test SuballocatorTest.*)
