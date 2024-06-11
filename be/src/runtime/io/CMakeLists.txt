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
set(LIBRARY_OUTPUT_PATH "${BUILD_OUTPUT_ROOT_DIRECTORY}/runtime/io")

# where to put generated binaries
set(EXECUTABLE_OUTPUT_PATH "${BUILD_OUTPUT_ROOT_DIRECTORY}/runtime/io")

add_library(Io
  data-cache.cc
  disk-io-mgr.cc
  disk-io-mgr-stress.cc
  disk-file.cc
  local-file-system.cc
  local-file-system-with-fault-injection.cc
  error-converter.cc
  request-context.cc
  scan-range.cc
  scan-buffer-manager.cc
  hdfs-file-reader.cc
  local-file-reader.cc
  local-file-writer.cc
  hdfs-monitored-ops.cc
  data-cache-trace.cc
)
add_dependencies(Io gen-deps)

add_library(IoTests STATIC
  data-cache-trace-test.cc
  disk-io-mgr-test.cc
  disk-file-test.cc
)
add_dependencies(IoTests gen-deps)

if (BUILD_WITH_NO_TESTS)
  return()
endif()

# This test runs forever so should not be part of 'make test'
add_executable(disk-io-mgr-stress-test disk-io-mgr-stress-test.cc)
target_link_libraries(disk-io-mgr-stress-test ${IMPALA_TEST_LINK_LIBS})

# This is a utility to replay access traces
add_executable(data-cache-trace-replayer data-cache-trace-replayer.cc)
target_link_libraries(data-cache-trace-replayer ${IMPALA_TEST_LINK_LIBS})

ADD_UNIFIED_BE_LSAN_TEST(disk-io-mgr-test DiskIoMgrTest.*)
ADD_UNIFIED_BE_LSAN_TEST(disk-file-test DiskFileTest.*:DiskFileDeathTest.*)
ADD_UNIFIED_BE_LSAN_TEST(data-cache-trace-test DataCacheTraceTest.*)
# Exception to unified be: Custom main function (platform tests)
ADD_BE_LSAN_TEST(data-cache-test)
