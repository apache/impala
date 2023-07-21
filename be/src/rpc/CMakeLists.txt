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
set(LIBRARY_OUTPUT_PATH "${BUILD_OUTPUT_ROOT_DIRECTORY}/rpc")

# where to put generated binaries
set(EXECUTABLE_OUTPUT_PATH "${BUILD_OUTPUT_ROOT_DIRECTORY}/rpc")

# Mark the protobuf files as generated
set_source_files_properties(${COMMON_PROTO_SRCS} PROPERTIES GENERATED TRUE)
set_source_files_properties(${RPC_TEST_PROTO_SRCS} PROPERTIES GENERATED TRUE)
set_source_files_properties(${PLANNER_PROTO_SRCS} PROPERTIES GENERATED TRUE)

add_library(Rpc
  authentication.cc
  ${COMMON_PROTO_SRCS}
  authentication-util.cc
  impala-service-pool.cc
  ${PLANNER_PROTO_SRCS}
  rpc-mgr.cc
  rpc-trace.cc
  TAcceptQueueServer.cpp
  thrift-util.cc
  thrift-client.cc
  thrift-server.cc
  thrift-thread.cc
)
add_dependencies(Rpc gen-deps)

if (BUILD_WITH_NO_TESTS)
  # thrift-util.cc uses the EXPECT_NO_THROW macro, explicitly linking gtest library
  # to avoid linker errors.
  target_link_libraries(Rpc gtest)
  return()
endif()

add_library(RpcTests STATIC
  thrift-util-test.cc
)
add_dependencies(RpcTests gen-deps)

ADD_UNIFIED_BE_LSAN_TEST(thrift-util-test ThriftUtil.*)

# Exception to unified be tests: The following tests have extra link libraries and some
# have custom main() functions.

ADD_BE_TEST(thrift-server-test) # TODO: this test leaks servers
# The thrift-server-test uses some utilities from the Kudu security test code.
target_link_libraries(thrift-server-test security-test-for-impala)

ADD_BE_TEST(hs2-http-test)
target_link_libraries(hs2-http-test security-test-for-impala)

ADD_BE_LSAN_TEST(authentication-test)

ADD_BE_TEST(rpc-mgr-test) # TODO: this test leaks various KRPC things
add_dependencies(rpc-mgr-test rpc_test_proto_tgt)
target_link_libraries(rpc-mgr-test rpc_test_proto_tgt)
target_link_libraries(rpc-mgr-test security-test-for-impala)
target_link_libraries(rpc-mgr-test ${KRB5_REALM_OVERRIDE})

ADD_BE_TEST(rpc-mgr-kerberized-test) # TODO: this test leaks various KRPC things
add_dependencies(rpc-mgr-kerberized-test rpc_test_proto_tgt)
target_link_libraries(rpc-mgr-kerberized-test rpc_test_proto_tgt)
target_link_libraries(rpc-mgr-kerberized-test security-test-for-impala)
target_link_libraries(rpc-mgr-kerberized-test ${KRB5_REALM_OVERRIDE})

add_library(rpc_test_proto_tgt ${RPC_TEST_PROTO_SRCS})
add_dependencies(rpc_test_proto_tgt rpc_test_proto krpc)
target_link_libraries(rpc_test_proto_tgt krpc)
