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
set_source_files_properties(${RPC_TEST_PROTO_SRCS} PROPERTIES GENERATED TRUE)

add_library(Rpc
  authentication.cc
  rpc-mgr.cc
  rpc-trace.cc
  TAcceptQueueServer.cpp
  thrift-util.cc
  thrift-client.cc
  thrift-server.cc
  thrift-thread.cc
)
add_dependencies(Rpc gen-deps)

ADD_BE_TEST(thrift-util-test)
ADD_BE_TEST(thrift-server-test)
# The thrift-server-test uses some utilites from the Kudu security test code.
target_link_libraries(thrift-server-test security-test-for-impala)

ADD_BE_TEST(authentication-test)

ADD_BE_TEST(rpc-mgr-test)
add_dependencies(rpc-mgr-test rpc_test_proto)
target_link_libraries(rpc-mgr-test rpc_test_proto)

add_library(rpc_test_proto ${RPC_TEST_PROTO_SRCS})
add_dependencies(rpc_test_proto rpc_test_proto_tgt krpc)
target_link_libraries(rpc_test_proto krpc)
