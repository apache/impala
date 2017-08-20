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


cmake_minimum_required(VERSION 2.6)

set(PROTOBUF_OUTPUT_DIR ${CMAKE_SOURCE_DIR}/be/generated-sources/gen-cpp/)

KRPC_GENERATE(RPC_TEST_PROTO_SRCS RPC_TEST_PROTO_HDRS
  RPC_TEST_PROTO_TGTS
  SOURCE_ROOT ${CMAKE_CURRENT_SOURCE_DIR}
  BINARY_ROOT ${PROTOBUF_OUTPUT_DIR}
  PROTO_FILES rpc_test.proto)
add_custom_target(rpc_test_proto_tgt DEPENDS ${RPC_TEST_PROTO_TGTS})
set(RPC_TEST_PROTO_SRCS ${RPC_TEST_PROTO_SRCS} PARENT_SCOPE)

add_custom_target(proto-deps ALL DEPENDS token_proto rpc_header_proto)