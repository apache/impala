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

add_custom_target(proto-deps)

set(PROTOBUF_OUTPUT_DIR ${CMAKE_SOURCE_DIR}/be/generated-sources/gen-cpp/)

foreach(pb_src common row_batch planner)
   string(TOUPPER ${pb_src} _PB_SRC_UPPER)
   set(_PROTO_SRCS ${_PB_SRC_UPPER}_PROTO_SRCS)
   set(_PROTO_HDRS ${_PB_SRC_UPPER}_PROTO_HDRS)
   set(_PROTO_TGTS ${_PB_SRC_UPPER}_PROTO_TGTS)
   set(_INPUT_SRC ${pb_src}.proto)
   PROTOBUF_GENERATE_CPP(
      ${_PROTO_SRCS} ${_PROTO_HDRS} ${_PROTO_TGTS}
      SOURCE_ROOT ${CMAKE_CURRENT_SOURCE_DIR}
      BINARY_ROOT ${PROTOBUF_OUTPUT_DIR}
      PROTO_FILES ${_INPUT_SRC})
   add_custom_target(${pb_src}_proto DEPENDS ${${_PROTO_TGTS}})
   add_dependencies(proto-deps ${pb_src}_proto)
   set(${_PROTO_SRCS} ${${_PROTO_SRCS}} PARENT_SCOPE)
endforeach()

foreach(pb_src data_stream_service control_service rpc_test statestore_service
      admission_control_service)
   string(TOUPPER ${pb_src} _PB_SRC_UPPER)
   set(_PROTO_SRCS ${_PB_SRC_UPPER}_PROTO_SRCS)
   set(_PROTO_HDRS ${_PB_SRC_UPPER}_PROTO_HDRS)
   set(_PROTO_TGTS ${_PB_SRC_UPPER}_PROTO_TGTS)
   set(_INPUT_SRC ${pb_src}.proto)
   KRPC_GENERATE(
      ${_PROTO_SRCS} ${_PROTO_HDRS} ${_PROTO_TGTS}
      SOURCE_ROOT ${CMAKE_CURRENT_SOURCE_DIR}
      BINARY_ROOT ${PROTOBUF_OUTPUT_DIR}
      PROTO_FILES ${_INPUT_SRC})
   add_custom_target(${pb_src}_proto DEPENDS ${${_PROTO_TGTS}})
   add_dependencies(proto-deps ${pb_src}_proto)
   set(${_PROTO_SRCS} ${${_PROTO_SRCS}} PARENT_SCOPE)
endforeach()
