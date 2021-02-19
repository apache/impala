// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#pragma once
#include <string>
#include <thrift/protocol/TDebugProtocol.h>
#include "util/debug-util.h"

DECLARE_string(debug_actions);

namespace impala {

  /// This method is a wrapper over Thrift's ThriftDebugString. It catches any exception
  /// that might be thrown by the underlying Thrift method. Typically, we use this method
  /// when we suspect the ThriftStruct could be a large object like a HdfsTable whose
  /// string representation could have significantly larger memory requirements than the
  /// binary representation.
  template<typename ThriftStruct>
  std::string ThriftDebugStringNoThrow(ThriftStruct ts) noexcept {
    try {
      Status status = DebugAction(FLAGS_debug_actions, "THRIFT_DEBUG_STRING");
      return apache::thrift::ThriftDebugString(ts);
    } catch (std::exception& e) {
      return "Unexpected exception received: " + std::string(e.what());
    }
  }
}
