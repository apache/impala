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

#include "observe/otel-log-handler.h"

#include <sstream>

#include <glog/logging.h>
#include <opentelemetry/common/attribute_value.h>
#include <opentelemetry/sdk/common/global_log_handler.h>
#include <opentelemetry/nostd/variant.h>

#include "common/compiler-util.h"
#include "gutil/strings/substitute.h"

using namespace std;
using namespace opentelemetry::nostd;
using namespace opentelemetry::sdk::common;
using namespace opentelemetry::sdk::common::internal_log;

namespace impala {

void OtelLogHandler::Handle(LogLevel level, const char* file, int line, const char* msg,
    const AttributeMap &attributes) noexcept {
  stringstream attr_stream;

  for (const auto& attribute : attributes) {
    attr_stream << " " << attribute.first << "=\"";
    if (holds_alternative<bool>(attribute.second)) {
      attr_stream << get<bool>(attribute.second);
    } else if (holds_alternative<int32_t>(attribute.second)) {
      attr_stream << to_string(get<int32_t>(attribute.second));
    } else if (holds_alternative<int64_t>(attribute.second)) {
      attr_stream << to_string(get<int64_t>(attribute.second));
    } else if (holds_alternative<uint32_t>(attribute.second)) {
      attr_stream << to_string(get<uint32_t>(attribute.second));
    } else if (holds_alternative<uint64_t>(attribute.second)) {
      attr_stream << to_string(get<uint64_t>(attribute.second));
    } else if (holds_alternative<double>(attribute.second)) {
      attr_stream << to_string(get<double>(attribute.second));
    } else if (holds_alternative<string>(attribute.second)) {
      attr_stream << get<string>(attribute.second);
    } else {
      attr_stream << "unsupported-type";
    }
    attr_stream << "\"";
  }

  string log_msg = strings::Substitute("$0 file=\"$1\" line=\"$2\"$3", msg, file, line,
      attr_stream.str());

  switch (level) {
    case LogLevel::Error:
      LOG(ERROR) << log_msg;
      break;
    case LogLevel::Warning:
      LOG(WARNING) << log_msg;
      break;
    case LogLevel::Info:
      LOG(INFO) << log_msg;
      break;
    default:
      VLOG(2) << log_msg;
      break;
  }
} // function OtelLogHandler::Handle

} // namespace impala
