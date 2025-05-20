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

#ifndef ENABLE_THREAD_INSTRUMENTATION_PREVIEW
#define ENABLE_THREAD_INSTRUMENTATION_PREVIEW
#endif

#include <string>

#include <glog/logging.h>
#include <opentelemetry/sdk/common/thread_instrumentation.h>

namespace impala {

class LoggingInstrumentation : public opentelemetry::sdk::common::ThreadInstrumentation {

public:
  LoggingInstrumentation(const std::string& thread_type) : thread_type_(thread_type) {}

  void OnStart() override {
    VLOG(2) << thread_type_ << " opentelemetry thread started";
  }

  void OnEnd() override {
    VLOG(2) << thread_type_ << " opentelemetry thread ended";
  }

  void BeforeWait() override {
    VLOG(2) << thread_type_ << " opentelemetry thread before wait";
  }

  void AfterWait() override {
    VLOG(2) << thread_type_ << " opentelemetry thread after wait";
  }

  void BeforeLoad() override {
    VLOG(2) << thread_type_ << " opentelemetry thread before load";
  }

  void AfterLoad() override {
    VLOG(2) << thread_type_ << " opentelemetry thread after load";
  }

private:
  const std::string thread_type_;
}; // class LoggingInstrumentation

} // namespace impala
