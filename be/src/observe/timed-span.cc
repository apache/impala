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

#include "observe/timed-span.h"

#include <chrono>
#include <iomanip>
#include <sstream>
#include <string>

#include <glog/logging.h>
#include <opentelemetry/common/attribute_value.h>
#include <opentelemetry/context/context.h>
#include <opentelemetry/nostd/shared_ptr.h>
#include <opentelemetry/nostd/span.h>
#include <opentelemetry/nostd/string_view.h>
#include <opentelemetry/trace/scope.h>
#include <opentelemetry/trace/span_metadata.h>
#include <opentelemetry/trace/span_startoptions.h>
#include <opentelemetry/trace/tracer.h>

#include "common/compiler-util.h"

using namespace opentelemetry;
using namespace std;

namespace impala {

// Helper function to get the current time in milliseconds since the epoch.
static int64_t get_epoch_milliseconds() {
  auto now = chrono::system_clock::now();
  return chrono::duration_cast<chrono::milliseconds>(now.time_since_epoch()).count();
} // function get_epoch_milliseconds

// Helper function to convert an OpenTelemetry ID to a string representation.
template <size_t N>
static string id_to_string(const nostd::span<const uint8_t, N> id) {
  DCHECK(N == 8 || N == 16) << "id_to_string only supports 8 or 16 byte IDs";

  ostringstream oss;
  for (auto byte : id) {
    oss << hex << setw(2) << setfill('0') << static_cast<int>(byte);
  }

  return oss.str();
} // function id_to_string

TimedSpan::TimedSpan(const nostd::shared_ptr<trace::Tracer>& tracer,
    const nostd::string_view& name, const nostd::string_view& start_time_attribute_name,
    const nostd::string_view& duration_attribute_name, OtelAttributesMap&& attributes,
    trace::SpanKind span_kind, const shared_ptr<TimedSpan>& root) :
    start_time_attribute_name_(start_time_attribute_name),
    duration_attribute_name_(duration_attribute_name),
    start_time_(get_epoch_milliseconds()) {

  trace::StartSpanOptions options;
  options.kind = span_kind;
  if (root) {
    options.parent = root->span_->GetContext();
  } else {
    options.parent = context::Context().SetValue(trace::kIsRootSpanKey, true);
  }

  attributes.insert_or_assign(start_time_attribute_name_,
      static_cast<int64_t>(start_time_));

  span_ = tracer->StartSpan(
    name,
    attributes,
    options);

    trace_id_ = id_to_string(span_->GetContext().trace_id().Id());
    span_id_ = id_to_string(span_->GetContext().span_id().Id());
} // constructor TimedSpan

void TimedSpan::End() {
  const int64_t end_time = get_epoch_milliseconds();

  span_->SetAttribute("EndTime", end_time);
  span_->SetAttribute(duration_attribute_name_, (end_time - start_time_));

  span_->End();
} // function End

void TimedSpan::SetAttribute(const nostd::string_view& key,
    const common::AttributeValue& value) noexcept {
  span_->SetAttribute(key, value);
} // function SetAttribute

void TimedSpan::SetAttributeEmpty(const nostd::string_view& key) noexcept {
  this->SetAttribute(key, "");
} // function SetAttributeEmpty

void TimedSpan::AddEvent(const nostd::string_view& name, const OtelAttributesMap&
    additional_attributes) noexcept {
  span_->AddEvent(name, additional_attributes);
} // function AddEvent

trace::Scope TimedSpan::SetActive() {
  return trace::Tracer::WithActiveSpan(span_);
} // function SetActive

const string& TimedSpan::SpanId() const {
  return span_id_;
} // function SpanId

const string& TimedSpan::TraceId() const {
  return trace_id_;
} // function TraceId

} // namespace impala