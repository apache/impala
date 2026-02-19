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

#include "observe/buffered-span.h"

#include <chrono>
#include <iomanip>
#include <sstream>
#include <string>
#include <utility>

#include <glog/logging.h>
#include <gutil/strings/substitute.h>
#include <opentelemetry/context/context.h>
#include <opentelemetry/trace/scope.h>
#include <opentelemetry/trace/span_metadata.h>

using namespace opentelemetry;
using namespace std;

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

namespace impala {

BufferedSpan::BufferedSpan(nostd::shared_ptr<trace::Tracer> tracer,
  const nostd::string_view& name,
  const nostd::string_view& start_time_attribute_name,
  const nostd::string_view& duration_attribute_name,
  BufferedAttributesMap&& attributes,
  const nostd::variant<trace::SpanContext, context::Context>& parent,
  const trace::SpanKind span_kind) :
  tracer_(std::move(tracer)),
  name_(name),
  start_time_attribute_name_(start_time_attribute_name),
  duration_attribute_name_(duration_attribute_name),
  attributes_(std::move(attributes)),
  parent_(parent),
  span_kind_(span_kind),
  start_system_time_(chrono::system_clock::now()),
  start_steady_time_(chrono::steady_clock::now()) {
  attributes_.insert_or_assign(start_time_attribute_name_,
    static_cast<int64_t>(chrono::duration_cast<chrono::milliseconds>(
    start_system_time_.time_since_epoch()).count()));
} // constructor

BufferedSpan::~BufferedSpan() {
  Submit();
} // destructor

void BufferedSpan::SetParent(opentelemetry::trace::SpanContext parent) {
  parent_ = parent;
} // function SetParent

void BufferedSpan::Start() {
  DCHECK(!span_) << "Attempted to start already started span '" << name_ << "'.";
  VLOG(2) << "Starting span '" << name_ << "'.";

  trace::StartSpanOptions options;
  options.kind = span_kind_;
  options.start_system_time = start_system_time_;
  options.start_steady_time = start_steady_time_;
  options.parent = parent_;

  OtelAttributesMap addl_attribs;
  for (const auto& a : attributes_) {
    addl_attribs.insert_or_assign(a.first,
        nostd::visit([](const auto& value) -> common::AttributeValue {
          return value;
        }, a.second));
  }

  span_ = tracer_->StartSpan(name_, addl_attribs, options);
  span_id_ = id_to_string(span_->GetContext().span_id().Id());
  trace_id_ = id_to_string(span_->GetContext().trace_id().Id());
  attributes_.clear();

  AddEventsToSpan();
} // function Start

void BufferedSpan::SetEnd() {
  RecordEndTimeNow();
} // function SetEnd

void BufferedSpan::SetAttributes(const BufferedAttributesMap& attributes) noexcept {
  for (const auto& a : attributes) {
    SetAttribute(a.first, a.second);
  }
} // function SetAttributes

void BufferedSpan::SetAttribute(const nostd::string_view& key,
    const BufferedAttributeValue& value) noexcept {
  attributes_.insert_or_assign(string(key), value);
} // function SetAttribute


void BufferedSpan::SetAttributeEmpty(const nostd::string_view& key) noexcept {
  SetAttribute(key, string(""));
} // function SetAttributeEmpty

inline void BufferedSpan::AddEventToSpan(const nostd::string_view name,
    const common::SystemTimestamp timestamp,
    const BufferedAttributesMap& additional_attributes) {

  OtelAttributesMap event_attrs;
  for (const auto& a : additional_attributes) {
    event_attrs.insert_or_assign(a.first,
        nostd::visit([](const auto& value) -> common::AttributeValue {
          return value;
        }, a.second));
  }

  span_->AddEvent(name, timestamp, event_attrs);
} // function AddEventToSpan

void BufferedSpan::AddEvent(const nostd::string_view& name,
    const BufferedAttributesMap& additional_attributes) noexcept {
  if (span_) {
    AddEventToSpan(name, chrono::system_clock::now(), additional_attributes);
  } else {
    events_.push_back({string(name), chrono::system_clock::now(), additional_attributes});
  }
}// function AddEvent

void BufferedSpan::Submit() {
  if (span_submitted_ || cancelled_) {
    VLOG(2) << strings::Substitute("Not submitting span '$0', submitted='$1' "
        "cancelled='$2'.", name_, span_submitted_, cancelled_);
    return;
  }

  VLOG(2) << "Submitting span '" << name_ << "'.";

  if (!span_) {
    Start();
  }

  if (!end_time_set_) {
    RecordEndTimeNow();
  }

  const int64_t end_time = chrono::duration_cast<chrono::milliseconds>(
      end_steady_time_.time_since_epoch()).count();
  const int64_t start_time = chrono::duration_cast<chrono::milliseconds>(
      start_steady_time_.time_since_epoch()).count();

  span_->SetAttribute("EndTime",
      static_cast<int64_t>(chrono::duration_cast<chrono::milliseconds>(
      end_steady_time_.time_since_epoch()).count()));
  span_->SetAttribute(duration_attribute_name_, (end_time - start_time));

  for (const auto& a : attributes_) {
    nostd::visit([&](const auto& value) {
      span_->SetAttribute(a.first, value);
    }, a.second);
  }

  AddEventsToSpan();

  trace::EndSpanOptions end_options;
  end_options.end_steady_time = end_steady_time_;
  span_->End(end_options);
  span_submitted_ = true;
  span_ = nullptr;
} // function Submit

const trace::SpanContext BufferedSpan::GetContext() const {
  if (span_) {
    return span_->GetContext();
  } else {
    DCHECK(false) << "Attempted to get span context for a span that is not started.";
    return trace::SpanContext::GetInvalid();
  }
} // function GetContext

trace::Scope BufferedSpan::SetActive() {
  return trace::Tracer::WithActiveSpan(span_);
} // function SetActive

void BufferedSpan::RecordEndTimeNow() {
  end_steady_time_ = chrono::steady_clock::now();
  end_time_set_ = true;
} // function RecordEndTimeNow

void BufferedSpan::AddEventsToSpan() {
  DCHECK(span_) << "Attempted to add events to a span that is not started.";

  if (span_) {
    for (const Event& event : events_) {
      AddEventToSpan(event.name, event.timestamp, event.attributes);
    }
    events_.clear();
  }
} // function AddEventsToSpan

} // namespace impala
