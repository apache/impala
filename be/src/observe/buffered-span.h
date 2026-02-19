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
#include <unordered_map>
#include <vector>

#include <opentelemetry/common/attribute_value.h>
#include <opentelemetry/common/timestamp.h>
#include <opentelemetry/context/context.h>
#include <opentelemetry/nostd/shared_ptr.h>
#include <opentelemetry/nostd/string_view.h>
#include <opentelemetry/nostd/variant.h>
#include <opentelemetry/trace/scope.h>
#include <opentelemetry/trace/span.h>
#include <opentelemetry/trace/span_context.h>
#include <opentelemetry/trace/span_metadata.h>
#include <opentelemetry/trace/tracer.h>

namespace impala {

// Buffered wrapper around an OpenTelemetry Span that stores timing, attributes, and
// events before submitting them to the SDK.
//
// Where nostd::string_view is used as a function parameter type, the BufferedSpan either
// makes a copy of the underlying string or provides the nostd::string_view to a blocking
// function in the OpenTelemetry SDK which makes its own copy of the string. Thus,
// callers are not required to keep the underlying string alive after any of the class
// member functions return.
//
// This class is not thread-safe. Callers are responsible for synchronizing access to it.
class BufferedSpan {
public:

  using BufferedAttributeValue =
     opentelemetry::nostd::variant<bool, int32_t, int64_t, std::string>;
  using BufferedAttributesMap = std::unordered_map<std::string, BufferedAttributeValue>;

  // Initializes a buffered span with the given name. The start time is recorded at
  // construction time.
  BufferedSpan(opentelemetry::nostd::shared_ptr<opentelemetry::trace::Tracer> tracer,
      const opentelemetry::nostd::string_view& name,
      const opentelemetry::nostd::string_view& start_time_attribute_name,
      const opentelemetry::nostd::string_view& duration_attribute_name,
      BufferedAttributesMap&& attributes,
      const opentelemetry::nostd::variant<opentelemetry::trace::SpanContext,
          opentelemetry::context::Context>& parent,
      const opentelemetry::trace::SpanKind span_kind);

  // Destructor that submits the span if it is not already submitted.
  ~BufferedSpan();

  // Creates a new OpenTelemetry Span object and starts it using buffered attributes,
  // events, and start time.
  void Start();

  // Provides a scope with this span as the active span.
  opentelemetry::trace::Scope SetActive();

  // Sets the parent context to use when starting the span. Must be called before Start().
  void SetParent(opentelemetry::trace::SpanContext parent);

  // Records an end time of now (without submitting).
  void SetEnd();

  // Set an attribute on the buffered span. If the key exists, it is replaced.
  void SetAttribute(const opentelemetry::nostd::string_view& key,
      const BufferedAttributeValue& value) noexcept;

  // Set multiple attributes on the buffered span. If a key exists, it is replaced.
  void SetAttributes(const BufferedAttributesMap& attributes) noexcept;

  // Set an attribute on the buffered span with a value of an empty string.
  void SetAttributeEmpty(const opentelemetry::nostd::string_view& key) noexcept;

  // Adds an event with the given name and current time to the buffered span. Can be
  // called before Start().
  void AddEvent(const opentelemetry::nostd::string_view& name,
      const BufferedAttributesMap& additional_attributes = {}) noexcept;

  // Creates the OpenTelemetry span with the buffered data and ends it with the
  // buffered end time.
  void Submit();

  // Returns a string representation of the span id of the underlying span or the empty
  // string if the span is not started.
  inline const std::string& SpanId() const noexcept {
    return span_id_;
  }

  // Returns a string representation of the trace id of the underlying span or the empty
  // string if the span is not started.
  inline const std::string& TraceId() const noexcept {
    return trace_id_;
  }

  // Returns the SpanContext of the underlying span or an invalid SpanContext if the span
  // is not started.
  const opentelemetry::trace::SpanContext GetContext() const;

  // Returns true if the end time has been set either by calling SetEnd() or Submit()
  // or if Cancel() has been called.
  bool HasEnded() const { return end_time_set_ || cancelled_; }

  // Mark the span as cancelled. Cancelled spans will not be submitted to the
  // OpenTelemetry SDK when Submit() is called.
  void Cancel() { cancelled_ = true; }

private:
  using OtelAttributesMap = std::unordered_map<opentelemetry::nostd::string_view,
    opentelemetry::common::AttributeValue>;

  struct Event {
    std::string name;
    opentelemetry::common::SystemTimestamp timestamp;
    BufferedAttributesMap attributes;
  };

  // Adds buffered events on an existing OpenTelemetry span using stored event data.
  // No-op if span is not started. Clears events only if they were added to the span.
  void AddEventsToSpan();

  // Adds a single event to an existing OpenTelemetry span. DOES NOT check if the span is
  // started. Callers must ensure the span is started before calling
  inline void AddEventToSpan(const opentelemetry::nostd::string_view name,
      const opentelemetry::common::SystemTimestamp timestamp,
      const BufferedAttributesMap& additional_attributes);

  // Sets the value of end_steady_time_ to now overwriting any previously set value.
  void RecordEndTimeNow();

  // Constructor set class data members.
  opentelemetry::nostd::shared_ptr<opentelemetry::trace::Tracer> tracer_;
  const std::string name_;
  const std::string start_time_attribute_name_;
  const std::string duration_attribute_name_;
  BufferedAttributesMap attributes_;
  opentelemetry::nostd::variant<opentelemetry::trace::SpanContext,
      opentelemetry::context::Context> parent_;
  opentelemetry::trace::SpanKind span_kind_;
  const opentelemetry::common::SystemTimestamp start_system_time_;
  const opentelemetry::common::SteadyTimestamp start_steady_time_;

  // Other class data members.
  opentelemetry::common::SteadyTimestamp end_steady_time_;
  bool end_time_set_ = false;
  std::vector<Event> events_;
  opentelemetry::nostd::shared_ptr<opentelemetry::trace::Span> span_;
  bool span_submitted_ = false;
  bool cancelled_ = false;
  std::string span_id_;
  std::string trace_id_;

};

} // namespace impala
