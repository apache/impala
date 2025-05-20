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

#include <unordered_map>
#include <string>

#include <opentelemetry/common/attribute_value.h>
#include <opentelemetry/nostd/shared_ptr.h>
#include <opentelemetry/nostd/string_view.h>
#include <opentelemetry/trace/scope.h>
#include <opentelemetry/trace/span.h>
#include <opentelemetry/trace/span_context.h>
#include <opentelemetry/trace/span_metadata.h>
#include <opentelemetry/trace/tracer.h>

#include "common/compiler-util.h"

namespace impala {

typedef std::unordered_map<opentelemetry::nostd::string_view,
    opentelemetry::common::AttributeValue> OtelAttributesMap;

// Proxy class for an OpenTelemetry Span that automatically adds attributes for start
// time, end time, and total span duration.
class TimedSpan {
public:
  // Initializes and starts a new span with the given name and attribute names.
  // Parameters:
  //   tracer -- The OpenTelemetry tracer to use to create the span. Not stored, only used
  //             during construction.
  //   name -- The name of the span.
  //   start_time_attribute_name -- The name of the attribute that contains the span start
  //                                time.
  //   duration_attribute_name -- The name of the attribute that contains the span
  //                              duration.
  //   attributes -- A map of attributes to set on the span at creation time.
  //   span_kind -- The kind of span. Default is INTERNAL.
  TimedSpan(const opentelemetry::nostd::shared_ptr<opentelemetry::trace::Tracer>& tracer,
      const opentelemetry::nostd::string_view& name,
      const opentelemetry::nostd::string_view& start_time_attribute_name,
      const opentelemetry::nostd::string_view& duration_attribute_name,
      OtelAttributesMap&& attributes,
      opentelemetry::trace::SpanKind span_kind =
      opentelemetry::trace::SpanKind::kInternal,
      const std::shared_ptr<TimedSpan>& root = nullptr);

  // Ends the span and sets the "EndTime", and duration attributes.
  void End();

  // Set any attribute on the underlying span.
  void SetAttribute(const opentelemetry::nostd::string_view& key,
      const opentelemetry::common::AttributeValue& value) noexcept;

  // Set any attribute on the underlying span with a value of an empty string.
  void SetAttributeEmpty(const opentelemetry::nostd::string_view& key) noexcept;

  // Adds an event with the given name to the underlying span.
  void AddEvent(const opentelemetry::nostd::string_view& name,
      const OtelAttributesMap& additional_attributes = {}) noexcept;

  // Provides a scope that sets this span as the currently active span.
  opentelemetry::trace::Scope SetActive();

  // Returns a string representation of the span id of the underlying span. Will never
  // return nullptr.
  const std::string& SpanId() const;

  // Returns a string representation of the trace id of the underlying span. Will never
  // return nullptr.
  const std::string& TraceId() const;

  // Returns the OpenTelemetry span context.
  opentelemetry::trace::SpanContext GetContext() const { return span_->GetContext(); }

private:
  const opentelemetry::nostd::string_view start_time_attribute_name_;
  const opentelemetry::nostd::string_view duration_attribute_name_;
  const int64_t start_time_;

  opentelemetry::nostd::shared_ptr<opentelemetry::trace::Span> span_;
  std::string span_id_;
  std::string trace_id_;

}; // class TimedSpan

} // namespace impala
