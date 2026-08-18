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

#include <opentelemetry/context/propagation/text_map_propagator.h>
#include <opentelemetry/nostd/string_view.h>
#include <opentelemetry/trace/span_context.h>

namespace impala {

/// TextMapCarrier implementation that reads traceparent and tracestate header values.
/// The caller must ensure the header values outlive this object.
class HttpHeaderCarrier : public opentelemetry::context::propagation::TextMapCarrier {
 public:
  HttpHeaderCarrier(opentelemetry::nostd::string_view traceparent,
      opentelemetry::nostd::string_view tracestate);

  opentelemetry::nostd::string_view Get(
      opentelemetry::nostd::string_view key) const noexcept override;

  void Set(opentelemetry::nostd::string_view key,
      opentelemetry::nostd::string_view value) noexcept override;

 private:
  opentelemetry::nostd::string_view traceparent_;
  opentelemetry::nostd::string_view tracestate_;
};

// Extracts a remote SpanContext from W3C Trace Context HTTP headers. Returns an invalid
// SpanContext if 'traceparent' is empty or cannot be parsed.
opentelemetry::trace::SpanContext ExtractSpanContextFromHttpHeaders(
    opentelemetry::nostd::string_view traceparent,
    opentelemetry::nostd::string_view tracestate);

} // namespace impala
