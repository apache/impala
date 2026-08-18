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

#include "observe/otel-propagation.h"

#include <opentelemetry/context/context.h>
#include <opentelemetry/trace/context.h>
#include <opentelemetry/trace/propagation/http_trace_context.h>
#include <opentelemetry/trace/span.h>

using namespace opentelemetry;

namespace impala {

HttpHeaderCarrier::HttpHeaderCarrier(nostd::string_view traceparent,
    nostd::string_view tracestate)
  : traceparent_(traceparent), tracestate_(tracestate) {}

nostd::string_view HttpHeaderCarrier::Get(nostd::string_view key) const noexcept {
  if (key == trace::propagation::kTraceParent) {
    return traceparent_;
  }
  if (key == trace::propagation::kTraceState) {
    return tracestate_;
  }
  return "";
}

void HttpHeaderCarrier::Set(nostd::string_view key,
    nostd::string_view value) noexcept {}

trace::SpanContext ExtractSpanContextFromHttpHeaders(nostd::string_view traceparent,
    nostd::string_view tracestate) {
  if (traceparent.empty()) {
    return trace::SpanContext::GetInvalid();
  }

  trace::propagation::HttpTraceContext propagator;
  HttpHeaderCarrier carrier(traceparent, tracestate);
  context::Context ctx;
  context::Context extracted = propagator.Extract(carrier, ctx);
  return trace::GetSpan(extracted)->GetContext();
}

} // namespace impala
