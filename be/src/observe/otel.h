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

#include <memory>
#include <string>
#include <string_view>

#include "common/status.h"
#include "gen-cpp/Query_types.h"
#include "observe/span-manager.h"
#include "service/client-request-state.h"

namespace impala {

// Version of the spec for representing Impala queries as OpenTelemetry traces.
const std::string SCOPE_SPAN_SPEC_VERSION = "1.0.0";

// Constants representing the supported OpenTelemetry exporters.
const std::string OTEL_EXPORTER_OTLP_HTTP = "otlp_http";
const std::string OTEL_EXPORTER_FILE = "file";

// Constants representing the supported OpenTelemetry span processor implementations.
const std::string SPAN_PROCESSOR_SIMPLE = "simple";
const std::string SPAN_PROCESSOR_BATCH = "batch";

// Returns true if an OpenTelemetry trace needs to be created for the given SQL query.
// The sql string_view will be trimmed of leading whitespace and comments.
bool should_otel_trace_query(std::string_view sql,
    const TSessionType::type& session_type);

// Initializes the OpenTelemetry tracer with the configuration defined in the coordinator
// startup flags (see otel-flags.cc and otel-flags-trace.cc for the list). Does not verify
// that OpenTelemetry tracing is enabled (otel_trace_enabled flag).
Status init_otel_tracer();

// Force flushes any buffered spans and shuts down the OpenTelemetry tracer.
void shutdown_otel_tracer();

// Builds a SpanManager instance for the given query.
std::shared_ptr<SpanManager> build_span_manager(ClientRequestState*);

} // namespace impala
