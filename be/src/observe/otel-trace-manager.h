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
#include <mutex>
#include <string>
#include <unordered_map>

#include <opentelemetry/nostd/shared_ptr.h>
#include <opentelemetry/trace/scope.h>
#include <opentelemetry/trace/tracer.h>

#include "common/status.h"
#include "observe/buffered-span.h"

namespace impala {

// Forward declaration to break cyclical imports.
class ClientRequestState;

// Manages the OpenTelemetry trace (root and child spans) for a single query. Provides
// convenience functions to start each child span with the appropriate name/attributes and
// to end each child span. Only one child span can be active at a time.
//
// All data for the root, Init, Submitted, and Planning spans are buffered until the
// frontend planner communicates whether the query should be traced or not. If the query
// should be traced, the root span is started via the OpenTelemetry SDK and made the
// active span. The Init and Submitted spans are also started/ended via the SDK, and the
// Planning span is started via the SDK. When OtelTraceManager instance is destroyed, any
// open child spans and the root span are ended.
class OtelTraceManager {
public:
  OtelTraceManager(
      opentelemetry::nostd::shared_ptr<opentelemetry::trace::Tracer> tracer,
      ClientRequestState* client_request_state);
  ~OtelTraceManager();

  // Adds an event to the currently active child span. If no child span is active,
  // logs a warning and does nothing else. The event time is set to the current time.
  // Parameters:
  //   name -- The name of the event to add.
  void AddChildSpanEvent(const opentelemetry::nostd::string_view& name);

  // Functions to start child spans. If another child span is active, it will be ended,
  // a warning will be logged, and, if the otel_trace_exhaustive_dchecks flag is true,
  // a DCHECK will fail.
  void StartChildSpanInit();
  void StartChildSpanSubmitted();
  void StartChildSpanPlanning();
  void StartChildSpanAdmissionControl();
  void StartChildSpanQueryExecution();

  // Starts the Close child span. If another child span is active, the specified cause
  // must not be nullptr or OK. If the specified cause is nullptr or OK, DCHECKS will
  // fail. This behavior is different from other start child span functions since the
  // Close span will be started in error scenarios where another child span may not have
  // been closed due to exiting the query lifecycle early.
  //
  // Parameters:
  //   cause - Pointer to the Status that caused the Close span to be started. If null,
  //           the query status from the client_request_state_ will be used. This
  //           parameter exists for error scenarios where the query status in the
  //           client_request_state_ will not yet be updated with the error.
  void StartChildSpanClose(const Status* cause = nullptr);

  // Functions to end child spans. If no child span is active, logs a warning and does
  // nothing else. These functions take ownership of child_span_mu_ and
  // client_request_state_->lock().
  void EndChildSpanInit();
  void EndChildSpanSubmitted();
  void EndChildSpanPlanning();
  void EndChildSpanAdmissionControl(const Status& cause);
  void EndChildSpanQueryExecution();
  void EndChildSpanClose();

  // Returns true if the root span has been initialized and also has ended.
  bool HasEnded() const;

  // Sets whether the query associated with this OtelTraceManager should be traced.
  // This function must be called during planning after the Planning child span has
  // started but before it has ended.
  void TraceQuery(bool do_trace);

private:
  // Enum defining the child span types.
  enum class ChildSpanType {
    INIT = 0,
    SUBMITTED = 1,
    PLANNING = 2,
    ADMISSION_CONTROL = 3,
    QUERY_EXEC = 4,
    CLOSE = 5
  }; // enum ChildSpanType

  // Struct to hold a BufferedSpan instance of child span and the corresponding end
  // function that populates the appropriate attributes and submits the span.
  struct ChildSpanEntry {
    using EndFuncType = void (OtelTraceManager::*)(const Status*);

    std::unique_ptr<BufferedSpan> span;
    EndFuncType end_func;

    explicit ChildSpanEntry(EndFuncType _end_func) : end_func(_end_func) {}
  }; // struct ChildSpanEntry

  // Converts a ChildSpanType enum value to its string representation.
  static constexpr char const* CHILD_SPAN_NAMES[] = { "Init", "Submitted", "Planning",
      "AdmissionControl", "QueryExecution", "Close"};

  std::string to_string(const ChildSpanType& val) {
    return CHILD_SPAN_NAMES[static_cast<int>(val)];
  }

  // Helper functions that build a child span and populates it with common attributes plus
  // the specified additional attributes.
  // Callers must hold child_span_mu_ but MUST NOT HOLD client_requst_state_->lock().
  BufferedSpan* ChildSpanBuilder(const ChildSpanType& span_type,
      BufferedSpan::BufferedAttributesMap&& additional_attributes = {},
      bool running = false);
  BufferedSpan* ChildSpanBuilder(const ChildSpanType& span_type, bool running);

  // Internal helper functions to perform the actual work of ending child spans.
  // Callers must already hold child_span_mu_ and client_request_state_->lock().
  //
  // Parameters:
  //   cause - See comments on StartChildSpanClose().
  void DoEndChildSpanInit(const Status* cause = nullptr);
  void DoEndChildSpanSubmitted(const Status* cause = nullptr);
  void DoEndChildSpanPlanning(const Status* cause = nullptr);
  void DoEndChildSpanAdmissionControl(const Status* cause = nullptr);
  void DoEndChildSpanQueryExecution(const Status* cause = nullptr);
  void DoEndChildSpanClose(const Status* cause = nullptr);

  // Helper function to end a child span and populate its common attributes.
  // Callers must already hold child_span_mu_ and client_request_state_->lock().
  //
  // Parameters:
  //   cause - See comments on StartChildSpanClose().
  //   additional_attributes - Span specific attributes that will be set on the span
  //                           before ending it.
  void EndChildSpan(const ChildSpanType& span_type, const Status* cause = nullptr,
      const BufferedSpan::BufferedAttributesMap& additional_attributes = {},
      bool submit = false);

  // Returns true if the Close child span is active.
  // Callers must already hold the child_span_mu_ lock.
  bool IsClosing() {
    return UNLIKELY(child_spans_.at(ChildSpanType::CLOSE).span);
  }

  // Helper function to handle the start or cancel of the trace. Callers must already hold
  // the child_span_mu_ lock.
  void DoTraceQuery(bool do_trace);

  // Tracer instance used to construct spans.
  opentelemetry::nostd::shared_ptr<opentelemetry::trace::Tracer> tracer_;

  // Scope to make the root span active and to deactivate the root span when it finishes.
  std::unique_ptr<opentelemetry::trace::Scope> scope_;

  // ClientRequestState for the query this OtelTraceManager is tracking.
  ClientRequestState* client_request_state_;

  // Query ID of the query this OtelTraceManager is tracking.
  const std::string query_id_;

  // BufferedSpan instance for the current child span and the mutex to protect it.
  // To ensure no deadlocks, locks must be acquired in the following order. Note that
  // ClientRequestState::lock_ only needs to be held when interacting with the
  // client_request_state_ variable. It should not be held otherwise.
  // 1. OtelTraceManager::child_span_mu_
  // 2. ClientRequestState::lock_
  std::unique_ptr<BufferedSpan> span_root_;
  std::unordered_map<ChildSpanType, ChildSpanEntry> child_spans_;
  mutable std::mutex child_span_mu_;

  // Indicator set when this OtelTraceManager represents a query that should not have an
  // OpenTelemetry trace created for it.
  bool do_trace_ = false;

  // Indicator set when a root span is created for this OtelTraceManager. Will be false if
  // the associated query will not be traced.
  bool started_ = false;
};  // class OtelTraceManager

} // namespace impala
