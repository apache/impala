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
#include <ostream>
#include <string>
#include <utility>

#include <opentelemetry/nostd/shared_ptr.h>
#include <opentelemetry/trace/scope.h>
#include <opentelemetry/trace/tracer.h>

#include "common/status.h"
#include "observe/timed-span.h"

namespace impala {

// Forward declaration to break cyclical imports.
class ClientRequestState;

// Enum defining the child span types.
enum class ChildSpanType {
  NONE = 0,
  INIT = 1,
  SUBMITTED = 2,
  PLANNING = 3,
  ADMISSION_CONTROL = 4,
  QUERY_EXEC = 5,
  CLOSE = 6
};

// Manages the root and child spans for a single query. Provides convenience methods to
// start each child span with the appropriate name/attributes and to end each child span.
// Only one child span can be active at a time.
//
// The root span is started when the SpanManager is constructed and ended when the
// SpanManager is destructed. The root span is made the active span for the duration of
// the SpanManager's lifetime.
class SpanManager {
public:
  SpanManager(
      opentelemetry::nostd::shared_ptr<opentelemetry::trace::Tracer> tracer,
      const ClientRequestState* client_request_state);
  ~SpanManager();

  // Adds an event to the currently active child span. If no child span is active,
  // logs a warning and does nothing else. The event time is set to the current time.
  // Parameters:
  //   name -- The name of the event to add.
  void AddChildSpanEvent(const opentelemetry::nostd::string_view& name);

  // Functions to start child spans. If another child span is active, it will be ended,
  // a warning will be logged, and a DCHECK failed.
  void StartChildSpanInit();
  void StartChildSpanSubmitted();
  void StartChildSpanPlanning();
  void StartChildSpanAdmissionControl();
  void StartChildSpanQueryExecution();

  // Starts the Close child span. If another child span is active, closes that span first
  // without logging and warnings or failing any DCHECKs. This behavior is different from
  // other start child span functions since the Close span will be started in error
  // scenarios where another child span may not have been closed due to exiting the code
  // happy path early.
  //
  // Parameters:
  //   cause - Pointer to the Status that caused the Close span to be started. If null,
  //           the query status from the client_request_state_ will be used. This
  //           parameter exists for error scenarios where the query status in the
  //           client_request_state_ will not yet be updated with the error.
  void StartChildSpanClose(const Status* cause = nullptr);

  // Functions to end child spans. If no child span is active, logs a warning and does
  // nothing else.
  void EndChildSpanInit();
  void EndChildSpanSubmitted();
  void EndChildSpanPlanning();
  void EndChildSpanAdmissionControl();
  void EndChildSpanQueryExecution();
  void EndChildSpanClose();

private:
  // Tracer instance used to construct spans.
  opentelemetry::nostd::shared_ptr<opentelemetry::trace::Tracer> tracer_;

  // Scope to make the root span active and to deactivate the root span when it finishes.
  std::unique_ptr<opentelemetry::trace::Scope> scope_;

  // ClientRequestState for the query this SpanManager is tracking.
  const ClientRequestState* client_request_state_;

  // Convenience constant string the string representation of the Query ID for the query
  // this SpanManager is tracking.
  const std::string query_id_;

  // TimedSpan instances for the root span. Only modified in the ctor, dtor, and
  // EndChildSpanClose functions.
  std::shared_ptr<TimedSpan> root_;

  // TimedSpan instance for the current child span and the mutex to protect it.
  std::unique_ptr<TimedSpan> current_child_;
  std::mutex child_span_mu_;

  // Span type of the current childspan. Will be ChildSpanType::NONE if no child span is
  // active.
  ChildSpanType child_span_type_;

  // Helper method that builds a child span and populates it with common attributes plus
  // the specified additional attributes. Does not take ownership of the child span mutex.
  // Callers must already hold the child_span_mu_ lock.
  void ChildSpanBuilder(const ChildSpanType& span_type,
      OtelAttributesMap&& additional_attributes = {}, bool running = false);

  // Internal helper functions to perform the actual work of ending child spans.
  // Callers must already hold the child_span_mu_ lock.
  //
  // Parameters:
  //   cause - See comments on StartChildSpanClose().
  void DoEndChildSpanInit(const Status* cause = nullptr);
  void DoEndChildSpanSubmitted(const Status* cause = nullptr);
  void DoEndChildSpanPlanning(const Status* cause = nullptr);
  void DoEndChildSpanAdmissionControl(const Status* cause = nullptr);
  void DoEndChildSpanQueryExecution(const Status* cause = nullptr);

  // Properly closes the active child span by calling the appropriate End method for the
  // active child span type. If no child span is active, does nothing.
  // Callers must already hold the child_span_mu_ lock.
  //
  // Parameters:
  //   cause - See comments on StartChildSpanClose().
  void EndActiveChildSpan(const Status* cause = nullptr);

  // Helper method to end a child span and populate its common attributes.
  // Callers must already hold the child_span_mu_ lock.
  //
  // Parameters:
  //   cause - See comments on StartChildSpanClose().
  //   additional_attributes - Span specific attributes that will be set on the span
  //                           before ending it.
  void EndChildSpan(const Status* cause = nullptr,
      const OtelAttributesMap& additional_attributes = {});

  // Returns true if the Close child span is active.
  // Callers must already hold the child_span_mu_ lock.
  inline bool IsClosing() {
      return UNLIKELY(current_child_ &&  child_span_type_ == ChildSpanType::CLOSE);
  }
};  // class SpanManager

} // namespace impala
