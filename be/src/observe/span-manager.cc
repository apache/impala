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

#include "observe/span-manager.h"

#include <chrono>
#include <memory>
#include <mutex>
#include <string>
#include <utility>

#include <gflags/gflags.h>
#include <glog/logging.h>
#include "gutil/strings/substitute.h"
#include <opentelemetry/nostd/shared_ptr.h>
#include <opentelemetry/trace/span_metadata.h>
#include <opentelemetry/trace/scope.h>
#include <opentelemetry/trace/tracer.h>

#include "common/compiler-util.h"
#include "gen-cpp/Types_types.h"
#include "observe/timed-span.h"
#include "scheduling/admission-control-client.h"
#include "service/client-request-state.h"
#include "util/debug-util.h"

using namespace opentelemetry;
using namespace std;

DECLARE_string(cluster_id);
DECLARE_int32(otel_trace_retry_policy_max_attempts);
DECLARE_int32(otel_trace_retry_policy_max_backoff_s);

// Names of attributes only on Root spans.
static constexpr char const* ATTR_ERROR_MESSAGE = "ErrorMessage";
static constexpr char const* ATTR_QUERY_START_TIME = "QueryStartTime";
static constexpr char const* ATTR_RETRIED_QUERY_ID = "RetriedQueryId";
static constexpr char const* ATTR_RUNTIME = "Runtime";
static constexpr char const* ATTR_STATE = "State";

// Names of attributes on both Root and one or more child spans.
static constexpr char const* ATTR_CLUSTER_ID = "ClusterId";
static constexpr char const* ATTR_ORIGINAL_QUERY_ID = "OriginalQueryId";
static constexpr char const* ATTR_QUERY_ID = "QueryId";
static constexpr char const* ATTR_QUERY_TYPE = "QueryType";
static constexpr char const* ATTR_REQUEST_POOL = "RequestPool";
static constexpr char const* ATTR_SESSION_ID = "SessionId";
static constexpr char const* ATTR_USER_NAME = "UserName";

// Names of attributes common to all child spans.
static constexpr char const* ATTR_BEGIN_TIME = "BeginTime";
static constexpr char const* ATTR_ELAPSED_TIME = "ElapsedTime";
static constexpr char const* ATTR_ERROR_MSG = "ErrorMsg";
static constexpr char const* ATTR_NAME = "Name";
static constexpr char const* ATTR_RUNNING = "Running";
static constexpr char const* ATTR_STATUS = "Status";

// Names of attributes only on Init child spans.
static constexpr char const* ATTR_DEFAULT_DB = "DefaultDb";
static constexpr char const* ATTR_QUERY_STRING = "QueryString";

// Names of attributes only on Admission Control child spans.
static constexpr char const* ATTR_ADM_RESULT = "AdmissionResult";
static constexpr char const* ATTR_QUEUED = "Queued";

// Names of attributes only on Query Execution child spans.
static constexpr char const* ATTR_NUM_DELETED_ROWS = "NumDeletedRows";
static constexpr char const* ATTR_NUM_MODIFIED_ROWS = "NumModifiedRows";
static constexpr char const* ATTR_NUM_ROWS_FETCHED = "NumRowsFetched";

// Names of the child spans.
static constexpr char const* CHILD_SPAN_NAMES[] = {
    "None", "Init", "Submitted", "Planning", "AdmissionControl", "QueryExecution",
    "Close"};

#define DCHECK_CHILD_SPAN_TYPE(expected_type) \
  DCHECK(child_span_type_ == expected_type) << "Expected child span type '" \
      << expected_type << "' but received '" << child_span_type_ << "' instead."
// macro DCHECK_CHILD_SPAN_TYPE

namespace impala {

// Helper function to convert ChildSpanType enum values to strings.
static inline string to_string(const ChildSpanType& val) {
  return CHILD_SPAN_NAMES[static_cast<int>(val)];
} // function to_string

// Helper functions to stream the string representation of ChildSpanType enum values.
static inline ostream& operator<<(ostream& out, const ChildSpanType& val) {
  out << to_string(val);
  return out;
} // operator<<

// Helper function to log the start and end of a span with debug information. Callers
// must hold the child_span_mu_ lock when calling this function.
static inline void debug_log_span(const TimedSpan* span, const string& span_name,
    const string& query_id, bool started) {
  DCHECK(span != nullptr) << "Cannot log null span.";

  if (LIKELY(span != nullptr)) {
    VLOG(2) << strings::Substitute("$0 '$1' span trace_id=\"$2\" span_id=\"$3\" "
        "query_id=\"$4\"", (started ? "Started" : "Ended"), span_name, span->TraceId(),
        span->SpanId(), query_id);
  } else {
    LOG(WARNING) << "Attempted to log span '" << span_name << "' but provided span is "
        "null.";
  }
} // function debug_log_span

SpanManager::SpanManager(nostd::shared_ptr<trace::Tracer> tracer,
    ClientRequestState* client_request_state) : tracer_(std::move(tracer)),
    client_request_state_(client_request_state),
    query_id_(PrintId(client_request_state_->query_id())) {
  child_span_type_ = ChildSpanType::NONE;

  DCHECK(client_request_state_ != nullptr) << "Cannot start root span without a valid "
      "client request state.";

  {
    lock_guard<mutex> crs_lock(*(client_request_state_->lock()));

    root_ = make_shared<TimedSpan>(tracer_, query_id_, ATTR_QUERY_START_TIME,
        ATTR_RUNTIME,
        OtelAttributesMap{
          {ATTR_CLUSTER_ID, FLAGS_cluster_id},
          {ATTR_QUERY_ID, query_id_},
          {ATTR_REQUEST_POOL, client_request_state_->request_pool()},
          {ATTR_SESSION_ID, PrintId(client_request_state_->session_id())},
          {ATTR_USER_NAME, client_request_state_->effective_user()}
        },
        trace::SpanKind::kServer);
  }

  scope_ = make_unique<trace::Scope>(root_->SetActive());
  debug_log_span(root_.get(), "Root", query_id_, true);
} // ctor

SpanManager::~SpanManager() {
  lock_guard<mutex> l(child_span_mu_);

  root_->End();
  debug_log_span(root_.get(), "Root", query_id_, false);
  LOG(INFO) << strings::Substitute("Closed OpenTelemetry trace with trace_id=\"$0\" "
      "span_id=\"$1\"", root_->TraceId(), root_->SpanId());

  scope_.reset();
  root_.reset();

  tracer_->Close(chrono::seconds(FLAGS_otel_trace_retry_policy_max_backoff_s *
    FLAGS_otel_trace_retry_policy_max_attempts));
} // dtor

void SpanManager::AddChildSpanEvent(const nostd::string_view& name) {
  lock_guard<mutex> l(child_span_mu_);

  if (LIKELY(current_child_)) {
    current_child_->AddEvent(name);
    VLOG(2) << strings::Substitute("Adding event named '$0' to child span '$1' "
        "trace_id=\"$2\" span_id=\"$3", name.data(), to_string(child_span_type_),
        current_child_->TraceId(), current_child_->SpanId());
  } else {
    LOG(WARNING) << strings::Substitute("Attempted to add event '$0' with no active "
        "child span trace_id=\"$1\" span_id=\"$2\"\n$3", name.data(), root_->TraceId(),
        root_->SpanId(), GetStackTrace());
    DCHECK(current_child_) << "Cannot add event when child span is not active.";
  }
} // function AddChildSpanEvent

void SpanManager::StartChildSpanInit() {
  lock_guard<mutex> l(child_span_mu_);
  ChildSpanBuilder(ChildSpanType::INIT,
      {
        {ATTR_CLUSTER_ID, FLAGS_cluster_id},
        {ATTR_QUERY_ID, query_id_}
      });

  {
    lock_guard<mutex> crs_lock(*(client_request_state_->lock()));

    current_child_->SetAttribute(ATTR_DEFAULT_DB,
        client_request_state_->default_db());
    current_child_->SetAttribute(ATTR_QUERY_STRING,
        client_request_state_->redacted_sql());
    current_child_->SetAttribute(ATTR_REQUEST_POOL,
        client_request_state_->request_pool());
    current_child_->SetAttribute(ATTR_SESSION_ID,
        PrintId(client_request_state_->session_id()));
    current_child_->SetAttribute(ATTR_USER_NAME,
        client_request_state_->effective_user());
  }
} // function StartChildSpanInit

void SpanManager::EndChildSpanInit() {
  lock_guard<mutex> l(child_span_mu_);
  lock_guard<mutex> crs_lock(*(client_request_state_->lock()));
  DoEndChildSpanInit();
} // function EndChildSpanInit

inline void SpanManager::DoEndChildSpanInit(const Status* cause) {
  DCHECK_CHILD_SPAN_TYPE(ChildSpanType::INIT);

  EndChildSpan(
      cause,
      OtelAttributesMap{
        {ATTR_ORIGINAL_QUERY_ID, (client_request_state_->IsRetriedQuery() ?
            PrintId(client_request_state_->original_id()) : "")}
      });
} // function DoEndChildSpanInit

void SpanManager::StartChildSpanSubmitted() {
  lock_guard<mutex> l(child_span_mu_);
  ChildSpanBuilder(ChildSpanType::SUBMITTED);
} // function StartChildSpanSubmitted

void SpanManager::EndChildSpanSubmitted() {
  lock_guard<mutex> l(child_span_mu_);
  lock_guard<mutex> crs_lock(*(client_request_state_->lock()));
  DoEndChildSpanSubmitted();
} // function EndChildSpanSubmitted

inline void SpanManager::DoEndChildSpanSubmitted(const Status* cause) {
  DCHECK_CHILD_SPAN_TYPE(ChildSpanType::SUBMITTED);
  EndChildSpan(cause);
} // function DoEndChildSpanSubmitted

void SpanManager::StartChildSpanPlanning() {
  lock_guard<mutex> l(child_span_mu_);
  ChildSpanBuilder(ChildSpanType::PLANNING);
} // function StartChildSpanPlanning

void SpanManager::EndChildSpanPlanning() {
  lock_guard<mutex> l(child_span_mu_);
  DoEndChildSpanPlanning();
} // function EndChildSpanPlanning

inline void SpanManager::DoEndChildSpanPlanning(const Status* cause) {
  DCHECK_CHILD_SPAN_TYPE(ChildSpanType::PLANNING);
  EndChildSpan(
      cause,
      OtelAttributesMap{
        {ATTR_QUERY_TYPE, to_string(client_request_state_->exec_request().stmt_type)}
      });
} // function DoEndChildSpanPlanning

void SpanManager::StartChildSpanAdmissionControl() {
  lock_guard<mutex> l(child_span_mu_);
  ChildSpanBuilder(ChildSpanType::ADMISSION_CONTROL);
} // function StartChildSpanAdmissionControl

void SpanManager::EndChildSpanAdmissionControl() {
  lock_guard<mutex> l(child_span_mu_);
  lock_guard<mutex> crs_lock(*(client_request_state_->lock()));
  DoEndChildSpanAdmissionControl();
} // function EndChildSpanAdmissionControl

inline void SpanManager::DoEndChildSpanAdmissionControl(const Status* cause) {
  if (IsClosing()) {
    // If we are already closing, silently return as some cases (such as FIRST_FETCH)
    // will end the admission control phase even though the query already finished.
   return; // <-- EARLY RETURN
  }

  DCHECK_CHILD_SPAN_TYPE(ChildSpanType::ADMISSION_CONTROL);

  bool was_queued = false;
  const string* adm_result = nullptr;

  if (LIKELY(client_request_state_->summary_profile() != nullptr)) {
      adm_result =
          client_request_state_->summary_profile()->GetInfoString("Admission result");
  }

  if (LIKELY(client_request_state_->admission_control_client() != nullptr)) {
    was_queued = client_request_state_->admission_control_client()->WasQueued();
  }

  EndChildSpan(
      cause,
      OtelAttributesMap{
        {ATTR_QUEUED, was_queued},
        {ATTR_ADM_RESULT, (adm_result == nullptr ? "" : *adm_result)},
        {ATTR_REQUEST_POOL, client_request_state_->request_pool()}
      });
} // function DoEndChildSpanAdmissionControl

void SpanManager::StartChildSpanQueryExecution() {
  lock_guard<mutex> l(child_span_mu_);

  if (IsClosing()) {
    // If we are already closing, silently return as some cases (such as FIRST_FETCH)
    // will start the query execution phase even though the query already failed.
   return; // <-- EARLY RETURN
  }

  ChildSpanBuilder(ChildSpanType::QUERY_EXEC, true);
} // function StartChildSpanQueryExecution

void SpanManager::EndChildSpanQueryExecution() {
  lock_guard<mutex> l(child_span_mu_);
  lock_guard<mutex> crs_lock(*(client_request_state_->lock()));
  DoEndChildSpanQueryExecution();
}  // function EndChildSpanQueryExecution

inline void SpanManager::DoEndChildSpanQueryExecution(const Status* cause) {
  if (IsClosing()) {
    // If we are already closing, silently return as some cases (such as FIRST_FETCH)
    // will end the query execution phase even though the query already failed.
   return; // <-- EARLY RETURN
  }

  DCHECK_CHILD_SPAN_TYPE(ChildSpanType::QUERY_EXEC);
  OtelAttributesMap attrs;

  if (client_request_state_->exec_request().stmt_type == TStmtType::QUERY) {
    attrs.emplace(ATTR_NUM_DELETED_ROWS, static_cast<int64_t>(0));
    attrs.emplace(ATTR_NUM_MODIFIED_ROWS, static_cast<int64_t>(0));
  } else {
    attrs.emplace(ATTR_NUM_DELETED_ROWS, static_cast<int64_t>(-1));
    attrs.emplace(ATTR_NUM_MODIFIED_ROWS, static_cast<int64_t>(-1));
  }

  attrs.emplace(ATTR_NUM_ROWS_FETCHED, client_request_state_->num_rows_fetched());

  EndChildSpan(cause, attrs);
} // function DoEndChildSpanQueryExecution

void SpanManager::StartChildSpanClose(const Status* cause) {
  lock_guard<mutex> l(child_span_mu_);

  // In an error scenario, another child span may still be active since the normal code
  // path was interrupted and thus the correct end child span function was not called.
  // In this case, we must first end the current child span.
  if (UNLIKELY(current_child_)) {
    DCHECK(cause != nullptr) << "Child span '" << child_span_type_ << "'is active when "
        "starting the Close span, a non-null cause must be provided to indicate why the "
        "query processing flow is being interrupted.";
    DCHECK(!cause->ok()) << "Child span '" << child_span_type_ << "'is active when "
        "starting the Close span, a non-OK cause must be provided to indicate why the "
        "query processing flow is being interrupted.";

    {
      lock_guard<mutex> crs_lock(*(client_request_state_->lock()));
      EndActiveChildSpan(cause);
    }
  }

  ChildSpanBuilder(ChildSpanType::CLOSE);
} // function StartChildSpanClose

void SpanManager::EndChildSpanClose() {
  DCHECK_CHILD_SPAN_TYPE(ChildSpanType::CLOSE);

  lock_guard<mutex> l(child_span_mu_);
  lock_guard<mutex> crs_lock(*(client_request_state_->lock()));
  EndChildSpan();

  // Set all root span attributes to avoid dereferencing the client_request_state_ in the
  // dtor (as the dtor is invoked when client_request_state_ is destroyed).
  root_->SetAttribute(ATTR_QUERY_TYPE,
    to_string(client_request_state_->exec_request().stmt_type));

  if (client_request_state_->query_status().ok()) {
    root_->SetAttributeEmpty(ATTR_ERROR_MESSAGE);
  } else {
    string error_msg = client_request_state_->query_status().msg().msg();

    for (const auto& detail : client_request_state_->query_status().msg().details()) {
      error_msg += "\n" + detail;
    }

    root_->SetAttribute(ATTR_ERROR_MESSAGE, error_msg);
  }

  if (UNLIKELY(client_request_state_->WasRetried())) {
    root_->SetAttribute(ATTR_STATE, ClientRequestState::RetryStateToString(
        client_request_state_->retry_state()));
    root_->SetAttribute(ATTR_RETRIED_QUERY_ID,
        PrintId(client_request_state_->retried_id()));
  } else {
    root_->SetAttribute(ATTR_STATE,
      ClientRequestState::ExecStateToString(client_request_state_->exec_state()));
      root_->SetAttributeEmpty(ATTR_RETRIED_QUERY_ID);
  }

  if (UNLIKELY(client_request_state_->IsRetriedQuery())) {
    root_->SetAttribute(ATTR_ORIGINAL_QUERY_ID,
        PrintId(client_request_state_->original_id()));
  } else {
    root_->SetAttributeEmpty(ATTR_ORIGINAL_QUERY_ID);
  }
} // function EndChildSpanClose

inline void SpanManager::ChildSpanBuilder(const ChildSpanType& span_type,
    OtelAttributesMap&& additional_attributes, bool running) {
  DCHECK(client_request_state_ != nullptr) << "Cannot start child span without a valid "
      "client request state.";
  DCHECK(span_type != ChildSpanType::NONE) << "Span type cannot be " << span_type << ".";

  if (UNLIKELY(current_child_)) {
    LOG(WARNING) << strings::Substitute("Attempted to start child span '$0' while "
        "another child span '$1' is still active trace_id=\"$2\" span_id=\"$3\"\n$4",
        to_string(span_type), to_string(child_span_type_), root_->TraceId(),
        root_->SpanId(), GetStackTrace());
    DCHECK(false) << "Should not start a new child span while one is already active.";

    {
      lock_guard<mutex> crs_lock(*(client_request_state_->lock()));
      EndActiveChildSpan();
    }
  }

  const string full_span_name = query_id_ + " - " + to_string(span_type);

  additional_attributes.insert_or_assign(ATTR_NAME, full_span_name);
  additional_attributes.insert_or_assign(ATTR_RUNNING, running);

  current_child_ = make_unique<TimedSpan>(tracer_, full_span_name, ATTR_BEGIN_TIME,
    ATTR_ELAPSED_TIME, std::move(additional_attributes), trace::SpanKind::kInternal,
    root_);
  child_span_type_ = span_type;

  debug_log_span(current_child_.get(), to_string(span_type), query_id_, true);
} // function ChildSpanBuilder

inline void SpanManager::ChildSpanBuilder(const ChildSpanType& span_type, bool running) {
  ChildSpanBuilder(span_type, {}, running);
} // function ChildSpanBuilder

inline void SpanManager::EndChildSpan(const Status* cause,
    const OtelAttributesMap& additional_attributes) {
  DCHECK(client_request_state_ != nullptr) << "Cannot end child span without a valid "
      "client request state.";

  if (LIKELY(current_child_)) {
    for (const auto& a : additional_attributes) {
      current_child_->SetAttribute(a.first, a.second);
    }

    const Status* query_status;
    if (cause != nullptr) {
      query_status = cause;
    } else {
      query_status = &client_request_state_->query_status();
    }

    if (query_status->ok()) {
      current_child_->SetAttributeEmpty(ATTR_ERROR_MSG);
      current_child_->SetAttribute(ATTR_STATUS,
        ClientRequestState::ExecStateToString(client_request_state_->exec_state()));
    } else {
      string error_msg = query_status->msg().msg();

      for (const auto& detail : query_status->msg().details()) {
        error_msg += "\n" + detail;
      }

      current_child_->SetAttribute(ATTR_ERROR_MSG, error_msg);
      current_child_->SetAttribute(ATTR_STATUS,
            ClientRequestState::ExecStateToString(ClientRequestState::ExecState::ERROR));
    }

    current_child_->End();

    debug_log_span(current_child_.get(), to_string(child_span_type_), query_id_, false);

    current_child_.reset();
    child_span_type_ = ChildSpanType::NONE;
  } else {
    LOG(WARNING) << strings::Substitute("Attempted to end a non-active child span "
        "trace_id=\"$0\" span_id=\"$1\"\n$2", root_->TraceId(), root_->SpanId(),
        GetStackTrace());
    DCHECK(current_child_) << "Cannot end child span when one is not active.";
  }
} // function EndChildSpan

inline void SpanManager::EndActiveChildSpan(const Status* cause) {
  switch (child_span_type_) {
    case ChildSpanType::INIT:
      DoEndChildSpanInit(cause);
      break;
    case ChildSpanType::SUBMITTED:
      DoEndChildSpanSubmitted(cause);
      break;
    case ChildSpanType::PLANNING:
      DoEndChildSpanPlanning(cause);
      break;
    case ChildSpanType::ADMISSION_CONTROL:
      DoEndChildSpanAdmissionControl(cause);
      break;
    case ChildSpanType::QUERY_EXEC:
      DoEndChildSpanQueryExecution(cause);
      break;
    case ChildSpanType::CLOSE:
      // If we are already in a Close child span, we cannot start a new one.
      LOG(WARNING) << "Attempted to start Close child span while another Close child "
          "span is already active trace_id=\"$0\" span_id=\"$1\"\n$2", root_->TraceId(),
          current_child_->SpanId(), GetStackTrace();
      DCHECK(false) << "Cannot start a new Close child span while a Close child span is "
          "already active.";
      break;
    default:
      // No-op, no active child span to end.
      break;
  }
} // function EndActiveChildSpan

} // namespace impala