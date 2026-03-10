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

#include "observe/otel-trace-manager.h"

#include <chrono>
#include <memory>
#include <mutex>
#include <string>
#include <utility>

#include <gflags/gflags.h>
#include <glog/logging.h>
#include <gutil/strings/substitute.h>
#include <opentelemetry/nostd/shared_ptr.h>
#include <opentelemetry/trace/scope.h>
#include <opentelemetry/trace/tracer.h>

#include "common/compiler-util.h"
#include "common/status.h"
#include "gen-cpp/Types_types.h"
#include "observe/buffered-span.h"
#include "runtime/coordinator.h"
#include "runtime/exec-env.h"
#include "scheduling/admission-control-client.h"
#include "service/client-request-state.h"
#include "util/debug-util.h"
#include "util/network-util.h"

using namespace opentelemetry;
using namespace std;

DECLARE_string(cluster_id);
DECLARE_int32(otel_trace_retry_policy_max_attempts);
DECLARE_int32(otel_trace_retry_policy_max_backoff_s);

#ifndef NDEBUG
DECLARE_bool(otel_trace_exhaustive_dchecks);
#endif

// Names of attributes only on Root spans.
static constexpr char const* ATTR_ERROR_MESSAGE = "ErrorMessage";
static constexpr char const* ATTR_QUERY_START_TIME = "QueryStartTime";
static constexpr char const* ATTR_RETRIED_QUERY_ID = "RetriedQueryId";
static constexpr char const* ATTR_RUNTIME = "Runtime";
static constexpr char const* ATTR_STATE = "State";

// Names of attributes on both Root and one or more child spans.
static constexpr char const* ATTR_CLUSTER_ID = "ClusterId";
static constexpr char const* ATTR_COORDINATOR = "Coordinator";
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

// Checks the expected child span type is active and that no other child span is active.
#ifndef NDEBUG
#define DCHECK_CHILD_SPAN_TYPE(expected_type) \
  DCHECK(child_spans_.at(expected_type).span) << "Expected child span type '" \
      << to_string(expected_type) << "' was not active."; \
  for (const auto& a : child_spans_) { \
    if (a.first != expected_type) { \
      DCHECK(!a.second.span || a.second.span->HasEnded()) << "Expected child span " \
          "type '" << to_string(a.first) << "' to not be active."; \
    } \
  }
// macro DCHECK_CHILD_SPAN_TYPE
#endif

namespace impala {

// Helper function to log the start and end of a span with debug information. Callers
// must hold the child_span_mu_ lock when calling this function.
static void debug_log_span(const BufferedSpan* span, const string& span_name,
    const string& query_id, bool started) {
  DCHECK(span != nullptr) << "Cannot log null span.";

  if (LIKELY(span != nullptr)) {
    VLOG(2) << strings::Substitute("$0 '$1' span trace_id=\"$2\" span_id=\"$3\" "
        "query_id=\"$4\"", (started ? "Started" : "Submitted"), span_name,
        span->TraceId(), span->SpanId(), query_id);
  } else {
    LOG(WARNING) << "Attempted to log span '" << span_name << "' but provided span is "
        "null.";
  }
} // function debug_log_span

OtelTraceManager::OtelTraceManager(nostd::shared_ptr<trace::Tracer> tracer,
    ClientRequestState* client_request_state) : tracer_(std::move(tracer)),
    client_request_state_(client_request_state),
    query_id_(PrintId(client_request_state_->query_id())) {
  VLOG(2) << "Creating OtelTraceManager for query_id='" << query_id_ << "'";
  DCHECK(client_request_state_ != nullptr) << "Cannot start root span without a valid "
      "client request state.";

  // Initialize placeholders for the child spans.
  child_spans_.emplace(ChildSpanType::INIT,
      ChildSpanEntry(&OtelTraceManager::DoEndChildSpanInit));
  child_spans_.emplace(ChildSpanType::SUBMITTED,
      ChildSpanEntry(&OtelTraceManager::DoEndChildSpanSubmitted));
  child_spans_.emplace(ChildSpanType::PLANNING,
      ChildSpanEntry(&OtelTraceManager::DoEndChildSpanPlanning));
  child_spans_.emplace(ChildSpanType::ADMISSION_CONTROL,
      ChildSpanEntry(&OtelTraceManager::DoEndChildSpanAdmissionControl));
  child_spans_.emplace(ChildSpanType::QUERY_EXEC,
      ChildSpanEntry(&OtelTraceManager::DoEndChildSpanQueryExecution));
  child_spans_.emplace(ChildSpanType::CLOSE,
      ChildSpanEntry(&OtelTraceManager::DoEndChildSpanClose));
  {
    lock_guard<mutex> crs_lock(*(client_request_state_->lock()));

    span_root_ = make_unique<BufferedSpan>(tracer_, query_id_, ATTR_QUERY_START_TIME,
        ATTR_RUNTIME,
        BufferedSpan::BufferedAttributesMap{
          {ATTR_CLUSTER_ID, FLAGS_cluster_id},
          {ATTR_QUERY_ID, query_id_},
          {ATTR_REQUEST_POOL, client_request_state_->request_pool()},
          {ATTR_SESSION_ID, PrintId(client_request_state_->session_id())},
          {ATTR_USER_NAME, client_request_state_->effective_user()},
          {ATTR_COORDINATOR, TNetworkAddressToString(
              ExecEnv::GetInstance()->configured_backend_address())}},
        context::Context().SetValue(trace::kIsRootSpanKey, true),
        trace::SpanKind::kServer);
  }
} // ctor

OtelTraceManager::~OtelTraceManager() {
  lock_guard<mutex> l(child_span_mu_);

  VLOG(2) << strings::Substitute("Destroying OtelTraceManager for query_id=\"$0\", " \
      "do_trace_=$1", query_id_, do_trace_);

  if (do_trace_) {
    // Ensure all child spans have been submitted before submitting the root span.
    for (auto& a : child_spans_) {
      a.second.span.reset();
    }

    span_root_->Submit();
    debug_log_span(span_root_.get(), "Root", query_id_, false);
    LOG(INFO) << strings::Substitute("Closed OpenTelemetry trace with trace_id=\"$0\" "
        "span_id=\"$1\" query_id=\"$2\"", span_root_->TraceId(), span_root_->SpanId(),
        query_id_);

    scope_.reset();
    span_root_.reset();
  } else {
    VLOG(2) << strings::Substitute("Not submitting OpenTelemetry trace for "
        "query_id='$0' because the trace was cancelled.", query_id_);
  }

  tracer_->Close(chrono::seconds(FLAGS_otel_trace_retry_policy_max_backoff_s *
    FLAGS_otel_trace_retry_policy_max_attempts));
} // dtor

void OtelTraceManager::TraceQuery(bool do_trace) {
  lock_guard<mutex> l(child_span_mu_);
  DoTraceQuery(do_trace);
} // function TraceQuery

void OtelTraceManager::DoTraceQuery(bool do_trace) {
  VLOG(2) << strings::Substitute("DoTraceQuery: do_trace=$0, started_=$1", \
      do_trace, started_);

  // In some cases, query analysis will happen more than once during planning.
  if (started_) {
    DCHECK(do_trace == do_trace_) << "TraceQuery was called more than once with "
        "different do_trace values.";
    return;
  }

  do_trace_ = do_trace;

  if (do_trace_) {
    DCHECK(child_spans_.at(ChildSpanType::INIT).span) << "Expected "
        << to_string(ChildSpanType::INIT) << " child span to be active if query is to be "
        "traced.";

    span_root_->Start();
    debug_log_span(span_root_.get(), "Root", query_id_, true);
    scope_ = make_unique<trace::Scope>(span_root_->SetActive());

    BufferedSpan* span = child_spans_.at(ChildSpanType::INIT).span.get();
    span->SetParent(span_root_->GetContext());
    span->Submit();
    debug_log_span(span, to_string(ChildSpanType::INIT), query_id_, false);

    // Retried queries do not have the SUBMITTED and PLANNING child spans since they
    // re-use the plan from the original query.
    span = child_spans_.at(ChildSpanType::SUBMITTED).span.get();
    if (span != nullptr) {
      span->SetParent(span_root_->GetContext());
      span->Submit();
      debug_log_span(span, to_string(ChildSpanType::SUBMITTED), query_id_, false);
    }

    span = child_spans_.at(ChildSpanType::PLANNING).span.get();
    if (span != nullptr) {
      span->SetParent(span_root_->GetContext());
      span->Start();
      debug_log_span(span, to_string(ChildSpanType::PLANNING), query_id_, true);
    }
    started_ = true;
  } else {
    for (auto& a : child_spans_) {
      if (a.second.span) {
        a.second.span->Cancel();
        a.second.span.reset();
      }
    }
    span_root_->Cancel();
  }
} // function DoTraceQuery

void OtelTraceManager::AddChildSpanEvent(const nostd::string_view& name) {
  lock_guard<mutex> l(child_span_mu_);

  // Locate the current active child span, if any, where the event will be added.
  for (const auto& a : child_spans_) {
    const auto& span = a.second.span;
    if (span && !span->HasEnded()) {
      span->AddEvent(name);
      VLOG(2) << strings::Substitute("Adding event named '$0' to child span '$1' "
        "trace_id=\"$2\" span_id=\"$3\"", name.data(), to_string(a.first),
        span->TraceId(), span->SpanId());
      return;
    }
  }

  LOG(WARNING) << strings::Substitute("Attempted to add event '$0' with no active "
      "child span trace_id=\"$1\" span_id=\"$2\"\n$3", name.data(),
      span_root_->TraceId(), span_root_->SpanId(), GetStackTrace());
#ifndef NDEBUG
  if (FLAGS_otel_trace_exhaustive_dchecks) {
    DCHECK(false) << strings::Substitute("Cannot add event '$0' when child "
        "span is not active.", name.data());
  }
#endif
} // function AddChildSpanEvent

void OtelTraceManager::StartChildSpanInit() {
  lock_guard<mutex> l(child_span_mu_);

  BufferedSpan* span = ChildSpanBuilder(ChildSpanType::INIT,
      BufferedSpan::BufferedAttributesMap{
        {ATTR_CLUSTER_ID, FLAGS_cluster_id},
        {ATTR_QUERY_ID, query_id_},
        {ATTR_COORDINATOR, TNetworkAddressToString(
          ExecEnv::GetInstance()->configured_backend_address())}});

  // ChildSpanBuilder locks client_request_state_->lock() if another span is active,
  // thus, to set the Init specific attributes, we need to acquire the lock here instead
  // of acquiring it at the beginning of this function.
  {
    lock_guard<mutex> crs_lock(*(client_request_state_->lock()));

    span->SetAttribute(ATTR_DEFAULT_DB,
        client_request_state_->default_db());
    span->SetAttribute(ATTR_QUERY_STRING,
        client_request_state_->redacted_sql());
    span->SetAttribute(ATTR_REQUEST_POOL,
        client_request_state_->request_pool());
    span->SetAttribute(ATTR_SESSION_ID,
        PrintId(client_request_state_->session_id()));
    span->SetAttribute(ATTR_USER_NAME,
        client_request_state_->effective_user());
  }
} // function StartChildSpanInit

void OtelTraceManager::EndChildSpanInit() {
  lock_guard<mutex> l(child_span_mu_);
  lock_guard<mutex> crs_lock(*(client_request_state_->lock()));
  DoEndChildSpanInit();
} // function EndChildSpanInit

void OtelTraceManager::DoEndChildSpanInit(const Status* cause) {
#ifndef NDEBUG
  if (FLAGS_otel_trace_exhaustive_dchecks) {
    DCHECK_CHILD_SPAN_TYPE(ChildSpanType::INIT);
  }
#endif

  EndChildSpan(ChildSpanType::INIT, cause,
      BufferedSpan::BufferedAttributesMap{
        {ATTR_ORIGINAL_QUERY_ID, (client_request_state_->IsRetriedQuery() ?
            PrintId(client_request_state_->original_id()) : "")}
      });
} // function DoEndChildSpanInit

void OtelTraceManager::StartChildSpanSubmitted() {
  lock_guard<mutex> l(child_span_mu_);
  ChildSpanBuilder(ChildSpanType::SUBMITTED);
} // function StartChildSpanSubmitted

void OtelTraceManager::EndChildSpanSubmitted() {
  lock_guard<mutex> l(child_span_mu_);
  lock_guard<mutex> crs_lock(*(client_request_state_->lock()));
  DoEndChildSpanSubmitted();
} // function EndChildSpanSubmitted

void OtelTraceManager::DoEndChildSpanSubmitted(const Status* cause) {
#ifndef NDEBUG
  if (FLAGS_otel_trace_exhaustive_dchecks) {
    DCHECK_CHILD_SPAN_TYPE(ChildSpanType::SUBMITTED);
  }
#endif
  EndChildSpan(ChildSpanType::SUBMITTED, cause);
} // function DoEndChildSpanSubmitted

void OtelTraceManager::StartChildSpanPlanning() {
  lock_guard<mutex> l(child_span_mu_);
  ChildSpanBuilder(ChildSpanType::PLANNING);
} // function StartChildSpanPlanning

void OtelTraceManager::EndChildSpanPlanning() {
  lock_guard<mutex> l(child_span_mu_);
  lock_guard<mutex> crs_lock(*(client_request_state_->lock()));
  DoEndChildSpanPlanning();
} // function EndChildSpanPlanning

void OtelTraceManager::DoEndChildSpanPlanning(const Status* cause) {
#ifndef NDEBUG
  if (FLAGS_otel_trace_exhaustive_dchecks) {
    DCHECK_CHILD_SPAN_TYPE(ChildSpanType::PLANNING);
  }
#endif

  EndChildSpan(ChildSpanType::PLANNING, cause, BufferedSpan::BufferedAttributesMap{
      {ATTR_QUERY_TYPE,
          impala::to_string(client_request_state_->exec_request().stmt_type)}}, true);
} // function DoEndChildSpanPlanning

void OtelTraceManager::StartChildSpanAdmissionControl() {
  lock_guard<mutex> l(child_span_mu_);

  BufferedSpan* span = ChildSpanBuilder(ChildSpanType::ADMISSION_CONTROL);
  span->SetParent(span_root_->GetContext());
  span->Start();
  debug_log_span(span, to_string(ChildSpanType::ADMISSION_CONTROL), query_id_, true);
} // function StartChildSpanAdmissionControl

void OtelTraceManager::EndChildSpanAdmissionControl(const Status& cause) {
  lock_guard<mutex> l(child_span_mu_);
  lock_guard<mutex> crs_lock(*(client_request_state_->lock()));
  DoEndChildSpanAdmissionControl(&cause);
} // function EndChildSpanAdmissionControl

void OtelTraceManager::DoEndChildSpanAdmissionControl(const Status* cause) {
  if (IsClosing()) {
    // If we are already closing, silently return as some cases (such as FIRST_FETCH)
    // will end the admission control phase even though the query already finished.
    return; // <-- EARLY RETURN
  }

#ifndef NDEBUG
  if (FLAGS_otel_trace_exhaustive_dchecks) {
    DCHECK_CHILD_SPAN_TYPE(ChildSpanType::ADMISSION_CONTROL);
  }
#endif

  bool queued = false;
  string adm_result;

  if (LIKELY(client_request_state_->admission_control_client() != nullptr)) {
    queued = client_request_state_->admission_control_client()->WasQueued();
  }

  if (LIKELY(client_request_state_->summary_profile() != nullptr)) {
    // The case of a query being cancelled while in the admission queue is handled here
    // because the summary profile may not be updated by the time this code runs.
    if (UNLIKELY(queued && cause != nullptr && cause->code() == TErrorCode::CANCELLED)) {
      adm_result = AdmissionController::PROFILE_INFO_VAL_CANCELLED_IN_QUEUE;
    } else {
      const string* profile_adm_res = client_request_state_->summary_profile()->
          GetInfoString("Admission result");
      if (UNLIKELY(profile_adm_res == nullptr)) {
        // Handle the case where the query closes during admission control before the
        // summary profile is updated with the admission result.
        adm_result = "";
      } else {
        adm_result = *profile_adm_res;
      }
    }
  }

  EndChildSpan(ChildSpanType::ADMISSION_CONTROL, cause,
      BufferedSpan::BufferedAttributesMap{
        {ATTR_QUEUED, queued},
        {ATTR_ADM_RESULT, adm_result},
        {ATTR_REQUEST_POOL, client_request_state_->request_pool()}
      }, true);
} // function DoEndChildSpanAdmissionControl

void OtelTraceManager::StartChildSpanQueryExecution() {
  lock_guard<mutex> l(child_span_mu_);

  if (IsClosing()) {
    // If we are already closing, silently return as some cases (such as FIRST_FETCH)
    // will start the query execution phase even though the query already failed.
   return; // <-- EARLY RETURN
  }

  BufferedSpan* span = ChildSpanBuilder(ChildSpanType::QUERY_EXEC, true);
  span->SetParent(span_root_->GetContext());
  span->Start();
  debug_log_span(span, to_string(ChildSpanType::QUERY_EXEC), query_id_, true);
} // function StartChildSpanQueryExecution

void OtelTraceManager::EndChildSpanQueryExecution() {
  lock_guard<mutex> l(child_span_mu_);
  lock_guard<mutex> crs_lock(*(client_request_state_->lock()));
  DoEndChildSpanQueryExecution();
}  // function EndChildSpanQueryExecution

void OtelTraceManager::DoEndChildSpanQueryExecution(const Status* cause) {
  if (IsClosing()) {
    // If we are already closing, silently return as some cases (such as FIRST_FETCH)
    // will end the query execution phase even though the query already failed.
   return; // <-- EARLY RETURN
  }

#ifndef NDEBUG
  if (FLAGS_otel_trace_exhaustive_dchecks) {
    DCHECK_CHILD_SPAN_TYPE(ChildSpanType::QUERY_EXEC);
  }
#endif
  BufferedSpan::BufferedAttributesMap attrs;

  if (client_request_state_->exec_request().stmt_type == TStmtType::QUERY) {
    attrs.insert_or_assign(ATTR_NUM_DELETED_ROWS, static_cast<int64_t>(0));
    attrs.insert_or_assign(ATTR_NUM_MODIFIED_ROWS, static_cast<int64_t>(0));
  } else {
    int64_t num_deleted_rows = 0;
    int64_t num_modified_rows = 0;

    if (LIKELY(client_request_state_->GetCoordinator() != nullptr)
        && client_request_state_->GetCoordinator()->dml_exec_state() != nullptr) {
      num_deleted_rows =
          client_request_state_->GetCoordinator()->dml_exec_state()->GetNumDeletedRows();
      num_modified_rows =
          client_request_state_->GetCoordinator()->dml_exec_state()->GetNumModifiedRows();
    }

    attrs.insert_or_assign(ATTR_NUM_DELETED_ROWS, num_deleted_rows);
    attrs.insert_or_assign(ATTR_NUM_MODIFIED_ROWS, num_modified_rows);
  }

  if (client_request_state_->stmt_type() == TStmtType::DDL) {
    attrs.insert_or_assign(ATTR_NUM_ROWS_FETCHED, 0);
  } else {
    attrs.insert_or_assign(ATTR_NUM_ROWS_FETCHED,
        client_request_state_->num_rows_fetched());
  }

  EndChildSpan(ChildSpanType::QUERY_EXEC, cause, attrs, true);
} // function DoEndChildSpanQueryExecution

void OtelTraceManager::StartChildSpanClose(const Status* cause) {
  lock_guard<mutex> l(child_span_mu_);

  // In a query cancellation scenario, the query could be cancelled before TraceQuery()
  // has been called. In that case, the Close span will be started during query
  // finalization and before the root span is started. Since TraceQuery() has not been
  // called, it is not possible to know if the query should be traced, thus default to
  // not tracing the query.
  if (!started_) {
    DoTraceQuery(false);
    return;
  }

  // If the query is ended during scheduling via an HS2 CancelOp() operation, the
  // Admission Control span will have been started but not ended. The provided cause will
  // be nullptr since HS2 CancelOp() operations do not set the query status as Cancelled.
  // In this case, set the cause.
  if (cause == nullptr && client_request_state_->is_cancelled()) {
    cause = &Status::CANCELLED;
  }

  // In an error scenario, another child span may still be active since the normal code
  // path was interrupted and thus the correct end child span function was not called.
  // In this case, we must first end the current child span.
  for (const auto& a : child_spans_) {
    const auto& span = a.second.span;
    if (span && !span->HasEnded()) {
      DCHECK(cause != nullptr) << "Child span '" << to_string(a.first) << "' is active "
          "when starting the Close span, a non-null cause must be provided to indicate "
          "why the query processing flow is being interrupted.";
      DCHECK(!cause->ok()) << "Child span '" << to_string(a.first) << "' is active when "
          "starting the Close span, a non-OK cause must be provided to indicate why the "
          "query processing flow is being interrupted.";

      {
        lock_guard<mutex> crs_lock(*(client_request_state_->lock()));
       (this->*a.second.end_func)(cause);
      }
    }
  }

  BufferedSpan* span = ChildSpanBuilder(ChildSpanType::CLOSE);
  span->SetParent(span_root_->GetContext());
  span->Start();
  debug_log_span(span, to_string(ChildSpanType::CLOSE), query_id_, true);
} // function StartChildSpanClose

void OtelTraceManager::EndChildSpanClose() {
#ifndef NDEBUG
  if (FLAGS_otel_trace_exhaustive_dchecks) {
    DCHECK_CHILD_SPAN_TYPE(ChildSpanType::CLOSE);
  }
#endif

  lock_guard<mutex> l(child_span_mu_);
  lock_guard<mutex> crs_lock(*(client_request_state_->lock()));
  DoEndChildSpanClose();
} // function EndChildSpanClose

void OtelTraceManager::DoEndChildSpanClose(const Status* cause) {
  EndChildSpan(ChildSpanType::CLOSE, nullptr, {}, true);

  // Set all root span attributes to avoid dereferencing the client_request_state_ in
  // the dtor (as the dtor is invoked when client_request_state_ is destroyed).
  span_root_->SetAttribute(ATTR_QUERY_TYPE,
    impala::to_string(client_request_state_->exec_request().stmt_type));

  if (LIKELY(client_request_state_->query_status().ok())) {
    span_root_->SetAttributeEmpty(ATTR_ERROR_MESSAGE);
  } else {
    string error_msg = client_request_state_->query_status().msg().msg();

    for (const auto& detail : client_request_state_->query_status().msg().details()) {
      error_msg += "\n" + detail;
    }

    span_root_->SetAttribute(ATTR_ERROR_MESSAGE, error_msg);
  }

  if (UNLIKELY(client_request_state_->WasRetried())) {
    span_root_->SetAttribute(ATTR_STATE, ClientRequestState::RetryStateToString(
        client_request_state_->retry_state()));
    span_root_->SetAttribute(ATTR_RETRIED_QUERY_ID,
        PrintId(client_request_state_->retried_id()));
  } else {
    span_root_->SetAttribute(ATTR_STATE,
      ClientRequestState::ExecStateToString(client_request_state_->exec_state()));
      span_root_->SetAttributeEmpty(ATTR_RETRIED_QUERY_ID);
  }

  if (UNLIKELY(client_request_state_->IsRetriedQuery())) {
    span_root_->SetAttribute(ATTR_ORIGINAL_QUERY_ID,
        PrintId(client_request_state_->original_id()));
  } else {
    span_root_->SetAttributeEmpty(ATTR_ORIGINAL_QUERY_ID);
  }
} // function EndChildSpanClose

bool OtelTraceManager::HasEnded() const {
  lock_guard<mutex> l(child_span_mu_);
  return span_root_ && span_root_->HasEnded();
} // function HasEnded

BufferedSpan* OtelTraceManager::ChildSpanBuilder(const ChildSpanType& span_type,
    BufferedSpan::BufferedAttributesMap&& additional_attributes, bool running) {
  DCHECK(client_request_state_ != nullptr) << "Cannot start child span without a valid "
      "client request state.";

  // If any other child spans are active, log a warning and end it. This should not happen
  // in normal execution but could in edge cases.
  for (const auto& a : child_spans_) {
    const auto& span = a.second.span;
    if (UNLIKELY(span && !span->HasEnded())) {
      LOG(WARNING) << strings::Substitute("Attempted to start child span '$0' while "
          "another child span '$1' is still active trace_id=\"$2\" span_id=\"$3\"\n$4",
          to_string(span_type), to_string(a.first), span->TraceId(),
          span->SpanId(), GetStackTrace());
#ifndef NDEBUG
      if (FLAGS_otel_trace_exhaustive_dchecks) {
        DCHECK(false) << strings::Substitute("Should not start child span '$0' when "
            "child span '$1' is already active.", to_string(span_type),
            to_string(a.first));
      }
#endif
      {
        lock_guard<mutex> crs_lock(*(client_request_state_->lock()));
        (this->*a.second.end_func)(nullptr);
      }
    }
  }

  const string full_span_name = query_id_ + " - " + to_string(span_type);

  additional_attributes.insert_or_assign(ATTR_NAME, full_span_name);
  additional_attributes.insert_or_assign(ATTR_RUNNING, running);

  child_spans_.at(span_type).span = make_unique<BufferedSpan>(tracer_, full_span_name,
    ATTR_BEGIN_TIME, ATTR_ELAPSED_TIME, std::move(additional_attributes),
    trace::SpanContext::GetInvalid(), trace::SpanKind::kInternal);

  return child_spans_.at(span_type).span.get();
} // function ChildSpanBuilder

BufferedSpan* OtelTraceManager::ChildSpanBuilder(const ChildSpanType& span_type,
    bool running) {
  return ChildSpanBuilder(span_type, {}, running);
} // function ChildSpanBuilder

void OtelTraceManager::EndChildSpan(const ChildSpanType& span_type,
    const Status* cause, const BufferedSpan::BufferedAttributesMap& additional_attributes,
    bool submit) {
  DCHECK(client_request_state_ != nullptr) << "Cannot end child span without a valid "
      "client request state.";

  unique_ptr<BufferedSpan>& span = child_spans_.at(span_type).span;
  if (LIKELY(span && !span->HasEnded())) {
    span->SetAttributes(additional_attributes);

    const Status* query_status;
    if (cause != nullptr) {
      query_status = cause;
    } else {
      query_status = &client_request_state_->query_status();
    }

    if (LIKELY(query_status->ok())) {
      span->SetAttributeEmpty(ATTR_ERROR_MSG);
      span->SetAttribute(ATTR_STATUS,
        ClientRequestState::ExecStateToString(client_request_state_->exec_state()));
    } else {
      string error_msg = query_status->msg().msg();

      for (const auto& detail : query_status->msg().details()) {
        error_msg += "\n" + detail;
      }

      span->SetAttribute(ATTR_ERROR_MSG, error_msg);
      span->SetAttribute(ATTR_STATUS,
            ClientRequestState::ExecStateToString(ClientRequestState::ExecState::ERROR));
    }

    span->SetEnd();

    if (submit) {
      span->Submit();
      debug_log_span(span.get(), to_string(span_type), query_id_, false);
      span.reset();
    }
  } else {
    LOG(WARNING) << strings::Substitute("Attempted to end a non-active child span "
        "trace_id=\"$0\" span_id=\"$1\"\n$2", span_root_->TraceId(), span_root_->SpanId(),
        GetStackTrace());
#ifndef NDEBUG
    if (FLAGS_otel_trace_exhaustive_dchecks) {
      DCHECK(false) << strings::Substitute("Cannot end child span '$0' when it is not "
          "already active.", to_string(span_type));
    }
#endif
  }
} // function EndChildSpan

} // namespace impala