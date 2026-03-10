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

#include <filesystem>
#include <memory>

#include <gtest/gtest.h>

#include "codegen/llvm-codegen.h"
#include "common/status.h"
#include "gen-cpp/Query_types.h"
#include "gen-cpp/Types_types.h"
#include "observe/otel.h"
#include "runtime/exec-env.h"
#include "runtime/query-driver.h"
#include "service/client-request-state.h"
#include "service/fe-support.h"
#include "service/impala-server.h"
#include "statestore/statestore.h"
#include "testutil/in-process-servers.h"
#include "testutil/rand-util.h"
#include "util/asan.h"
#include "util/metrics.h"

DECLARE_bool(abort_on_config_error);
DECLARE_bool(otel_trace_enabled);
DECLARE_string(otel_trace_exporter);
DECLARE_string(otel_trace_span_processor);
DECLARE_string(otel_file_pattern);
DECLARE_string(otel_file_alias_pattern);
DECLARE_int32(otel_file_flush_interval_ms);
DECLARE_int32(otel_file_flush_count);
DECLARE_int32(otel_file_max_file_size);
DECLARE_int32(otel_file_max_file_count);

using namespace std;
using namespace impala;

static InProcessImpalaServer* impala_server_ = nullptr;

static std::filesystem::path CreateRandomTempDir() {
  return RandTestUtil::CreateRandomTempDir("otel-trace-manager-test", false)
      / "export-trace.jsonl";
} // function CreateRandomTempDir

// Tests the case where a query is cancelled after planning starts but before the
// planner calls TraceQuery(). In this case, no trace will be generated.
TEST(OtelTraceManagerTest, TraceQueryNotCalled) {
  const std::filesystem::path trace_file = CreateRandomTempDir();
  ASSERT_FALSE(trace_file.empty());

  FLAGS_otel_trace_enabled = true;
  FLAGS_otel_trace_exporter = "file";
  FLAGS_otel_trace_span_processor = "simple";
  FLAGS_otel_file_pattern = string(trace_file);
  FLAGS_otel_file_alias_pattern = "";
  FLAGS_otel_file_flush_interval_ms = 1;
  FLAGS_otel_file_flush_count = 1;
  FLAGS_otel_file_max_file_size = 1024 * 1024;
  FLAGS_otel_file_max_file_count = 1;

  FLAGS_abort_on_config_error = false;

  InitFeSupport(false);
  ABORT_IF_ERROR(LlvmCodeGen::InitializeLlvm());

  MetricGroup* statestore_metrics = new MetricGroup("statestore_metrics");
  Statestore* statestore = new Statestore(statestore_metrics);
  ABORT_IF_ERROR(statestore->Init(0));

  ABORT_IF_ERROR(InProcessImpalaServer::StartWithEphemeralPorts(FLAGS_hostname,
    statestore->port(), &impala_server_));
  IGNORE_LEAKING_OBJECT(impala_server_);

  init_otel_tracer();

  {
    shared_ptr<ImpalaServer::SessionState> session =
        make_shared<ImpalaServer::SessionState>(impala_server_->impala_server(),
        TUniqueId(), TUniqueId());
    QueryDriver query_driver(nullptr);
    TSessionState session_state;
    TQueryCtx query_ctx;

    session_state.__set_session_type(TSessionType::HIVESERVER2);

    query_ctx.__set_session(session_state);
    query_ctx.__set_client_request(TClientRequest());

    ClientRequestState crs(query_ctx, ExecEnv::GetInstance()->frontend(),
        impala_server_->impala_server(), session, &query_driver);
    crs.otel_trace_manager()->EndChildSpanInit();
    crs.otel_trace_manager()->StartChildSpanSubmitted();
    crs.otel_trace_manager()->EndChildSpanSubmitted();
    crs.otel_trace_manager()->StartChildSpanPlanning();
    query_driver.Abandon();
    crs.Finalize(&Status::CANCELLED);
    crs.UnRegisterRemainingRPCs();
  } // OtelTraceManager dtor runs here.

  shutdown_otel_tracer();

  EXPECT_FALSE(filesystem::exists(trace_file));
} // test OtelTraceManagerTest.TraceQueryNotCalled

