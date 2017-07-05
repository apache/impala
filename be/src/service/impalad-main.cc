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

//
// This file contains the main() function for the impala daemon process,
// which exports the Thrift services ImpalaService and ImpalaInternalService.

#include <unistd.h>
#include <jni.h>

#include "common/logging.h"
#include "common/init.h"
#include "exec/hbase-table-scanner.h"
#include "exec/hbase-table-writer.h"
#include "exprs/hive-udf-call.h"
#include "exprs/timezone_db.h"
#include "runtime/hbase-table.h"
#include "codegen/llvm-codegen.h"
#include "common/status.h"
#include "runtime/coordinator.h"
#include "runtime/exec-env.h"
#include "util/common-metrics.h"
#include "util/jni-util.h"
#include "util/network-util.h"
#include "rpc/thrift-util.h"
#include "rpc/thrift-server.h"
#include "rpc/rpc-trace.h"
#include "service/impala-server.h"
#include "service/fe-support.h"
#include "gen-cpp/ImpalaService.h"
#include "gen-cpp/ImpalaInternalService.h"
#include "util/impalad-metrics.h"
#include "util/thread.h"

#include "common/names.h"

using namespace impala;

DECLARE_int32(beeswax_port);
DECLARE_int32(hs2_port);
DECLARE_int32(be_port);
DECLARE_bool(enable_rm);
DECLARE_bool(is_coordinator);

int ImpaladMain(int argc, char** argv) {
  InitCommonRuntime(argc, argv, true);

  ABORT_IF_ERROR(TimezoneDatabase::Initialize());
  ABORT_IF_ERROR(LlvmCodeGen::InitializeLlvm());
  JniUtil::InitLibhdfs();
  ABORT_IF_ERROR(HBaseTableScanner::Init());
  ABORT_IF_ERROR(HBaseTable::InitJNI());
  ABORT_IF_ERROR(HBaseTableWriter::InitJNI());
  ABORT_IF_ERROR(HiveUdfCall::InitEnv());
  InitFeSupport();

  if (FLAGS_enable_rm) {
    // TODO: Remove in Impala 3.0.
    LOG(WARNING) << "*****************************************************************";
    LOG(WARNING) << "Llama support has been deprecated. FLAGS_enable_rm has no effect.";
    LOG(WARNING) << "*****************************************************************";
  }

  // start backend service for the coordinator on be_port
  ExecEnv exec_env;
  StartThreadInstrumentation(exec_env.metrics(), exec_env.webserver(), true);
  InitRpcEventTracing(exec_env.webserver());

  CommonMetrics::InitCommonMetrics(exec_env.metrics());

  ThriftServer* beeswax_server = NULL;
  ThriftServer* hs2_server = NULL;
  ThriftServer* be_server = NULL;
  boost::shared_ptr<ImpalaServer> server;
  ABORT_IF_ERROR(CreateImpalaServer(&exec_env, FLAGS_beeswax_port, FLAGS_hs2_port,
      FLAGS_be_port, &beeswax_server, &hs2_server, &be_server, &server));

  Status status = exec_env.StartServices();
  if (!status.ok()) {
    LOG(ERROR) << "Impalad services did not start correctly, exiting.  Error: "
               << status.GetDetail();
    ShutdownLogging();
    exit(1);
  }

  DCHECK(exec_env.process_mem_tracker() != nullptr)
      << "ExecEnv::StartServices() must be called before starting RPC services";
  ABORT_IF_ERROR(be_server->Start());

  if (FLAGS_is_coordinator) {
    ABORT_IF_ERROR(beeswax_server->Start());
    ABORT_IF_ERROR(hs2_server->Start());
  }

  ImpaladMetrics::IMPALA_SERVER_READY->set_value(true);
  LOG(INFO) << "Impala has started.";

  be_server->Join();
  delete be_server;

  if (FLAGS_is_coordinator) {
    // this blocks until the beeswax and hs2 servers terminate
    beeswax_server->Join();
    hs2_server->Join();
    delete beeswax_server;
    delete hs2_server;
  }

  return 0;
}
