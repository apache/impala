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

#include "catalog/catalog-util.h"
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
  ABORT_IF_ERROR(JniCatalogCacheUpdateIterator::InitJNI());
  InitFeSupport();

  ExecEnv exec_env;
  ABORT_IF_ERROR(exec_env.Init());
  CommonMetrics::InitCommonMetrics(exec_env.metrics());
  ABORT_IF_ERROR(StartMemoryMaintenanceThread()); // Memory metrics are created in Init().
  ABORT_IF_ERROR(
      StartThreadInstrumentation(exec_env.metrics(), exec_env.webserver(), true));
  InitRpcEventTracing(exec_env.webserver(), exec_env.rpc_mgr());

  boost::shared_ptr<ImpalaServer> impala_server(new ImpalaServer(&exec_env));
  Status status =
      impala_server->Start(FLAGS_be_port, FLAGS_beeswax_port, FLAGS_hs2_port);
  if (!status.ok()) {
    LOG(ERROR) << "Impalad services did not start correctly, exiting.  Error: "
        << status.GetDetail();
    ShutdownLogging();
    exit(1);
  }

  impala_server->Join();

  return 0;
}
