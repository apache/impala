// Copyright 2012 Cloudera Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//
// A standalone test utility that starts multiple Impala backends and a state store
// within a single process.

#include <boost/foreach.hpp>

#include "codegen/llvm-codegen.h"
#include "common/logging.h"
#include "common/init.h"
#include "exec/hbase-table-scanner.h"
#include "exec/hbase-table-writer.h"
#include "rpc/authentication.h"
#include "rpc/thrift-util.h"
#include "rpc/thrift-server.h"
#include "runtime/hbase-table.h"
#include "service/fe-support.h"
#include "service/impala-server.h"
#include "util/jni-util.h"
#include "testutil/in-process-servers.h"

DEFINE_int32(num_backends, 3, "The number of backends to start");
DECLARE_int32(be_port);
DECLARE_int32(beeswax_port);
DECLARE_int32(hs2_port);
DECLARE_string(principal);
DECLARE_bool(use_statestore);

#include "common/names.h"

using namespace impala;

int main(int argc, char** argv) {
  InitCommonRuntime(argc, argv, true);
  if (FLAGS_num_backends <= 0) {
    LOG(ERROR) << "-num_backends arg must be > 0";
    exit(1);
  }

  LlvmCodeGen::InitializeLlvm();
  JniUtil::InitLibhdfs();
  EXIT_IF_ERROR(HBaseTableScanner::Init());
  EXIT_IF_ERROR(HBaseTable::InitJNI());
  EXIT_IF_ERROR(HBaseTableWriter::InitJNI());
  InitFeSupport();

  int base_be_port = FLAGS_be_port;
  int base_subscriber_port = 21500;
  int base_webserver_port = 25000;

  int beeswax_port = 21000;
  int hs2_port = 21050;

  scoped_ptr<InProcessStatestore> statestore(new InProcessStatestore(23000, 25100));
  if (FLAGS_use_statestore) EXIT_IF_ERROR(statestore->Start());
  LOG(INFO) << "Started in-process statestore";

  vector<InProcessImpalaServer*> impala_servers;
  for (int i = 0; i < FLAGS_num_backends; ++i) {
    impala_servers.push_back(
        new InProcessImpalaServer(FLAGS_hostname, base_be_port + i,
                                  base_subscriber_port + i, base_webserver_port + i,
                                  FLAGS_hostname, 23000));
    // First server in the list runs client servers
    if (i == 0) {
      EXIT_IF_ERROR(impala_servers[i]->StartWithClientServers(beeswax_port, hs2_port,
                                                              FLAGS_use_statestore));
    } else {
      EXIT_IF_ERROR(impala_servers[i]->StartAsBackendOnly(FLAGS_use_statestore));
    }
  }

  impala_servers[0]->Join();

  BOOST_FOREACH(InProcessImpalaServer* server, impala_servers) {
    delete server;
  }
}
