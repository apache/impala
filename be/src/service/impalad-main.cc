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
// This file contains the main() function for the impala daemon process,
// which exports the Thrift services ImpalaService and ImpalaInternalService.

#include <unistd.h>
#include <jni.h>
#include <boost/scoped_ptr.hpp>
#include <boost/unordered_map.hpp>
#include <boost/algorithm/string.hpp>
#include <boost/lexical_cast.hpp>

#include "common/logging.h"
#include "common/daemon.h"
// TODO: fix this: we currently need to include uid-util.h before impala-server.h
#include "util/uid-util.h"
#include "exec/hbase-table-scanner.h"
#include "runtime/hbase-table-cache.h"
#include "codegen/llvm-codegen.h"
#include "common/status.h"
#include "runtime/coordinator.h"
#include "runtime/exec-env.h"
#include "testutil/test-exec-env.h"
#include "util/jni-util.h"
#include "util/logging.h"
#include "util/thrift-util.h"
#include "statestore/subscription-manager.h"
#include "util/thrift-server.h"
#include "common/service-ids.h"
#include "util/authorization.h"
#include "service/impala-server.h"
#include "service/fe-support.h"
#include "gen-cpp/ImpalaService.h"
#include "gen-cpp/ImpalaInternalService.h"
#include "util/impalad-metrics.h"

using namespace impala;
using namespace std;
using namespace boost;
using namespace apache::thrift::server;
using namespace apache::thrift::protocol;
using namespace apache::thrift::transport;
using namespace apache::thrift::concurrency;

DECLARE_string(classpath);
DECLARE_string(ipaddress);
DECLARE_bool(use_statestore);
DECLARE_int32(beeswax_port);
DECLARE_int32(hs2_port);
DECLARE_int32(be_port);
DECLARE_string(principal);
DECLARE_string(nn);
DECLARE_int32(nn_port);

int main(int argc, char** argv) {
  InitDaemon(argc, argv);

  LlvmCodeGen::InitializeLlvm();

  // Enable Kerberos security if requested.
  if (!FLAGS_principal.empty()) {
    EXIT_IF_ERROR(InitKerberos("Impalad"));
  }

  JniUtil::InitLibhdfs();
  EXIT_IF_ERROR(JniUtil::Init());
  EXIT_IF_ERROR(HBaseTableScanner::Init());
  EXIT_IF_ERROR(HBaseTableCache::Init());
  InitFeSupport();

  // start backend service for the coordinator on be_port
  ExecEnv exec_env;
  ThriftServer* beeswax_server = NULL;
  ThriftServer* hs2_server = NULL;
  ThriftServer* be_server = NULL;
  ImpalaServer* server =
      CreateImpalaServer(&exec_env, FLAGS_beeswax_port, FLAGS_hs2_port, FLAGS_be_port,
          &beeswax_server, &hs2_server, &be_server);

  // If the user hasn't deliberately specified a namenode URI, read it
  // from the frontend and parse it into FLAGS_nn{_port}.

  // This must be done *after* ImpalaServer's constructor which
  // creates a JNI environment but before any queries are run (which
  // cause FLAGS_nn to be read)
  if (FLAGS_nn.empty()) {
    // Read the namenode name and port from the Hadoop config.
    string default_fs;
    EXIT_IF_ERROR(server->GetHadoopConfigValue("fs.defaultFS", &default_fs));
    if (default_fs.empty()) {
      EXIT_IF_ERROR(server->GetHadoopConfigValue("fs.default.name", &default_fs));
      if (!default_fs.empty()) {
        LOG(INFO) << "fs.defaultFS not found. Falling back to fs.default.name from Hadoop"
                  << " config: " << default_fs;
      }
    } else {
      LOG(INFO) << "Read fs.defaultFS from Hadoop config: " << default_fs;
    }

    if (!default_fs.empty()) {
      size_t double_slash_pos = default_fs.find("//");
      if (double_slash_pos != string::npos) {
        default_fs.erase(0, double_slash_pos + 2);
      }
      vector<string> strs;
      split(strs, default_fs, is_any_of(":"));
      FLAGS_nn = strs[0];
      DCHECK(!strs[0].empty());
      LOG(INFO) << "Setting default name (-nn): " << strs[0];
      if (strs.size() > 1) {
        LOG(INFO) << "Setting default port (-nn_port): " << strs[1];
        try {
          FLAGS_nn_port = lexical_cast<int>(strs[1]);
        } catch (bad_lexical_cast) {
          LOG(ERROR) << "Could not set -nn_port from Hadoop configuration. Port was: "
                     << strs[1];
        }
      }
    } else {
      LOG(ERROR) << "Could not find valid namenode URI. Set fs.defaultFS in Impala's "
                 << "Hadoop configuration files";
      exit(1);
    }
  }


  be_server->Start();

  Status status = exec_env.StartServices();
  if (!status.ok()) {
    LOG(ERROR) << "Impalad services did not start correctly, exiting";
    ShutdownLogging();
    exit(1);
  }

  // register be service *after* starting the be server thread and after starting
  // the subscription mgr handler thread
  scoped_ptr<SubscriptionManager::UpdateCallback> cb;
  if (FLAGS_use_statestore) {
    THostPort host_port;
    host_port.port = FLAGS_be_port;
    host_port.ipaddress = FLAGS_ipaddress;
    host_port.hostname = FLAGS_hostname;
    // TODO: Unregister on tear-down (after impala service changes)
    Status status =
        exec_env.subscription_mgr()->RegisterService(IMPALA_SERVICE_ID, host_port);

    unordered_set<ServiceId> services;
    services.insert(IMPALA_SERVICE_ID);
    cb.reset(new SubscriptionManager::UpdateCallback(
        bind<void>(mem_fn(&ImpalaServer::MembershipCallback), server, _1)));
    exec_env.subscription_mgr()->RegisterSubscription(services, "impala.server",
        cb.get());

    if (!status.ok()) {
      LOG(ERROR) << "Could not register with state store service: "
                 << status.GetErrorMsg();
      ShutdownLogging();
      exit(1);
    }
  }

  // this blocks until the beeswax and hs2 servers terminate
  beeswax_server->Start();
  hs2_server->Start();
  ImpaladMetrics::IMPALA_SERVER_READY->Update(true);
  LOG(INFO) << "Impala has started.";
  beeswax_server->Join();
  hs2_server->Join();

  delete be_server;
  delete beeswax_server;
  delete hs2_server;
}
