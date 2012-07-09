// (c) 2012 Cloudera, Inc. All rights reserved.
//
// This file contains the main() function for the impala daemon process,
// which exports the Thrift services ImpalaService and ImpalaBackendService.

#include <jni.h>
#include <boost/scoped_ptr.hpp>
#include <boost/unordered_map.hpp>
#include <glog/logging.h>
#include <gflags/gflags.h>

#include <protocol/TBinaryProtocol.h>
#include <server/TThreadPoolServer.h>
#include <transport/TServerSocket.h>
#include <server/TServer.h>
#include <transport/TTransportUtils.h>
#include <concurrency/PosixThreadFactory.h>

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
#include "service/backend-service.h"
#include "service/impala-service.h"
#include "sparrow/subscription-manager.h"
#include "common/service-ids.h"
#include "gen-cpp/ImpalaService.h"
#include "gen-cpp/ImpalaBackendService.h"

using namespace impala;
using namespace std;
using namespace sparrow;
using namespace boost;
using namespace apache::thrift::server;
using namespace apache::thrift::protocol;
using namespace apache::thrift::transport;
using namespace apache::thrift::concurrency;

DEFINE_int32(fe_port, 21000, "port on which ImpalaService is exported");
DECLARE_int32(be_port);
DEFINE_string(classpath, "", "java classpath");

DEFINE_bool(use_planservice, false, "Use external planservice if true");
DEFINE_string(planservice_host, "localhost",
    "Host on which external planservice is running");
DEFINE_int32(planservice_port, 20000, "Port on which external planservice is running");
DEFINE_string(be_host, "localhost", "Hostname on which to export the backend service");
DECLARE_bool(use_statestore);

namespace impala {

static void RunServer(TServer* server) {
  VLOG(1) << "started backend server thread";
  server->serve();
}

// Start jvm and backend service. If state store is being used, must be already
// initialised (that is, exec_env->StartServices() must already have been called)
static void StartImpalaService(ExecEnv* exec_env, int port) {
  shared_ptr<TProtocolFactory> protocol_factory(new TBinaryProtocolFactory());
  LOG(INFO) << "ImpalaService trying to listen on " << port;

  shared_ptr<ImpalaService> handler(new ImpalaService(exec_env, FLAGS_fe_port));
  // this first call to getJNIEnv() (which should be this one) creates a jvm
  JNIEnv* env = getJNIEnv();
  handler->Init(env);
  shared_ptr<TProcessor> processor(new ImpalaServiceProcessor(handler));
  shared_ptr<TServerTransport> server_transport(new TServerSocket(port));
  shared_ptr<TTransportFactory> transport_factory(new TBufferedTransportFactory());
  shared_ptr<ThreadManager> thread_mgr(ThreadManager::newSimpleThreadManager());
  // TODO: do we want a BoostThreadFactory?
  shared_ptr<ThreadFactory> thread_factory(new PosixThreadFactory());
  thread_mgr->threadFactory(thread_factory);
  thread_mgr->start();

  LOG(INFO) << "ImpalaService listening on " << port;
  TThreadPoolServer* server = new TThreadPoolServer(
      processor, server_transport, transport_factory, protocol_factory,
      thread_mgr);

  SubscriptionManager* subscription_manager = exec_env->subscription_manager();
  if (FLAGS_use_statestore) {
    THostPort host_port;
    host_port.port = FLAGS_be_port;
    host_port.host = FLAGS_be_host;
    // TODO: Unregister on tear-down (after impala service changes)
    Status status = subscription_manager->RegisterService(IMPALA_SERVICE_ID, host_port);

    if (!status.ok()) {
      LOG(ERROR) << "Could not register with state store service: "
                 << status.GetErrorMsg();
      return;
    }
  }

  server->serve();
}

}

int main(int argc, char** argv) {
  InitGoogleLoggingSafe(argv[0]);
  google::ParseCommandLineFlags(&argc, &argv, true);
  InitThriftLogging();
  LlvmCodeGen::InitializeLlvm();
  EXIT_IF_ERROR(JniUtil::Init());
  EXIT_IF_ERROR(HBaseTableScanner::Init());
  EXIT_IF_ERROR(HBaseTableCache::Init());

  // start backend service for the coordinator on be_port
  ExecEnv exec_env;
  TServer* be_server = StartImpalaBackendService(&exec_env, FLAGS_be_port);
  thread be_server_thread = thread(&RunServer, be_server);

  EXIT_IF_ERROR(exec_env.StartServices());

  // this blocks until the fe server terminates
  StartImpalaService(&exec_env, FLAGS_fe_port);
}
