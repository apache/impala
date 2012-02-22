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

#include "common/status.h"
#include "runtime/coordinator.h"
#include "runtime/exec-env.h"
#include "testutil/test-exec-env.h"
#include "util/jni-util.h"
#include "service/backend-service.h"
#include "gen-cpp/ImpalaService.h"
#include "gen-cpp/ImpalaBackendService.h"

using namespace impala;
using namespace std;
using namespace boost;
using namespace apache::thrift::server;
using namespace apache::thrift::protocol;
using namespace apache::thrift::transport;
using namespace apache::thrift::concurrency;

DEFINE_int32(fe_port, 21000, "port on which ImpalaService is exported");
DEFINE_int32(be_port, 22000, "port on which ImpalaBackendService is exported");
DEFINE_string(classpath, "", "java classpath");

namespace impala {

class ImpalaService : public ImpalaServiceIf {
 public:
  ImpalaService(JavaVM* jvm): jvm_(jvm) {}
  virtual ~ImpalaService() {}

  // Initialize state. Terminates process on error.
  void Init();

  virtual void RunQuery(TRunQueryResult& result, const TQueryRequest& request);
  virtual void FetchResults(TFetchResultsResult& result, const TUniqueId& query_id);
  virtual void CancelQuery(TStatus& result, const TUniqueId& query_id);

 private:
  JavaVM* jvm_;
  jobject fe_;  // instance of com.cloudera.impala.service.Frontend

  // map from query id to coordinator executing that query
  typedef unordered_map<TUniqueId, Coordinator*> ExecStateMap;
  ExecStateMap exec_state_map_;

  // Call FE to get TQueryExecRequest.
  Status GetExecRequest(
      const TQueryRequest& query_request, TQueryExecRequest* exec_request);
};

void ImpalaService::Init() {
#if 0
  JNIEnv* env;
  EXIT_IF_JNIERROR(GetEnv(jvm_, &env, JNI_VERSION_1_6));
  // create instance of java class Frontend
  jclass fe_class = env->FindClass("com/cloudera/impala/service/Frontend");
  jmethodID fe_ctor = env->GetMethodID("<init>", "()V");
  EXIT_IF_EXC(env, JniUtil::throwable_to_string_id());
  jobject fe = env->NewObject(fe_class, fe_ctor);
  EXIT_IF_EXC(env, JniUtil::throwable_to_string_id());
  EXIT_IF_ERROR(JniUtil::LocalToGlobalRef(env, fe, &fe_));
#endif
}

void ImpalaService::RunQuery(
    impala::TRunQueryResult& result, const TQueryRequest& request) {
#if 0
  result.status.status_code = OK;
  JNIEnv* jni_env;
  SET_TSTATUS_IF_JNIERROR(AttachCurrentThread(jvm_, &jni_env, NULL), &result.status);

  TQueryExecRequest exec_request;
  SET_TSTATUS_IF_ERROR(GetExecRequest(request, &exec_request), &result.status);
  Coordinator* coord = new Coordinator(host_, port_, stream_mgr_, test_env_);
  exec_state_map_.insert(make_pair(exec_request.queryId, coord));
  SET_TSTATUS_IF_ERROR(coord->Exec(exec_request), &result.status);

  SET_TSTATUS_IF_JNIERROR(DetachCurrentThread(jvm_), &result.status);
#endif
}

Status ImpalaService::GetExecRequest(
    const TQueryRequest& query_request, TQueryExecRequest* exec_request) {
  return Status::OK;
}

void ImpalaService::FetchResults(
    TFetchResultsResult& result, const TUniqueId& query_id) {
#if 0
  result.status.status_code = OK;

  ExecStateMap::iterator i = exec_state_map_.find(query_id);
  if (i == exec_state_map_.end()) {
    result.status.status_code = INTERNAL_ERROR;
    return;
  }
  RowBatch* batch;
  SET_TSTATUS_IF_ERROR(i->second->GetNext(&batch), &result.status);
#endif
}

void ImpalaService::CancelQuery(TStatus& result, const TUniqueId& query_id) {
}

static JavaVM* CreateJvm() {
  JavaVM* jvm;
  JNIEnv* env;
  JavaVMInitArgs vm_args;
  JavaVMOption options;
  options.optionString = const_cast<char*>(FLAGS_classpath.c_str());
  vm_args.version = JNI_VERSION_1_6;
  vm_args.nOptions = 1;
  vm_args.options = &options;
  vm_args.ignoreUnrecognized = false;
  JNI_CreateJavaVM(&jvm, (void**)&env, &vm_args);
  // TODO: check status
  return jvm;
}

static void RunServer(TServer* server) {
  VLOG(1) << "started backend server thread";
  server->serve();
}

static void StartImpalaService(JavaVM* jvm, int port) {
  shared_ptr<TProtocolFactory> protocol_factory(new TBinaryProtocolFactory());
  shared_ptr<ImpalaService> handler(new ImpalaService(jvm));
  handler->Init();
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
  server->serve();
}

}

int main(int argc, char** argv) {
  google::InitGoogleLogging(argv[0]);
  google::ParseCommandLineFlags(&argc, &argv, true);
  JniUtil::InitLibhdfs();

  JavaVM* jvm = CreateJvm();

  // start backend service for the coordinator on backend_port
  ExecEnv exec_env;
  TServer* be_server = StartImpalaBackendService(&exec_env, FLAGS_be_port);
  be_server->serve();
  thread be_server_thread = thread(&RunServer, be_server);

  // this blocks until the fe server terminates
  StartImpalaService(jvm, FLAGS_fe_port);

  if (jvm->DestroyJavaVM() != JNI_OK) {
    LOG(ERROR) << "error destroying jvm";
  }
}
