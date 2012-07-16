// Copyright (c) 2012 Cloudera, Inc. All rights reserved.

#include "service/backend-service.h"

#include <protocol/TBinaryProtocol.h>
#include <server/TThreadPoolServer.h>
#include <transport/TServerSocket.h>
#include <transport/TTransportUtils.h>
//#include <concurrency/Thread.h>
#include <concurrency/PosixThreadFactory.h>
#include <gtest/gtest.h>
#include <glog/logging.h>
// include gflags.h *after* logging.h, otherwise the linker will complain about
// undefined references to FLAGS_v
//#include <gflags/gflags.h>

#include "exec/hdfs-table-sink.h"
#include "runtime/data-stream-mgr.h"
#include "runtime/descriptors.h"
#include "runtime/data-stream-sender.h"
#include "runtime/row-batch.h"
#include "runtime/plan-executor.h"
#include "runtime/hdfs-fs-cache.h"
#include "runtime/exec-env.h"
#include "exec/exec-node.h"
#include "exec/scan-node.h"
#include "util/debug-util.h"
#include "gen-cpp/ImpalaBackendService.h"
#include "gen-cpp/DataSinks_types.h"
#include "gen-cpp/Types_types.h"

using namespace std;
using namespace boost;
using namespace apache::thrift;
using namespace apache::thrift::protocol;
using namespace apache::thrift::transport;
using namespace apache::thrift::server;
using namespace apache::thrift::concurrency;

namespace impala {

class DataSink;

class ImpalaBackend : public ImpalaBackendServiceIf {
 public:
  ImpalaBackend(ExecEnv* exec_env)
    : exec_env_(exec_env) {}

  void ExecPlanFragment(
      TExecPlanFragmentResult& return_val, const TPlanExecRequest& request,
      const TPlanExecParams& params);

  void TransmitData(
      TStatus& return_val, const TUniqueId& query_id, const TPlanNodeId dest_node_id,
      const TRowBatch& thrift_batch);

  void CloseChannel(
    TStatus& return_val, const TUniqueId& query_id, const TPlanNodeId dest_node_id);

 private:
  ExecEnv* exec_env_;  // not owned

  Status ExecPlanFragment(
      const TPlanExecRequest& request, const TPlanExecParams& params,
      vector<TRuntimeProfileNode>* profiles);

  Status CreateDataSink(
      const TPlanExecRequest& request, const TPlanExecParams& params,
      const RowDescriptor& row_desc, DataSink** sink);
};


void ImpalaBackend::ExecPlanFragment(
    TExecPlanFragmentResult& return_val, const TPlanExecRequest& request,
    const TPlanExecParams& params) {
  ExecPlanFragment(request, params, &return_val.profiles.nodes).ToThrift(&return_val.status);
}

Status ImpalaBackend::ExecPlanFragment(
    const TPlanExecRequest& request, const TPlanExecParams& params,
    vector<TRuntimeProfileNode>* profiles) {
  VLOG_QUERY << "starting ExecPlanFragment";
  if (!request.dataSink.__isset.dataStreamSink) {
    Status status("missing data stream sink");
    return status;
  }

  PlanExecutor executor(exec_env_);
  RETURN_IF_ERROR(executor.Prepare(request, params));

  scoped_ptr<DataSink> sink;

  RETURN_IF_ERROR(DataSink::CreateDataSink(request,
      params, executor.row_desc(), &sink));

  RETURN_IF_ERROR(sink->Init(executor.runtime_state()));

  RETURN_IF_ERROR(executor.Open());
  RowBatch* batch;
  while (true) {
    RETURN_IF_ERROR(executor.GetNext(&batch));
    if (batch == NULL) break;
    VLOG_QUERY << "ExecPlanFragment: #rows=" << batch->num_rows();
    if (VLOG_ROW_IS_ON) {
      for (int i = 0; i < batch->num_rows(); ++i) {
        TupleRow* row = batch->GetRow(i);
        VLOG_ROW << PrintRow(row, executor.row_desc());
      }
    }
    RETURN_IF_ERROR(sink->Send(executor.runtime_state(), batch));
    batch = NULL;
  }
  RETURN_IF_ERROR(sink->Close(executor.runtime_state()));

  executor.query_profile()->ToThrift(profiles);

  VLOG_QUERY << "finished ExecPlanFragment";
  return Status::OK;
}

void ImpalaBackend::TransmitData(
    TStatus& return_val, const TUniqueId& query_id, const TPlanNodeId dest_node_id,
    const TRowBatch& thrift_batch) {
  // TODO: fix Thrift so we can simply take ownership of thrift_batch instead
  // of having to copy its data
  exec_env_->stream_mgr()->AddData(
      query_id, dest_node_id, thrift_batch).ToThrift(&return_val);
}

void ImpalaBackend::CloseChannel(
    TStatus& return_val, const TUniqueId& query_id, const TPlanNodeId dest_node_id) {
  exec_env_->stream_mgr()->CloseChannel(
      query_id, dest_node_id).ToThrift(&return_val);
}

TServer* StartImpalaBackendService(ExecEnv* exec_env, int port) {
  shared_ptr<TProtocolFactory> protocol_factory(new TBinaryProtocolFactory());
  shared_ptr<ImpalaBackend> handler(new ImpalaBackend(exec_env));
  shared_ptr<TProcessor> processor(new ImpalaBackendServiceProcessor(handler));
  shared_ptr<TServerTransport> server_transport(new TServerSocket(port));
  shared_ptr<TTransportFactory> transport_factory(new TBufferedTransportFactory());
  shared_ptr<ThreadManager> thread_mgr(ThreadManager::newSimpleThreadManager());
  // TODO: do we want a BoostThreadFactory?
  shared_ptr<ThreadFactory> thread_factory(new PosixThreadFactory());
  thread_mgr->threadFactory(thread_factory);
  thread_mgr->start();

  LOG(INFO) << "ImpalaBackend listening on " << port;
  TThreadPoolServer* server = new TThreadPoolServer(
      processor, server_transport, transport_factory, protocol_factory,
      thread_mgr);
  return server;
}

}
