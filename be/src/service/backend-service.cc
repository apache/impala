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

#include "runtime/data-stream-mgr.h"
#include "runtime/descriptors.h"
#include "runtime/data-stream-sender.h"
#include "runtime/row-batch.h"
#include "runtime/plan-executor.h"
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

class ImpalaBackend : public ImpalaBackendServiceIf {
 public:
  ImpalaBackend(DataStreamMgr* stream_mgr): stream_mgr_(stream_mgr) {}

  void ExecPlanFragment(
      TStatus& return_val, const TPlanExecRequest& request,
      const TPlanExecParams& params);

  void TransmitData(
      TStatus& return_val, const TUniqueId& query_id, const TPlanNodeId dest_node_id,
      const TRowBatch& thrift_batch);

  void CloseChannel(
    TStatus& return_val, const TUniqueId& query_id, const TPlanNodeId dest_node_id);

 private:
  DataStreamMgr* stream_mgr_;  // not owned

  Status ExecPlanFragment(
      const TPlanExecRequest& request, const TPlanExecParams& params);
};


void ImpalaBackend::ExecPlanFragment(
    TStatus& return_val, const TPlanExecRequest& request,
    const TPlanExecParams& params) {
  ExecPlanFragment(request, params).ToThrift(&return_val);
}

Status ImpalaBackend::ExecPlanFragment(
    const TPlanExecRequest& request, const TPlanExecParams& params) {
  VLOG(1) << "starting ExecPlanFragment";
  if (request.dataSink.dataSinkType != TDataSinkType::DATA_STREAM_SINK) {
    Status status("output type other than data stream not supported");
    return status;
  }
  if (!request.dataSink.__isset.dataStreamSink) {
    Status status("missing data stream sink");
    return status;
  }

  PlanExecutor executor(stream_mgr_);
  RETURN_IF_ERROR(executor.Prepare(request, params));

  // TODO: figure out good buffer size based on size of output row
  DataStreamSender sender(
      executor.row_desc(), request.queryId, request.dataSink.dataStreamSink,
      params.destinations, 16 * 1024);
  RETURN_IF_ERROR(sender.Init());

  RETURN_IF_ERROR(executor.Open());
  RowBatch* batch;
  while (true) {
    RETURN_IF_ERROR(executor.GetNext(&batch));
    if (batch == NULL) break;
    VLOG(1) << "ExecPlanFragment: #rows=" << batch->num_rows();
    if (VLOG_IS_ON(2)) {
      for (int i = 0; i < batch->num_rows(); ++i) {
        TupleRow* row = batch->GetRow(i);
        VLOG(2) << PrintRow(row, executor.row_desc());
      }
    }
    RETURN_IF_ERROR(sender.Send(batch));
    batch = NULL;
  }
  RETURN_IF_ERROR(sender.Close());

  VLOG(1) << "finished ExecPlanFragment";
  return Status::OK;
}

void ImpalaBackend::TransmitData(
    TStatus& return_val, const TUniqueId& query_id, const TPlanNodeId dest_node_id,
    const TRowBatch& thrift_batch) {
  // TODO: fix Thrift so it doesn't force me to make copies all over the place
  TRowBatch* batch_copy = new TRowBatch(thrift_batch);
  stream_mgr_->AddData(query_id, dest_node_id, batch_copy).ToThrift(&return_val);
}

void ImpalaBackend::CloseChannel(
    TStatus& return_val, const TUniqueId& query_id, const TPlanNodeId dest_node_id) {
  stream_mgr_->CloseChannel(query_id, dest_node_id).ToThrift(&return_val);
}

TServer* StartImpalaBackendService(DataStreamMgr* stream_mgr, int port) {
  shared_ptr<TProtocolFactory> protocol_factory(new TBinaryProtocolFactory());
  shared_ptr<ImpalaBackend> handler(new ImpalaBackend(stream_mgr));
  shared_ptr<TProcessor> processor(new ImpalaBackendServiceProcessor(handler));
  shared_ptr<TServerTransport> server_transport(new TServerSocket(port));
  shared_ptr<TTransportFactory> transport_factory(new TBufferedTransportFactory());
  shared_ptr<ThreadManager> thread_mgr(ThreadManager::newSimpleThreadManager());
  // TODO: do we want a BoostThreadFactory?
  shared_ptr<ThreadFactory> thread_factory(new PosixThreadFactory());
  thread_mgr->threadFactory(thread_factory);
  thread_mgr->start();

  VLOG(1) << "ImpalaBackend listening on " << port;
  TThreadPoolServer* server = new TThreadPoolServer(
      processor, server_transport, transport_factory, protocol_factory,
      thread_mgr);
  return server;
}

}
