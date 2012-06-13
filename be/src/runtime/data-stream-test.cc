// Copyright (c) 2011 Cloudera, Inc. All rights reserved.

#include <boost/thread/thread.hpp>

#include <protocol/TBinaryProtocol.h>
#include <server/TSimpleServer.h>
//#include <server/TThreadPoolServer.h>
//#include <server/TThreadedServer.h>
#include <transport/TServerSocket.h>
#include <transport/TTransportUtils.h>
#include <gtest/gtest.h>
#include <glog/logging.h>
// include gflags.h *after* logging.h, otherwise the linker will complain about
// undefined references to FLAGS_v
#include <gflags/gflags.h>

#include "codegen/llvm-codegen.h"
#include "runtime/row-batch.h"
#include "runtime/data-stream-mgr.h"
#include "runtime/data-stream-sender.h"
#include "runtime/data-stream-recvr.h"
#include "runtime/descriptors.h"
#include "testutil/in-process-query-executor.h"
#include "testutil/test-exec-env.h"
#include "util/debug-util.h"
#include "gen-cpp/ImpalaInternalService.h"
#include "gen-cpp/Types_types.h"

using namespace std;
using namespace tr1;
using namespace boost;

using namespace apache::thrift;
using namespace apache::thrift::protocol;
using namespace apache::thrift::transport;
using namespace apache::thrift::server;

DECLARE_int32(port);

namespace impala {

class ImpalaTestBackend : public ImpalaInternalServiceIf {
 public:
  ImpalaTestBackend(DataStreamMgr* stream_mgr): mgr_(stream_mgr) {}
  virtual ~ImpalaTestBackend() {}

  virtual void ExecPlanFragment(
      TExecPlanFragmentResult& return_val, const TExecPlanFragmentParams& params) {}

  virtual void ReportExecStatus(
      TReportExecStatusResult& return_val, const TReportExecStatusParams& params) {}

  virtual void CancelPlanFragment(
      TCancelPlanFragmentResult& return_val, const TCancelPlanFragmentParams& params) {}

  virtual void TransmitData(
      TTransmitDataResult& return_val, const TTransmitDataParams& params) {
    if (!params.eos) {
      mgr_->AddData(params.dest_fragment_id, params.dest_node_id, params.row_batch)
          .SetTStatus(&return_val);
    } else {
      mgr_->CloseStream(params.dest_fragment_id, params.dest_node_id)
          .SetTStatus(&return_val);
    }
  }

 private:
  DataStreamMgr* mgr_;
};

class DataStreamTest : public testing::Test {
 protected:
  DataStreamTest()
    : test_env_(1, 0),
      exec_(&test_env_) {
  }

  virtual void SetUp() {
    query_id_.lo = 0;
    query_id_.hi = 0;
    stream_mgr_ = new DataStreamMgr();
    EXPECT_TRUE(exec_.Setup().ok());
    sink_.destNodeId = DEST_NODE_ID;
    dest_.push_back(THostPort());
    dest_.back().host = "localhost";
    dest_.back().port = FLAGS_port;
    backend_thread_ = thread(&DataStreamTest::StartBackend, this);
  }

  static const PlanNodeId DEST_NODE_ID = 1;

  const RowDescriptor* row_desc_;
  TestExecEnv test_env_;
  InProcessQueryExecutor exec_;
  TUniqueId query_id_;
  string stmt_;

  // receiving node
  DataStreamMgr* stream_mgr_;
  DataStreamRecvr* stream_recvr_;
  thread recvr_thread_;
  thread backend_thread_;
  TSimpleServer* server_;

  // sending node(s)
  TDataStreamSink sink_;
  vector<THostPort> dest_;

  struct SenderInfo {
    thread* thread_handle;
    Status status;
    int num_bytes_sent;

    SenderInfo(): thread_handle(NULL), num_bytes_sent(0) {}
  };

  vector<SenderInfo> sender_info_;

  void PrepareQuery(const string& stmt) {
    stmt_ = stmt;
    Status status = exec_.Exec(stmt, NULL);
    EXPECT_TRUE(status.ok()) << status.GetErrorMsg();
    row_desc_ = &exec_.row_desc();
  }

  // Start receiver (expecting given number of senders) in separate thread.
  void StartReceiver(int num_senders, int buffer_size) {
    stream_recvr_ =
        stream_mgr_->CreateRecvr(*row_desc_, query_id_, DEST_NODE_ID, num_senders,
                                 buffer_size);
    recvr_thread_ = thread(&DataStreamTest::ReadStream, this);
  }

  void JoinReceiver() {
    recvr_thread_.join();
  }

  // Deplete stream and print batches
  void ReadStream() {
    RowBatch* batch;
    VLOG_QUERY <<  "start reading\n";
    while ((batch = stream_recvr_->GetBatch()) != NULL) {
      VLOG_QUERY << "read batch #rows=" << batch->num_rows() << "\n";
      usleep(100000);  // slow down receiver to exercise buffering logic
    }
    VLOG_QUERY << "done reading\n";
  }


  // Start backend in separate thread.
  void StartBackend() {
    shared_ptr<TProtocolFactory> protocol_factory(new TBinaryProtocolFactory());
    shared_ptr<ImpalaTestBackend> handler(new ImpalaTestBackend(stream_mgr_));
    shared_ptr<TProcessor> processor(new ImpalaInternalServiceProcessor(handler));
    shared_ptr<TServerTransport> server_transport(new TServerSocket(FLAGS_port));
    shared_ptr<TTransportFactory> transport_factory(new TBufferedTransportFactory());

    server_ = new TSimpleServer(
        processor, server_transport, transport_factory, protocol_factory);
    server_->serve();
  }

  void StopBackend() {
    server_->stop();
    backend_thread_.join();
    delete server_;
  }

  void StartSender() {
    int num_senders = sender_info_.size();
    sender_info_.push_back(SenderInfo());
    SenderInfo& info = sender_info_.back();
    info.thread_handle = new thread(&DataStreamTest::Sender, this, num_senders);
  }

  void JoinSenders() {
    for (int i = 0; i < sender_info_.size(); ++i) {
      sender_info_[i].thread_handle->join();
    }
  }

  void Sender(int sender_num) {
    InProcessQueryExecutor exec(&test_env_);
    VLOG_QUERY << "exec setup";
    EXPECT_TRUE(exec.Setup().ok());
    VLOG_QUERY << "exec::exec";
    EXPECT_TRUE(exec.Exec(stmt_, NULL).ok());
    VLOG_QUERY << "create sender";
    DataStreamSender sender(exec.row_desc(), query_id_, sink_, dest_, 1024);
    EXPECT_TRUE(sender.Init(exec.runtime_state()).ok());
    RowBatch* batch = NULL;
    SenderInfo& info = sender_info_[sender_num];
    for (;;) {
      EXPECT_TRUE(exec.FetchResult(&batch).ok());
      if (batch == NULL) break;
      VLOG_QUERY << "#rows=" << batch->num_rows();
      info.status = sender.Send(exec.runtime_state(), batch);
      if (!info.status.ok()) break;
    }
    VLOG_QUERY << "closing sender\n";
    info.status = sender.Close(exec.runtime_state());
    info.num_bytes_sent = sender.GetNumDataBytesSent();
  }

};

TEST_F(DataStreamTest, SingleSenderSmallBuffer) {
  PrepareQuery("select * from alltypesagg");
  VLOG_QUERY << "start receiver\n";
  StartReceiver(1, 1024);
  VLOG_QUERY << "start sender\n";
  StartSender();
  VLOG_QUERY << "join senders\n";
  JoinSenders();
  EXPECT_TRUE(sender_info_[0].status.ok());
  EXPECT_GT(sender_info_[0].num_bytes_sent, 0);
  VLOG_QUERY << "join receiver\n";
  JoinReceiver();
  VLOG_QUERY << "stop backend\n";
  StopBackend();
}

TEST_F(DataStreamTest, SingleSenderLargeBuffer) {
  PrepareQuery("select * from alltypesagg");
  VLOG_QUERY << "start receiver\n";
  StartReceiver(1, 1024 * 1024);
  VLOG_QUERY << "start sender\n";
  StartSender();
  VLOG_QUERY << "join senders\n";
  JoinSenders();
  EXPECT_TRUE(sender_info_[0].status.ok());
  EXPECT_GT(sender_info_[0].num_bytes_sent, 0);
  VLOG_QUERY << "join receiver\n";
  JoinReceiver();
  VLOG_QUERY << "stop backend\n";
  StopBackend();
}

TEST_F(DataStreamTest, MultipleSendersSmallBuffer) {
  PrepareQuery("select * from alltypessmall");
  VLOG_QUERY << "start receiver\n";
  StartReceiver(4, 4 * 1024);
  VLOG_QUERY << "start senders\n";
  StartSender();
  StartSender();
  StartSender();
  StartSender();
  VLOG_QUERY << "join senders\n";
  JoinSenders();
  EXPECT_TRUE(sender_info_[0].status.ok());
  EXPECT_GT(sender_info_[0].num_bytes_sent, 0);
  EXPECT_TRUE(sender_info_[1].status.ok());
  EXPECT_GT(sender_info_[1].num_bytes_sent, 0);
  EXPECT_TRUE(sender_info_[2].status.ok());
  EXPECT_GT(sender_info_[2].num_bytes_sent, 0);
  EXPECT_TRUE(sender_info_[3].status.ok());
  EXPECT_GT(sender_info_[3].num_bytes_sent, 0);
  VLOG_QUERY << "join receiver\n";
  JoinReceiver();
  VLOG_QUERY << "stop backend\n";
  StopBackend();
}

TEST_F(DataStreamTest, MultipleSendersLargeBuffer) {
  PrepareQuery("select * from alltypessmall");
  VLOG_QUERY << "start receiver\n";
  StartReceiver(4, 4 * 1024 * 1024);
  VLOG_QUERY << "start senders\n";
  StartSender();
  StartSender();
  StartSender();
  StartSender();
  VLOG_QUERY << "join senders\n";
  JoinSenders();
  EXPECT_TRUE(sender_info_[0].status.ok());
  EXPECT_GT(sender_info_[0].num_bytes_sent, 0);
  EXPECT_TRUE(sender_info_[1].status.ok());
  EXPECT_GT(sender_info_[1].num_bytes_sent, 0);
  EXPECT_TRUE(sender_info_[2].status.ok());
  EXPECT_GT(sender_info_[2].num_bytes_sent, 0);
  EXPECT_TRUE(sender_info_[3].status.ok());
  EXPECT_GT(sender_info_[3].num_bytes_sent, 0);
  VLOG_QUERY << "join receiver\n";
  JoinReceiver();
  VLOG_QUERY << "stop backend\n";
  StopBackend();
}

TEST_F(DataStreamTest, UnknownSenderSmallResult) {
  // starting a sender w/o a corresponding receiver should result in an error
  // on the sending side
  // case 1: entire query result fits in single buffer, close() returns error status
  PrepareQuery("select * from alltypessmall");
  StartSender();
  JoinSenders();
  EXPECT_FALSE(sender_info_[0].status.ok());
  EXPECT_EQ(sender_info_[0].num_bytes_sent, 0);
}

TEST_F(DataStreamTest, UnknownSenderLargeResult) {
  // case 2: query result requires multiple buffers, send() returns error status
  PrepareQuery("select * from alltypesagg");
  StartSender();
  JoinSenders();
  EXPECT_FALSE(sender_info_[0].status.ok());
  EXPECT_EQ(sender_info_[0].num_bytes_sent, 0);
}

// TODO: more tests:
// - TEST_F(DataStreamTest, SingleSenderMultipleReceivers)
// - TEST_F(DataStreamTest, MultipleSendersMultipleReceivers)
// - test case for transmission error in last batch
// - receivers getting created concurrently

}

int main(int argc, char **argv) {
  google::InitGoogleLogging(argv[0]);
  ::testing::InitGoogleTest(&argc, argv);
  impala::LlvmCodeGen::InitializeLlvm();
  return RUN_ALL_TESTS();
}
