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

#include "runtime/row-batch.h"
#include "runtime/data-stream-mgr.h"
#include "runtime/data-stream-sender.h"
#include "runtime/data-stream-recvr.h"
#include "runtime/descriptors.h"
#include "testutil/query-executor.h"
#include "testutil/test-env.h"
#include "util/debug-util.h"
#include "gen-cpp/ImpalaBackendService.h"
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

class ImpalaBackend : public ImpalaBackendServiceIf {
 public:
  ImpalaBackend(DataStreamMgr* stream_mgr): mgr_(stream_mgr) {}
  virtual ~ImpalaBackend() {}

  virtual void ExecPlanFragment(
      TExecPlanFragmentResult& _return, const TPlanExecRequest& request, 
      const TPlanExecParams& params) {
  }

  virtual void TransmitData(
      TStatus& return_val, const TUniqueId& query_id, const TPlanNodeId dest_node_id,
      const TRowBatch& thrift_batch) {
    mgr_->AddData(query_id, dest_node_id, thrift_batch).ToThrift(&return_val);
  }

  virtual void CloseChannel(
    TStatus& return_val, const TUniqueId& query_id, const TPlanNodeId dest_node_id) {
    mgr_->CloseChannel(query_id, dest_node_id).ToThrift(&return_val);
  }

 private:
  DataStreamMgr* mgr_;
};

class DataStreamTest : public testing::Test {
 protected:
  DataStreamTest()
    : test_env_(1, 0),
      exec_(NULL, &test_env_) {
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

  const DescriptorTbl* desc_tbl_;
  TestEnv test_env_;
  QueryExecutor exec_;
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
    EXPECT_TRUE(exec_.Exec(stmt, NULL).ok());
    desc_tbl_ = &exec_.runtime_state()->desc_tbl();
  }

  // Start receiver (expecting given number of senders) in separate thread.
  void StartReceiver(int num_senders, int buffer_size) {
    stream_recvr_ =
        stream_mgr_->CreateRecvr(*desc_tbl_, query_id_, DEST_NODE_ID, num_senders,
                                 buffer_size);
    recvr_thread_ = thread(&DataStreamTest::ReadStream, this);
  }

  void JoinReceiver() {
    recvr_thread_.join();
  }

  // Deplete stream and print batches
  void ReadStream() {
    RowBatch* batch;
    VLOG(1) <<  "start reading\n";
    while ((batch = stream_recvr_->GetBatch()) != NULL) {
      VLOG(1) << "read batch #rows=" << batch->num_rows() << "\n";
      usleep(100000);  // slow down receiver to exercise buffering logic
    }
    VLOG(1) << "done reading\n";
  }


  // Start backend in separate thread.
  void StartBackend() {
    shared_ptr<TProtocolFactory> protocol_factory(new TBinaryProtocolFactory());
    shared_ptr<ImpalaBackend> handler(new ImpalaBackend(stream_mgr_));
    shared_ptr<TProcessor> processor(new ImpalaBackendServiceProcessor(handler));
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
    QueryExecutor exec(NULL, &test_env_);
    VLOG(1) << "exec setup";
    EXPECT_TRUE(exec.Setup().ok());
    VLOG(1) << "exec::exec";
    EXPECT_TRUE(exec.Exec(stmt_, NULL).ok());
    VLOG(1) << "create sender";
    DataStreamSender sender(exec.row_desc(), query_id_, sink_, dest_, 1024);
    EXPECT_TRUE(sender.Init().ok());
    RowBatch* batch = NULL;
    SenderInfo& info = sender_info_[sender_num];
    for (;;) {
      EXPECT_TRUE(exec.FetchResult(&batch).ok());
      if (batch == NULL) break;
      VLOG(1) << "#rows=" << batch->num_rows();
      info.status = sender.Send(batch);
      if (!info.status.ok()) break;
    }
    VLOG(1) << "closing sender\n";
    info.status = sender.Close();
    info.num_bytes_sent = sender.GetNumDataBytesSent();
  }

};

TEST_F(DataStreamTest, SingleSenderSmallBuffer) {
  PrepareQuery("select * from alltypesagg");
  VLOG(1) << "start receiver\n";
  StartReceiver(1, 1024);
  VLOG(1) << "start sender\n";
  StartSender();
  VLOG(1) << "join senders\n";
  JoinSenders();
  EXPECT_TRUE(sender_info_[0].status.ok());
  EXPECT_GT(sender_info_[0].num_bytes_sent, 0);
  VLOG(1) << "join receiver\n";
  JoinReceiver();
  VLOG(1) << "stop backend\n";
  StopBackend();
}

TEST_F(DataStreamTest, SingleSenderLargeBuffer) {
  PrepareQuery("select * from alltypesagg");
  VLOG(1) << "start receiver\n";
  StartReceiver(1, 1024 * 1024);
  VLOG(1) << "start sender\n";
  StartSender();
  VLOG(1) << "join senders\n";
  JoinSenders();
  EXPECT_TRUE(sender_info_[0].status.ok());
  EXPECT_GT(sender_info_[0].num_bytes_sent, 0);
  VLOG(1) << "join receiver\n";
  JoinReceiver();
  VLOG(1) << "stop backend\n";
  StopBackend();
}

TEST_F(DataStreamTest, MultipleSendersSmallBuffer) {
  PrepareQuery("select * from alltypessmall");
  VLOG(1) << "start receiver\n";
  StartReceiver(4, 4 * 1024);
  VLOG(1) << "start senders\n";
  StartSender();
  StartSender();
  StartSender();
  StartSender();
  VLOG(1) << "join senders\n";
  JoinSenders();
  EXPECT_TRUE(sender_info_[0].status.ok());
  EXPECT_GT(sender_info_[0].num_bytes_sent, 0);
  EXPECT_TRUE(sender_info_[1].status.ok());
  EXPECT_GT(sender_info_[1].num_bytes_sent, 0);
  EXPECT_TRUE(sender_info_[2].status.ok());
  EXPECT_GT(sender_info_[2].num_bytes_sent, 0);
  EXPECT_TRUE(sender_info_[3].status.ok());
  EXPECT_GT(sender_info_[3].num_bytes_sent, 0);
  VLOG(1) << "join receiver\n";
  JoinReceiver();
  VLOG(1) << "stop backend\n";
  StopBackend();
}

TEST_F(DataStreamTest, MultipleSendersLargeBuffer) {
  PrepareQuery("select * from alltypessmall");
  VLOG(1) << "start receiver\n";
  StartReceiver(4, 4 * 1024 * 1024);
  VLOG(1) << "start senders\n";
  StartSender();
  StartSender();
  StartSender();
  StartSender();
  VLOG(1) << "join senders\n";
  JoinSenders();
  EXPECT_TRUE(sender_info_[0].status.ok());
  EXPECT_GT(sender_info_[0].num_bytes_sent, 0);
  EXPECT_TRUE(sender_info_[1].status.ok());
  EXPECT_GT(sender_info_[1].num_bytes_sent, 0);
  EXPECT_TRUE(sender_info_[2].status.ok());
  EXPECT_GT(sender_info_[2].num_bytes_sent, 0);
  EXPECT_TRUE(sender_info_[3].status.ok());
  EXPECT_GT(sender_info_[3].num_bytes_sent, 0);
  VLOG(1) << "join receiver\n";
  JoinReceiver();
  VLOG(1) << "stop backend\n";
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
  return RUN_ALL_TESTS();
}
