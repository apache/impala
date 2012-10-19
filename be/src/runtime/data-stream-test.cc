// Copyright (c) 2012 Cloudera, Inc. All rights reserved.

#include <boost/thread/thread.hpp>
#include <protocol/TBinaryProtocol.h>
#include <transport/TServerSocket.h>
#include <transport/TTransportUtils.h>
#include <gtest/gtest.h>

#include "common/logging.h"
#include "common/status.h"
#include "codegen/llvm-codegen.h"
#include "runtime/row-batch.h"
#include "runtime/data-stream-mgr.h"
#include "runtime/data-stream-sender.h"
#include "runtime/data-stream-recvr.h"
#include "runtime/descriptors.h"
#include "testutil/test-exec-env.h"
#include "util/authorization.h"
#include "util/cpu-info.h"
#include "util/disk-info.h"
#include "util/debug-util.h"
#include "util/thrift-server.h"
#include "gen-cpp/ImpalaInternalService.h"
#include "gen-cpp/ImpalaInternalService_types.h"
#include "gen-cpp/Types_types.h"
#include "gen-cpp/Descriptors_types.h"

using namespace std;
using namespace tr1;
using namespace boost;

using namespace impala;
using namespace apache::thrift;
using namespace apache::thrift::protocol;

DECLARE_int32(port);
DEFINE_string(principal, "", "Kerberos principal");
DEFINE_string(keytab_file, "", "Kerberos keytab");

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
      mgr_->AddData(params.dest_fragment_instance_id, params.dest_node_id,
                    params.row_batch).SetTStatus(&return_val);
    } else {
      mgr_->CloseSender(params.dest_fragment_instance_id, params.dest_node_id)
          .SetTStatus(&return_val);
    }
  }

 private:
  DataStreamMgr* mgr_;
};

class DataStreamTest : public testing::Test {
 protected:
  DataStreamTest(): next_val_(0) {}

  virtual void SetUp() {
    next_instance_id_.lo = 0;
    next_instance_id_.hi = 0;
    stream_mgr_ = new DataStreamMgr();
    sink_.dest_node_id = DEST_NODE_ID;
    sink_.output_partition.type = TPartitionType::UNPARTITIONED;
    // Ensure that individual sender info addresses don't change
    sender_info_.reserve(MAX_SENDERS);
    receiver_info_.reserve(MAX_RECEIVERS);
    CreateRowDesc();
    CreateRowBatch();
    StartBackend();
  }

  // We reserve contiguous memory for senders in SetUp. If a test uses more
  // senders, a DCHECK will fail and you should increase this value.
  static const int MAX_SENDERS = 16;
  static const int MAX_RECEIVERS = 16;
  static const PlanNodeId DEST_NODE_ID = 1;
  static const int BATCH_CAPACITY = 100;  // rows
  static const int PER_ROW_DATA = 8;
  static const int TOTAL_DATA_SIZE = 8 * 1024;
  static const int NUM_BATCHES = TOTAL_DATA_SIZE / BATCH_CAPACITY / PER_ROW_DATA;

  ObjectPool obj_pool_;
  DescriptorTbl* desc_tbl_;
  const RowDescriptor* row_desc_;
  TUniqueId next_instance_id_;
  string stmt_;

  // RowBatch generation
  scoped_ptr<RowBatch> batch_;
  int next_val_;
  int64_t* tuple_mem_;

  // receiving node
  DataStreamMgr* stream_mgr_;
  ThriftServer* server_;

  // sending node(s)
  TDataStreamSink sink_;
  vector<TPlanFragmentDestination> dest_;

  struct SenderInfo {
    thread* thread_handle;
    Status status;
    int num_bytes_sent;

    SenderInfo(): thread_handle(NULL), num_bytes_sent(0) {}
  };
  vector<SenderInfo> sender_info_;

  struct ReceiverInfo {
    thread* thread_handle;
    DataStreamRecvr* stream_recvr;
    Status status;
    int num_rows_received;

    ReceiverInfo(): thread_handle(NULL), num_rows_received(0) {}
  };
  vector<ReceiverInfo> receiver_info_;

  // Create an instance id and add it to dest_
  void GetNextInstanceId(TUniqueId* instance_id) {
    dest_.push_back(TPlanFragmentDestination());
    TPlanFragmentDestination& dest = dest_.back();
    dest.fragment_instance_id = next_instance_id_;
    dest.server.ipaddress = "127.0.0.1";
    dest.server.port = FLAGS_port;
    *instance_id = next_instance_id_;
    ++next_instance_id_.lo;
  }

  // RowDescriptor to mimic "select bigint_col from alltypesagg", except the slot
  // isn't nullable
  void CreateRowDesc() {
    // create DescriptorTbl
    TTupleDescriptor tuple_desc;
    tuple_desc.__set_id(0);
    tuple_desc.__set_byteSize(8);
    tuple_desc.__set_numNullBytes(0);
    TDescriptorTable thrift_desc_tbl;
    thrift_desc_tbl.tupleDescriptors.push_back(tuple_desc);
    TSlotDescriptor slot_desc;
    slot_desc.__set_id(0);
    slot_desc.__set_parent(0);
    slot_desc.__set_slotType(TPrimitiveType::BIGINT);
    slot_desc.__set_columnPos(0);
    slot_desc.__set_byteOffset(0);
    slot_desc.__set_nullIndicatorByte(-1);
    slot_desc.__set_nullIndicatorBit(-1);
    slot_desc.__set_slotIdx(0);
    slot_desc.__set_isMaterialized(true);
    thrift_desc_tbl.slotDescriptors.push_back(slot_desc);
    EXPECT_TRUE(DescriptorTbl::Create(&obj_pool_, thrift_desc_tbl, &desc_tbl_).ok());

    vector<TTupleId> row_tids;
    row_tids.push_back(0);
    vector<bool> nullable_tuples;
    nullable_tuples.push_back(false);
    row_desc_ = obj_pool_.Add(new RowDescriptor(*desc_tbl_, row_tids, nullable_tuples));
  }

  // Create batch_, but don't fill it with data yet. Assumes we created row_desc_.
  RowBatch* CreateRowBatch() {
    RowBatch* batch = new RowBatch(*row_desc_, BATCH_CAPACITY);
    int64_t* tuple_mem = reinterpret_cast<int64_t*>(
        batch->tuple_data_pool()->Allocate(BATCH_CAPACITY * 8));
    bzero(tuple_mem, BATCH_CAPACITY * 8);
    for (int i = 0; i < BATCH_CAPACITY; ++i) {
      int idx = batch->AddRow();
      TupleRow* row = batch->GetRow(idx);
      row->SetTuple(0, reinterpret_cast<Tuple*>(&tuple_mem[i]));
      batch->CommitLastRow();
    }
    return batch;
  }

 void GetNextBatch(RowBatch* batch, int* next_val) {
    for (int i = 0; i < BATCH_CAPACITY; ++i) {
      TupleRow* row = batch->GetRow(i);
      int64_t* val = reinterpret_cast<int64_t*>(row->GetTuple(0)->GetSlot(0));
      *val = (*next_val)++;
    }
  }

  // Start receiver (expecting given number of senders) in separate thread.
  void StartReceiver(int num_senders, int buffer_size, TUniqueId* out_id = NULL) {
    TUniqueId instance_id;
    GetNextInstanceId(&instance_id);
    receiver_info_.push_back(ReceiverInfo());
    ReceiverInfo& info = receiver_info_.back();
    info.stream_recvr =
        stream_mgr_->CreateRecvr(
            *row_desc_, instance_id, DEST_NODE_ID, num_senders, buffer_size);
    info.thread_handle =
        new thread(&DataStreamTest::ReadStream, this, num_senders, &info);
    if (out_id != NULL) *out_id = instance_id;
  }

  void JoinReceivers() {
    for (int i = 0; i < receiver_info_.size(); ++i) {
      receiver_info_[i].thread_handle->join();
    }
  }

  // Deplete stream and print batches
  void ReadStream(int num_senders, ReceiverInfo* info) {
    RowBatch* batch;
    VLOG_QUERY <<  "start reading";
    bool is_cancelled;
    multiset<int64_t> data_values;
    while ((batch = info->stream_recvr->GetBatch(&is_cancelled)) != NULL
        && !is_cancelled) {
      VLOG_QUERY << "read batch #rows=" << (batch != NULL ? batch->num_rows() : 0);
      info->num_rows_received += batch->num_rows();
      for (int i = 0; i < batch->num_rows(); ++i) {
        TupleRow* row = batch->GetRow(i);
        data_values.insert(*static_cast<int64_t*>(row->GetTuple(0)->GetSlot(0)));
      }
      usleep(100000);  // slow down receiver to exercise buffering logic
    }
    if (is_cancelled) VLOG_QUERY << "reader is cancelled";
    info->status = (is_cancelled ? Status::CANCELLED : Status::OK);

    if (!is_cancelled) {
      // check contents of batches
      int64_t expected_val;
      EXPECT_EQ(data_values.size(), NUM_BATCHES * BATCH_CAPACITY * num_senders);
      int j = 0;
      for (multiset<int64_t>::iterator i = data_values.begin(); i != data_values.end();
           ++i, ++j) {
        expected_val = j / num_senders;
        EXPECT_EQ(expected_val, *i);
      }
    }

    VLOG_QUERY << "done reading";
  }


  // Start backend in separate thread.
  void StartBackend() {
    shared_ptr<ImpalaTestBackend> handler(new ImpalaTestBackend(stream_mgr_));
    shared_ptr<TProcessor> processor(new ImpalaInternalServiceProcessor(handler));
    server_ = new ThriftServer("DataStreamTest backend", processor, FLAGS_port);
    server_->Start();
  }

  void StopBackend() {
    server_->StopForTesting();
    delete server_;
  }

  void StartSender(int channel_buffer_size = 1024) {
    int num_senders = sender_info_.size();
    DCHECK_LT(num_senders, MAX_SENDERS);
    sender_info_.push_back(SenderInfo());
    SenderInfo& info = sender_info_.back();
    info.thread_handle =
        new thread(&DataStreamTest::Sender, this, num_senders, channel_buffer_size);
  }

  void JoinSenders() {
    for (int i = 0; i < sender_info_.size(); ++i) {
      sender_info_[i].thread_handle->join();
    }
  }

  void Sender(int sender_num, int channel_buffer_size) {
    VLOG_QUERY << "create sender " << sender_num;
    DataStreamSender sender(
        *row_desc_, sink_, dest_, channel_buffer_size);
    EXPECT_TRUE(sender.Init(NULL).ok());
    scoped_ptr<RowBatch> batch(CreateRowBatch());
    SenderInfo& info = sender_info_[sender_num];
    int next_val = 0;
    for (int i = 0; i < NUM_BATCHES; ++i) {
      GetNextBatch(batch.get(), &next_val);
      VLOG_QUERY << "sender " << sender_num << ": #rows=" << batch->num_rows();
      info.status = sender.Send(NULL, batch.get());
      if (!info.status.ok()) break;
    }
    VLOG_QUERY << "closing sender" << sender_num;
    info.status = sender.Close(NULL);
    info.num_bytes_sent = sender.GetNumDataBytesSent();
  }
};

TEST_F(DataStreamTest, SingleSenderSmallBuffer) {
  VLOG_QUERY << "start receiver\n";
  StartReceiver(1, 1024);
  VLOG_QUERY << "start sender\n";
  StartSender();
  VLOG_QUERY << "join senders\n";
  JoinSenders();
  EXPECT_TRUE(sender_info_[0].status.ok());
  EXPECT_GT(sender_info_[0].num_bytes_sent, 0);
  VLOG_QUERY << "join receiver\n";
  JoinReceivers();
  EXPECT_TRUE(receiver_info_[0].status.ok());
  EXPECT_EQ(receiver_info_[0].num_rows_received, NUM_BATCHES * BATCH_CAPACITY);
  VLOG_QUERY << "stop backend\n";
  StopBackend();
}

TEST_F(DataStreamTest, MultipleReceiversSmallBuffer) {
  TUniqueId instance1, instance2;
  StartReceiver(1, 1024);
  StartReceiver(1, 1024);
  StartSender();
  VLOG_QUERY << "join senders\n";
  JoinSenders();
  EXPECT_TRUE(sender_info_[0].status.ok());
  EXPECT_GT(sender_info_[0].num_bytes_sent, 0);
  VLOG_QUERY << "join receiver\n";
  JoinReceivers();
  EXPECT_TRUE(receiver_info_[0].status.ok());
  EXPECT_EQ(receiver_info_[0].num_rows_received, NUM_BATCHES * BATCH_CAPACITY);
  EXPECT_TRUE(receiver_info_[1].status.ok());
  EXPECT_EQ(receiver_info_[1].num_rows_received, NUM_BATCHES * BATCH_CAPACITY);
  VLOG_QUERY << "stop backend\n";
  StopBackend();
}

TEST_F(DataStreamTest, SingleSenderLargeBuffer) {
  VLOG_QUERY << "start receiver\n";
  StartReceiver(1, 1024 * 1024);
  VLOG_QUERY << "start sender\n";
  StartSender();
  VLOG_QUERY << "join senders\n";
  JoinSenders();
  EXPECT_TRUE(sender_info_[0].status.ok());
  EXPECT_GT(sender_info_[0].num_bytes_sent, 0);
  VLOG_QUERY << "join receiver\n";
  JoinReceivers();
  EXPECT_TRUE(receiver_info_[0].status.ok());
  EXPECT_EQ(receiver_info_[0].num_rows_received, NUM_BATCHES * BATCH_CAPACITY);
  VLOG_QUERY << "stop backend\n";
  StopBackend();
}

TEST_F(DataStreamTest, MultipleSendersSmallBuffer) {
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
  JoinReceivers();
  EXPECT_TRUE(receiver_info_[0].status.ok());
  EXPECT_EQ(receiver_info_[0].num_rows_received, 4 * NUM_BATCHES * BATCH_CAPACITY);
  VLOG_QUERY << "stop backend\n";
  StopBackend();
}

TEST_F(DataStreamTest, MultipleSendersLargeBuffer) {
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
  JoinReceivers();
  EXPECT_TRUE(receiver_info_[0].status.ok());
  EXPECT_EQ(receiver_info_[0].num_rows_received, 4 * NUM_BATCHES * BATCH_CAPACITY);
  VLOG_QUERY << "stop backend\n";
  StopBackend();
}

TEST_F(DataStreamTest, UnknownSenderSmallResult) {
  // starting a sender w/o a corresponding receiver should result in an error
  // on the sending side
  // case 1: entire query result fits in single buffer, close() returns error status
  TUniqueId dummy_id;
  GetNextInstanceId(&dummy_id);
  StartSender(TOTAL_DATA_SIZE + 1024);
  JoinSenders();
  EXPECT_FALSE(sender_info_[0].status.ok());
  EXPECT_EQ(sender_info_[0].num_bytes_sent, 0);
  StopBackend();
}

TEST_F(DataStreamTest, UnknownSenderLargeResult) {
  // case 2: query result requires multiple buffers, send() returns error status
  TUniqueId dummy_id;
  GetNextInstanceId(&dummy_id);
  StartSender();
  JoinSenders();
  EXPECT_FALSE(sender_info_[0].status.ok());
  EXPECT_EQ(sender_info_[0].num_bytes_sent, 0);
  StopBackend();
}

TEST_F(DataStreamTest, Cancel) {
  TUniqueId instance_id;
  StartReceiver(1, 1024, &instance_id);
  stream_mgr_->Cancel(instance_id);
  JoinReceivers();
  EXPECT_TRUE(receiver_info_[0].status.IsCancelled());
  StopBackend();
}

// TODO: more tests:
// - TEST_F(DataStreamTest, SingleSenderMultipleReceivers)
// - TEST_F(DataStreamTest, MultipleSendersMultipleReceivers)
// - test case for transmission error in last batch
// - receivers getting created concurrently

}

int main(int argc, char **argv) {
  google::ParseCommandLineFlags(&argc, &argv, true);
  google::InitGoogleLogging(argv[0]);
  ::testing::InitGoogleTest(&argc, argv);
  impala::CpuInfo::Init();
  impala::DiskInfo::Init();
  impala::LlvmCodeGen::InitializeLlvm();
  if (!FLAGS_principal.empty()) {
    EXIT_IF_ERROR(InitKerberos("data-stream-test"));
  }

  return RUN_ALL_TESTS();
}
