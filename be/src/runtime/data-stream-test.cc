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

#include <boost/thread/thread.hpp>
#include <gtest/gtest.h>

#include "common/init.h"
#include "common/logging.h"
#include "common/status.h"
#include "codegen/llvm-codegen.h"
#include "rpc/auth-provider.h"
#include "rpc/thrift-server.h"
#include "runtime/row-batch.h"
#include "runtime/runtime-state.h"
#include "runtime/data-stream-mgr.h"
#include "runtime/data-stream-sender.h"
#include "runtime/data-stream-recvr.h"
#include "runtime/descriptors.h"
#include "runtime/client-cache.h"
#include "runtime/raw-value.h"
#include "util/cpu-info.h"
#include "util/disk-info.h"
#include "util/debug-util.h"
#include "util/thread.h"
#include "util/time.h"
#include "util/mem-info.h"
#include "gen-cpp/ImpalaInternalService.h"
#include "gen-cpp/ImpalaInternalService_types.h"
#include "gen-cpp/Types_types.h"
#include "gen-cpp/Descriptors_types.h"

#include <iostream>

using namespace std;
using namespace tr1;
using namespace boost;

using namespace impala;
using namespace apache::thrift;
using namespace apache::thrift::protocol;

DEFINE_int32(port, 20001, "port on which to run Impala test backend");
DECLARE_string(principal);

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
  DataStreamTest()
    : runtime_state_(TUniqueId(), TUniqueId(), TQueryOptions(), "", "", &exec_env_),
      next_val_(0) {
  }

  virtual void SetUp() {
    CreateRowDesc();
    CreateRowBatch();

    next_instance_id_.lo = 0;
    next_instance_id_.hi = 0;
    stream_mgr_ = new DataStreamMgr();

    broadcast_sink_.dest_node_id = DEST_NODE_ID;
    broadcast_sink_.output_partition.type = TPartitionType::UNPARTITIONED;

    hash_sink_.dest_node_id = DEST_NODE_ID;
    hash_sink_.output_partition.type = TPartitionType::HASH_PARTITIONED;
    // there's only one column to partition on
    TExprNode expr_node;
    expr_node.node_type = TExprNodeType::SLOT_REF;
    expr_node.type.type = TPrimitiveType::BIGINT;
    expr_node.num_children = 0;
    TSlotRef slot_ref;
    slot_ref.slot_id = 0;
    expr_node.__set_slot_ref(slot_ref);
    TExpr expr;
    expr.nodes.push_back(expr_node);
    hash_sink_.output_partition.__isset.partition_exprs = true;
    hash_sink_.output_partition.partition_exprs.push_back(expr);

    // Ensure that individual sender info addresses don't change
    sender_info_.reserve(MAX_SENDERS);
    receiver_info_.reserve(MAX_RECEIVERS);
    StartBackend();
  }

  virtual void TearDown() {
    exec_env_.client_cache()->TestShutdown();
    StopBackend();
  }

  void Reset() {
    sender_info_.clear();
    receiver_info_.clear();
    dest_.clear();
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
  MemTracker tracker_;
  DescriptorTbl* desc_tbl_;
  const RowDescriptor* row_desc_;
  MemTracker dummy_mem_tracker_;
  ExecEnv exec_env_;
  RuntimeState runtime_state_;
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
  TDataStreamSink broadcast_sink_;
  TDataStreamSink hash_sink_;
  vector<TPlanFragmentDestination> dest_;

  struct SenderInfo {
    thread* thread_handle;
    Status status;
    int num_bytes_sent;

    SenderInfo(): thread_handle(NULL), num_bytes_sent(0) {}
  };
  vector<SenderInfo> sender_info_;

  struct ReceiverInfo {
    TPartitionType::type stream_type;
    int num_senders;
    int receiver_num;

    thread* thread_handle;
    DataStreamRecvr* stream_recvr;
    Status status;
    int num_rows_received;
    multiset<int64_t> data_values;

    ReceiverInfo(TPartitionType::type stream_type, int num_senders, int receiver_num)
      : stream_type(stream_type),
        num_senders(num_senders),
        receiver_num(receiver_num),
        thread_handle(NULL),
        stream_recvr(NULL),
        num_rows_received(0) {}

    ~ReceiverInfo() {
      delete thread_handle;
      delete stream_recvr;
    }
  };
  vector<ReceiverInfo> receiver_info_;

  // Create an instance id and add it to dest_
  void GetNextInstanceId(TUniqueId* instance_id) {
    dest_.push_back(TPlanFragmentDestination());
    TPlanFragmentDestination& dest = dest_.back();
    dest.fragment_instance_id = next_instance_id_;
    dest.server.hostname = "127.0.0.1";
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
    slot_desc.__set_nullIndicatorByte(0);
    slot_desc.__set_nullIndicatorBit(-1);
    slot_desc.__set_slotIdx(0);
    slot_desc.__set_isMaterialized(true);
    thrift_desc_tbl.slotDescriptors.push_back(slot_desc);
    EXPECT_TRUE(DescriptorTbl::Create(&obj_pool_, thrift_desc_tbl, &desc_tbl_).ok());
    runtime_state_.set_desc_tbl(desc_tbl_);

    vector<TTupleId> row_tids;
    row_tids.push_back(0);
    vector<bool> nullable_tuples;
    nullable_tuples.push_back(false);
    row_desc_ = obj_pool_.Add(new RowDescriptor(*desc_tbl_, row_tids, nullable_tuples));
  }

  // Create batch_, but don't fill it with data yet. Assumes we created row_desc_.
  RowBatch* CreateRowBatch() {
    RowBatch* batch = new RowBatch(*row_desc_, BATCH_CAPACITY, &tracker_);
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
  void StartReceiver(TPartitionType::type stream_type, int num_senders, int receiver_num,
                     int buffer_size, TUniqueId* out_id = NULL) {
    VLOG_QUERY << "start receiver";
    RuntimeProfile* profile =
        obj_pool_.Add(new RuntimeProfile(&obj_pool_, "TestReceiver"));
    TUniqueId instance_id;
    GetNextInstanceId(&instance_id);
    receiver_info_.push_back(ReceiverInfo(stream_type, num_senders, receiver_num));
    ReceiverInfo& info = receiver_info_.back();
    info.stream_recvr =
        stream_mgr_->CreateRecvr(&runtime_state_,
            *row_desc_, instance_id, DEST_NODE_ID, num_senders, buffer_size, profile);
    info.thread_handle =
        new thread(&DataStreamTest::ReadStream, this, &info);
    if (out_id != NULL) *out_id = instance_id;
  }

  void JoinReceivers() {
    VLOG_QUERY << "join receiver\n";
    for (int i = 0; i < receiver_info_.size(); ++i) {
      receiver_info_[i].thread_handle->join();
      receiver_info_[i].stream_recvr->Close();
    }
  }

  // Deplete stream and print batches
  void ReadStream(ReceiverInfo* info) {
    RowBatch* batch;
    VLOG_QUERY <<  "start reading";
    bool is_cancelled;
    while ((batch = info->stream_recvr->GetBatch(&is_cancelled)) != NULL
        && !is_cancelled) {
      VLOG_QUERY << "read batch #rows=" << (batch != NULL ? batch->num_rows() : 0);
      for (int i = 0; i < batch->num_rows(); ++i) {
        TupleRow* row = batch->GetRow(i);
        info->data_values.insert(*static_cast<int64_t*>(row->GetTuple(0)->GetSlot(0)));
      }
      SleepForMs(100);  // slow down receiver to exercise buffering logic
    }
    if (is_cancelled) VLOG_QUERY << "reader is cancelled";
    info->status = (is_cancelled ? Status::CANCELLED : Status::OK);
    VLOG_QUERY << "done reading";
  }

  // Verify correctness of receivers' data values.
  void CheckReceivers(TPartitionType::type stream_type, int num_senders) {
    int64_t total = 0;
    multiset<int64_t> all_data_values;
    for (int i = 0; i < receiver_info_.size(); ++i) {
      ReceiverInfo& info = receiver_info_[i];
      EXPECT_TRUE(info.status.ok());
      total += info.data_values.size();
      DCHECK_EQ(info.stream_type, stream_type);
      DCHECK_EQ(info.num_senders, num_senders);
      if (stream_type == TPartitionType::UNPARTITIONED) {
        EXPECT_EQ(
            NUM_BATCHES * BATCH_CAPACITY * num_senders, info.data_values.size());
      }
      all_data_values.insert(info.data_values.begin(), info.data_values.end());

      int k = 0;
      for (multiset<int64_t>::iterator j = info.data_values.begin();
           j != info.data_values.end(); ++j, ++k) {
        if (stream_type == TPartitionType::UNPARTITIONED) {
          // unpartitioned streams contain all values as many times as there are
          // senders
          EXPECT_EQ(k / num_senders, *j);
        } else if (stream_type == TPartitionType::HASH_PARTITIONED) {
          // hash-partitioned streams send values to the right partition
          int64_t value = *j;
          EXPECT_EQ(
              RawValue::GetHashValueFnv(&value, TYPE_BIGINT, 0) % receiver_info_.size(),
              info.receiver_num);
        }
      }
    }

    if (stream_type == TPartitionType::HASH_PARTITIONED) {
      EXPECT_EQ(NUM_BATCHES * BATCH_CAPACITY * num_senders, total);

      int k = 0;
      for (multiset<int64_t>::iterator j = all_data_values.begin();
           j != all_data_values.end(); ++j, ++k) {
        // each sender sent all values
        EXPECT_EQ(k / num_senders, *j);
        if (k/num_senders != *j) break;
      }
    }
  }

  void CheckSenders() {
    for (int i = 0; i < sender_info_.size(); ++i) {
      EXPECT_TRUE(sender_info_[i].status.ok());
      EXPECT_GT(sender_info_[i].num_bytes_sent, 0);
    }
  }

  // Start backend in separate thread.
  void StartBackend() {
    shared_ptr<ImpalaTestBackend> handler(new ImpalaTestBackend(stream_mgr_));
    shared_ptr<TProcessor> processor(new ImpalaInternalServiceProcessor(handler));
    server_ = new ThriftServer("DataStreamTest backend", processor, FLAGS_port, NULL);
    server_->Start();
  }

  void StopBackend() {
    VLOG_QUERY << "stop backend\n";
    server_->StopForTesting();
    delete server_;
  }

  void StartSender(TPartitionType::type partition_type = TPartitionType::UNPARTITIONED,
                   int channel_buffer_size = 1024) {
    VLOG_QUERY << "start sender";
    int num_senders = sender_info_.size();
    DCHECK_LT(num_senders, MAX_SENDERS);
    sender_info_.push_back(SenderInfo());
    SenderInfo& info = sender_info_.back();
    info.thread_handle =
        new thread(&DataStreamTest::Sender, this, num_senders, channel_buffer_size,
                   partition_type);
  }

  void JoinSenders() {
    VLOG_QUERY << "join senders\n";
    for (int i = 0; i < sender_info_.size(); ++i) {
      sender_info_[i].thread_handle->join();
    }
  }

  void Sender(int sender_num, int channel_buffer_size,
              TPartitionType::type partition_type) {
    RuntimeState state(TUniqueId(), TUniqueId(), TQueryOptions(), "", "", &exec_env_);
    state.set_desc_tbl(desc_tbl_);
    VLOG_QUERY << "create sender " << sender_num;
    const TDataStreamSink& sink =
        (partition_type == TPartitionType::UNPARTITIONED ? broadcast_sink_ : hash_sink_);
    DataStreamSender sender(
        &obj_pool_, *row_desc_, sink, dest_, channel_buffer_size);
    EXPECT_TRUE(sender.Init(&state).ok());
    scoped_ptr<RowBatch> batch(CreateRowBatch());
    SenderInfo& info = sender_info_[sender_num];
    int next_val = 0;
    for (int i = 0; i < NUM_BATCHES; ++i) {
      GetNextBatch(batch.get(), &next_val);
      VLOG_QUERY << "sender " << sender_num << ": #rows=" << batch->num_rows();
      info.status = sender.Send(&state, batch.get(), false);
      if (!info.status.ok()) break;
    }
    VLOG_QUERY << "closing sender" << sender_num;
    sender.Close(&state);
    info.num_bytes_sent = sender.GetNumDataBytesSent();

    batch->Reset();
  }

  void TestStream(TPartitionType::type stream_type, int num_senders,
                  int num_receivers, int buffer_size) {
    LOG(INFO) << "Testing stream=" << stream_type << " #senders=" << num_senders
              << " #receivers=" << num_receivers << " buffer_size=" << buffer_size;
    Reset();
    for (int i = 0; i < num_receivers; ++i) {
      StartReceiver(stream_type, num_senders, i, buffer_size);
    }
    for (int i = 0; i < num_senders; ++i) {
      StartSender(stream_type, buffer_size);
    }
    JoinSenders();
    CheckSenders();
    JoinReceivers();
    CheckReceivers(stream_type, num_senders);
  }
};

TEST_F(DataStreamTest, UnknownSenderSmallResult) {
  // starting a sender w/o a corresponding receiver does not result in an error because
  // we cannot distinguish whether a receiver was never created or the receiver
  // willingly tore down the stream
  // case 1: entire query result fits in single buffer, close() returns ok
  TUniqueId dummy_id;
  GetNextInstanceId(&dummy_id);
  StartSender(TPartitionType::UNPARTITIONED, TOTAL_DATA_SIZE + 1024);
  JoinSenders();
  EXPECT_TRUE(sender_info_[0].status.ok());
  EXPECT_GT(sender_info_[0].num_bytes_sent, 0);
}

TEST_F(DataStreamTest, UnknownSenderLargeResult) {
  // case 2: query result requires multiple buffers, send() returns ok
  TUniqueId dummy_id;
  GetNextInstanceId(&dummy_id);
  StartSender();
  JoinSenders();
  EXPECT_TRUE(sender_info_[0].status.ok());
  EXPECT_GT(sender_info_[0].num_bytes_sent, 0);
}

TEST_F(DataStreamTest, Cancel) {
  TUniqueId instance_id;
  StartReceiver(TPartitionType::UNPARTITIONED, 1, 1, 1024, &instance_id);
  stream_mgr_->Cancel(instance_id);
  JoinReceivers();
  EXPECT_TRUE(receiver_info_[0].status.IsCancelled());
}

TEST_F(DataStreamTest, BasicTest) {
  // TODO: also test that all client connections have been returned
  TPartitionType::type stream_types[] =
      {TPartitionType::UNPARTITIONED, TPartitionType::HASH_PARTITIONED};
  int sender_nums[] = {1, 4};
  int receiver_nums[] = {1, 4};
  int buffer_sizes[] = {1024, 1024 * 1024};
  for (int i = 0; i < sizeof(stream_types) / sizeof(*stream_types); ++i) {
    for (int j = 0; j < sizeof(sender_nums) / sizeof(int); ++j) {
      for (int k = 0; k < sizeof(receiver_nums) / sizeof(int); ++k) {
        for (int l = 0; l < sizeof(buffer_sizes) / sizeof(int); ++l) {
          TestStream(stream_types[i], sender_nums[j], receiver_nums[k],
                     buffer_sizes[l]);
        }
      }
    }
  }
}

// TODO: more tests:
// - test case for transmission error in last batch
// - receivers getting created concurrently

}

int main(int argc, char **argv) {
  InitCommonRuntime(argc, argv, true);
  impala::LlvmCodeGen::InitializeLlvm();
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
