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

#include "testutil/gtest-util.h"
#include "common/init.h"
#include "common/logging.h"
#include "common/status.h"
#include "codegen/llvm-codegen.h"
#include "exprs/slot-ref.h"
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
#include "service/fe-support.h"
#include "util/cpu-info.h"
#include "util/disk-info.h"
#include "util/debug-util.h"
#include "util/thread.h"
#include "util/time.h"
#include "util/mem-info.h"
#include "util/test-info.h"
#include "util/tuple-row-compare.h"
#include "gen-cpp/ImpalaInternalService.h"
#include "gen-cpp/ImpalaInternalService_types.h"
#include "gen-cpp/Types_types.h"
#include "gen-cpp/Descriptors_types.h"
#include "service/fe-support.h"

#include <iostream>

#include "common/names.h"

using namespace impala;
using namespace apache::thrift;
using namespace apache::thrift::protocol;

DEFINE_int32(port, 20001, "port on which to run Impala test backend");
DECLARE_string(principal);
DECLARE_int32(datastream_timeout_ms);

// We reserve contiguous memory for senders in SetUp. If a test uses more
// senders, a DCHECK will fail and you should increase this value.
static const int MAX_SENDERS = 16;
static const int MAX_RECEIVERS = 16;
static const PlanNodeId DEST_NODE_ID = 1;
static const int BATCH_CAPACITY = 100;  // rows
static const int PER_ROW_DATA = 8;
static const int TOTAL_DATA_SIZE = 8 * 1024;
static const int NUM_BATCHES = TOTAL_DATA_SIZE / BATCH_CAPACITY / PER_ROW_DATA;


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

  virtual void UpdateFilter(
      TUpdateFilterResult& return_val, const TUpdateFilterParams& params) {}

  virtual void PublishFilter(
      TPublishFilterResult& return_val, const TPublishFilterParams& params) {}

  virtual void TransmitData(
      TTransmitDataResult& return_val, const TTransmitDataParams& params) {
    if (!params.eos) {
      mgr_->AddData(params.dest_fragment_instance_id, params.dest_node_id,
                    params.row_batch, params.sender_id).SetTStatus(&return_val);
    } else {
      mgr_->CloseSender(params.dest_fragment_instance_id, params.dest_node_id,
          params.sender_id).SetTStatus(&return_val);
    }
  }

 private:
  DataStreamMgr* mgr_;
};

class DataStreamTest : public testing::Test {
 protected:
  DataStreamTest()
    : runtime_state_(TExecPlanFragmentParams(), "", &exec_env_),
      next_val_(0) {
    // Initialize Mem trackers for use by the data stream receiver.
    exec_env_.InitForFeTests();
    runtime_state_.InitMemTrackers(TUniqueId(), NULL, -1);

    // Stop tests that rely on mismatched sender / receiver pairs timing out.
    FLAGS_datastream_timeout_ms = 250;
  }

  virtual void SetUp() {
    CreateRowDesc();
    CreateTupleComparator();
    CreateRowBatch();

    next_instance_id_.lo = 0;
    next_instance_id_.hi = 0;
    stream_mgr_ = new DataStreamMgr(new MetricGroup(""));

    broadcast_sink_.dest_node_id = DEST_NODE_ID;
    broadcast_sink_.output_partition.type = TPartitionType::UNPARTITIONED;

    random_sink_.dest_node_id = DEST_NODE_ID;
    random_sink_.output_partition.type = TPartitionType::RANDOM;

    hash_sink_.dest_node_id = DEST_NODE_ID;
    hash_sink_.output_partition.type = TPartitionType::HASH_PARTITIONED;
    // there's only one column to partition on
    TExprNode expr_node;
    expr_node.node_type = TExprNodeType::SLOT_REF;
    expr_node.type.types.push_back(TTypeNode());
    expr_node.type.types.back().__isset.scalar_type = true;
    expr_node.type.types.back().scalar_type.type = TPrimitiveType::BIGINT;
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

  const TDataStreamSink& GetSink(TPartitionType::type partition_type) {
    switch (partition_type) {
      case TPartitionType::UNPARTITIONED: return broadcast_sink_;
      case TPartitionType::RANDOM: return random_sink_;
      case TPartitionType::HASH_PARTITIONED: return hash_sink_;
      default: EXPECT_TRUE(false) << "Unhandled sink type: " << partition_type;
    }
    // Should never reach this.
    return broadcast_sink_;
  }

  virtual void TearDown() {
    lhs_slot_ctx_->Close(NULL);
    rhs_slot_ctx_->Close(NULL);
    exec_env_.impalad_client_cache()->TestShutdown();
    StopBackend();
  }

  void Reset() {
    sender_info_.clear();
    receiver_info_.clear();
    dest_.clear();
  }

  ObjectPool obj_pool_;
  MemTracker tracker_;
  DescriptorTbl* desc_tbl_;
  const RowDescriptor* row_desc_;
  TupleRowComparator* less_than_;
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
  TDataStreamSink random_sink_;
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
    shared_ptr<DataStreamRecvr> stream_recvr;
    Status status;
    int num_rows_received;
    multiset<int64_t> data_values;

    ReceiverInfo(TPartitionType::type stream_type, int num_senders, int receiver_num)
      : stream_type(stream_type),
        num_senders(num_senders),
        receiver_num(receiver_num),
        thread_handle(NULL),
        num_rows_received(0) {}

    ~ReceiverInfo() {
      delete thread_handle;
      stream_recvr.reset();
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
    ColumnType type(TYPE_BIGINT);
    slot_desc.__set_slotType(type.ToThrift());
    slot_desc.__set_materializedPath(vector<int>(1, 0));
    slot_desc.__set_byteOffset(0);
    slot_desc.__set_nullIndicatorByte(0);
    slot_desc.__set_nullIndicatorBit(-1);
    slot_desc.__set_slotIdx(0);
    thrift_desc_tbl.slotDescriptors.push_back(slot_desc);
    EXPECT_OK(DescriptorTbl::Create(&obj_pool_, thrift_desc_tbl, &desc_tbl_));
    runtime_state_.set_desc_tbl(desc_tbl_);

    vector<TTupleId> row_tids;
    row_tids.push_back(0);
    vector<bool> nullable_tuples;
    nullable_tuples.push_back(false);
    row_desc_ = obj_pool_.Add(new RowDescriptor(*desc_tbl_, row_tids, nullable_tuples));
  }

  // Create a tuple comparator to sort in ascending order on the single bigint column.
  void CreateTupleComparator() {
    SlotRef* lhs_slot = obj_pool_.Add(new SlotRef(TYPE_BIGINT, 0));
    lhs_slot_ctx_ = obj_pool_.Add(new ExprContext(lhs_slot));
    SlotRef* rhs_slot = obj_pool_.Add(new SlotRef(TYPE_BIGINT, 0));
    rhs_slot_ctx_ = obj_pool_.Add(new ExprContext(rhs_slot));

    lhs_slot_ctx_->Prepare(NULL, *row_desc_, &tracker_);
    rhs_slot_ctx_->Prepare(NULL, *row_desc_, &tracker_);
    lhs_slot_ctx_->Open(NULL);
    rhs_slot_ctx_->Open(NULL);
    SortExecExprs* sort_exprs = obj_pool_.Add(new SortExecExprs());
    sort_exprs->Init(
        vector<ExprContext*>(1, lhs_slot_ctx_), vector<ExprContext*>(1, rhs_slot_ctx_));
    less_than_ = obj_pool_.Add(new TupleRowComparator(
        *sort_exprs, vector<bool>(1, true), vector<bool>(1, false)));
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
                     int buffer_size, bool is_merging, TUniqueId* out_id = NULL) {
    VLOG_QUERY << "start receiver";
    RuntimeProfile* profile =
        obj_pool_.Add(new RuntimeProfile(&obj_pool_, "TestReceiver"));
    TUniqueId instance_id;
    GetNextInstanceId(&instance_id);
    receiver_info_.push_back(ReceiverInfo(stream_type, num_senders, receiver_num));
    ReceiverInfo& info = receiver_info_.back();
    info.stream_recvr =
        stream_mgr_->CreateRecvr(&runtime_state_,
            *row_desc_, instance_id, DEST_NODE_ID, num_senders, buffer_size, profile,
            is_merging);
    if (!is_merging) {
      info.thread_handle = new thread(&DataStreamTest::ReadStream, this, &info);
    } else {
      info.thread_handle = new thread(&DataStreamTest::ReadStreamMerging, this, &info,
          profile);
    }
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
    while (!(info->status = info->stream_recvr->GetBatch(&batch)).IsCancelled() &&
        (batch != NULL)) {
      VLOG_QUERY << "read batch #rows=" << batch->num_rows();
      for (int i = 0; i < batch->num_rows(); ++i) {
        TupleRow* row = batch->GetRow(i);
        info->data_values.insert(*static_cast<int64_t*>(row->GetTuple(0)->GetSlot(0)));
      }
      SleepForMs(100);  // slow down receiver to exercise buffering logic
    }
    if (info->status.IsCancelled()) VLOG_QUERY << "reader is cancelled";
    VLOG_QUERY << "done reading";
  }

  void ReadStreamMerging(ReceiverInfo* info, RuntimeProfile* profile) {
    info->status = info->stream_recvr->CreateMerger(*less_than_);
    if (info->status.IsCancelled()) return;
    RowBatch batch(*row_desc_, 1024, &tracker_);
    VLOG_QUERY << "start reading merging";
    bool eos;
    while (!(info->status = info->stream_recvr->GetNext(&batch, &eos)).IsCancelled()) {
      VLOG_QUERY << "read batch #rows=" << batch.num_rows();
      for (int i = 0; i < batch.num_rows(); ++i) {
        TupleRow* row = batch.GetRow(i);
        info->data_values.insert(*static_cast<int64_t*>(row->GetTuple(0)->GetSlot(0)));
      }
      SleepForMs(100);
      batch.Reset();
      if (eos) break;
    }
    if (info->status.IsCancelled()) VLOG_QUERY << "reader is cancelled";
    VLOG_QUERY << "done reading";
  }

  // Verify correctness of receivers' data values.
  void CheckReceivers(TPartitionType::type stream_type, int num_senders) {
    int64_t total = 0;
    multiset<int64_t> all_data_values;
    for (int i = 0; i < receiver_info_.size(); ++i) {
      ReceiverInfo& info = receiver_info_[i];
      EXPECT_OK(info.status);
      total += info.data_values.size();
      ASSERT_EQ(info.stream_type, stream_type);
      ASSERT_EQ(info.num_senders, num_senders);
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
          uint32_t hash_val =
              RawValue::GetHashValueFnv(&value, TYPE_BIGINT, HashUtil::FNV_SEED);
          EXPECT_EQ(hash_val % receiver_info_.size(), info.receiver_num);
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
      EXPECT_OK(sender_info_[i].status);
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
    ASSERT_LT(num_senders, MAX_SENDERS);
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
    RuntimeState state(TExecPlanFragmentParams(), "", &exec_env_);
    state.set_desc_tbl(desc_tbl_);
    state.InitMemTrackers(TUniqueId(), NULL, -1);
    VLOG_QUERY << "create sender " << sender_num;
    const TDataStreamSink& sink = GetSink(partition_type);
    DataStreamSender sender(
        &obj_pool_, sender_num, *row_desc_, sink, dest_, channel_buffer_size);
    EXPECT_OK(sender.Prepare(&state));
    EXPECT_OK(sender.Open(&state));
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
                  int num_receivers, int buffer_size, bool is_merging) {
    VLOG_QUERY << "Testing stream=" << stream_type << " #senders=" << num_senders
               << " #receivers=" << num_receivers << " buffer_size=" << buffer_size
               << " is_merging=" << is_merging;
    Reset();
    for (int i = 0; i < num_receivers; ++i) {
      StartReceiver(stream_type, num_senders, i, buffer_size, is_merging);
    }
    for (int i = 0; i < num_senders; ++i) {
      StartSender(stream_type, buffer_size);
    }
    JoinSenders();
    CheckSenders();
    JoinReceivers();
    CheckReceivers(stream_type, num_senders);
  }

 private:
  ExprContext* lhs_slot_ctx_;
  ExprContext* rhs_slot_ctx_;
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
  EXPECT_OK(sender_info_[0].status);
  EXPECT_GT(sender_info_[0].num_bytes_sent, 0);
}

TEST_F(DataStreamTest, UnknownSenderLargeResult) {
  // case 2: query result requires multiple buffers, send() returns ok
  TUniqueId dummy_id;
  GetNextInstanceId(&dummy_id);
  StartSender();
  JoinSenders();
  EXPECT_OK(sender_info_[0].status);
  EXPECT_GT(sender_info_[0].num_bytes_sent, 0);
}

TEST_F(DataStreamTest, Cancel) {
  TUniqueId instance_id;
  StartReceiver(TPartitionType::UNPARTITIONED, 1, 1, 1024, false, &instance_id);
  stream_mgr_->Cancel(instance_id);
  StartReceiver(TPartitionType::UNPARTITIONED, 1, 1, 1024, true, &instance_id);
  stream_mgr_->Cancel(instance_id);
  JoinReceivers();
  EXPECT_TRUE(receiver_info_[0].status.IsCancelled());
  EXPECT_TRUE(receiver_info_[1].status.IsCancelled());
}

TEST_F(DataStreamTest, BasicTest) {
  // TODO: also test that all client connections have been returned
  TPartitionType::type stream_types[] =
      {TPartitionType::UNPARTITIONED, TPartitionType::RANDOM,
          TPartitionType::HASH_PARTITIONED};
  int sender_nums[] = {1, 4};
  int receiver_nums[] = {1, 4};
  int buffer_sizes[] = {1024, 1024 * 1024};
  bool merging[] = {false, true};
  for (int i = 0; i < sizeof(stream_types) / sizeof(*stream_types); ++i) {
    for (int j = 0; j < sizeof(sender_nums) / sizeof(int); ++j) {
      for (int k = 0; k < sizeof(receiver_nums) / sizeof(int); ++k) {
        for (int l = 0; l < sizeof(buffer_sizes) / sizeof(int); ++l) {
          for (int m = 0; m < sizeof(merging) / sizeof(bool); ++m) {
            TestStream(stream_types[i], sender_nums[j], receiver_nums[k],
                       buffer_sizes[l], merging[m]);
          }
        }
      }
    }
  }
}

// This test checks for the avoidance of IMPALA-2931, which is a crash that would occur if
// the parent memtracker of a DataStreamRecvr's memtracker was deleted before the
// DataStreamRecvr was destroyed. The fix was to move decoupling the child tracker from
// the parent into DataStreamRecvr::Close() which should always be called before the
// parent is destroyed. In practice the parent is a member of the query's runtime state.
//
// TODO: Make lifecycle requirements more explicit.
TEST_F(DataStreamTest, CloseRecvrWhileReferencesRemain) {
  scoped_ptr<RuntimeState> runtime_state(
      new RuntimeState(TExecPlanFragmentParams(), "", &exec_env_));
  runtime_state->InitMemTrackers(TUniqueId(), NULL, -1);

  scoped_ptr<RuntimeProfile> profile(new RuntimeProfile(&obj_pool_, "TestReceiver"));

  // Start just one receiver.
  TUniqueId instance_id;
  GetNextInstanceId(&instance_id);
  shared_ptr<DataStreamRecvr> stream_recvr = stream_mgr_->CreateRecvr(runtime_state.get(),
      *row_desc_, instance_id, DEST_NODE_ID, 1, 1, profile.get(), false);

  // Perform tear down, but keep a reference to the receiver so that it is deleted last
  // (to confirm that the destructor does not access invalid state after tear-down).
  stream_recvr->Close();

  // Force deletion of the parent memtracker by destroying it's owning runtime state.
  runtime_state.reset();

  // Send an eos RPC to the receiver. Not required for tear-down, but confirms that the
  // RPC does not cause an error (the receiver will still be called, since it is only
  // Close()'d, not deleted from the data stream manager).
  Status rpc_status;
  ImpalaInternalServiceConnection client(exec_env_.impalad_client_cache(),
      MakeNetworkAddress("localhost", FLAGS_port), &rpc_status);
  EXPECT_TRUE(rpc_status.ok());
  TTransmitDataParams params;
  params.protocol_version = ImpalaInternalServiceVersion::V1;
  params.__set_eos(true);
  params.__set_dest_fragment_instance_id(instance_id);
  params.__set_dest_node_id(DEST_NODE_ID);
  TUniqueId dummy_id;
  params.__set_sender_id(0);

  TTransmitDataResult result;
  rpc_status =
      client.DoRpc(&ImpalaInternalServiceClient::TransmitData, params, &result);

  // Finally, stream_recvr destructor happens here. Before fix for IMPALA-2931, this
  // would have resulted in a crash.
  stream_recvr.reset();
}

// TODO: more tests:
// - test case for transmission error in last batch
// - receivers getting created concurrently

}

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  InitCommonRuntime(argc, argv, true, TestInfo::BE_TEST);
  InitFeSupport();
  impala::LlvmCodeGen::InitializeLlvm();
  return RUN_ALL_TESTS();
}
