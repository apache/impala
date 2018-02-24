// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include <boost/thread/thread.hpp>

#include "testutil/gtest-util.h"
#include "common/init.h"
#include "common/logging.h"
#include "common/status.h"
#include "codegen/llvm-codegen.h"
#include "exprs/slot-ref.h"
#include "kudu/rpc/rpc_context.h"
#include "kudu/rpc/service_if.h"
#include "rpc/auth-provider.h"
#include "rpc/thrift-server.h"
#include "rpc/rpc-mgr.h"
#include "runtime/row-batch.h"
#include "runtime/runtime-state.h"
#include "runtime/data-stream-mgr-base.h"
#include "runtime/data-stream-mgr.h"
#include "runtime/exec-env.h"
#include "runtime/krpc-data-stream-mgr.h"
#include "runtime/krpc-data-stream-sender.h"
#include "runtime/data-stream-sender.h"
#include "runtime/data-stream-recvr-base.h"
#include "runtime/data-stream-recvr.h"
#include "runtime/descriptors.h"
#include "runtime/client-cache.h"
#include "runtime/backend-client.h"
#include "runtime/mem-tracker.h"
#include "runtime/raw-value.inline.h"
#include "service/data-stream-service.h"
#include "service/fe-support.h"
#include "util/cpu-info.h"
#include "util/disk-info.h"
#include "util/debug-util.h"
#include "util/thread.h"
#include "util/time.h"
#include "util/mem-info.h"
#include "util/parse-util.h"
#include "util/test-info.h"
#include "util/tuple-row-compare.h"
#include "gen-cpp/data_stream_service.pb.h"
#include "gen-cpp/ImpalaInternalService.h"
#include "gen-cpp/ImpalaInternalService_types.h"
#include "gen-cpp/Types_types.h"
#include "gen-cpp/Descriptors_types.h"
#include "service/fe-support.h"

#include <iostream>
#include <string>
#include <unistd.h>

#include "common/names.h"

using namespace impala;
using namespace apache::thrift;
using namespace apache::thrift::protocol;

using kudu::MetricEntity;
using kudu::rpc::ResultTracker;
using kudu::rpc::RpcContext;
using kudu::rpc::ServiceIf;

DEFINE_int32(port, 20001, "port on which to run Impala Thrift based test backend.");
DECLARE_int32(datastream_sender_timeout_ms);
DECLARE_int32(datastream_service_num_deserialization_threads);
DECLARE_int32(datastream_service_deserialization_queue_size);
DECLARE_string(datastream_service_queue_mem_limit);

DECLARE_bool(use_krpc);

// We reserve contiguous memory for senders in SetUp. If a test uses more
// senders, a DCHECK will fail and you should increase this value.
static const int MAX_SENDERS = 16;
static const int MAX_RECEIVERS = 16;
static const PlanNodeId DEST_NODE_ID = 1;
static const int BATCH_CAPACITY = 100;  // rows
static const int PER_ROW_DATA = 8;
static const int TOTAL_DATA_SIZE = 8 * 1024;
static const int NUM_BATCHES = TOTAL_DATA_SIZE / BATCH_CAPACITY / PER_ROW_DATA;
static const int SHORT_SERVICE_QUEUE_MEM_LIMIT = 16;

namespace impala {

// This class acts as a service interface for all Thrift related communication within
// this test file.
class ImpalaThriftTestBackend : public ImpalaInternalServiceIf {
 public:
  ImpalaThriftTestBackend(DataStreamMgr* stream_mgr): mgr_(stream_mgr) {}
  virtual ~ImpalaThriftTestBackend() {}

  virtual void ExecQueryFInstances(TExecQueryFInstancesResult& return_val,
      const TExecQueryFInstancesParams& params) {}
  virtual void CancelQueryFInstances(TCancelQueryFInstancesResult& return_val,
      const TCancelQueryFInstancesParams& params) {}
  virtual void ReportExecStatus(TReportExecStatusResult& return_val,
      const TReportExecStatusParams& params) {}
  virtual void UpdateFilter(TUpdateFilterResult& return_val,
      const TUpdateFilterParams& params) {}
  virtual void PublishFilter(TPublishFilterResult& return_val,
      const TPublishFilterParams& params) {}

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

// This class acts as a service interface for all KRPC related communication within
// this test file.
class ImpalaKRPCTestBackend : public DataStreamServiceIf {
 public:
  ImpalaKRPCTestBackend(RpcMgr* rpc_mgr, KrpcDataStreamMgr* stream_mgr,
      MemTracker* process_mem_tracker)
    : DataStreamServiceIf(rpc_mgr->metric_entity(), rpc_mgr->result_tracker()),
      rpc_mgr_(rpc_mgr),
      stream_mgr_(stream_mgr) {
    bool is_percent;
    int64_t bytes_limit = ParseUtil::ParseMemSpec(FLAGS_datastream_service_queue_mem_limit,
        &is_percent, process_mem_tracker->limit());
    mem_tracker_.reset(
        new MemTracker(bytes_limit, "DataStream Test", process_mem_tracker));
  }

  virtual ~ImpalaKRPCTestBackend() {}

  Status Init() {
    return rpc_mgr_->RegisterService(CpuInfo::num_cores(), 1024, this, mem_tracker());
  }

  virtual void TransmitData(const TransmitDataRequestPB* request,
      TransmitDataResponsePB* response, RpcContext* rpc_context) {
    stream_mgr_->AddData(request, response, rpc_context);
  }

  virtual void EndDataStream(const EndDataStreamRequestPB* request,
      EndDataStreamResponsePB* response, RpcContext* rpc_context) {
    stream_mgr_->CloseSender(request, response, rpc_context);
  }

  MemTracker* mem_tracker() { return mem_tracker_.get(); }

 private:
  RpcMgr* rpc_mgr_;
  KrpcDataStreamMgr* stream_mgr_;
  unique_ptr<MemTracker> mem_tracker_;
};

template <class T> class DataStreamTestBase : public T {
 protected:
  virtual void SetUp() {}
  virtual void TearDown() {}
};

enum KrpcSwitch {
  USE_THRIFT,
  USE_KRPC
};

class DataStreamTest : public DataStreamTestBase<testing::TestWithParam<KrpcSwitch>> {
 protected:
  DataStreamTest() : next_val_(0) {
    // Stop tests that rely on mismatched sender / receiver pairs timing out from failing.
    FLAGS_datastream_sender_timeout_ms = 250;
  }
  ~DataStreamTest() { runtime_state_->ReleaseResources(); }

  virtual void SetUp() {
    // Initialize MemTrackers and RuntimeState for use by the data stream receiver.
    FLAGS_use_krpc = GetParam() == USE_KRPC;

    exec_env_.reset(new ExecEnv());
    ABORT_IF_ERROR(exec_env_->InitForFeTests());
    exec_env_->InitBufferPool(32 * 1024, 1024 * 1024 * 1024, 32 * 1024);
    runtime_state_.reset(new RuntimeState(TQueryCtx(), exec_env_.get()));
    mem_pool_.reset(new MemPool(&tracker_));

    // Register a BufferPool client for allocating buffers for row batches.
    ABORT_IF_ERROR(exec_env_->buffer_pool()->RegisterClient(
        "DataStream Test Recvr", nullptr, exec_env_->buffer_reservation(), &tracker_,
        numeric_limits<int64_t>::max(), runtime_state_->runtime_profile(),
        &buffer_pool_client_));

    CreateRowDesc();

    is_asc_.push_back(true);
    nulls_first_.push_back(true);
    CreateTupleComparator();

    next_instance_id_.lo = 0;
    next_instance_id_.hi = 0;
    stream_mgr_ = exec_env_->stream_mgr();

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
    if (GetParam() == USE_THRIFT) {
      StartThriftBackend();
    } else {
      IpAddr ip;
      ASSERT_OK(HostnameToIpAddr(FLAGS_hostname, &ip));
      krpc_address_ = MakeNetworkAddress(ip, FLAGS_port);
      StartKrpcBackend();
    }
  }

  const TDataSink GetSink(TPartitionType::type partition_type) {
    TDataSink tdata_sink;
    switch (partition_type) {
      case TPartitionType::UNPARTITIONED:
        tdata_sink.__set_stream_sink(broadcast_sink_);
        break;
      case TPartitionType::RANDOM:
        tdata_sink.__set_stream_sink(random_sink_);
        break;
      case TPartitionType::HASH_PARTITIONED:
        tdata_sink.__set_stream_sink(hash_sink_);
        break;
      default:
        EXPECT_TRUE(false) << "Unhandled sink type: " << partition_type;
    }
    return tdata_sink;
  }

  virtual void TearDown() {
    desc_tbl_->ReleaseResources();
    less_than_->Close(runtime_state_.get());
    ScalarExpr::Close(ordering_exprs_);
    mem_pool_->FreeAll();
    if (GetParam() == USE_THRIFT) {
      exec_env_->impalad_client_cache()->TestShutdown();
      StopThriftBackend();
    } else {
      StopKrpcBackend();
    }
    exec_env_->buffer_pool()->DeregisterClient(&buffer_pool_client_);
  }

  void Reset() {
    sender_info_.clear();
    receiver_info_.clear();
    dest_.clear();
  }

  ObjectPool obj_pool_;
  MemTracker tracker_;
  scoped_ptr<MemPool> mem_pool_;
  DescriptorTbl* desc_tbl_;
  const RowDescriptor* row_desc_;
  vector<bool> is_asc_;
  vector<bool> nulls_first_;
  TupleRowComparator* less_than_;
  boost::scoped_ptr<ExecEnv> exec_env_;
  scoped_ptr<RuntimeState> runtime_state_;
  TUniqueId next_instance_id_;
  string stmt_;
  // The sorting expression for the single BIGINT column.
  vector<ScalarExpr*> ordering_exprs_;

  // Client for allocating buffers for row batches.
  BufferPool::ClientHandle buffer_pool_client_;

  // RowBatch generation
  scoped_ptr<RowBatch> batch_;
  int next_val_;
  int64_t* tuple_mem_;

  // Only used for KRPC. Not owned.
  TNetworkAddress krpc_address_;

  // The test service implementation. Owned by this class.
  unique_ptr<ImpalaKRPCTestBackend> test_service_;

  // receiving node
  DataStreamMgrBase* stream_mgr_ = nullptr;
  ThriftServer* server_ = nullptr;

  // sending node(s)
  TDataStreamSink broadcast_sink_;
  TDataStreamSink random_sink_;
  TDataStreamSink hash_sink_;
  vector<TPlanFragmentDestination> dest_;

  struct SenderInfo {
    thread* thread_handle;
    Status status;
    int num_bytes_sent;

    SenderInfo(): thread_handle(nullptr), num_bytes_sent(0) {}
  };
  vector<SenderInfo> sender_info_;

  struct ReceiverInfo {
    TPartitionType::type stream_type;
    int num_senders;
    int receiver_num;

    thread* thread_handle;
    shared_ptr<DataStreamRecvrBase> stream_recvr;
    Status status;
    int num_rows_received;
    multiset<int64_t> data_values;

    ReceiverInfo(TPartitionType::type stream_type, int num_senders, int receiver_num)
      : stream_type(stream_type),
        num_senders(num_senders),
        receiver_num(receiver_num),
        thread_handle(nullptr),
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
    if (GetParam() == USE_KRPC) {
      dest.__set_krpc_server(krpc_address_);
    }
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

    vector<TTupleId> row_tids;
    row_tids.push_back(0);
    vector<bool> nullable_tuples;
    nullable_tuples.push_back(false);
    row_desc_ = obj_pool_.Add(new RowDescriptor(*desc_tbl_, row_tids, nullable_tuples));
  }

  // Create a tuple comparator to sort in ascending order on the single bigint column.
  void CreateTupleComparator() {
    SlotRef* lhs_slot = obj_pool_.Add(new SlotRef(TYPE_BIGINT, 0));
    ASSERT_OK(lhs_slot->Init(RowDescriptor(), runtime_state_.get()));
    ordering_exprs_.push_back(lhs_slot);
    less_than_ = obj_pool_.Add(new TupleRowComparator(ordering_exprs_,
        is_asc_, nulls_first_));
    ASSERT_OK(less_than_->Open(
        &obj_pool_, runtime_state_.get(), mem_pool_.get(), mem_pool_.get()));
  }

  // Create batch_, but don't fill it with data yet. Assumes we created row_desc_.
  RowBatch* CreateRowBatch() {
    RowBatch* batch = new RowBatch(row_desc_, BATCH_CAPACITY, &tracker_);
    int64_t* tuple_mem = reinterpret_cast<int64_t*>(
        batch->tuple_data_pool()->Allocate(BATCH_CAPACITY * PER_ROW_DATA));
    bzero(tuple_mem, BATCH_CAPACITY * PER_ROW_DATA);
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
      int buffer_size, bool is_merging, TUniqueId* out_id = nullptr) {
    VLOG_QUERY << "start receiver";
    RuntimeProfile* profile = RuntimeProfile::Create(&obj_pool_, "TestReceiver");
    TUniqueId instance_id;
    GetNextInstanceId(&instance_id);
    receiver_info_.push_back(ReceiverInfo(stream_type, num_senders, receiver_num));
    ReceiverInfo& info = receiver_info_.back();
    info.stream_recvr = stream_mgr_->CreateRecvr(row_desc_, instance_id, DEST_NODE_ID,
        num_senders, buffer_size, is_merging, profile, &tracker_, &buffer_pool_client_);
    if (!is_merging) {
      info.thread_handle = new thread(&DataStreamTest::ReadStream, this, &info);
    } else {
      info.thread_handle = new thread(&DataStreamTest::ReadStreamMerging, this, &info,
          profile);
    }
    if (out_id != nullptr) *out_id = instance_id;
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
        (batch != nullptr)) {
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
    RowBatch batch(row_desc_, 1024, &tracker_);
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
          uint64_t hash_val = RawValue::GetHashValueFastHash(&value, TYPE_BIGINT,
              DataStreamSender::EXCHANGE_HASH_SEED);
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

  // Start Thrift based backend in separate thread.
  void StartThriftBackend() {
    // Dynamic cast stream_mgr_ which is of type DataStreamMgrBase to derived type
    // DataStreamMgr, since ImpalaThriftTestBackend() accepts only DataStreamMgr*.
    boost::shared_ptr<ImpalaThriftTestBackend> handler(
        new ImpalaThriftTestBackend(exec_env_->ThriftStreamMgr()));
    boost::shared_ptr<TProcessor> processor(new ImpalaInternalServiceProcessor(handler));
    ThriftServerBuilder builder("DataStreamTest backend", processor, FLAGS_port);
    ASSERT_OK(builder.Build(&server_));
    ASSERT_OK(server_->Start());
  }

  void StartKrpcBackend() {
    RpcMgr* rpc_mgr = exec_env_->rpc_mgr();
    KrpcDataStreamMgr* krpc_stream_mgr = exec_env_->KrpcStreamMgr();
    ASSERT_OK(rpc_mgr->Init());
    test_service_.reset(new ImpalaKRPCTestBackend(rpc_mgr, krpc_stream_mgr,
        exec_env_->process_mem_tracker()));
    ASSERT_OK(test_service_->Init());
    ASSERT_OK(krpc_stream_mgr->Init(test_service_->mem_tracker()));
    ASSERT_OK(rpc_mgr->StartServices(krpc_address_));
  }

  void StopThriftBackend() {
    VLOG_QUERY << "stop backend\n";
    server_->StopForTesting();
    delete server_;
  }

  void StopKrpcBackend() {
    exec_env_->rpc_mgr()->Shutdown();
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
                   partition_type, GetParam() == USE_THRIFT);
  }

  void JoinSenders() {
    VLOG_QUERY << "join senders\n";
    for (int i = 0; i < sender_info_.size(); ++i) {
      sender_info_[i].thread_handle->join();
    }
  }

  void Sender(int sender_num,
      int channel_buffer_size, TPartitionType::type partition_type, bool is_thrift) {
    RuntimeState state(TQueryCtx(), exec_env_.get(), desc_tbl_);
    VLOG_QUERY << "create sender " << sender_num;
    const TDataSink& sink = GetSink(partition_type);

    // We create an object of the base class DataSink and cast to the appropriate sender
    // according to the 'is_thrift' option.
    scoped_ptr<DataSink> sender;

    TExprNode expr_node;
    expr_node.node_type = TExprNodeType::SLOT_REF;
    TExpr output_exprs;
    output_exprs.nodes.push_back(expr_node);

    if (is_thrift) {
      sender.reset(new DataStreamSender(
          sender_num, row_desc_, sink.stream_sink, dest_, channel_buffer_size, &state));
      EXPECT_OK(static_cast<DataStreamSender*>(
          sender.get())->Init(vector<TExpr>({output_exprs}), sink, &state));
    } else {
      sender.reset(new KrpcDataStreamSender(
          sender_num, row_desc_, sink.stream_sink, dest_, channel_buffer_size, &state));
      EXPECT_OK(static_cast<KrpcDataStreamSender*>(
          sender.get())->Init(vector<TExpr>({output_exprs}), sink, &state));
    }

    EXPECT_OK(sender->Prepare(&state, &tracker_));
    EXPECT_OK(sender->Open(&state));
    scoped_ptr<RowBatch> batch(CreateRowBatch());
    SenderInfo& info = sender_info_[sender_num];
    int next_val = 0;
    for (int i = 0; i < NUM_BATCHES; ++i) {
      GetNextBatch(batch.get(), &next_val);
      VLOG_QUERY << "sender " << sender_num << ": #rows=" << batch->num_rows();
      info.status = sender->Send(&state, batch.get());
      if (!info.status.ok()) break;
    }
    VLOG_QUERY << "closing sender" << sender_num;
    info.status.MergeStatus(sender->FlushFinal(&state));
    sender->Close(&state);
    if (is_thrift) {
      info.num_bytes_sent = static_cast<DataStreamSender*>(
          sender.get())->GetNumDataBytesSent();
    } else {
      info.num_bytes_sent = static_cast<KrpcDataStreamSender*>(
          sender.get())->GetNumDataBytesSent();
    }

    batch->Reset();
    state.ReleaseResources();
  }

  void TestStream(TPartitionType::type stream_type, int num_senders, int num_receivers,
      int buffer_size, bool is_merging) {
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
};

// A seperate class for tests that are required to be run against Thrift only.
class DataStreamTestThriftOnly : public DataStreamTest {
 protected:
  virtual void SetUp() {
    DataStreamTest::SetUp();
  }

  virtual void TearDown() {
    DataStreamTest::TearDown();
  }
};

// A seperate test class which simulates the behavior in which deserialization queue
// fills up and all deserialization threads are busy.
class DataStreamTestShortDeserQueue : public DataStreamTest {
 protected:
  virtual void SetUp() {
    FLAGS_datastream_sender_timeout_ms = 10000;
    FLAGS_datastream_service_num_deserialization_threads = 1;
    FLAGS_datastream_service_deserialization_queue_size = 1;
    DataStreamTest::SetUp();
  }

  virtual void TearDown() {
    DataStreamTest::TearDown();
  }
};

// A separate test class which simulates that the service queue fills up.
class DataStreamTestShortServiceQueue : public DataStreamTest {
 protected:
  virtual void SetUp() {
    // Set the memory limit to very low to make the soft limit easy to surpass.
    FLAGS_datastream_service_queue_mem_limit =
        std::to_string(SHORT_SERVICE_QUEUE_MEM_LIMIT);
    DataStreamTest::SetUp();
  }

  virtual void TearDown() {
    DataStreamTest::TearDown();
  }
};

INSTANTIATE_TEST_CASE_P(ThriftOrKrpc, DataStreamTest,
    ::testing::Values(USE_KRPC, USE_THRIFT));

INSTANTIATE_TEST_CASE_P(ThriftOnly, DataStreamTestThriftOnly,
    ::testing::Values(USE_THRIFT));

INSTANTIATE_TEST_CASE_P(KrpcOnly, DataStreamTestShortDeserQueue,
    ::testing::Values(USE_KRPC));

INSTANTIATE_TEST_CASE_P(KrpcOnly, DataStreamTestShortServiceQueue,
    ::testing::Values(USE_KRPC));

TEST_P(DataStreamTest, UnknownSenderSmallResult) {
  // starting a sender w/o a corresponding receiver results in an error. No bytes should
  // be sent.
  // case 1: entire query result fits in single buffer
  TUniqueId dummy_id;
  GetNextInstanceId(&dummy_id);
  StartSender(TPartitionType::UNPARTITIONED, TOTAL_DATA_SIZE + 1024);
  JoinSenders();
  EXPECT_EQ(sender_info_[0].status.code(), TErrorCode::DATASTREAM_SENDER_TIMEOUT);
  EXPECT_EQ(sender_info_[0].num_bytes_sent, 0);
}

TEST_P(DataStreamTest, UnknownSenderLargeResult) {
  // case 2: query result requires multiple buffers
  TUniqueId dummy_id;
  GetNextInstanceId(&dummy_id);
  StartSender();
  JoinSenders();
  EXPECT_EQ(sender_info_[0].status.code(), TErrorCode::DATASTREAM_SENDER_TIMEOUT);
  EXPECT_EQ(sender_info_[0].num_bytes_sent, 0);
}

TEST_P(DataStreamTest, Cancel) {
  TUniqueId instance_id;
  StartReceiver(TPartitionType::UNPARTITIONED, 1, 1, 1024, false, &instance_id);
  stream_mgr_->Cancel(instance_id);
  StartReceiver(TPartitionType::UNPARTITIONED, 1, 1, 1024, true, &instance_id);
  stream_mgr_->Cancel(instance_id);
  JoinReceivers();
  EXPECT_TRUE(receiver_info_[0].status.IsCancelled());
  EXPECT_TRUE(receiver_info_[1].status.IsCancelled());
}

TEST_P(DataStreamTest, BasicTest) {
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
TEST_P(DataStreamTestThriftOnly, CloseRecvrWhileReferencesRemain) {
  scoped_ptr<RuntimeState> runtime_state(new RuntimeState(TQueryCtx(), exec_env_.get()));
  RuntimeProfile* profile = RuntimeProfile::Create(&obj_pool_, "TestReceiver");

  // Start just one receiver.
  TUniqueId instance_id;
  GetNextInstanceId(&instance_id);
  shared_ptr<DataStreamRecvrBase> stream_recvr = stream_mgr_->CreateRecvr(row_desc_,
      instance_id, DEST_NODE_ID, 1, 1, false, profile, &tracker_, nullptr);

  // Perform tear down, but keep a reference to the receiver so that it is deleted last
  // (to confirm that the destructor does not access invalid state after tear-down).
  stream_recvr->Close();

  // Force deletion of the parent memtracker by destroying it's owning runtime state.
  runtime_state->ReleaseResources();
  runtime_state.reset();

  // Send an eos RPC to the receiver. Not required for tear-down, but confirms that the
  // RPC does not cause an error (the receiver will still be called, since it is only
  // Close()'d, not deleted from the data stream manager).
  Status rpc_status;
  ImpalaBackendConnection client(exec_env_->impalad_client_cache(),
      MakeNetworkAddress("localhost", FLAGS_port), &rpc_status);
  EXPECT_OK(rpc_status);
  TTransmitDataParams params;
  params.protocol_version = ImpalaInternalServiceVersion::V1;
  params.__set_eos(true);
  params.__set_dest_fragment_instance_id(instance_id);
  params.__set_dest_node_id(DEST_NODE_ID);
  TUniqueId dummy_id;
  params.__set_sender_id(0);

  TTransmitDataResult result;
  rpc_status = client.DoRpc(&ImpalaBackendClient::TransmitData, params, &result);

  // Finally, stream_recvr destructor happens here. Before fix for IMPALA-2931, this
  // would have resulted in a crash.
  stream_recvr.reset();
}

// This test is to exercise a previously present deadlock path which is now fixed, to
// ensure that the deadlock does not happen anymore. It does this by doing the following:
// This test starts multiple senders to send to the same receiver. It makes sure that
// the senders' payloads reach the receiver before the receiver is setup. Once the
// receiver is being created, it will notice that there are multiple payloads waiting
// to be processed already and it would hold the KrpcDataStreamMgr::lock_ and call
// TakeOverEarlySender() which calls EnqueueDeserializeTask() which tries to Offer()
// the payload to the deserialization_pool_. However, we've set the queue size to 1,
// which will cause the payload to be stuck on the Offer(). Now any payload that is
// already being deserialized will be waiting on the KrpcDataStreamMgr::lock_ as well.
// But the first thread will never release the lock since it's stuck on Offer(), causing
// a deadlock. This is fixed with IMPALA-6346.
TEST_P(DataStreamTestShortDeserQueue, TestNoDeadlock) {
  TUniqueId instance_id;
  GetNextInstanceId(&instance_id);

  // Start 4 senders.
  StartSender(TPartitionType::UNPARTITIONED, 1024 * 1024);
  StartSender(TPartitionType::UNPARTITIONED, 1024 * 1024);
  StartSender(TPartitionType::UNPARTITIONED, 1024 * 1024);
  StartSender(TPartitionType::UNPARTITIONED, 1024 * 1024);

  // Do a small sleep to ensure that the sent payloads reach before the receivers
  // are created.
  sleep(2);

  // Setup the receiver.
  RuntimeProfile* profile = RuntimeProfile::Create(&obj_pool_, "TestReceiver");
  receiver_info_.push_back(ReceiverInfo(TPartitionType::UNPARTITIONED, 4, 1));
  ReceiverInfo& info = receiver_info_.back();
  info.stream_recvr = stream_mgr_->CreateRecvr(row_desc_, instance_id, DEST_NODE_ID,
      4, 1024 * 1024, false, profile, &tracker_, &buffer_pool_client_);
  info.thread_handle = new thread(
      &DataStreamTestShortDeserQueue_TestNoDeadlock_Test::ReadStream, this, &info);

  JoinSenders();
  CheckSenders();
  JoinReceivers();

  // Check that 4 payloads have been received.
  CheckReceivers(TPartitionType::UNPARTITIONED, 4);
}

// Test that payloads larger than the service queue's soft mem limit can be transmitted.
TEST_P(DataStreamTestShortServiceQueue, TestLargePayload) {
  TestStream(
      TPartitionType::UNPARTITIONED, 4, 1, SHORT_SERVICE_QUEUE_MEM_LIMIT * 2, false);
}

// TODO: more tests:
// - test case for transmission error in last batch
// - receivers getting created concurrently

}

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  InitCommonRuntime(argc, argv, true, TestInfo::BE_TEST);
  InitFeSupport();
  ABORT_IF_ERROR(impala::LlvmCodeGen::InitializeLlvm());
  return RUN_ALL_TESTS();
}
