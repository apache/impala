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
#include "runtime/fragment-state.h"
#include "runtime/query-state.h"
#include "runtime/row-batch.h"
#include "runtime/runtime-state.h"
#include "runtime/exec-env.h"
#include "runtime/krpc-data-stream-mgr.h"
#include "runtime/krpc-data-stream-recvr.h"
#include "runtime/krpc-data-stream-sender.h"
#include "runtime/descriptors.h"
#include "runtime/client-cache.h"
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
#include "util/uid-util.h"
#include "gen-cpp/data_stream_service.pb.h"
#include "gen-cpp/Types_types.h"
#include "gen-cpp/Descriptors_types.h"
#include "service/fe-support.h"

#include <atomic>
#include <iostream>
#include <string>
#include <unistd.h>

#include "common/names.h"

using boost::uuids::random_generator;
using boost::uuids::uuid;
using namespace impala;
using namespace apache::thrift;
using namespace apache::thrift::protocol;

using kudu::MetricEntity;
using kudu::rpc::ResultTracker;
using kudu::rpc::RpcContext;
using kudu::rpc::ServiceIf;

DEFINE_int32(port, 20001, "port on which to run Impala krpc based test backend.");
DECLARE_int32(datastream_sender_timeout_ms);
DECLARE_int32(datastream_service_num_deserialization_threads);
DECLARE_int32(datastream_service_deserialization_queue_size);
DECLARE_string(datastream_service_queue_mem_limit);
DECLARE_int32(datastream_service_num_svc_threads);
DECLARE_string(debug_actions);

static const PlanNodeId DEST_NODE_ID = 1;
static const int BATCH_CAPACITY = 100;  // rows
static const int PER_ROW_DATA = 8;
static const int TOTAL_DATA_SIZE = 8 * 1024;
static const int NUM_BATCHES = TOTAL_DATA_SIZE / BATCH_CAPACITY / PER_ROW_DATA;
static const int SHORT_SERVICE_QUEUE_MEM_LIMIT = 16;

namespace impala {

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
    int num_svc_threads = FLAGS_datastream_service_num_svc_threads > 0 ?
        FLAGS_datastream_service_num_svc_threads : CpuInfo::num_cores();
    LOG(INFO) << "Num svc thread=" << num_svc_threads;
    return rpc_mgr_->RegisterService(num_svc_threads, 1024, this, mem_tracker(),
        ExecEnv::GetInstance()->rpc_metrics());
  }

  virtual bool Authorize(const google::protobuf::Message* req,
      google::protobuf::Message* resp, kudu::rpc::RpcContext* context) {
    return true;
  }

  virtual void TransmitData(const TransmitDataRequestPB* request,
      TransmitDataResponsePB* response, RpcContext* rpc_context) {
    transmit_counter_++;
    DebugActionNoFail(FLAGS_debug_actions, "TRANSMIT_DATA_DELAY");
    stream_mgr_->AddData(request, response, rpc_context);
  }

  virtual void EndDataStream(const EndDataStreamRequestPB* request,
      EndDataStreamResponsePB* response, RpcContext* rpc_context) {
    stream_mgr_->CloseSender(request, response, rpc_context);
  }

  virtual void UpdateFilter(
      const UpdateFilterParamsPB* req, UpdateFilterResultPB* resp, RpcContext* context) {}

  virtual void UpdateFilterFromRemote(
      const UpdateFilterParamsPB* req, UpdateFilterResultPB* resp, RpcContext* context) {}

  virtual void PublishFilter(const PublishFilterParamsPB* req,
      PublishFilterResultPB* resp, RpcContext* context) {}

  MemTracker* mem_tracker() { return mem_tracker_.get(); }

  int NumTransmitsReceived() const {
    return transmit_counter_.load();
  }

  int QueueSize() {
    rapidjson::Document doc(rapidjson::kObjectType);
    rpc_mgr_->ToJson(&doc);
    return doc["services"][0]["queue_size"].GetInt();
  }

 private:
  RpcMgr* rpc_mgr_;
  KrpcDataStreamMgr* stream_mgr_;
  unique_ptr<MemTracker> mem_tracker_;
  std::atomic<int> transmit_counter_;
};

class DataStreamTest : public testing::Test {
 protected:
  DataStreamTest() : next_val_(0) {
    // Stop tests that rely on mismatched sender / receiver pairs timing out from failing.
    FLAGS_datastream_sender_timeout_ms = 250;
    // MemTracker is not given a mem limit, so the default would be 5% * 0 = 0.
    FLAGS_datastream_service_queue_mem_limit = std::to_string(TOTAL_DATA_SIZE);
  }
  ~DataStreamTest() { runtime_state_->ReleaseResources(); }

  virtual void SetUp() {
    exec_env_.reset(new ExecEnv());
    ABORT_IF_ERROR(exec_env_->InitForFeSupport());
    exec_env_->InitBufferPool(32 * 1024, 1024 * 1024 * 1024, 32 * 1024);
    runtime_state_.reset(new RuntimeState(TQueryCtx(), exec_env_.get()));
    TPlanFragment* fragment = runtime_state_->obj_pool()->Add(new TPlanFragment());
    PlanFragmentCtxPB* fragment_ctx =
        runtime_state_->obj_pool()->Add(new PlanFragmentCtxPB());
    fragment_state_ = runtime_state_->obj_pool()->Add(
        new FragmentState(runtime_state_->query_state(), *fragment, *fragment_ctx));
    mem_pool_.reset(new MemPool(&tracker_));

    // Register a BufferPool client for allocating buffers for row batches.
    ABORT_IF_ERROR(exec_env_->buffer_pool()->RegisterClient(
        "DataStream Test Recvr", nullptr, exec_env_->buffer_reservation(), &tracker_,
        numeric_limits<int64_t>::max(), runtime_state_->runtime_profile(),
        &buffer_pool_client_));

    CreateRowDesc();

    SlotRef* lhs_slot = obj_pool_.Add(new SlotRef(ColumnType(TYPE_BIGINT), 0));
    ASSERT_OK(lhs_slot->Init(RowDescriptor(), true, fragment_state_));
    ordering_exprs_.push_back(lhs_slot);

    tsort_info_.sorting_order = TSortingOrder::LEXICAL;
    tsort_info_.is_asc_order.push_back(true);
    tsort_info_.nulls_first.push_back(true);

    next_instance_id_.set_lo(0);
    next_instance_id_.set_hi(0);
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

    IpAddr ip;
    ASSERT_OK(HostnameToIpAddr(FLAGS_hostname, &ip));
    UUIDToUniqueIdPB(boost::uuids::random_generator()(), &backend_id_);
    krpc_address_ = MakeNetworkAddressPB(
        ip, FLAGS_port, backend_id_, exec_env_->rpc_mgr()->GetUdsAddressUniqueId());
    StartKrpcBackend();
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
    for (auto less_than_comparator : less_than_comparators_) {
      less_than_comparator->Close(runtime_state_.get());
    }
    ScalarExpr::Close(ordering_exprs_);
    fragment_state_->ReleaseResources();
    fragment_state_ = nullptr;
    mem_pool_->FreeAll();
    StopKrpcBackend();
    exec_env_->buffer_pool()->DeregisterClient(&buffer_pool_client_);
  }

  void Reset() {
    sender_info_.clear();
    receiver_info_.clear();
    dest_.Clear();
  }

  // Ensure gflags are reset in each test class.
  gflags::FlagSaver saver_;

  ObjectPool obj_pool_;
  MemTracker tracker_;
  scoped_ptr<MemPool> mem_pool_;
  DescriptorTbl* desc_tbl_;
  const RowDescriptor* row_desc_;
  TSortInfo tsort_info_;
  vector<TupleRowComparator*> less_than_comparators_;
  boost::scoped_ptr<ExecEnv> exec_env_;
  scoped_ptr<RuntimeState> runtime_state_;
  FragmentState* fragment_state_;
  UniqueIdPB next_instance_id_;
  string stmt_;
  // The sorting expression for the single BIGINT column.
  vector<ScalarExpr*> ordering_exprs_;

  // Client for allocating buffers for row batches.
  BufferPool::ClientHandle buffer_pool_client_;

  // RowBatch generation
  scoped_ptr<RowBatch> batch_;
  int next_val_;
  int64_t* tuple_mem_;

  // Backend Id
  UniqueIdPB backend_id_;

  // Only used for KRPC. Not owned.
  NetworkAddressPB krpc_address_;

  // The test service implementation. Owned by this class.
  unique_ptr<ImpalaKRPCTestBackend> test_service_;

  // receiving node
  KrpcDataStreamMgr* stream_mgr_ = nullptr;

  // sending node(s)
  TDataStreamSink broadcast_sink_;
  TDataStreamSink random_sink_;
  TDataStreamSink hash_sink_;
  google::protobuf::RepeatedPtrField<PlanFragmentDestinationPB> dest_;

  struct SenderInfo {
    unique_ptr<thread> thread_handle;
    Status status;
    int num_bytes_sent = 0;
  };
  // Allocate each SenderInfo separately so the address doesn't change.
  vector<unique_ptr<SenderInfo>> sender_info_;

  struct ReceiverInfo {
    TPartitionType::type stream_type;
    int num_senders;
    int receiver_num;

    unique_ptr<thread> thread_handle;
    shared_ptr<KrpcDataStreamRecvr> stream_recvr;
    Status status;
    int num_rows_received = 0;
    multiset<int64_t> data_values;

    ReceiverInfo(TPartitionType::type stream_type, int num_senders, int receiver_num)
      : stream_type(stream_type),
        num_senders(num_senders),
        receiver_num(receiver_num) {}

    ~ReceiverInfo() {
      thread_handle.reset();
      stream_recvr.reset();
    }
  };
  // Allocate each ReceiveInfo separately so the address doesn't change.
  vector<unique_ptr<ReceiverInfo>> receiver_info_;

  // Create an instance id and add it to dest_
  void GetNextInstanceId(TUniqueId* instance_id) {
    PlanFragmentDestinationPB* dest = dest_.Add();
    *dest->mutable_fragment_instance_id() = next_instance_id_;
    *dest->mutable_address() = MakeNetworkAddressPB("localhost", FLAGS_port, backend_id_,
        exec_env_->rpc_mgr()->GetUdsAddressUniqueId());
    *dest->mutable_krpc_backend() = krpc_address_;
    UniqueIdPBToTUniqueId(next_instance_id_, instance_id);
    next_instance_id_.set_lo(next_instance_id_.lo() + 1);
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
    EXPECT_OK(DescriptorTbl::CreateInternal(&obj_pool_, thrift_desc_tbl, &desc_tbl_));

    vector<TTupleId> row_tids;
    row_tids.push_back(0);
    vector<bool> nullable_tuples;
    nullable_tuples.push_back(false);
    row_desc_ = obj_pool_.Add(new RowDescriptor(*desc_tbl_, row_tids, nullable_tuples));
  }

  // Create a tuple comparator to sort in ascending order on the single bigint column.
  void CreateTupleComparator(TupleRowComparator** less_than_comparator) {
    TupleRowComparatorConfig* comparator_config =
        obj_pool_.Add(new TupleRowComparatorConfig(tsort_info_, ordering_exprs_));
    *less_than_comparator =
        obj_pool_.Add(new TupleRowLexicalComparator(*comparator_config));
    ASSERT_OK((*less_than_comparator)->Open(
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
      int buffer_size, bool is_merging, TUniqueId* out_id = nullptr,
      RuntimeProfile** out_profile = nullptr) {
    VLOG_QUERY << "start receiver";
    RuntimeProfile* profile = RuntimeProfile::Create(&obj_pool_, "TestReceiver");
    TUniqueId instance_id;
    GetNextInstanceId(&instance_id);
    receiver_info_.emplace_back(
        make_unique<ReceiverInfo>(stream_type, num_senders, receiver_num));
    ReceiverInfo* info = receiver_info_.back().get();
    info->stream_recvr = stream_mgr_->CreateRecvr(row_desc_, *runtime_state_.get(),
        instance_id, DEST_NODE_ID, num_senders, buffer_size, is_merging, profile,
        &tracker_, &buffer_pool_client_);
   if (!is_merging) {
      info->thread_handle.reset(new thread(&DataStreamTest::ReadStream, this, info));
    } else {
      TupleRowComparator* less_than_comparator = nullptr;
      CreateTupleComparator(&less_than_comparator);
      DCHECK(less_than_comparator != nullptr);
      less_than_comparators_.push_back(less_than_comparator);
      info->thread_handle.reset(new thread(&DataStreamTest::ReadStreamMerging, this, info,
          profile, less_than_comparator));
    }
    if (out_id != nullptr) *out_id = instance_id;
    if (out_profile != nullptr) *out_profile = profile;
  }

  void JoinReceivers() {
    VLOG_QUERY << "join receiver\n";
    for (int i = 0; i < receiver_info_.size(); ++i) {
      receiver_info_[i]->thread_handle->join();
      receiver_info_[i]->stream_recvr->Close();
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

  void ReadStreamMerging(ReceiverInfo* info, RuntimeProfile* profile,
      TupleRowComparator* less_than_comparator) {
    /// Note that codegend_heapify_helper_fn currently stores a nullptr.
    /// The codegened case of this function is covered in end-to-end tests.
    CodegenFnPtr<SortedRunMerger::HeapifyHelperFn> codegend_heapify_helper_fn;
    info->status = info->stream_recvr->CreateMerger(
        *less_than_comparator, codegend_heapify_helper_fn);
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
  void CheckReceivers(TPartitionType::type stream_type, int num_senders,
      int num_batches = NUM_BATCHES) {
    int64_t total = 0;
    multiset<int64_t> all_data_values;
    for (int i = 0; i < receiver_info_.size(); ++i) {
      ReceiverInfo* info = receiver_info_[i].get();
      EXPECT_OK(info->status);
      total += info->data_values.size();
      ASSERT_EQ(info->stream_type, stream_type);
      ASSERT_EQ(info->num_senders, num_senders);
      if (stream_type == TPartitionType::UNPARTITIONED) {
        EXPECT_EQ(
            num_batches * BATCH_CAPACITY * num_senders, info->data_values.size());
      }
      all_data_values.insert(info->data_values.begin(), info->data_values.end());

      int k = 0;
      for (multiset<int64_t>::iterator j = info->data_values.begin();
           j != info->data_values.end(); ++j, ++k) {
        if (stream_type == TPartitionType::UNPARTITIONED) {
          // unpartitioned streams contain all values as many times as there are
          // senders
          EXPECT_EQ(k / num_senders, *j);
        } else if (stream_type == TPartitionType::HASH_PARTITIONED) {
          // hash-partitioned streams send values to the right partition
          int64_t value = *j;
          uint64_t hash_val = RawValue::GetHashValueFastHash(
              &value, ColumnType(TYPE_BIGINT),
              GetExchangeHashSeed(runtime_state_->query_id()));
          EXPECT_EQ(hash_val % receiver_info_.size(), info->receiver_num);
        }
      }
    }

    if (stream_type == TPartitionType::HASH_PARTITIONED) {
      EXPECT_EQ(num_batches * BATCH_CAPACITY * num_senders, total);

      int k = 0;
      for (multiset<int64_t>::iterator j = all_data_values.begin();
           j != all_data_values.end(); ++j, ++k) {
        // each sender sent all values
        EXPECT_EQ(k / num_senders, *j);
        if (k/num_senders != *j) break;
      }
    }
  }

  // Returns a map of reciever to all it's data values.
  unordered_map<int, multiset<int64_t>> GetHashPartitionedReceiversDataMap(
      int num_receivers, bool reset_hash_seed) {
    int num_senders = 1;
    int buffer_size = 1024;
    bool merging = false;
    unordered_map<int, multiset<int64_t>> receiver_data_map;
    Reset();
    for (int i = 0; i < num_receivers; ++i) {
      StartReceiver(TPartitionType::HASH_PARTITIONED, num_senders, i,
          buffer_size, merging);
    }
    for (int i = 0; i < num_senders; ++i) {
      StartSender(TPartitionType::HASH_PARTITIONED, buffer_size,
          reset_hash_seed);
    }
    JoinSenders();
    CheckSenders();
    JoinReceivers();
    receiver_data_map.clear();

    for (int i = 0; i < receiver_info_.size(); i++) {
      // Store a map of receiver and list of it's data values.
      ReceiverInfo* info = receiver_info_[i].get();
      multiset<int64_t> data_set;
      for (multiset<int64_t>::iterator j = info->data_values.begin();
           j != info->data_values.end(); ++j) {
        data_set.insert(*j);
      }
      receiver_data_map[info->receiver_num] = data_set;
    }
    return receiver_data_map;
  }

  /// Returns a hash seed with query_id.
  uint64_t GetExchangeHashSeed(TUniqueId query_id) {
    return 0x66bd68df22c3ef37 ^ query_id.hi;
  }

  void CheckSenders() {
    for (int i = 0; i < sender_info_.size(); ++i) {
      EXPECT_OK(sender_info_[i]->status);
      EXPECT_GT(sender_info_[i]->num_bytes_sent, 0);
    }
  }

  void StartKrpcBackend() {
    RpcMgr* rpc_mgr = exec_env_->rpc_mgr();
    KrpcDataStreamMgr* krpc_stream_mgr = exec_env_->stream_mgr();
    ASSERT_OK(rpc_mgr->Init(krpc_address_));
    test_service_.reset(new ImpalaKRPCTestBackend(rpc_mgr, krpc_stream_mgr,
        exec_env_->process_mem_tracker()));
    ASSERT_OK(test_service_->Init());
    ASSERT_OK(krpc_stream_mgr->Init(test_service_->mem_tracker()));
    ASSERT_OK(rpc_mgr->StartServices());
  }

  void StopKrpcBackend() {
    exec_env_->rpc_mgr()->Shutdown();
  }

  void StartSender(TPartitionType::type partition_type = TPartitionType::UNPARTITIONED,
      int channel_buffer_size = 1024, bool reset_hash_seed = false,
      int num_batches = NUM_BATCHES) {
    VLOG_QUERY << "start sender";
    int num_senders = sender_info_.size();
    sender_info_.emplace_back(make_unique<SenderInfo>());
    sender_info_.back()->thread_handle.reset(
        new thread(&DataStreamTest::Sender, this, num_senders, channel_buffer_size,
            partition_type, sender_info_[num_senders].get(), reset_hash_seed,
            num_batches));
  }

  void JoinSenders() {
    VLOG_QUERY << "join senders\n";
    for (int i = 0; i < sender_info_.size(); ++i) {
      sender_info_[i]->thread_handle->join();
    }
  }

  void Sender(int sender_num, int channel_buffer_size,
      TPartitionType::type partition_type, SenderInfo* info, bool reset_hash_seed,
      int num_batches) {
    RuntimeState state(TQueryCtx(), exec_env_.get(), desc_tbl_);
    VLOG_QUERY << "create sender " << sender_num;
    const TDataSink sink = GetSink(partition_type);
    TPlanFragment fragment;
    fragment.output_sink = sink;
    PlanFragmentCtxPB fragment_ctx;
    *fragment_ctx.mutable_destinations() = dest_;
    FragmentState fragment_state(state.query_state(), fragment, fragment_ctx);
    DataSinkConfig* data_sink = nullptr;
    EXPECT_OK(DataSinkConfig::CreateConfig(sink, row_desc_, &fragment_state, &data_sink));

    // We create an object of the base class DataSink and cast to the appropriate sender
    // according to the 'is_thrift' option.
    scoped_ptr<DataSink> sender;

    KrpcDataStreamSenderConfig& config =
        *(static_cast<KrpcDataStreamSenderConfig*>(data_sink));

    // Reset the hash seed with a new query id. Useful for testing hash exchanges are
    // random.
    if (reset_hash_seed) {
      config.exchange_hash_seed_ =
          GetExchangeHashSeed(UuidToQueryId(random_generator()()));
    }

    sender.reset(new KrpcDataStreamSender(-1, sender_num, config,
        data_sink->tsink_->stream_sink, dest_, channel_buffer_size, &state));
    EXPECT_OK(sender->Prepare(&state, &tracker_));
    EXPECT_OK(sender->Open(&state));
    scoped_ptr<RowBatch> batch(CreateRowBatch());
    int next_val = 0;
    for (int i = 0; i < num_batches; ++i) {
      GetNextBatch(batch.get(), &next_val);
      VLOG_QUERY << "sender " << sender_num << ": #rows=" << batch->num_rows();
      info->status = sender->Send(&state, batch.get());
      if (!info->status.ok()) break;
    }
    VLOG_QUERY << "closing sender" << sender_num;
    info->status.MergeStatus(sender->FlushFinal(&state));
    sender->Close(&state);
    info->num_bytes_sent = static_cast<KrpcDataStreamSender*>(
        sender.get())->GetNumDataBytesSent();

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

// A separate test class which simulates that the service queue is slow processing RPCs.
class DataStreamTestSlowServiceQueue : public DataStreamTest {
 protected:
  virtual void SetUp() {
    FLAGS_datastream_service_num_svc_threads = 1;
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

TEST_F(DataStreamTest, UnknownSenderSmallResult) {
  // starting a sender w/o a corresponding receiver results in an error. No bytes should
  // be sent.
  // case 1: entire query result fits in single buffer
  TUniqueId dummy_id;
  GetNextInstanceId(&dummy_id);
  StartSender(TPartitionType::UNPARTITIONED, TOTAL_DATA_SIZE + 1024);
  JoinSenders();
  EXPECT_EQ(sender_info_[0]->status.code(), TErrorCode::DATASTREAM_SENDER_TIMEOUT);
}

TEST_F(DataStreamTest, UnknownSenderLargeResult) {
  // case 2: query result requires multiple buffers
  TUniqueId dummy_id;
  GetNextInstanceId(&dummy_id);
  StartSender();
  JoinSenders();
  EXPECT_EQ(sender_info_[0]->status.code(), TErrorCode::DATASTREAM_SENDER_TIMEOUT);
}

TEST_F(DataStreamTest, Cancel) {
  TUniqueId instance_id;
  StartReceiver(TPartitionType::UNPARTITIONED, 1, 1, 1024, false, &instance_id);
  stream_mgr_->Cancel(GetQueryId(instance_id));
  StartReceiver(TPartitionType::UNPARTITIONED, 1, 1, 1024, true, &instance_id);
  stream_mgr_->Cancel(GetQueryId(instance_id));
  JoinReceivers();
  EXPECT_TRUE(receiver_info_[0]->status.IsCancelled());
  EXPECT_TRUE(receiver_info_[1]->status.IsCancelled());
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

// Test streams with different query ids should hash to different destinations.
TEST_F(DataStreamTest, HashPartitionTest) {
  bool result = false;
  int num_receivers = 4;

  unordered_map<int, multiset<int64_t>> receiver_data_map_1 =
      GetHashPartitionedReceiversDataMap(num_receivers, false);

  unordered_map<int, multiset<int64_t>> receiver_data_map_2 =
      GetHashPartitionedReceiversDataMap(num_receivers, true);

  // Check the sizes of the receiver data values in each receiver is different.
  for (int i = 0; i < num_receivers; ++i) {
    // Compare the data values in the recievers for the two queries. Verify the values
    // don't match for at least one reciever.
    if (receiver_data_map_1[i] != receiver_data_map_2[i]) {
      result = true;
      break;
    }
  }
  ASSERT_EQ(result, true);
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
TEST_F(DataStreamTestShortDeserQueue, TestNoDeadlock) {
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
  receiver_info_.emplace_back(
      make_unique<ReceiverInfo>(TPartitionType::UNPARTITIONED, 4, 1));
  ReceiverInfo* info = receiver_info_.back().get();
  info->stream_recvr = stream_mgr_->CreateRecvr(row_desc_, *runtime_state_.get(),
      instance_id, DEST_NODE_ID, 4, 1024 * 1024, false, profile, &tracker_,
      &buffer_pool_client_);
  info->thread_handle.reset(new thread(
      &DataStreamTestShortDeserQueue_TestNoDeadlock_Test::ReadStream, this, info));

  JoinSenders();
  CheckSenders();
  JoinReceivers();

  // Check that 4 payloads have been received.
  CheckReceivers(TPartitionType::UNPARTITIONED, 4);
}

// Test that EndDataStream RPCs are prioritized over other RPCs in the ImpalaServicePool
// queue. It starts a receiver with 1 service thread and a 3s delay on processing
// TransmitData RPCs, and starts 2 senders each trying to send data. The 1st sender's RPC
// starts processing, and the 2nd sender's RPC is queued in the ImpalaServicePool. Then
// starts a 3rd sender that sends no data, just EndDataStream, and asserts it completes
// before the 2nd block that's already queued i.e. before the 2nd 3s delay has passed.
TEST_F(DataStreamTestSlowServiceQueue, TestPrioritizeEos) {
  FLAGS_debug_actions="TRANSMIT_DATA_DELAY:SLEEP@3000";
  TPartitionType::type stream_type = TPartitionType::UNPARTITIONED;
  constexpr int buffer_size = 1024;
  constexpr int num_batches = 1;
  Reset();

  TUniqueId instance_id;
  RuntimeProfile* profile;
  StartReceiver(stream_type, 3, 0, buffer_size, false, &instance_id, &profile);

  int64_t start = MonotonicNanos();
  // Start sender; TRANSMIT_DATA_DELAY:SLEEP will block up the queue.
  StartSender(stream_type, buffer_size, false, num_batches);
  StartSender(stream_type, buffer_size, false, num_batches);
  // Wait for 1st sender batch to be received and 2nd sender batch queued.
  while (test_service_->NumTransmitsReceived() == 0) SleepForMs(1);
  while (test_service_->QueueSize() == 0) SleepForMs(1);

  // Start last sender with no batches so it just sends EndDataStream.
  StartSender(stream_type, buffer_size, false, 0);

  // Wait for last sender to complete.
  sender_info_.back()->thread_handle->join();
  sender_info_.pop_back();
  // Verify receiver got EOS.
  vector<RuntimeProfileBase::Counter*> counters;
  profile->GetCounters("TotalEosReceived", &counters);
  ASSERT_EQ(1, counters.size());
  EXPECT_EQ(1, counters.front()->value());
  // Assert 2nd TRANSMIT_DATA_DELAY sleep is not finished.
  int64_t duration = MonotonicNanos() - start;
  EXPECT_GE(duration, 3 * NANOS_PER_SEC);
  EXPECT_LT(duration, 6 * NANOS_PER_SEC);

  // Clean up.
  JoinSenders();
  JoinReceivers();
  EXPECT_GE(MonotonicNanos() - start, 6 * NANOS_PER_SEC);
  EXPECT_EQ(2 * num_batches * BATCH_CAPACITY, receiver_info_[0]->data_values.size());
  EXPECT_EQ(3, counters.front()->value());
}

// Test that payloads larger than the service queue's soft mem limit can be transmitted.
TEST_F(DataStreamTestShortServiceQueue, TestLargePayload) {
  TestStream(
      TPartitionType::UNPARTITIONED, 4, 1, SHORT_SERVICE_QUEUE_MEM_LIMIT * 2, false);
}

// Test that EndDataStream messages are admitted to the ImpalaServicePool queue when the
// pool memory limit has been reached. Starts a receiver with a very low memory limit on
// the pool so only one message is admitted to the queue at a time; others will be
// rejected and must be retried by the sender. Starts a sender that sends a batch, with
// processing of TransmitData delayed for 3s; then starts another sender with no blocks
// that just sends EndDataStream and verifies the EndDataStream message is processed
// before the 3s delay finishes, so it must have been admitted while memory was still
// held for the TransmitData message.
TEST_F(DataStreamTestShortServiceQueue, TestFullQueue) {
  FLAGS_debug_actions="TRANSMIT_DATA_DELAY:SLEEP@3000";
  TPartitionType::type stream_type = TPartitionType::UNPARTITIONED;
  constexpr int buffer_size = SHORT_SERVICE_QUEUE_MEM_LIMIT * 2;
  constexpr int num_batches = 1;
  Reset();

  TUniqueId instance_id;
  RuntimeProfile* profile;
  StartReceiver(stream_type, 2, 0, buffer_size, false, &instance_id, &profile);

  int64_t start = MonotonicNanos();
  // Start sender; TRANSMIT_DATA_DELAY:SLEEP will block up the queue.
  StartSender(stream_type, buffer_size, false, num_batches);
  // Wait for 1st batch to be received.
  while (test_service_->NumTransmitsReceived() == 0) SleepForMs(1);

  // Start last sender with no batches so it just sends EndDataStream.
  StartSender(stream_type, buffer_size, false, 0);

  // Wait for last sender to complete.
  sender_info_.back()->thread_handle->join();
  sender_info_.pop_back();
  // Verify receiver got EOS.
  vector<RuntimeProfileBase::Counter*> counters;
  profile->GetCounters("TotalEosReceived", &counters);
  ASSERT_EQ(1, counters.size());
  EXPECT_EQ(1, counters.front()->value());
  // Assert TRANSMIT_DATA_DELAY sleep is not finished.
  EXPECT_LT(MonotonicNanos() - start, 3 * NANOS_PER_SEC);

  // Clean up.
  JoinSenders();
  JoinReceivers();
  EXPECT_GE(MonotonicNanos() - start, 3 * NANOS_PER_SEC);
  EXPECT_EQ(num_batches * BATCH_CAPACITY, receiver_info_[0]->data_values.size());
  EXPECT_EQ(2, counters.front()->value());
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
