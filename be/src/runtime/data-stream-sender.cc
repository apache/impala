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

#include "runtime/data-stream-sender.h"

#include <iostream>
#include <thrift/protocol/TDebugProtocol.h>

#include "common/logging.h"
#include "exprs/scalar-expr.h"
#include "exprs/scalar-expr-evaluator.h"
#include "gutil/strings/substitute.h"
#include "runtime/descriptors.h"
#include "runtime/tuple-row.h"
#include "runtime/row-batch.h"
#include "runtime/raw-value.inline.h"
#include "runtime/runtime-state.h"
#include "runtime/client-cache.h"
#include "runtime/mem-tracker.h"
#include "runtime/backend-client.h"
#include "util/aligned-new.h"
#include "util/condition-variable.h"
#include "util/debug-util.h"
#include "util/network-util.h"
#include "util/thread-pool.h"
#include "rpc/thrift-client.h"
#include "rpc/thrift-util.h"

#include "gen-cpp/Types_types.h"
#include "gen-cpp/ImpalaInternalService.h"
#include "gen-cpp/ImpalaInternalService_types.h"

#include "common/names.h"

using namespace apache::thrift;
using namespace apache::thrift::protocol;
using namespace apache::thrift::transport;

namespace impala {

// A channel sends data asynchronously via calls to TransmitData
// to a single destination ipaddress/node.
// It has a fixed-capacity buffer and allows the caller either to add rows to
// that buffer individually (AddRow()), or circumvent the buffer altogether and send
// TRowBatches directly (SendBatch()). Either way, there can only be one in-flight RPC
// at any one time (ie, sending will block if the most recent rpc hasn't finished,
// which allows the receiver node to throttle the sender by withholding acks).
// *Not* thread-safe.
class DataStreamSender::Channel : public CacheLineAligned {
 public:
  // Create channel to send data to particular ipaddress/port/query/node
  // combination. buffer_size is specified in bytes and a soft limit on
  // how much tuple data is getting accumulated before being sent; it only applies
  // when data is added via AddRow() and not sent directly via SendBatch().
  Channel(DataStreamSender* parent, const RowDescriptor* row_desc,
      const TNetworkAddress& destination, const TUniqueId& fragment_instance_id,
      PlanNodeId dest_node_id, int buffer_size)
    : parent_(parent),
      buffer_size_(buffer_size),
      row_desc_(row_desc),
      address_(MakeNetworkAddress(destination.hostname, destination.port)),
      fragment_instance_id_(fragment_instance_id),
      dest_node_id_(dest_node_id),
      num_data_bytes_sent_(0),
      rpc_thread_("DataStreamSender", "SenderThread", 1, 1,
          bind<void>(mem_fn(&Channel::TransmitData), this, _1, _2), true),
      rpc_in_flight_(false) {}

  // Initialize channel.
  // Returns OK if successful, error indication otherwise.
  Status Init(RuntimeState* state) WARN_UNUSED_RESULT;

  // Copies a single row into this channel's output buffer and flushes buffer
  // if it reaches capacity.
  // Returns error status if any of the preceding rpcs failed, OK otherwise.
  Status ALWAYS_INLINE AddRow(TupleRow* row) WARN_UNUSED_RESULT;

  // Asynchronously sends a row batch.
  // Returns the status of the most recently finished TransmitData
  // rpc (or OK if there wasn't one that hasn't been reported yet).
  Status SendBatch(TRowBatch* batch) WARN_UNUSED_RESULT;

  // Return status of last TransmitData rpc (initiated by the most recent call
  // to either SendBatch() or SendCurrentBatch()).
  Status GetSendStatus() WARN_UNUSED_RESULT;

  // Waits for the rpc thread pool to finish the current rpc.
  void WaitForRpc();

  // Drain and shutdown the rpc thread and free the row batch allocation.
  void Teardown(RuntimeState* state);

  // Flushes any buffered row batches and sends the EOS RPC to close the channel.
  Status FlushAndSendEos(RuntimeState* state) WARN_UNUSED_RESULT;

  int64_t num_data_bytes_sent() const { return num_data_bytes_sent_; }
  TRowBatch* thrift_batch() { return &thrift_batch_; }

 private:
  DataStreamSender* parent_;
  int buffer_size_;

  const RowDescriptor* row_desc_;
  TNetworkAddress address_;
  TUniqueId fragment_instance_id_;
  PlanNodeId dest_node_id_;

  // the number of TRowBatch.data bytes sent successfully
  int64_t num_data_bytes_sent_;

  // we're accumulating rows into this batch
  scoped_ptr<RowBatch> batch_;
  TRowBatch thrift_batch_;

  // We want to reuse the rpc thread to prevent creating a thread per rowbatch.
  // TODO: currently we only have one batch in flight, but we should buffer more
  // batches. This is a bit tricky since the channels share the outgoing batch
  // pointer we need some mechanism to coordinate when the batch is all done.
  // TODO: if the order of row batches does not matter, we can consider increasing
  // the number of threads.
  ThreadPool<TRowBatch*> rpc_thread_; // sender thread.
  ConditionVariable rpc_done_cv_;   // signaled when rpc_in_flight_ is set to true.
  mutex rpc_thread_lock_; // Lock with rpc_done_cv_ protecting rpc_in_flight_
  bool rpc_in_flight_;  // true if the rpc_thread_ is busy sending.

  Status rpc_status_;  // status of most recently finished TransmitData rpc
  RuntimeState* runtime_state_;

  // Serialize batch_ into thrift_batch_ and send via SendBatch().
  // Returns SendBatch() status.
  Status SendCurrentBatch();

  // Synchronously call TransmitData() on a client from impalad_client_cache and
  // update rpc_status_ based on return value (or set to error if RPC failed).
  // Called from a thread from the rpc_thread_ pool.
  void TransmitData(int thread_id, const TRowBatch*);
  void TransmitDataHelper(const TRowBatch*);

  // Send RPC and retry waiting for response if get RPC timeout error.
  Status DoTransmitDataRpc(ImpalaBackendConnection* client,
      const TTransmitDataParams& params, TTransmitDataResult* res);
};

Status DataStreamSender::Channel::Init(RuntimeState* state) {
  RETURN_IF_ERROR(rpc_thread_.Init());
  runtime_state_ = state;
  // TODO: figure out how to size batch_
  int capacity = max(1, buffer_size_ / max(row_desc_->GetRowSize(), 1));
  batch_.reset(new RowBatch(row_desc_, capacity, parent_->mem_tracker()));
  return Status::OK();
}

Status DataStreamSender::Channel::SendBatch(TRowBatch* batch) {
  VLOG_ROW << "Channel::SendBatch() fragment_instance_id=" << fragment_instance_id_
           << " dest_node=" << dest_node_id_ << " #rows=" << batch->num_rows;
  // return if the previous batch saw an error
  RETURN_IF_ERROR(GetSendStatus());
  {
    unique_lock<mutex> l(rpc_thread_lock_);
    rpc_in_flight_ = true;
  }
  if (!rpc_thread_.Offer(batch)) {
    unique_lock<mutex> l(rpc_thread_lock_);
    rpc_in_flight_ = false;
  }
  return Status::OK();
}

void DataStreamSender::Channel::TransmitData(int thread_id, const TRowBatch* batch) {
  DCHECK(rpc_in_flight_);
  TransmitDataHelper(batch);

  {
    unique_lock<mutex> l(rpc_thread_lock_);
    rpc_in_flight_ = false;
  }
  rpc_done_cv_.NotifyOne();
}

void DataStreamSender::Channel::TransmitDataHelper(const TRowBatch* batch) {
  DCHECK(batch != NULL);
  VLOG_ROW << "Channel::TransmitData() fragment_instance_id=" << fragment_instance_id_
           << " dest_node=" << dest_node_id_
           << " #rows=" << batch->num_rows;
  TTransmitDataParams params;
  params.protocol_version = ImpalaInternalServiceVersion::V1;
  params.__set_dest_fragment_instance_id(fragment_instance_id_);
  params.__set_dest_node_id(dest_node_id_);
  params.__set_row_batch(*batch);  // yet another copy
  params.__set_eos(false);
  params.__set_sender_id(parent_->sender_id_);

  ImpalaBackendConnection client(runtime_state_->impalad_client_cache(),
      address_, &rpc_status_);
  if (!rpc_status_.ok()) return;

  TTransmitDataResult res;
  client->SetTransmitDataCounter(parent_->thrift_transmit_timer_);
  rpc_status_ = DoTransmitDataRpc(&client, params, &res);
  client->ResetTransmitDataCounter();
  if (!rpc_status_.ok()) return;
  COUNTER_ADD(parent_->profile_->total_time_counter(),
      parent_->thrift_transmit_timer_->LapTime());

  if (res.status.status_code != TErrorCode::OK) {
    rpc_status_ = res.status;
  } else {
    num_data_bytes_sent_ += RowBatch::GetSerializedSize(*batch);
    VLOG_ROW << "incremented #data_bytes_sent="
             << num_data_bytes_sent_;
  }
}

Status DataStreamSender::Channel::DoTransmitDataRpc(ImpalaBackendConnection* client,
    const TTransmitDataParams& params, TTransmitDataResult* res) {
  Status status = client->DoRpc(&ImpalaBackendClient::TransmitData, params, res);
  while (status.code() == TErrorCode::RPC_RECV_TIMEOUT &&
      !runtime_state_->is_cancelled()) {
    status = client->RetryRpcRecv(&ImpalaBackendClient::recv_TransmitData, res);
  }
  return status;
}

void DataStreamSender::Channel::WaitForRpc() {
  SCOPED_TIMER(parent_->state_->total_network_send_timer());
  unique_lock<mutex> l(rpc_thread_lock_);
  while (rpc_in_flight_) {
    rpc_done_cv_.Wait(l);
  }
}

inline Status DataStreamSender::Channel::AddRow(TupleRow* row) {
  if (batch_->AtCapacity()) {
    // batch_ is full, let's send it; but first wait for an ongoing
    // transmission to finish before modifying thrift_batch_
    RETURN_IF_ERROR(SendCurrentBatch());
  }
  TupleRow* dest = batch_->GetRow(batch_->AddRow());
  const vector<TupleDescriptor*>& descs = row_desc_->tuple_descriptors();
  for (int i = 0; i < descs.size(); ++i) {
    if (UNLIKELY(row->GetTuple(i) == NULL)) {
      dest->SetTuple(i, NULL);
    } else {
      dest->SetTuple(i, row->GetTuple(i)->DeepCopy(*descs[i], batch_->tuple_data_pool()));
    }
  }
  batch_->CommitLastRow();
  return Status::OK();
}

Status DataStreamSender::Channel::SendCurrentBatch() {
  // make sure there's no in-flight TransmitData() call that might still want to
  // access thrift_batch_
  WaitForRpc();
  RETURN_IF_ERROR(parent_->SerializeBatch(batch_.get(), &thrift_batch_));
  batch_->Reset();
  RETURN_IF_ERROR(SendBatch(&thrift_batch_));
  return Status::OK();
}

Status DataStreamSender::Channel::GetSendStatus() {
  WaitForRpc();
  if (!rpc_status_.ok()) {
    LOG(ERROR) << "channel send to " << TNetworkAddressToString(address_) << " failed "
               << "(fragment_instance_id=" << fragment_instance_id_ << "): "
               << rpc_status_.GetDetail();
  }
  return rpc_status_;
}

Status DataStreamSender::Channel::FlushAndSendEos(RuntimeState* state) {
  VLOG_RPC << "Channel::FlushAndSendEos() fragment_instance_id=" << fragment_instance_id_
           << " dest_node=" << dest_node_id_
           << " #rows= " << batch_->num_rows();

  // We can return an error here and not go on to send the EOS RPC because the error that
  // we returned will be sent to the coordinator who will then cancel all the remote
  // fragments including the one that this sender is sending to.
  if (batch_->num_rows() > 0) {
    // flush
    RETURN_IF_ERROR(SendCurrentBatch());
  }

  RETURN_IF_ERROR(GetSendStatus());

  Status client_cnxn_status;
  ImpalaBackendConnection client(runtime_state_->impalad_client_cache(),
      address_, &client_cnxn_status);
  RETURN_IF_ERROR(client_cnxn_status);

  TTransmitDataParams params;
  params.protocol_version = ImpalaInternalServiceVersion::V1;
  params.__set_dest_fragment_instance_id(fragment_instance_id_);
  params.__set_dest_node_id(dest_node_id_);
  params.__set_sender_id(parent_->sender_id_);
  params.__set_eos(true);
  TTransmitDataResult res;

  VLOG_RPC << "calling TransmitData(eos=true) to terminate channel.";
  rpc_status_ = DoTransmitDataRpc(&client, params, &res);
  if (!rpc_status_.ok()) {
    LOG(ERROR) << "Failed to send EOS to " << TNetworkAddressToString(address_)
               << " (fragment_instance_id=" << fragment_instance_id_ << "): "
               << rpc_status_.GetDetail();
    return rpc_status_;
  }
  return Status(res.status);
}

void DataStreamSender::Channel::Teardown(RuntimeState* state) {
  // FlushAndSendEos() should have been called before calling Teardown(), which means that
  // all the data should already be drained. Calling DrainAndShutdown() only to shutdown.
  rpc_thread_.DrainAndShutdown();
  batch_.reset();
}

DataStreamSender::DataStreamSender(int sender_id, const RowDescriptor* row_desc,
    const TDataStreamSink& sink, const vector<TPlanFragmentDestination>& destinations,
    int per_channel_buffer_size, RuntimeState* state)
  : DataSink(row_desc,
        Substitute("DataStreamSender (dst_id=$0)", sink.dest_node_id), state),
    sender_id_(sender_id),
    partition_type_(sink.output_partition.type),
    current_channel_idx_(0),
    flushed_(false),
    closed_(false),
    current_thrift_batch_(&thrift_batch1_),
    serialize_batch_timer_(NULL),
    thrift_transmit_timer_(NULL),
    bytes_sent_counter_(NULL),
    total_sent_rows_counter_(NULL),
    dest_node_id_(sink.dest_node_id),
    next_unknown_partition_(0) {
  DCHECK_GT(destinations.size(), 0);
  DCHECK(sink.output_partition.type == TPartitionType::UNPARTITIONED
      || sink.output_partition.type == TPartitionType::HASH_PARTITIONED
      || sink.output_partition.type == TPartitionType::RANDOM
      || sink.output_partition.type == TPartitionType::KUDU);
  // TODO: use something like google3's linked_ptr here (scoped_ptr isn't copyable)
  for (int i = 0; i < destinations.size(); ++i) {
    channels_.push_back(
        new Channel(this, row_desc, destinations[i].server,
            destinations[i].fragment_instance_id, sink.dest_node_id,
            per_channel_buffer_size));
  }

  if (partition_type_ == TPartitionType::UNPARTITIONED
      || partition_type_ == TPartitionType::RANDOM) {
    // Randomize the order we open/transmit to channels to avoid thundering herd problems.
    srand(reinterpret_cast<uint64_t>(this));
    random_shuffle(channels_.begin(), channels_.end());
  }
}

DataStreamSender::~DataStreamSender() {
  // TODO: check that sender was either already closed() or there was an error
  // on some channel
  for (int i = 0; i < channels_.size(); ++i) {
    delete channels_[i];
  }
}

Status DataStreamSender::Init(const vector<TExpr>& thrift_output_exprs,
    const TDataSink& tsink, RuntimeState* state) {
  DCHECK(tsink.__isset.stream_sink);
  if (partition_type_ == TPartitionType::HASH_PARTITIONED ||
      partition_type_ == TPartitionType::KUDU) {
    RETURN_IF_ERROR(ScalarExpr::Create(tsink.stream_sink.output_partition.partition_exprs,
        *row_desc_, state, &partition_exprs_));
  }
  return Status::OK();
}

Status DataStreamSender::Prepare(RuntimeState* state, MemTracker* parent_mem_tracker) {
  RETURN_IF_ERROR(DataSink::Prepare(state, parent_mem_tracker));
  state_ = state;
  SCOPED_TIMER(profile_->total_time_counter());
  RETURN_IF_ERROR(ScalarExprEvaluator::Create(partition_exprs_, state,
      state->obj_pool(), expr_perm_pool_.get(), expr_results_pool_.get(),
      &partition_expr_evals_));
  bytes_sent_counter_ = ADD_COUNTER(profile(), "BytesSent", TUnit::BYTES);
  uncompressed_bytes_counter_ =
      ADD_COUNTER(profile(), "UncompressedRowBatchSize", TUnit::BYTES);
  serialize_batch_timer_ = ADD_TIMER(profile(), "SerializeBatchTime");
  thrift_transmit_timer_ =
      profile()->AddConcurrentTimerCounter("TransmitDataRPCTime", TUnit::TIME_NS);
  network_throughput_ =
      profile()->AddDerivedCounter("NetworkThroughput(*)", TUnit::BYTES_PER_SECOND,
          bind<int64_t>(&RuntimeProfile::UnitsPerSecond, bytes_sent_counter_,
                                       thrift_transmit_timer_));
  overall_throughput_ =
      profile()->AddDerivedCounter("OverallThroughput", TUnit::BYTES_PER_SECOND,
           bind<int64_t>(&RuntimeProfile::UnitsPerSecond, bytes_sent_counter_,
                         profile()->total_time_counter()));

  total_sent_rows_counter_= ADD_COUNTER(profile(), "RowsReturned", TUnit::UNIT);
  for (int i = 0; i < channels_.size(); ++i) {
    RETURN_IF_ERROR(channels_[i]->Init(state));
  }
  return Status::OK();
}

Status DataStreamSender::Open(RuntimeState* state) {
  return ScalarExprEvaluator::Open(partition_expr_evals_, state);
}

Status DataStreamSender::Send(RuntimeState* state, RowBatch* batch) {
  DCHECK(!closed_);
  DCHECK(!flushed_);

  if (batch->num_rows() == 0) return Status::OK();
  if (partition_type_ == TPartitionType::UNPARTITIONED || channels_.size() == 1) {
    // current_thrift_batch_ is *not* the one that was written by the last call
    // to Serialize()
    RETURN_IF_ERROR(SerializeBatch(batch, current_thrift_batch_, channels_.size()));
    // SendBatch() will block if there are still in-flight rpcs (and those will
    // reference the previously written thrift batch)
    for (int i = 0; i < channels_.size(); ++i) {
      RETURN_IF_ERROR(channels_[i]->SendBatch(current_thrift_batch_));
    }
    current_thrift_batch_ =
        (current_thrift_batch_ == &thrift_batch1_ ? &thrift_batch2_ : &thrift_batch1_);
  } else if (partition_type_ == TPartitionType::RANDOM) {
    // Round-robin batches among channels. Wait for the current channel to finish its
    // rpc before overwriting its batch.
    Channel* current_channel = channels_[current_channel_idx_];
    current_channel->WaitForRpc();
    RETURN_IF_ERROR(SerializeBatch(batch, current_channel->thrift_batch()));
    RETURN_IF_ERROR(current_channel->SendBatch(current_channel->thrift_batch()));
    current_channel_idx_ = (current_channel_idx_ + 1) % channels_.size();
  } else if (partition_type_ == TPartitionType::KUDU) {
    DCHECK_EQ(partition_expr_evals_.size(), 1);
    int num_channels = channels_.size();
    const int num_rows = batch->num_rows();
    const int hash_batch_size = RowBatch::HASH_BATCH_SIZE;
    int channel_ids[hash_batch_size];

    for (int batch_start = 0; batch_start < num_rows; batch_start += hash_batch_size) {
      const int batch_window_size = min(num_rows - batch_start, hash_batch_size);
      for (int i = 0; i < batch_window_size; ++i) {
        TupleRow* row = batch->GetRow(i + batch_start);
        int32_t partition =
            *reinterpret_cast<int32_t*>(partition_expr_evals_[0]->GetValue(row));
        if (partition < 0) {
          // This row doesn't correspond to a partition,
          //  e.g. it's outside the given ranges.
          partition = next_unknown_partition_;
          ++next_unknown_partition_;
        }
        channel_ids[i] = partition % num_channels;
      }

      for (int i = 0; i < batch_window_size; ++i) {
        TupleRow* row = batch->GetRow(i + batch_start);
        RETURN_IF_ERROR(channels_[channel_ids[i]]->AddRow(row));
      }
    }
  } else {
    DCHECK(partition_type_ == TPartitionType::HASH_PARTITIONED);
    // hash-partition batch's rows across channels
    // TODO: encapsulate this in an Expr as we've done for Kudu above and remove this case
    // once we have codegen here.
    int num_channels = channels_.size();
    const int num_partition_exprs = partition_exprs_.size();
    const int num_rows = batch->num_rows();
    const int hash_batch_size = RowBatch::HASH_BATCH_SIZE;
    int channel_ids[hash_batch_size];

    // Break the loop into two parts break the data dependency between computing
    // the hash and calling AddRow()
    // To keep stack allocation small a RowBatch::HASH_BATCH is used
    for (int batch_start = 0; batch_start < num_rows; batch_start += hash_batch_size) {
      int batch_window_size = min(num_rows - batch_start, hash_batch_size);
      for (int i = 0; i < batch_window_size; ++i) {
        TupleRow* row = batch->GetRow(i + batch_start);
        uint64_t hash_val = EXCHANGE_HASH_SEED;
        for (int j = 0; j < num_partition_exprs; ++j) {
          ScalarExprEvaluator* eval = partition_expr_evals_[j];
          void* partition_val = eval->GetValue(row);
          // We can't use the crc hash function here because it does not result in
          // uncorrelated hashes with different seeds. Instead we use FastHash.
          // TODO: fix crc hash/GetHashValue()
          DCHECK(&(eval->root()) == partition_exprs_[j]);
          hash_val = RawValue::GetHashValueFastHash(
              partition_val, partition_exprs_[j]->type(), hash_val);
        }
        channel_ids[i] = hash_val % num_channels;
      }

      for (int i = 0; i < batch_window_size; ++i) {
        TupleRow* row = batch->GetRow(i + batch_start);
        RETURN_IF_ERROR(channels_[channel_ids[i]]->AddRow(row));
      }
    }
  }
  COUNTER_ADD(total_sent_rows_counter_, batch->num_rows());
  expr_results_pool_->Clear();
  RETURN_IF_ERROR(state->CheckQueryState());
  return Status::OK();
}

Status DataStreamSender::FlushFinal(RuntimeState* state) {
  DCHECK(!flushed_);
  DCHECK(!closed_);
  flushed_ = true;
  for (int i = 0; i < channels_.size(); ++i) {
    // If we hit an error here, we can return without closing the remaining channels as
    // the error is propagated back to the coordinator, which in turn cancels the query,
    // which will cause the remaining open channels to be closed.
    RETURN_IF_ERROR(channels_[i]->FlushAndSendEos(state));
  }
  return Status::OK();
}

void DataStreamSender::Close(RuntimeState* state) {
  if (closed_) return;
  for (int i = 0; i < channels_.size(); ++i) {
    channels_[i]->Teardown(state);
  }
  ScalarExprEvaluator::Close(partition_expr_evals_, state);
  ScalarExpr::Close(partition_exprs_);
  DataSink::Close(state);
  closed_ = true;
}

Status DataStreamSender::SerializeBatch(
    RowBatch* src, TRowBatch* dest, int num_receivers) {
  VLOG_ROW << "serializing " << src->num_rows() << " rows";
  {
    SCOPED_TIMER(profile_->total_time_counter());
    SCOPED_TIMER(serialize_batch_timer_);
    RETURN_IF_ERROR(src->Serialize(dest));
    int64_t bytes = RowBatch::GetSerializedSize(*dest);
    int64_t uncompressed_bytes = RowBatch::GetDeserializedSize(*dest);
    COUNTER_ADD(bytes_sent_counter_, bytes * num_receivers);
    COUNTER_ADD(uncompressed_bytes_counter_, uncompressed_bytes * num_receivers);
  }
  return Status::OK();
}

int64_t DataStreamSender::GetNumDataBytesSent() const {
  // TODO: do we need synchronization here or are reads & writes to 8-byte ints
  // atomic?
  int64_t result = 0;
  for (int i = 0; i < channels_.size(); ++i) {
    result += channels_[i]->num_data_bytes_sent();
  }
  return result;
}

}
