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

#include "runtime/data-stream-sender.h"

#include <iostream>
#include <boost/shared_ptr.hpp>
#include <thrift/protocol/TDebugProtocol.h>

#include "common/logging.h"
#include "exprs/expr.h"
#include "runtime/descriptors.h"
#include "runtime/tuple-row.h"
#include "runtime/row-batch.h"
#include "runtime/raw-value.h"
#include "runtime/runtime-state.h"
#include "runtime/client-cache.h"
#include "runtime/mem-tracker.h"
#include "util/debug-util.h"
#include "util/network-util.h"
#include "util/thrift-client.h"
#include "util/thrift-util.h"

#include "gen-cpp/Types_types.h"
#include "gen-cpp/ImpalaInternalService.h"
#include "gen-cpp/ImpalaInternalService_types.h"

using namespace std;
using namespace boost;
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
class DataStreamSender::Channel {
 public:
  // Create channel to send data to particular ipaddress/port/query/node
  // combination. buffer_size is specified in bytes and a soft limit on
  // how much tuple data is getting accumulated before being sent; it only applies
  // when data is added via AddRow() and not sent directly via SendBatch().
  Channel(DataStreamSender* parent, const RowDescriptor& row_desc,
          const TNetworkAddress& destination, const TUniqueId& fragment_instance_id,
          PlanNodeId dest_node_id, int buffer_size)
    : parent_(parent),
      buffer_size_(buffer_size),
      client_cache_(NULL),
      row_desc_(row_desc),
      address_(MakeNetworkAddress(destination.hostname, destination.port)),
      fragment_instance_id_(fragment_instance_id),
      dest_node_id_(dest_node_id),
      num_data_bytes_sent_(0),
      in_flight_batch_(NULL) {
  }

  // Initialize channel.
  // Returns OK if successful, error indication otherwise.
  Status Init(RuntimeState* state);

  // Copies a single row into this channel's output buffer and flushes buffer
  // if it reaches capacity.
  // Returns error status if any of the preceding rpcs failed, OK otherwise.
  Status AddRow(TupleRow* row);

  // Asynchronously sends a row batch.
  // Returns the status of the most recently finished TransmitData
  // rpc (or OK if there wasn't one that hasn't been reported yet).
  Status SendBatch(TRowBatch* batch);

  // Return status of last TransmitData rpc (initiated by the most recent call
  // to either SendBatch() or SendCurrentBatch()).
  Status GetSendStatus();

  // Flush buffered rows and close channel.
  // Logs errors if any of the preceding rpcs failed.
  void Close(RuntimeState* state);

  int64_t num_data_bytes_sent() const { return num_data_bytes_sent_; }

 private:
  DataStreamSender* parent_;
  int buffer_size_;

  ImpalaInternalServiceClientCache* client_cache_;

  const RowDescriptor& row_desc_;
  TNetworkAddress address_;
  TUniqueId fragment_instance_id_;
  PlanNodeId dest_node_id_;

  // the number of TRowBatch.data bytes sent successfully
  int64_t num_data_bytes_sent_;

  // we're accumulating rows into this batch
  scoped_ptr<RowBatch> batch_;
  TRowBatch thrift_batch_;

  // accessed by rpc_thread_ and by channel only if there is no in-flight rpc
  TRowBatch* in_flight_batch_;
  scoped_ptr<Thread> rpc_thread_;  // sender thread
  Status rpc_status_;  // status of most recently finished TransmitData rpc

  // Synchronously call TransmitData() on a client from client_cache_ and update
  // rpc_status_ based on return value (or set to error if RPC failed).
  // Should only run in rpc_thread_.
  void TransmitData();

  // Serialize batch_ into thrift_batch_ and send via SendBatch().
  // Returns SendBatch() status.
  Status SendCurrentBatch();

  Status CloseInternal();
};

Status DataStreamSender::Channel::Init(RuntimeState* state) {
  client_cache_ = state->client_cache();
  // TODO: figure out how to size batch_
  int capacity = max(1, buffer_size_ / max(row_desc_.GetRowSize(), 1));
  batch_.reset(new RowBatch(row_desc_, capacity, parent_->mem_tracker_.get()));
  return Status::OK;
}

Status DataStreamSender::Channel::SendBatch(TRowBatch* batch) {
  VLOG_ROW << "Channel::SendBatch() instance_id=" << fragment_instance_id_
           << " dest_node=" << dest_node_id_ << " #rows=" << batch->num_rows;
  // return if the previous batch saw an error
  RETURN_IF_ERROR(GetSendStatus());
  DCHECK(in_flight_batch_ == NULL);
  in_flight_batch_ = batch;
  rpc_thread_.reset(new Thread("data-stream-sender", "rpc-thread",
                               &DataStreamSender::Channel::TransmitData, this));
  return Status::OK;
}

void DataStreamSender::Channel::TransmitData() {
  DCHECK(in_flight_batch_ != NULL);
  try {
    VLOG_ROW << "Channel::TransmitData() instance_id=" << fragment_instance_id_
             << " dest_node=" << dest_node_id_
             << " #rows=" << in_flight_batch_->num_rows;
    TTransmitDataParams params;
    params.protocol_version = ImpalaInternalServiceVersion::V1;
    params.__set_dest_fragment_instance_id(fragment_instance_id_);
    params.__set_dest_node_id(dest_node_id_);
    params.__set_row_batch(*in_flight_batch_);  // yet another copy
    params.__set_eos(false);

    ImpalaInternalServiceConnection client(client_cache_, address_, &rpc_status_);
    if (!rpc_status_.ok()) {
      return;
    }

    TTransmitDataResult res;
    {
      SCOPED_TIMER(parent_->thrift_transmit_timer_);
      try {
        client->TransmitData(res, params);
      } catch (TTransportException& e) {
        VLOG_RPC << "Retrying TransmitData: " << e.what();
        rpc_status_ = client.Reopen();
        if (!rpc_status_.ok()) {
          return;
        }
        client->TransmitData(res, params);
      }
    }

    if (res.status.status_code != TStatusCode::OK) {
      rpc_status_ = res.status;
    } else {
      num_data_bytes_sent_ += RowBatch::GetBatchSize(*in_flight_batch_);
      VLOG_ROW << "incremented #data_bytes_sent="
               << num_data_bytes_sent_;
    }
  } catch (TException& e) {
    stringstream msg;
    msg << "TransmitData() to " << address_ << " failed:\n" << e.what();
    rpc_status_ = Status(msg.str());
    return;
  }
  in_flight_batch_ = NULL;
}

Status DataStreamSender::Channel::AddRow(TupleRow* row) {
  int row_num = batch_->AddRow();
  if (row_num == RowBatch::INVALID_ROW_INDEX) {
    // batch_ is full, let's send it; but first wait for an ongoing
    // transmission to finish before modifying thrift_batch_
    RETURN_IF_ERROR(SendCurrentBatch());
    row_num = batch_->AddRow();
    DCHECK_NE(row_num, RowBatch::INVALID_ROW_INDEX);
  }

  TupleRow* dest = batch_->GetRow(row_num);
  batch_->CopyRow(row, dest);
  const vector<TupleDescriptor*>& descs = row_desc_.tuple_descriptors();
  for (int i = 0; i < descs.size(); ++i) {
    if (UNLIKELY(row->GetTuple(i) == NULL)) {
      dest->SetTuple(i, NULL);
    } else {
      dest->SetTuple(i, row->GetTuple(i)->DeepCopy(*descs[i],
          batch_->tuple_data_pool()));
    }
  }
  batch_->CommitLastRow();
  return Status::OK;
}

Status DataStreamSender::Channel::SendCurrentBatch() {
  // make sure there's no in-flight TransmitData() call that might still want to
  // access thrift_batch_
  if (rpc_thread_.get() != NULL) rpc_thread_->Join();
  {
    SCOPED_TIMER(parent_->serialize_batch_timer_);
    int uncompressed_bytes = batch_->Serialize(&thrift_batch_);
    COUNTER_UPDATE(parent_->bytes_sent_counter_, RowBatch::GetBatchSize(thrift_batch_));
    COUNTER_UPDATE(parent_->uncompressed_bytes_counter_, uncompressed_bytes);
  }
  batch_->Reset();
  RETURN_IF_ERROR(SendBatch(&thrift_batch_));
  return Status::OK;
}

Status DataStreamSender::Channel::GetSendStatus() {
  // TODO: This is getting called before rpc_thread_ is initialised
  if (rpc_thread_.get() != NULL) rpc_thread_->Join();
  if (!rpc_status_.ok()) {
    LOG(ERROR) << "channel send status: " << rpc_status_.GetErrorMsg();
  }
  return rpc_status_;
}

Status DataStreamSender::Channel::CloseInternal() {
  VLOG_RPC << "Channel::Close() instance_id=" << fragment_instance_id_
           << " dest_node=" << dest_node_id_
           << " #rows= " << batch_->num_rows();

  if (batch_->num_rows() > 0) {
    // flush
    RETURN_IF_ERROR(SendCurrentBatch());
  }
  // if the last transmitted batch resulted in a error, return that error
  RETURN_IF_ERROR(GetSendStatus());
  Status status;
  ImpalaInternalServiceConnection client(client_cache_, address_, &status);
  if (!status.ok()) {
    return status;
  }
  try {
    TTransmitDataParams params;
    params.protocol_version = ImpalaInternalServiceVersion::V1;
    params.__set_dest_fragment_instance_id(fragment_instance_id_);
    params.__set_dest_node_id(dest_node_id_);
    params.__set_eos(true);
    TTransmitDataResult res;
    VLOG_RPC << "calling TransmitData to close channel";
    try {
      client->TransmitData(res, params);
    } catch (TTransportException& e) {
      VLOG_RPC << "Retrying TransmitData: " << e.what();
      rpc_status_ = client.Reopen();
      if (!rpc_status_.ok()) {
        return rpc_status_;
      }
      client->TransmitData(res, params);
    }
    return Status(res.status);
  } catch (TException& e) {
    stringstream msg;
    msg << "CloseChannel() to " << address_ << " failed:\n" << e.what();
    return Status(msg.str());
  }
  return Status::OK;
}

void DataStreamSender::Channel::Close(RuntimeState* state) {
  state->LogError(CloseInternal());
  batch_.reset();
}

DataStreamSender::DataStreamSender(ObjectPool* pool,
    const RowDescriptor& row_desc, const TDataStreamSink& sink,
    const vector<TPlanFragmentDestination>& destinations,
    int per_channel_buffer_size)
  : pool_(pool),
    row_desc_(row_desc),
    current_thrift_batch_(&thrift_batch1_),
    profile_(NULL),
    serialize_batch_timer_(NULL),
    thrift_transmit_timer_(NULL),
    bytes_sent_counter_(NULL),
    dest_node_id_(sink.dest_node_id) {
  DCHECK_GT(destinations.size(), 0);
  DCHECK(sink.output_partition.type == TPartitionType::UNPARTITIONED
      || sink.output_partition.type == TPartitionType::HASH_PARTITIONED);
  broadcast_ = sink.output_partition.type == TPartitionType::UNPARTITIONED;
  // TODO: use something like google3's linked_ptr here (scoped_ptr isn't copyable)
  for (int i = 0; i < destinations.size(); ++i) {
    channels_.push_back(
        new Channel(this, row_desc, destinations[i].server,
                    destinations[i].fragment_instance_id,
                    sink.dest_node_id, per_channel_buffer_size));
  }

  if (broadcast_) {
    // Randomize the order we open/transmit to channels to avoid thundering herd problems.
    srand(reinterpret_cast<uint64_t>(this));
    random_shuffle(channels_.begin(), channels_.end());
  }

  if (sink.output_partition.type == TPartitionType::HASH_PARTITIONED) {
    // TODO: move this to Init()? would need to save 'sink' somewhere
    Status status =
        Expr::CreateExprTrees(
          pool, sink.output_partition.partition_exprs, &partition_exprs_);
    DCHECK(status.ok());
  }
}

DataStreamSender::~DataStreamSender() {
  // TODO: check that sender was either already closed() or there was an error
  // on some channel
  for (int i = 0; i < channels_.size(); ++i) {
    delete channels_[i];
  }
}

Status DataStreamSender::Init(RuntimeState* state) {
  DCHECK(state != NULL);
  stringstream title;
  title << "DataStreamSender (dst_id=" << dest_node_id_ << ")";
  profile_ = pool_->Add(new RuntimeProfile(pool_, title.str()));
  SCOPED_TIMER(profile_->total_time_counter());

  RETURN_IF_ERROR(Expr::Prepare(partition_exprs_, state, row_desc_));

  mem_tracker_.reset(
      new MemTracker(profile(), -1, "DataStreamSender", state->instance_mem_tracker()));
  state->RegisterMemTracker(mem_tracker_.get());
  bytes_sent_counter_ =
      ADD_COUNTER(profile(), "BytesSent", TCounterType::BYTES);
  uncompressed_bytes_counter_ =
      ADD_COUNTER(profile(), "UncompressedRowBatchSize", TCounterType::BYTES);
  serialize_batch_timer_ =
      ADD_TIMER(profile(), "SerializeBatchTime");
  thrift_transmit_timer_ = ADD_TIMER(profile(), "ThriftTransmitTime(*)");
  network_throughput_ =
      profile()->AddDerivedCounter("NetworkThroughput(*)", TCounterType::BYTES_PER_SECOND,
          bind<int64_t>(&RuntimeProfile::UnitsPerSecond, bytes_sent_counter_,
                        thrift_transmit_timer_));
  overall_throughput_ =
      profile()->AddDerivedCounter("OverallThroughput", TCounterType::BYTES_PER_SECOND,
           bind<int64_t>(&RuntimeProfile::UnitsPerSecond, bytes_sent_counter_,
                         profile()->total_time_counter()));

  for (int i = 0; i < channels_.size(); ++i) {
    RETURN_IF_ERROR(channels_[i]->Init(state));
  }
  return Status::OK;
}

Status DataStreamSender::Send(RuntimeState* state, RowBatch* batch, bool eos) {
  SCOPED_TIMER(profile_->total_time_counter());
  if (broadcast_ || channels_.size() == 1) {
    // current_thrift_batch_ is *not* the one that was written by the last call
    // to Serialize()
    VLOG_ROW << "serializing " << batch->num_rows() << " rows";
    {
      SCOPED_TIMER(serialize_batch_timer_);
      int uncompressed_bytes = batch->Serialize(current_thrift_batch_);
      COUNTER_UPDATE(bytes_sent_counter_, RowBatch::GetBatchSize(*current_thrift_batch_));
      COUNTER_UPDATE(uncompressed_bytes_counter_, uncompressed_bytes);
    }

    // SendBatch() will block if there are still in-flight rpcs (and those will
    // reference the previously written thrift batch)
    for (int i = 0; i < channels_.size(); ++i) {
      RETURN_IF_ERROR(channels_[i]->SendBatch(current_thrift_batch_));
    }
    current_thrift_batch_ =
        (current_thrift_batch_ == &thrift_batch1_ ? &thrift_batch2_ : &thrift_batch1_);
  } else {
    // hash-partition batch's rows across channels
    int num_channels = channels_.size();
    for (int i = 0; i < batch->num_rows(); ++i) {
      TupleRow* row = batch->GetRow(i);
      size_t hash_val = 0;
      for (vector<Expr*>::iterator expr = partition_exprs_.begin();
           expr != partition_exprs_.end(); ++expr) {
        void* partition_val = (*expr)->GetValue(row);
        // We can't use the crc hash function here because it does not result
        // in uncorrelated hashes with different seeds.  Instead we must use
        // fvn hash.
        // TODO: fix crc hash/GetHashValue()
        hash_val =
            RawValue::GetHashValueFvn(partition_val, (*expr)->type(), hash_val);
      }
      RETURN_IF_ERROR(channels_[hash_val % num_channels]->AddRow(row));
    }
  }
  return Status::OK;
}

void DataStreamSender::Close(RuntimeState* state) {
  // TODO: only close channels that didn't have any errors
  for (int i = 0; i < channels_.size(); ++i) {
    channels_[i]->Close(state);
  }
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
