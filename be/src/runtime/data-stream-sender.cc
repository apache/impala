// Copyright (c) 2011 Cloudera, Inc. All rights reserved.

#include "runtime/data-stream-sender.h"

#include <iostream>
#include <boost/shared_ptr.hpp>
#include <boost/thread/thread.hpp>
#include <protocol/TBinaryProtocol.h>
#include <protocol/TDebugProtocol.h>
#include <transport/TSocket.h>
#include <transport/TTransportUtils.h>
#include <glog/logging.h>
#include <gflags/gflags.h>

#include "exprs/expr.h"
#include "runtime/descriptors.h"
#include "runtime/tuple-row.h"
#include "runtime/row-batch.h"
#include "runtime/raw-value.h"

#include "gen-cpp/ImpalaBackendService.h"
#include "gen-cpp/ImpalaBackendService_types.h"

using namespace std;
using namespace boost;
using namespace apache::thrift;
using namespace apache::thrift::protocol;
using namespace apache::thrift::transport;

// TODO: move this to backend-main.cc (which we don't have yet)
DEFINE_int32(port, 20001, "port on which to run Impala backend");

namespace impala {

// A channel sends data asynchronously via calls to TransmitData
// to a single destination host/node.
// It has a fixed-capacity buffer and allows the caller either to add rows to
// that buffer individually (AddRow()), or circumvent the buffer altogether and send
// TRowBatches directly (SendBatch()). Either way, there can only be one in-flight RPC
// at any one time (ie, sending will block if the most recent rpc hasn't finished,
// which allows the receiver node to throttle the sender by withholding acks).
// *Not* thread-safe.
class DataStreamSender::Channel {
 public:
  // Create channel to send data to particular host/port/query/node
  // combination. buffer_size is specified in bytes and a soft limit on
  // how much tuple data is getting accumulated before being sent; it only applies
  // when data is added via AddRow() and not sent directly via SendBatch().
  Channel(const RowDescriptor& row_desc, const THostPort& destination,
          const TUniqueId& query_id, PlanNodeId dest_node_id, int buffer_size)
    : row_desc_(row_desc),
      host_(destination.host),
      port_(destination.port),
      query_id_(query_id),
      dest_node_id_(dest_node_id),
      num_data_bytes_sent_(0),
      batch_(new RowBatch(row_desc, max(1, buffer_size / row_desc.GetRowSize()))),
      in_flight_batch_(NULL) {
      // TODO: figure out how to size batch_
  }

  // Initialize channel.
  // Returns OK if successful, error indication otherwise.
  Status Init();

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
  // Returns error status if any of the preceding rpcs failed, OK otherwise.
  Status Close();

  int64_t num_data_bytes_sent() const { return num_data_bytes_sent_; }

 private:
  shared_ptr<TTransport> socket_;
  shared_ptr<TTransport> transport_;
  shared_ptr<TProtocol> protocol_;
  scoped_ptr<ImpalaBackendServiceClient> client_;

  const RowDescriptor& row_desc_;
  string host_;
  int port_;
  TUniqueId query_id_;
  PlanNodeId dest_node_id_;

  // the number of TRowBatch.data bytes sent successfully
  int64_t num_data_bytes_sent_;

  // we're accumulating rows into this batch
  scoped_ptr<RowBatch> batch_;
  TRowBatch thrift_batch_;

  // accessed by rpc_thread_ and by channel only if there is no in-flight rpc
  TRowBatch* in_flight_batch_;
  thread rpc_thread_;  // sender thread
  Status rpc_status_;  // status of most recently finished TransmitData rpc

  // Synchronously call client_'s TransmitData() and update rpc_status_
  // based on return value (or set to error if RPC failed).
  // Should only run in rpc_thread_.
  void TransmitData();

  // Serialize batch_ into thrift_batch_ and send via SendBatch().
  // Returns SendBatch() status.
  Status SendCurrentBatch();
};

Status DataStreamSender::Channel::Init() {
  socket_.reset(new TSocket(host_, port_));
  transport_.reset(new TBufferedTransport(socket_));
  protocol_.reset(new TBinaryProtocol(transport_));
  client_.reset(new ImpalaBackendServiceClient(protocol_));

  try {
    transport_->open();
  } catch (TTransportException& e) {
    stringstream msg;
    msg << "couldn't create ImpalaBackendService client for " << host_ << ":"
        << port_ << ":\n" << e.what();
    return Status(msg.str());
  }

  return Status::OK;
}

Status DataStreamSender::Channel::SendBatch(TRowBatch* batch) {
  VLOG(1) << "Channel::SendBatch(" << batch << ")\n";
  // return if the previous batch saw an error
  RETURN_IF_ERROR(GetSendStatus());
  DCHECK(in_flight_batch_ == NULL);
  in_flight_batch_ = batch;
  rpc_thread_ = thread(&DataStreamSender::Channel::TransmitData, this);
  return Status::OK;
}

void DataStreamSender::Channel::TransmitData() {
  DCHECK(in_flight_batch_ != NULL);
  try {
    VLOG(1) << "calling transmitdata(" << in_flight_batch_ << ")\n";
    TStatus rpc_status;
    client_->TransmitData(rpc_status, query_id_, dest_node_id_, *in_flight_batch_);
    if (rpc_status.status_code != 0) {
      rpc_status_ = rpc_status;
    } else {
      num_data_bytes_sent_ += RowBatch::GetBatchSize(*in_flight_batch_);
      VLOG(1) << "incremented #data_bytes_sent=" << num_data_bytes_sent_;
    }
  } catch (TException& e) {
    stringstream msg;
    msg << "TransmitData() to " << host_ << ":" << port_ << " failed:\n" << e.what();
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
    dest->SetTuple(i, row->GetTuple(i)->DeepCopy(*descs[i], batch_->tuple_data_pool()));
  }
  return Status::OK;
}

Status DataStreamSender::Channel::SendCurrentBatch() {
  // make sure there's no in-flight TransmitData() call that might still want to
  // access thrift_batch_
  rpc_thread_.join();
  batch_->Serialize(&thrift_batch_);
  batch_->Reset();
  RETURN_IF_ERROR(SendBatch(&thrift_batch_));
  return Status::OK;
}

Status DataStreamSender::Channel::GetSendStatus() {
  rpc_thread_.join();
  return rpc_status_;
}

Status DataStreamSender::Channel::Close() {
  VLOG(1) << "Channel::Close()\n";
  if (batch_->num_rows() > 0) {
    // flush
    RETURN_IF_ERROR(SendCurrentBatch());
  }
  // if the last transmitted batch resulted in a error, return that error
  RETURN_IF_ERROR(GetSendStatus());
  try {
    TStatus rpc_status;
    VLOG(1) << "calling closechannel()\n";
    client_->CloseChannel(rpc_status, query_id_, dest_node_id_);
    return Status(rpc_status);
  } catch (TException& e) {
    stringstream msg;
    msg << "CloseChannel() to " << host_ << ":" << port_ << " failed:\n" << e.what();
    return Status(msg.str());
  }
  return Status::OK;
}

DataStreamSender::DataStreamSender(
    const RowDescriptor& row_desc, const TUniqueId& query_id,
    const TDataStreamSink& sink, const vector<THostPort>& destinations,
    int per_channel_buffer_size)
  : current_thrift_batch_(&thrift_batch1_) {
  DCHECK_GT(destinations.size(), 0);
  // TODO: get rid of this when we re-enable the multiple-receiver case
  DCHECK_EQ(destinations.size(), 1);
  // broadcast on all channels if there's no partitioning
  broadcast_ = !sink.__isset.outputPartitionSpec;
  if (sink.__isset.outputPartitionSpec) {
    // for now, we can only do hash partitioning
    const TOutputPartitionSpec& partition_spec = sink.outputPartitionSpec;
    DCHECK(partition_spec.isHashPartitioned);
    if (!partition_spec.partitionBoundaries.empty()) {
      LOG(INFO) << "partition boundaries not empty for hash-partitioned output; "
                   "ignoring";
    }
    // we need a partitioning expr if we send to more than one host
    DCHECK(partition_spec.__isset.partitionExpr || destinations.size() == 1);
    // TODO: switch to Init() function that returns Status
    DCHECK(Expr::CreateExprTree(
        &pool_, partition_spec.partitionExpr, &partition_expr_).ok());
  }
  // TODO: use something like google3's linked_ptr here (scoped_ptr isn't copyable)
  for (int i = 0; i < destinations.size(); ++i) {
    channels_.push_back(
        new Channel(row_desc, destinations[i], query_id, sink.destNodeId,
                    per_channel_buffer_size));
  }
}

DataStreamSender::~DataStreamSender() {
  // TODO: check that sender was either already closed() or there was an error
  // on some channel
  for (int i = 0; i < channels_.size(); ++i) {
    delete channels_[i];
  }
}

Status DataStreamSender::Init() {
  for (int i = 0; i < channels_.size(); ++i) {
    RETURN_IF_ERROR(channels_[i]->Init());
  }
  return Status::OK;
}

Status DataStreamSender::Send(RowBatch* batch) {
  if (broadcast_ || channels_.size() == 1) {
    // current_thrift_batch_ is *not* the one that was written by the last call
    // to Serialize()
    VLOG(1) << "serializing into " << current_thrift_batch_ << "\n";
    batch->Serialize(current_thrift_batch_);
    // SendBatch() will block if there are still in-flight rpcs (and those will
    // reference the previously written thrift batch)
    for (int i = 0; i < channels_.size(); ++i) {
      RETURN_IF_ERROR(channels_[i]->SendBatch(current_thrift_batch_));
    }
    current_thrift_batch_ =
        (current_thrift_batch_ == &thrift_batch1_ ? &thrift_batch2_ : &thrift_batch1_);
  } else {
    // hash-partition batch's rows across channelS
    int num_channels = channels_.size();
    for (int i = 0; i < batch->num_rows(); ++i) {
      TupleRow* row = batch->GetRow(i);
      void* partition_val = partition_expr_->GetValue(row);
      size_t hash_val = RawValue::GetHashValue(partition_val, partition_expr_->type());
      RETURN_IF_ERROR(channels_[hash_val % num_channels]->AddRow(row));
    }
  }
  return Status::OK;
}

Status DataStreamSender::Close() {
  // TODO: only close channels that didn't have any errors
  for (int i = 0; i < channels_.size(); ++i) {
    RETURN_IF_ERROR(channels_[i]->Close());
  }
  return Status::OK;
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
