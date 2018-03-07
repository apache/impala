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

#include "runtime/krpc-data-stream-sender.h"

#include <boost/bind.hpp>

#include <chrono>
#include <condition_variable>
#include <iostream>
#include <thrift/protocol/TDebugProtocol.h>

#include "common/logging.h"
#include "exec/kudu-util.h"
#include "exprs/scalar-expr.h"
#include "exprs/scalar-expr-evaluator.h"
#include "gutil/strings/substitute.h"
#include "kudu/rpc/rpc_controller.h"
#include "kudu/rpc/rpc_sidecar.h"
#include "kudu/util/monotime.h"
#include "kudu/util/status.h"
#include "rpc/rpc-mgr.inline.h"
#include "runtime/descriptors.h"
#include "runtime/exec-env.h"
#include "runtime/mem-tracker.h"
#include "runtime/raw-value.inline.h"
#include "runtime/row-batch.h"
#include "runtime/runtime-state.h"
#include "runtime/tuple-row.h"
#include "util/aligned-new.h"
#include "util/debug-util.h"
#include "util/network-util.h"

#include "gen-cpp/data_stream_service.pb.h"
#include "gen-cpp/data_stream_service.proxy.h"
#include "gen-cpp/Types_types.h"

#include "common/names.h"

using std::condition_variable_any;
using namespace apache::thrift;
using kudu::rpc::RpcController;
using kudu::rpc::RpcSidecar;
using kudu::MonoDelta;

DECLARE_int32(rpc_retry_interval_ms);

namespace impala {

// A datastream sender may send row batches to multiple destinations. There is one
// channel for each destination.
//
// Clients can call TransmitData() to directly send a serialized row batch to the
// destination or it can call AddRow() to accumulate rows in an internal row batch
// to certain capacity before sending it. The underlying RPC layer is implemented
// with KRPC, which provides interfaces for asynchronous RPC calls. Normally, the
// calls above will return before the RPC has completed but they may block if there
// is already an in-flight RPC.
//
// Each channel internally has two OutboundRowBatch to serialize to. They are reused
// across multiple RPC calls. Having two OutboundRowBatch allows client to serialize
// the next row batch while the current row batch is being sent. Upon completion of
// a RPC, the callback TransmitDataCompleteCb() is invoked. If the RPC fails due to
// remote service's queue being full, TransmitDataCompleteCb() will schedule the retry
// callback RetryCb() after some delay dervied from 'FLAGS_rpc_retry_internal_ms'.
//
// When a data stream sender is shut down, it will call Teardown() on all channels to
// release resources. Teardown() will cancel any in-flight RPC and wait for the
// completion callback to be called before returning. It's expected that the execution
// thread to call FlushAndSendEos() before closing the data stream sender to flush all
// buffered row batches and send the end-of-stream message to the remote receiver.
// Note that the RPC payloads are owned solely by the channel and the KRPC layer will
// relinquish references of them before the completion callback is invoked so it's
// safe to free them once the callback has been invoked.
//
// Note that due to KUDU-2011, timeout cannot be used with outbound sidecars. The client
// has no idea when it is safe to reclaim the sidecar buffer (~RpcSidecar() should be the
// right place, except that's currently called too early). RpcController::Cancel() ensures
// that the callback is called only after the RPC layer no longer references the sidecar
// buffers.
class KrpcDataStreamSender::Channel : public CacheLineAligned {
 public:
  // Creates a channel to send data to particular ipaddress/port/fragment instance id/node
  // combination. buffer_size is specified in bytes and a soft limit on how much tuple
  // data is getting accumulated before being sent; it only applies when data is added via
  // AddRow() and not sent directly via SendBatch().
  Channel(KrpcDataStreamSender* parent, const RowDescriptor* row_desc,
      const TNetworkAddress& destination, const TUniqueId& fragment_instance_id,
      PlanNodeId dest_node_id, int buffer_size)
    : parent_(parent),
      row_desc_(row_desc),
      address_(destination),
      fragment_instance_id_(fragment_instance_id),
      dest_node_id_(dest_node_id) {
    DCHECK(IsResolvedAddress(address_));
  }

  // Initializes the channel.
  // Returns OK if successful, error indication otherwise.
  Status Init(RuntimeState* state);

  // Serializes the given row batch and send it to the destination. If the preceding
  // RPC is in progress, this function may block until the previous RPC finishes.
  // Return error status if serialization or the preceding RPC failed. Return OK
  // otherwise.
  Status SerializeAndSendBatch(RowBatch* batch);

  // Transmits the serialized row batch 'outbound_batch'. This function may block if the
  // preceding RPC is still in-flight. This is expected to be called from the fragment
  // instance execution thread. Return error status if initialization of the RPC request
  // parameters failed or if the preceding RPC failed. Returns OK otherwise.
  Status TransmitData(const OutboundRowBatch* outbound_batch);

  // Copies a single row into this channel's row batch and flushes the row batch once
  // it reaches capacity. This call may block if the row batch's capacity is reached
  // and the preceding RPC is still in progress. Returns error status if serialization
  // failed or if the preceding RPC failed. Return OK otherwise.
  Status ALWAYS_INLINE AddRow(TupleRow* row);

  // Shutdowns the channel and frees the row batch allocation. Any in-flight RPC will
  // be cancelled. It's expected that clients normally call FlushAndSendEos() before
  // calling Teardown() to flush all buffered row batches to destinations. Teardown()
  // may be called without FlushAndSendEos() in cases such as cancellation or error.
  void Teardown(RuntimeState* state);

  // Flushes any buffered row batches and sends the EOS RPC to close the channel.
  // Return error status if either the last TransmitData() RPC or EOS RPC failed.
  // This function blocks until the EOS RPC is complete.
  Status FlushAndSendEos(RuntimeState* state);

  int64_t num_data_bytes_sent() const { return num_data_bytes_sent_; }

  // The type for a RPC worker function.
  typedef boost::function<Status()> DoRpcFn;

 private:
  // The parent data stream sender owning this channel. Not owned.
  KrpcDataStreamSender* parent_;

  // The descriptor of the accumulated rows in 'batch_' below. Used for computing
  // the capacity of 'batch_' and also when adding a row in AddRow().
  const RowDescriptor* row_desc_;

  // The triplet of IP-address:port/finst-id/node-id uniquely identifies the receiver.
  const TNetworkAddress address_;
  const TUniqueId fragment_instance_id_;
  const PlanNodeId dest_node_id_;

  // Number of bytes of all serialized row batches sent successfully.
  int64_t num_data_bytes_sent_ = 0;

  // The row batch for accumulating rows copied from AddRow().
  // Only used if the partitioning scheme is "KUDU" or "HASH_PARTITIONED".
  scoped_ptr<RowBatch> batch_;

  // The outbound row batches are double-buffered so that we can serialize the next
  // batch while the other is still referenced by the in-flight RPC. Each entry contains
  // a RowBatchHeaderPB and the buffers for the serialized tuple offsets and data.
  //
  // TODO: replace this with an actual queue. Schedule another RPC callback in the
  // completion callback if the queue is not empty.
  // TODO: rethink whether to keep per-channel buffers vs having all buffers in the
  // datastream sender and sharing them across all channels. These buffers are not used in
  // "UNPARTITIONED" scheme.
  OutboundRowBatch outbound_batches_[NUM_OUTBOUND_BATCHES];

  // Index into 'outbound_batches_' for the next available OutboundRowBatch to serialize
  // into. This is read and written by the main execution thread.
  int next_batch_idx_ = 0;

  // Synchronize accesses to the following fields between the main execution thread and
  // the KRPC reactor thread. Note that there should be only one reactor thread invoking
  // the callbacks for a channel so there should be no races between multiple reactor
  // threads. Protect all subsequent fields.
  SpinLock lock_;

  // 'lock_' needs to be held when accessing the following fields.
  // The client interface for making RPC calls to the remote DataStreamService.
  std::unique_ptr<DataStreamServiceProxy> proxy_;

  // Controller for managing properties of a single RPC call (such as features required
  // in the remote servers) and passing the payloads to the actual OutboundCall object.
  RpcController rpc_controller_;

  // Protobuf response buffer for TransmitData() RPC.
  TransmitDataResponsePB resp_;

  // Protobuf response buffer for EndDataStream() RPC.
  EndDataStreamResponsePB eos_resp_;

  // Signaled when the in-flight RPC completes.
  condition_variable_any rpc_done_cv_;

  // Status of the most recently completed RPC.
  Status rpc_status_;

  // The pointer to the current serialized row batch being sent.
  const OutboundRowBatch* rpc_in_flight_batch_ = nullptr;

  // True if there is an in-flight RPC.
  bool rpc_in_flight_ = false;

  // True if the channel is being shut down or shut down already.
  bool shutdown_ = false;

  // True if the remote receiver is closed already. In which case, all rows would
  // be dropped silently.
  // TODO: Fix IMPALA-3990
  bool remote_recvr_closed_ = false;

  // Returns true if the channel should terminate because the parent sender
  // has been closed or cancelled.
  bool ShouldTerminate() const { return shutdown_ || parent_->state_->is_cancelled(); }

  // Send the rows accumulated in the internal row batch. This will serialize the
  // internal row batch before sending them to the destination. This may block if
  // the preceding RPC is still in progress. Returns error status if serialization
  // fails or if the preceding RPC fails.
  Status SendCurrentBatch();

  // Called when an RPC failed. If it turns out that the RPC failed because the
  // remote server is too busy, this function will schedule RetryCb() to be called
  // after FLAGS_rpc_retry_interval_ms milliseconds, which in turn re-invokes the RPC.
  // Otherwise, it will call MarkDone() to mark the RPC as done and failed.
  // 'controller_status' is a Kudu status returned from the KRPC layer.
  // 'rpc_fn' is a worker function which initializes the RPC parameters and invokes
  // the actual RPC when the RPC is rescheduled.
  // 'err_msg' is an error message to be prepended to the status converted from the
  // Kudu status 'controller_status'.
  void HandleFailedRPC(const DoRpcFn& rpc_fn, const kudu::Status& controller_status,
      const string& err_msg);

  // Waits for the preceding RPC to complete. Expects to be called with 'lock_' held.
  // May drop the lock while waiting for the RPC to complete. Return error status if
  // the preceding RPC fails. Returns CANCELLED if the parent sender is cancelled or
  // shut down. Returns OK otherwise. This should be only called from a fragment
  // executor thread.
  Status WaitForRpc(std::unique_lock<SpinLock>* lock);

  // A callback function called from KRPC reactor thread to retry an RPC which failed
  // previously due to remote server being too busy. This will re-arm the request
  // parameters of the RPC. The retry may not happen if the callback has been aborted
  // internally by KRPC code (e.g. the reactor thread was being shut down) or if the
  // parent sender has been cancelled or closed since the scheduling of this callback.
  // In which case, MarkDone() will be called with the error status and the RPC is
  // considered complete. 'status' is the error status passed by KRPC code in case the
  // callback was aborted.
  void RetryCb(DoRpcFn rpc_fn, const kudu::Status& status);

  // A callback function called from KRPC reactor threads upon completion of an in-flight
  // TransmitData() RPC. This is called when the remote server responds to the RPC or
  // when the RPC ends prematurely due to various reasons (e.g. cancellation). Upon a
  // successful KRPC call, MarkDone() is called to update 'rpc_status_' based on the
  // response. HandleFailedRPC() is called to handle failed KRPC call. The RPC may be
  // rescheduled if it's due to remote server being too busy.
  void TransmitDataCompleteCb();

  // Initializes the parameters for TransmitData() RPC and invokes the async RPC call.
  // It will add 'tuple_offsets_' and 'tuple_data_' in 'rpc_in_flight_batch_' as sidecars
  // to the RpcController and store the sidecars' indices to TransmitDataRequestPB sent as
  // part of the RPC. Returns error status if adding sidecars to the RpcController failed.
  Status DoTransmitDataRpc();

  // A callback function called from KRPC reactor threads upon completion of an in-flight
  // EndDataStream() RPC. This is called when the remote server responds to the RPC or
  // when the RPC ends prematurely due to various reasons (e.g. cancellation). Upon a
  // successful KRPC call, MarkDone() is called to update 'rpc_status_' based on the
  // response. HandleFailedRPC() is called to handle failed KRPC calls. The RPC may be
  // rescheduled if it's due to remote server being too busy.
  void EndDataStreamCompleteCb();

  // Initializes the parameters for EndDataStream() RPC and invokes the async RPC call.
  Status DoEndDataStreamRpc();

  // Marks the in-flight RPC as completed, updates 'rpc_status_' with the status of the
  // RPC (indicated in parameter 'status') and notifies any thread waiting for RPC
  // completion. Expects to be called with 'lock_' held. Called in the context of a
  // reactor thread.
  void MarkDone(const Status& status);
};

Status KrpcDataStreamSender::Channel::Init(RuntimeState* state) {
  // TODO: take into account of var-len data at runtime.
  int capacity =
      max(1, parent_->per_channel_buffer_size_ / max(row_desc_->GetRowSize(), 1));
  batch_.reset(new RowBatch(row_desc_, capacity, parent_->mem_tracker()));

  // Create a DataStreamService proxy to the destination.
  RpcMgr* rpc_mgr = ExecEnv::GetInstance()->rpc_mgr();
  RETURN_IF_ERROR(rpc_mgr->GetProxy(address_, &proxy_));
  return Status::OK();
}

void KrpcDataStreamSender::Channel::MarkDone(const Status& status) {
  if (UNLIKELY(!status.ok())) COUNTER_ADD(parent_->rpc_failure_counter_, 1);
  rpc_status_ = status;
  rpc_in_flight_ = false;
  rpc_in_flight_batch_ = nullptr;
  rpc_done_cv_.notify_one();
}

Status KrpcDataStreamSender::Channel::WaitForRpc(std::unique_lock<SpinLock>* lock) {
  DCHECK(lock != nullptr);
  DCHECK(lock->owns_lock());

  SCOPED_TIMER(parent_->state_->total_network_send_timer());

  // Wait for in-flight RPCs to complete unless the parent sender is closed or cancelled.
  while(rpc_in_flight_ && !ShouldTerminate()) {
    rpc_done_cv_.wait_for(*lock, std::chrono::milliseconds(50));
  }

  if (UNLIKELY(ShouldTerminate())) {
    // DSS is single-threaded so it's impossible for shutdown_ to be true here.
    DCHECK(!shutdown_);
    return Status::CANCELLED;
  }

  DCHECK(!rpc_in_flight_);
  if (UNLIKELY(!rpc_status_.ok())) {
    LOG(ERROR) << "channel send to " << TNetworkAddressToString(address_) << " failed: "
               << "(fragment_instance_id=" << fragment_instance_id_ << "): "
               << rpc_status_.GetDetail();
    return rpc_status_;
  }
  return Status::OK();
}

void KrpcDataStreamSender::Channel::RetryCb(
    DoRpcFn rpc_fn, const kudu::Status& cb_status) {
  COUNTER_ADD(parent_->rpc_retry_counter_, 1);
  std::unique_lock<SpinLock> l(lock_);
  DCHECK(rpc_in_flight_);
  // Aborted by KRPC layer as reactor thread was being shut down.
  if (UNLIKELY(!cb_status.ok())) {
    MarkDone(FromKuduStatus(cb_status, "KRPC retry failed"));
    return;
  }
  // Parent datastream sender has been closed or cancelled.
  if (UNLIKELY(ShouldTerminate())) {
    MarkDone(Status::CANCELLED);
    return;
  }
  // Retry the RPC.
  Status status = rpc_fn();
  if (UNLIKELY(!status.ok())) {
    MarkDone(status);
  }
}

void KrpcDataStreamSender::Channel::HandleFailedRPC(const DoRpcFn& rpc_fn,
    const kudu::Status& controller_status, const string& prepend) {
  // Retrying later if the destination is busy. We don't call ShouldTerminate()
  // here as this is always checked in RetryCb() anyway.
  // TODO: IMPALA-6159. Handle 'connection reset by peer' due to stale connections.
  if (RpcMgr::IsServerTooBusy(rpc_controller_)) {
    RpcMgr* rpc_mgr = ExecEnv::GetInstance()->rpc_mgr();
    // RetryCb() is scheduled to be called in a reactor context.
    rpc_mgr->messenger()->ScheduleOnReactor(
        boost::bind(&KrpcDataStreamSender::Channel::RetryCb, this, rpc_fn, _1),
        MonoDelta::FromMilliseconds(FLAGS_rpc_retry_interval_ms));
    return;
  }
  MarkDone(FromKuduStatus(controller_status, prepend));
}

void KrpcDataStreamSender::Channel::TransmitDataCompleteCb() {
  std::unique_lock<SpinLock> l(lock_);
  DCHECK(rpc_in_flight_);
  const kudu::Status controller_status = rpc_controller_.status();
  if (LIKELY(controller_status.ok())) {
    Status rpc_status = Status::OK();
    int32_t status_code = resp_.status().status_code();
    if (LIKELY(status_code == TErrorCode::OK)) {
      DCHECK(rpc_in_flight_batch_ != nullptr);
      num_data_bytes_sent_ += RowBatch::GetSerializedSize(*rpc_in_flight_batch_);
      VLOG_ROW << "incremented #data_bytes_sent=" << num_data_bytes_sent_;
    } else if (status_code == TErrorCode::DATASTREAM_RECVR_CLOSED) {
      remote_recvr_closed_ = true;
    } else {
      rpc_status = Status(resp_.status());
    }
    MarkDone(rpc_status);
  } else {
    DoRpcFn rpc_fn =
        boost::bind(&KrpcDataStreamSender::Channel::DoTransmitDataRpc, this);
    const string& prepend =
        Substitute("TransmitData() to $0 failed", TNetworkAddressToString(address_));
    HandleFailedRPC(rpc_fn, controller_status, prepend);
  }
}

Status KrpcDataStreamSender::Channel::DoTransmitDataRpc() {
  DCHECK(rpc_in_flight_batch_ != nullptr);
  DCHECK(rpc_in_flight_batch_->IsInitialized());

  // Initialize some constant fields in the request protobuf.
  TransmitDataRequestPB req;
  UniqueIdPB* finstance_id_pb = req.mutable_dest_fragment_instance_id();
  finstance_id_pb->set_lo(fragment_instance_id_.lo);
  finstance_id_pb->set_hi(fragment_instance_id_.hi);
  req.set_sender_id(parent_->sender_id_);
  req.set_dest_node_id(dest_node_id_);

  // Set the RowBatchHeader in the request.
  req.set_allocated_row_batch_header(
      const_cast<RowBatchHeaderPB*>(rpc_in_flight_batch_->header()));

  rpc_controller_.Reset();
  int sidecar_idx;
  // Add 'tuple_offsets_' as sidecar.
  KUDU_RETURN_IF_ERROR(rpc_controller_.AddOutboundSidecar(RpcSidecar::FromSlice(
      rpc_in_flight_batch_->TupleOffsetsAsSlice()), &sidecar_idx),
      "Unable to add tuple offsets to sidecar");
  req.set_tuple_offsets_sidecar_idx(sidecar_idx);

  // Add 'tuple_data_' as sidecar.
  KUDU_RETURN_IF_ERROR(rpc_controller_.AddOutboundSidecar(
      RpcSidecar::FromSlice(rpc_in_flight_batch_->TupleDataAsSlice()), &sidecar_idx),
      "Unable to add tuple data to sidecar");
  req.set_tuple_data_sidecar_idx(sidecar_idx);

  resp_.Clear();
  proxy_->TransmitDataAsync(req, &resp_, &rpc_controller_,
      boost::bind(&KrpcDataStreamSender::Channel::TransmitDataCompleteCb, this));
  // 'req' took ownership of 'header'. Need to release its ownership or 'header' will be
  // deleted by destructor.
  req.release_row_batch_header();
  return Status::OK();
}

Status KrpcDataStreamSender::Channel::TransmitData(
    const OutboundRowBatch* outbound_batch) {
  VLOG_ROW << "Channel::TransmitData() fragment_instance_id=" << fragment_instance_id_
           << " dest_node=" << dest_node_id_
           << " #rows=" << outbound_batch->header()->num_rows();
  std::unique_lock<SpinLock> l(lock_);
  RETURN_IF_ERROR(WaitForRpc(&l));
  DCHECK(!rpc_in_flight_);
  DCHECK(rpc_in_flight_batch_ == nullptr);
  // If the remote receiver is closed already, there is no point in sending anything.
  // TODO: Needs better solution for IMPALA-3990 in the long run.
  if (UNLIKELY(remote_recvr_closed_)) return Status::OK();
  rpc_in_flight_ = true;
  rpc_in_flight_batch_ = outbound_batch;
  RETURN_IF_ERROR(DoTransmitDataRpc());
  return Status::OK();
}

Status KrpcDataStreamSender::Channel::SerializeAndSendBatch(RowBatch* batch) {
  OutboundRowBatch* outbound_batch = &outbound_batches_[next_batch_idx_];
  DCHECK(outbound_batch != rpc_in_flight_batch_);
  RETURN_IF_ERROR(parent_->SerializeBatch(batch, outbound_batch));
  RETURN_IF_ERROR(TransmitData(outbound_batch));
  next_batch_idx_ = (next_batch_idx_ + 1) % NUM_OUTBOUND_BATCHES;
  return Status::OK();
}

Status KrpcDataStreamSender::Channel::SendCurrentBatch() {
  RETURN_IF_ERROR(SerializeAndSendBatch(batch_.get()));
  batch_->Reset();
  return Status::OK();
}

inline Status KrpcDataStreamSender::Channel::AddRow(TupleRow* row) {
  if (batch_->AtCapacity()) {
    // batch_ is full, let's send it.
    RETURN_IF_ERROR(SendCurrentBatch());
  }
  TupleRow* dest = batch_->GetRow(batch_->AddRow());
  const vector<TupleDescriptor*>& descs = row_desc_->tuple_descriptors();
  for (int i = 0; i < descs.size(); ++i) {
    if (UNLIKELY(row->GetTuple(i) == nullptr)) {
      dest->SetTuple(i, nullptr);
    } else {
      dest->SetTuple(i, row->GetTuple(i)->DeepCopy(*descs[i], batch_->tuple_data_pool()));
    }
  }
  batch_->CommitLastRow();
  return Status::OK();
}

void KrpcDataStreamSender::Channel::EndDataStreamCompleteCb() {
  std::unique_lock<SpinLock> l(lock_);
  DCHECK(rpc_in_flight_);
  const kudu::Status controller_status = rpc_controller_.status();
  if (LIKELY(controller_status.ok())) {
    MarkDone(Status(eos_resp_.status()));
  } else {
    DoRpcFn rpc_fn =
        boost::bind(&KrpcDataStreamSender::Channel::DoEndDataStreamRpc, this);
    const string& prepend =
        Substitute("EndDataStream() to $0 failed", TNetworkAddressToString(address_));
    HandleFailedRPC(rpc_fn, controller_status, prepend);
  }
}

Status KrpcDataStreamSender::Channel::DoEndDataStreamRpc() {
  DCHECK(rpc_in_flight_);
  EndDataStreamRequestPB eos_req;
  rpc_controller_.Reset();
  UniqueIdPB* finstance_id_pb = eos_req.mutable_dest_fragment_instance_id();
  finstance_id_pb->set_lo(fragment_instance_id_.lo);
  finstance_id_pb->set_hi(fragment_instance_id_.hi);
  eos_req.set_sender_id(parent_->sender_id_);
  eos_req.set_dest_node_id(dest_node_id_);
  eos_resp_.Clear();
  proxy_->EndDataStreamAsync(eos_req, &eos_resp_, &rpc_controller_,
      boost::bind(&KrpcDataStreamSender::Channel::EndDataStreamCompleteCb, this));
  return Status::OK();
}

Status KrpcDataStreamSender::Channel::FlushAndSendEos(RuntimeState* state) {
  VLOG_RPC << "Channel::FlushAndSendEos() fragment_instance_id=" << fragment_instance_id_
           << " dest_node=" << dest_node_id_
           << " #rows= " << batch_->num_rows();

  // We can return an error here and not go on to send the EOS RPC because the error that
  // we returned will be sent to the coordinator who will then cancel all the remote
  // fragments including the one that this sender is sending to.
  if (batch_->num_rows() > 0) RETURN_IF_ERROR(SendCurrentBatch());
  {
    std::unique_lock<SpinLock> l(lock_);
    RETURN_IF_ERROR(WaitForRpc(&l));
    DCHECK(!rpc_in_flight_);
    DCHECK(rpc_status_.ok());
    if (UNLIKELY(remote_recvr_closed_)) return Status::OK();
    VLOG_RPC << "calling EndDataStream() to terminate channel. fragment_instance_id="
             << fragment_instance_id_;
    rpc_in_flight_ = true;
    COUNTER_ADD(parent_->eos_sent_counter_, 1);
    RETURN_IF_ERROR(DoEndDataStreamRpc());
    RETURN_IF_ERROR(WaitForRpc(&l));
  }
  return Status::OK();
}

void KrpcDataStreamSender::Channel::Teardown(RuntimeState* state) {
  // Normally, FlushAndSendEos() should have been called before calling Teardown(),
  // which means that all the data should already be drained. If the fragment was
  // was closed or cancelled, there may still be some in-flight RPCs and buffered
  // row batches to be flushed.
  std::unique_lock<SpinLock> l(lock_);
  shutdown_ = true;
  // Cancel any in-flight RPC.
  if (rpc_in_flight_) {
    rpc_controller_.Cancel();
    while (rpc_in_flight_) rpc_done_cv_.wait(l);
  }
  batch_.reset();
}

KrpcDataStreamSender::KrpcDataStreamSender(int sender_id, const RowDescriptor* row_desc,
    const TDataStreamSink& sink, const vector<TPlanFragmentDestination>& destinations,
    int per_channel_buffer_size, RuntimeState* state)
  : DataSink(row_desc,
        Substitute("KrpcDataStreamSender (dst_id=$0)", sink.dest_node_id), state),
    sender_id_(sender_id),
    partition_type_(sink.output_partition.type),
    per_channel_buffer_size_(per_channel_buffer_size),
    dest_node_id_(sink.dest_node_id),
    next_unknown_partition_(0) {
  DCHECK_GT(destinations.size(), 0);
  DCHECK(sink.output_partition.type == TPartitionType::UNPARTITIONED
      || sink.output_partition.type == TPartitionType::HASH_PARTITIONED
      || sink.output_partition.type == TPartitionType::RANDOM
      || sink.output_partition.type == TPartitionType::KUDU);

  for (int i = 0; i < destinations.size(); ++i) {
    channels_.push_back(
        new Channel(this, row_desc, destinations[i].krpc_server,
            destinations[i].fragment_instance_id, sink.dest_node_id,
            per_channel_buffer_size));
  }

  if (partition_type_ == TPartitionType::UNPARTITIONED ||
      partition_type_ == TPartitionType::RANDOM) {
    // Randomize the order we open/transmit to channels to avoid thundering herd problems.
    srand(reinterpret_cast<uint64_t>(this));
    random_shuffle(channels_.begin(), channels_.end());
  }
}

KrpcDataStreamSender::~KrpcDataStreamSender() {
  // TODO: check that sender was either already closed() or there was an error
  // on some channel
  for (int i = 0; i < channels_.size(); ++i) {
    delete channels_[i];
  }
}

Status KrpcDataStreamSender::Init(const vector<TExpr>& thrift_output_exprs,
    const TDataSink& tsink, RuntimeState* state) {
  DCHECK(tsink.__isset.stream_sink);
  if (partition_type_ == TPartitionType::HASH_PARTITIONED ||
      partition_type_ == TPartitionType::KUDU) {
    RETURN_IF_ERROR(ScalarExpr::Create(tsink.stream_sink.output_partition.partition_exprs,
        *row_desc_, state, &partition_exprs_));
  }
  return Status::OK();
}

Status KrpcDataStreamSender::Prepare(
    RuntimeState* state, MemTracker* parent_mem_tracker) {
  RETURN_IF_ERROR(DataSink::Prepare(state, parent_mem_tracker));
  state_ = state;
  SCOPED_TIMER(profile_->total_time_counter());
  RETURN_IF_ERROR(ScalarExprEvaluator::Create(partition_exprs_, state,
      state->obj_pool(), expr_perm_pool_.get(), expr_results_pool_.get(),
      &partition_expr_evals_));
  serialize_batch_timer_ = ADD_TIMER(profile(), "SerializeBatchTime");
  rpc_retry_counter_ = ADD_COUNTER(profile(), "RpcRetry", TUnit::UNIT);
  rpc_failure_counter_ = ADD_COUNTER(profile(), "RpcFailure", TUnit::UNIT);
  bytes_sent_counter_ = ADD_COUNTER(profile(), "BytesSent", TUnit::BYTES);
  eos_sent_counter_ = ADD_COUNTER(profile(), "EosSent", TUnit::UNIT);
  uncompressed_bytes_counter_ =
      ADD_COUNTER(profile(), "UncompressedRowBatchSize", TUnit::BYTES);
  total_sent_rows_counter_= ADD_COUNTER(profile(), "RowsReturned", TUnit::UNIT);
  overall_throughput_ =
      profile()->AddDerivedCounter("OverallThroughput", TUnit::BYTES_PER_SECOND,
           bind<int64_t>(&RuntimeProfile::UnitsPerSecond, bytes_sent_counter_,
                         profile()->total_time_counter()));
  for (int i = 0; i < channels_.size(); ++i) {
    RETURN_IF_ERROR(channels_[i]->Init(state));
  }
  return Status::OK();
}

Status KrpcDataStreamSender::Open(RuntimeState* state) {
  return ScalarExprEvaluator::Open(partition_expr_evals_, state);
}

Status KrpcDataStreamSender::Send(RuntimeState* state, RowBatch* batch) {
  DCHECK(!closed_);
  DCHECK(!flushed_);

  if (batch->num_rows() == 0) return Status::OK();
  if (partition_type_ == TPartitionType::UNPARTITIONED) {
    OutboundRowBatch* outbound_batch = &outbound_batches_[next_batch_idx_];
    RETURN_IF_ERROR(SerializeBatch(batch, outbound_batch));
    // TransmitData() will block if there are still in-flight rpcs (and those will
    // reference the previously written serialized batch).
    for (int i = 0; i < channels_.size(); ++i) {
      RETURN_IF_ERROR(channels_[i]->TransmitData(outbound_batch));
    }
    next_batch_idx_ = (next_batch_idx_ + 1) % NUM_OUTBOUND_BATCHES;
  } else if (partition_type_ == TPartitionType::RANDOM || channels_.size() == 1) {
    // Round-robin batches among channels. Wait for the current channel to finish its
    // rpc before overwriting its batch.
    Channel* current_channel = channels_[current_channel_idx_];
    RETURN_IF_ERROR(current_channel->SerializeAndSendBatch(batch));
    current_channel_idx_ = (current_channel_idx_ + 1) % channels_.size();
  } else if (partition_type_ == TPartitionType::KUDU) {
    DCHECK_EQ(partition_expr_evals_.size(), 1);
    int num_channels = channels_.size();
    const int num_rows = batch->num_rows();
    const int hash_batch_size = RowBatch::HASH_BATCH_SIZE;
    int channel_ids[hash_batch_size];

    for (int batch_start = 0; batch_start < num_rows; batch_start += hash_batch_size) {
      int batch_window_size = min(num_rows - batch_start, hash_batch_size);
      for (int i = 0; i < batch_window_size; ++i) {
        TupleRow* row = batch->GetRow(i + batch_start);
        int32_t partition =
            *reinterpret_cast<int32_t*>(partition_expr_evals_[0]->GetValue(row));
        if (partition < 0) {
          // This row doesn't correspond to a partition,
          // e.g. it's outside the given ranges.
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
    DCHECK_EQ(partition_type_, TPartitionType::HASH_PARTITIONED);
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

Status KrpcDataStreamSender::FlushFinal(RuntimeState* state) {
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

void KrpcDataStreamSender::Close(RuntimeState* state) {
  if (closed_) return;
  for (int i = 0; i < channels_.size(); ++i) {
    channels_[i]->Teardown(state);
  }
  ScalarExprEvaluator::Close(partition_expr_evals_, state);
  ScalarExpr::Close(partition_exprs_);
  DataSink::Close(state);
  closed_ = true;
}

Status KrpcDataStreamSender::SerializeBatch(
    RowBatch* src, OutboundRowBatch* dest, int num_receivers) {
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

int64_t KrpcDataStreamSender::GetNumDataBytesSent() const {
  // TODO: do we need synchronization here or are reads & writes to 8-byte ints
  // atomic?
  int64_t result = 0;
  for (int i = 0; i < channels_.size(); ++i) {
    result += channels_[i]->num_data_bytes_sent();
  }
  return result;
}

} // namespace impala

