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
#include "codegen/codegen-anyval.h"
#include "codegen/llvm-codegen.h"
#include "exec/kudu/kudu-util.h"
#include "exprs/scalar-expr.h"
#include "exprs/scalar-expr-evaluator.h"
#include "gutil/strings/substitute.h"
#include "kudu/rpc/rpc_controller.h"
#include "kudu/rpc/rpc_sidecar.h"
#include "kudu/util/monotime.h"
#include "kudu/util/status.h"
#include "rpc/rpc-mgr.h"
#include "runtime/descriptors.h"
#include "runtime/exec-env.h"
#include "runtime/fragment-state.h"
#include "runtime/mem-tracker.h"
#include "runtime/raw-value.inline.h"
#include "runtime/row-batch.h"
#include "runtime/runtime-state.h"
#include "runtime/tuple-row.h"
#include "service/data-stream-service.h"
#include "util/aligned-new.h"
#include "util/compress.h"
#include "util/debug-util.h"
#include "util/network-util.h"
#include "util/pretty-printer.h"

#include "gen-cpp/data_stream_service.pb.h"
#include "gen-cpp/data_stream_service.proxy.h"
#include "gen-cpp/Types_types.h"

#include "common/names.h"

DEFINE_int64(data_stream_sender_buffer_size, 16 * 1024,
    "(Advanced) Max size in bytes which a row batch in a data stream sender's channel "
    "can accumulate before the row batch is sent over the wire.");
DEFINE_int64_hidden(data_stream_sender_eos_timeout_ms, 60*60*1000,
    "Timeout for EndDataStream (EOS) RPCs. Setting a timeout prioritizes them over other "
    "DataStreamService RPCs. Defaults to 1 hour. Set to 0 or negative value to disable "
    "the timeout.");

using std::condition_variable_any;
using namespace apache::thrift;
using kudu::rpc::RpcController;
using kudu::rpc::RpcSidecar;
using kudu::MonoDelta;

DECLARE_int64(impala_slow_rpc_threshold_ms);
DECLARE_int32(rpc_retry_interval_ms);

namespace impala {

const char* KrpcDataStreamSender::HASH_ROW_SYMBOL =
    "KrpcDataStreamSender7HashRowEPNS_8TupleRowEm";
const char* KrpcDataStreamSender::LLVM_CLASS_NAME = "class.impala::KrpcDataStreamSender";
const char* KrpcDataStreamSender::TOTAL_BYTES_SENT_COUNTER = "TotalBytesSent";

Status KrpcDataStreamSenderConfig::Init(
    const TDataSink& tsink, const RowDescriptor* input_row_desc, FragmentState* state) {
  RETURN_IF_ERROR(DataSinkConfig::Init(tsink, input_row_desc, state));
  DCHECK(tsink_->__isset.stream_sink);
  partition_type_ = tsink_->stream_sink.output_partition.type;
  if (partition_type_ == TPartitionType::HASH_PARTITIONED
      || partition_type_ == TPartitionType::KUDU) {
    RETURN_IF_ERROR(
        ScalarExpr::Create(tsink_->stream_sink.output_partition.partition_exprs,
            *input_row_desc_, state, &partition_exprs_));
    exchange_hash_seed_ =
        KrpcDataStreamSender::EXCHANGE_HASH_SEED_CONST ^ state->query_id().hi;
  }
  num_channels_ = state->fragment_ctx().destinations().size();
  state->CheckAndAddCodegenDisabledMessage(codegen_status_msgs_);
  return Status::OK();
}

DataSink* KrpcDataStreamSenderConfig::CreateSink(RuntimeState* state) const {
  // We have one fragment per sink, so we can use the fragment index as the sink ID.
  TDataSinkId sink_id = state->fragment().idx;
  return state->obj_pool()->Add(
      new KrpcDataStreamSender(sink_id, state->instance_ctx().sender_id, *this,
          tsink_->stream_sink, state->fragment_ctx().destinations(),
          FLAGS_data_stream_sender_buffer_size, state));
}

void KrpcDataStreamSenderConfig::Close() {
  ScalarExpr::Close(partition_exprs_);
  DataSinkConfig::Close();
}

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
// Each channel owns a single OutboundRowBatch outbound_batch_. This holds the buffers
// backing the in-flight RPC or kept as reserve if there is no ongoing RPC. The actual
// serialization happens to a shared OutboundRowBatch (see IMPALA-12433 for details) which
// can be swapped with outbound_batch_ once the previous in-flight RPC is finished.
// This means that the client can serialize the next row batch while the current row batch
// is being sent. outbound_batch_ is only used in the partitioned case - in the
// unpartitioned case the shared outbound_batch_ holds the data for the the in-flight RPC.
//
// Upon completion of a RPC, the callback TransmitDataCompleteCb() is invoked. If the RPC
// fails due to remote service's queue being full, TransmitDataCompleteCb() will schedule
// the retry callback RetryCb() after some delay dervied from
// 'FLAGS_rpc_retry_internal_ms'.
//
// When a data stream sender is shut down, it will call Teardown() on all channels to
// release resources. Teardown() will cancel any in-flight RPC and wait for the
// completion callback to be called before returning. It's expected that the execution
// thread to flush all buffered row batches and send the end-of-stream message (by
// calling FlushBatches(), SendEosAsync() and WaitForRpc()) before closing the data
// stream sender.
//
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
      const std::string& hostname, const NetworkAddressPB& destination,
      const UniqueIdPB& fragment_instance_id, PlanNodeId dest_node_id, int buffer_size,
      bool is_local)
    : parent_(parent),
      row_desc_(row_desc),
      hostname_(hostname),
      address_(destination),
      fragment_instance_id_(fragment_instance_id),
      dest_node_id_(dest_node_id),
      is_local_(is_local) {
    DCHECK(IsResolvedAddress(address_));
  }

  // Initializes the channel.
  // Returns OK if successful, error indication otherwise.
  Status Init(RuntimeState* state, std::shared_ptr<CharMemTrackerAllocator> allocator);

  // Serializes the given row batch and send it to the destination. If the preceding
  // RPC is in progress, this function may block until the previous RPC finishes.
  // Return error status if serialization or the preceding RPC failed. Return OK
  // otherwise.
  Status SerializeAndSendBatch(RowBatch* batch);

  // Transmits the serialized row batch 'outbound_batch'. This function may block if the
  // preceding RPC is still in-flight. This is expected to be called from the fragment
  // instance execution thread. Return error status if initialization of the RPC request
  // parameters failed or if the preceding RPC failed. Returns OK otherwise.
  // If 'swap_batch' is true, 'outbound_batch' is swapped with the Channel's
  // 'outbound_batch_'. This happens after the previous RPC is finished so its buffers
  // can be reused.
  Status TransmitData(std::unique_ptr<OutboundRowBatch>* outbound_batch, bool swap_batch);

  // Copies a single row into this channel's row batch and flushes the row batch once
  // it reaches capacity. This call may block if the row batch's capacity is reached
  // and the preceding RPC is still in progress. Returns error status if serialization
  // failed or if the preceding RPC failed. Return OK otherwise.
  Status ALWAYS_INLINE AddRow(TupleRow* row);

  // Shutdowns the channel and frees the row batch allocation. Any in-flight RPC will
  // be cancelled. It's expected that clients normally call FlushBatches(), SendEosAsync()
  // and WaitForRpc() before calling Teardown() to flush all buffered row batches to
  // destinations. Teardown() may be called without flushing the channel in cases such
  // as cancellation or error.
  void Teardown(RuntimeState* state);

  // Flushes any buffered row batches. Return error status if the TransmitData() RPC
  // fails. The RPC is sent asynchrononously. WaitForRpc() must be called to wait
  // for the RPC. This should be only called from a fragment executor thread.
  Status FlushBatches();

  // Sends the EOS RPC to close the channel. Return error status if sending the EOS RPC
  // failed. The RPC is sent asynchrononously. WaitForRpc() must be called to wait for
  // the RPC. This should be only called from a fragment executor thread.
  Status SendEosAsync();

  // Waits for the preceding RPC to complete. Return error status if the preceding
  // RPC fails. Returns CANCELLED if the parent sender is cancelled or shut down.
  // Returns OK otherwise. This should be only called from a fragment executor thread.
  Status WaitForRpc();

  // The type for a RPC worker function.
  typedef boost::function<Status()> DoRpcFn;

  bool IsLocal() const { return is_local_; }

 private:
  // The parent data stream sender owning this channel. Not owned.
  KrpcDataStreamSender* parent_;

  // The descriptor of the accumulated rows in 'batch_' below. Used for computing
  // the capacity of 'batch_' and also when adding a row in AddRow().
  const RowDescriptor* row_desc_;

  // The triplet of IP-address:port/finst-id/node-id uniquely identifies the receiver.
  const std::string hostname_;
  const NetworkAddressPB address_;
  const UniqueIdPB fragment_instance_id_;
  const PlanNodeId dest_node_id_;

  // True if the target fragment instance runs within the same process.
  const bool is_local_;

  // The row batch for accumulating rows copied from AddRow().
  // Only used if the partitioning scheme is "KUDU" or "HASH_PARTITIONED".
  scoped_ptr<RowBatch> batch_;

  // Owns an outbound row batch that can be referenced by the in-flight RPC. Contains
  // a RowBatchHeaderPB and the buffers for the serialized tuple offsets and data.
  //
  // TODO: replace this with a queue. Schedule another RPC callback in the
  // completion callback if the queue is not empty.
  // datastream sender and sharing them across all channels. This buffer is not used in
  // "UNPARTITIONED" scheme.
  std::unique_ptr<OutboundRowBatch> outbound_batch_;

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

  // The monotonic time in nanoseconds of when current RPC started.
  int64_t rpc_start_time_ns_ = 0;

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

  // Same as WaitForRpc() except expects to be called with 'lock_' held and
  // may drop the lock while waiting for the RPC to complete.
  Status WaitForRpcLocked(std::unique_lock<SpinLock>* lock);

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

  // Return true if the RPC exceeds the slow RPC threshold and should be logged.
  inline bool IsSlowRpc(int64_t total_time_ns) {
    int64_t total_time_ms = total_time_ns / NANOS_PER_MICRO / MICROS_PER_MILLI;
    return total_time_ms > FLAGS_impala_slow_rpc_threshold_ms;
  }

  // Logs a slow RPC that took 'total_time_ns'. resp.receiver_latency_ns() is used to
  // distinguish processing time on the receiver from network time.
  template <typename ResponsePBType>
  void LogSlowRpc(
      const char* rpc_name, int64_t total_time_ns, const ResponsePBType& resp);

  // Logs a slow RPC that took 'total_time_ns' and failed with 'error'.
  void LogSlowFailedRpc(
      const char* rpc_name, int64_t total_time_ns, const kudu::Status& err);
};

Status KrpcDataStreamSender::Channel::Init(
    RuntimeState* state, std::shared_ptr<CharMemTrackerAllocator> allocator) {
  // TODO: take into account of var-len data at runtime.
  int capacity =
      max(1, parent_->per_channel_buffer_size_ / max(row_desc_->GetRowSize(), 1));
  batch_.reset(new RowBatch(row_desc_, capacity, parent_->mem_tracker()));

  // Create a DataStreamService proxy to the destination.
  RETURN_IF_ERROR(DataStreamService::GetProxy(address_, hostname_, &proxy_));

  // Init outbound_batch_.
  outbound_batch_.reset(new OutboundRowBatch(allocator));

  return Status::OK();
}

void KrpcDataStreamSender::Channel::MarkDone(const Status& status) {
  if (UNLIKELY(!status.ok())) COUNTER_ADD(parent_->rpc_failure_counter_, 1);
  rpc_status_ = status;
  rpc_in_flight_ = false;
  rpc_in_flight_batch_ = nullptr;
  rpc_done_cv_.notify_one();
  rpc_start_time_ns_ = 0;
}

template <typename ResponsePBType>
void KrpcDataStreamSender::Channel::LogSlowRpc(
    const char* rpc_name, int64_t total_time_ns, const ResponsePBType& resp) {
  int64_t network_time_ns = total_time_ns - resp.receiver_latency_ns();
  LOG(INFO) << "Slow " << rpc_name << " RPC (request call id "
            << rpc_controller_.call_id() << ") to " << address_
            << " (fragment_instance_id=" << PrintId(fragment_instance_id_) << "): "
            << "took " << PrettyPrinter::Print(total_time_ns, TUnit::TIME_NS) << ". "
            << "Receiver time: "
            << PrettyPrinter::Print(resp.receiver_latency_ns(), TUnit::TIME_NS)
            << " Network time: " << PrettyPrinter::Print(network_time_ns, TUnit::TIME_NS);
}

void KrpcDataStreamSender::Channel::LogSlowFailedRpc(
    const char* rpc_name, int64_t total_time_ns, const kudu::Status& err) {
  LOG(INFO) << "Slow " << rpc_name << " RPC to " << address_
            << " (fragment_instance_id=" << PrintId(fragment_instance_id_) << "): "
            << "took " << PrettyPrinter::Print(total_time_ns, TUnit::TIME_NS) << ". "
            << "Error: " << err.ToString();
}

Status KrpcDataStreamSender::Channel::WaitForRpc() {
  std::unique_lock<SpinLock> l(lock_);
  return WaitForRpcLocked(&l);
}

Status KrpcDataStreamSender::Channel::WaitForRpcLocked(std::unique_lock<SpinLock>* lock) {
  DCHECK(lock != nullptr);
  DCHECK(lock->owns_lock());

  ScopedTimer<MonotonicStopWatch> timer(parent_->profile()->inactive_timer(),
      parent_->state_->total_network_send_timer());

  // Wait for in-flight RPCs to complete unless the parent sender is closed or cancelled.
  while(rpc_in_flight_ && !ShouldTerminate()) {
    rpc_done_cv_.wait_for(*lock, std::chrono::milliseconds(50));
  }
  int64_t elapsed_time_ns = timer.ElapsedTime();
  if (IsSlowRpc(elapsed_time_ns)) {
    LOG(INFO) << "Long delay waiting for RPC to " << address_
              << " (fragment_instance_id=" << PrintId(fragment_instance_id_) << "): "
              << "took " << PrettyPrinter::Print(elapsed_time_ns, TUnit::TIME_NS);
  }

  if (UNLIKELY(ShouldTerminate())) {
    // DSS is single-threaded so it's impossible for shutdown_ to be true here.
    DCHECK(!shutdown_);
    return Status::CANCELLED;
  }

  DCHECK(!rpc_in_flight_);
  if (UNLIKELY(!rpc_status_.ok())) {
    LOG(ERROR) << "channel send to " << address_ << " failed: (fragment_instance_id="
               << PrintId(fragment_instance_id_)
               << "): " << rpc_status_.GetDetail();
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
  // If the RPC failed due to a network error, set the RPC error info in RuntimeState.
  if (controller_status.IsNetworkError()) {
    parent_->state_->SetRPCErrorInfo(address_, controller_status.posix_code());
  }
  MarkDone(FromKuduStatus(controller_status, prepend));
}

void KrpcDataStreamSender::Channel::TransmitDataCompleteCb() {
  std::unique_lock<SpinLock> l(lock_);
  DCHECK(rpc_in_flight_);
  DCHECK_NE(rpc_start_time_ns_, 0);
  int64_t total_time = MonotonicNanos() - rpc_start_time_ns_;
  const kudu::Status controller_status = rpc_controller_.status();
  if (LIKELY(controller_status.ok())) {
    DCHECK(rpc_in_flight_batch_ != nullptr);
    // 'receiver_latency_ns' is calculated with MonoTime, so it must be non-negative.
    DCHECK_GE(resp_.receiver_latency_ns(), 0);
    DCHECK_GE(total_time, resp_.receiver_latency_ns());
    int64_t row_batch_size = RowBatch::GetSerializedSize(*rpc_in_flight_batch_);
    int64_t network_time = total_time - resp_.receiver_latency_ns();
    COUNTER_ADD(parent_->bytes_sent_counter_, row_batch_size);
    if (LIKELY(network_time > 0)) {
      // 'row_batch_size' is bounded by FLAGS_rpc_max_message_size which shouldn't exceed
      // max 32-bit signed value so multiplication below should not overflow.
      DCHECK_LE(row_batch_size, numeric_limits<int32_t>::max());
      int64_t network_throughput = row_batch_size * NANOS_PER_SEC / network_time;
      parent_->network_throughput_counter_->UpdateCounter(network_throughput);
      parent_->network_time_stats_->UpdateCounter(network_time);
    }
    parent_->recvr_time_stats_->UpdateCounter(resp_.receiver_latency_ns());
    if (IsSlowRpc(total_time)) LogSlowRpc("TransmitData", total_time, resp_);
    Status rpc_status = Status::OK();
    int32_t status_code = resp_.status().status_code();
    if (status_code == TErrorCode::DATASTREAM_RECVR_CLOSED) {
      remote_recvr_closed_ = true;
    } else {
      rpc_status = Status(resp_.status());
    }
    MarkDone(rpc_status);
  } else {
    if (IsSlowRpc(total_time)) {
      LogSlowFailedRpc("TransmitData", total_time, controller_status);
    }
    DoRpcFn rpc_fn =
        boost::bind(&KrpcDataStreamSender::Channel::DoTransmitDataRpc, this);
    const string& prepend =
        Substitute("TransmitData() to $0 failed", NetworkAddressPBToString(address_));
    HandleFailedRPC(rpc_fn, controller_status, prepend);
  }
}

Status KrpcDataStreamSender::Channel::DoTransmitDataRpc() {
  DCHECK(rpc_in_flight_batch_ != nullptr);
  DCHECK(rpc_in_flight_batch_->IsInitialized());

  // Initialize some constant fields in the request protobuf.
  TransmitDataRequestPB req;
  *req.mutable_dest_fragment_instance_id() = fragment_instance_id_;
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
  rpc_start_time_ns_ = MonotonicNanos();
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
    unique_ptr<OutboundRowBatch>* outbound_batch, bool swap_batch) {
  VLOG_ROW << "Channel::TransmitData() fragment_instance_id="
           << PrintId(fragment_instance_id_) << " dest_node=" << dest_node_id_
           << " #rows=" << outbound_batch->get()->header()->num_rows();
  std::unique_lock<SpinLock> l(lock_);
  RETURN_IF_ERROR(WaitForRpcLocked(&l));
  DCHECK(!rpc_in_flight_);
  DCHECK(rpc_in_flight_batch_ == nullptr);
  // If the remote receiver is closed already, there is no point in sending anything.
  // TODO: Needs better solution for IMPALA-3990 in the long run.
  if (UNLIKELY(remote_recvr_closed_)) return Status::OK();
  rpc_in_flight_ = true;
  rpc_in_flight_batch_ = outbound_batch->get();
  RETURN_IF_ERROR(DoTransmitDataRpc());
  // At this point the previous RPC must be already finished and the previous buffer
  // in outbound_batch_ can be reused.
  DCHECK_NE(rpc_in_flight_batch_, outbound_batch_.get());
  if (swap_batch) outbound_batch_.swap(*outbound_batch);
  return Status::OK();
}

Status KrpcDataStreamSender::Channel::SerializeAndSendBatch(RowBatch* batch) {
  unique_ptr<OutboundRowBatch>* serialization_batch = &parent_->serialization_batch_;
  DCHECK(serialization_batch->get() != nullptr);
  // Reads 'rpc_in_flight_batch_' without acquiring 'lock_', so reads can be racey.
  ANNOTATE_IGNORE_READS_BEGIN();
  DCHECK(serialization_batch->get() != rpc_in_flight_batch_);
  ANNOTATE_IGNORE_READS_END();
  RETURN_IF_ERROR(parent_->SerializeBatch(batch, serialization_batch->get(), !is_local_));
  // Swap serialization_batch with outbound_batch_ once the old RPC is finished.
  RETURN_IF_ERROR(TransmitData(serialization_batch, true /*swap_batch*/));
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
  DCHECK_NE(rpc_start_time_ns_, 0);
  int64_t total_time_ns = MonotonicNanos() - rpc_start_time_ns_;
  const kudu::Status controller_status = rpc_controller_.status();
  if (LIKELY(controller_status.ok())) {
    // 'receiver_latency_ns' is calculated with MonoTime, so it must be non-negative.
    DCHECK_GE(eos_resp_.receiver_latency_ns(), 0);
    DCHECK_GE(total_time_ns, eos_resp_.receiver_latency_ns());
    int64_t network_time_ns = total_time_ns - eos_resp_.receiver_latency_ns();
    parent_->network_time_stats_->UpdateCounter(network_time_ns);
    parent_->recvr_time_stats_->UpdateCounter(eos_resp_.receiver_latency_ns());
    if (IsSlowRpc(total_time_ns)) LogSlowRpc("EndDataStream", total_time_ns, eos_resp_);
    MarkDone(Status(eos_resp_.status()));
  } else {
    if (IsSlowRpc(total_time_ns)) {
      LogSlowFailedRpc("EndDataStream", total_time_ns, controller_status);
    }
    DoRpcFn rpc_fn =
        boost::bind(&KrpcDataStreamSender::Channel::DoEndDataStreamRpc, this);
    const string& prepend =
        Substitute("EndDataStream() to $0 failed", NetworkAddressPBToString(address_));
    HandleFailedRPC(rpc_fn, controller_status, prepend);
  }
}

Status KrpcDataStreamSender::Channel::DoEndDataStreamRpc() {
  DCHECK(rpc_in_flight_);
  EndDataStreamRequestPB eos_req;
  rpc_controller_.Reset();
  if (FLAGS_data_stream_sender_eos_timeout_ms > 0) {
    // Provide a timeout so EOS RPCs are prioritized over others, as completing a stream
    // can help free up resources.
    rpc_controller_.set_timeout(
        MonoDelta::FromMilliseconds(FLAGS_data_stream_sender_eos_timeout_ms));
  }
  *eos_req.mutable_dest_fragment_instance_id() = fragment_instance_id_;
  eos_req.set_sender_id(parent_->sender_id_);
  eos_req.set_dest_node_id(dest_node_id_);
  eos_resp_.Clear();
  rpc_start_time_ns_ = MonotonicNanos();
  proxy_->EndDataStreamAsync(eos_req, &eos_resp_, &rpc_controller_,
      boost::bind(&KrpcDataStreamSender::Channel::EndDataStreamCompleteCb, this));
  return Status::OK();
}

Status KrpcDataStreamSender::Channel::FlushBatches() {
  VLOG_RPC << "Channel::FlushBatches() fragment_instance_id="
           << PrintId(fragment_instance_id_) << " dest_node=" << dest_node_id_
           << " #rows= " << batch_->num_rows();

  // We can return an error here and not go on to send the EOS RPC because the error that
  // we returned will be sent to the coordinator who will then cancel all the remote
  // fragments including the one that this sender is sending to.
  if (batch_->num_rows() > 0) RETURN_IF_ERROR(SendCurrentBatch());
  return Status::OK();
}

Status KrpcDataStreamSender::Channel::SendEosAsync() {
  VLOG_RPC << "Channel::SendEosAsync() fragment_instance_id="
           << PrintId(fragment_instance_id_) << " dest_node=" << dest_node_id_
           << " #rows= " << batch_->num_rows();
  DCHECK_EQ(0, batch_->num_rows()) << "Batches must be flushed";
  {
    std::unique_lock<SpinLock> l(lock_);
    DCHECK(!rpc_in_flight_);
    DCHECK(rpc_status_.ok());
    if (UNLIKELY(remote_recvr_closed_)) return Status::OK();
    VLOG_RPC << "calling EndDataStream() to terminate channel. fragment_instance_id="
             << PrintId(fragment_instance_id_);
    rpc_in_flight_ = true;
    COUNTER_ADD(parent_->eos_sent_counter_, 1);
    RETURN_IF_ERROR(DoEndDataStreamRpc());
  }
  return Status::OK();
}

void KrpcDataStreamSender::Channel::Teardown(RuntimeState* state) {
  // Normally, the channel should have been flushed before calling Teardown(), which means
  // that all the data should already be drained. If the fragment was was closed or
  // cancelled, there may still be some in-flight RPCs and buffered row batches to be
  // flushed.
  std::unique_lock<SpinLock> l(lock_);
  shutdown_ = true;
  // Cancel any in-flight RPC.
  if (rpc_in_flight_) {
    rpc_controller_.Cancel();
    while (rpc_in_flight_) rpc_done_cv_.wait(l);
  }
  batch_.reset();
  outbound_batch_.reset(nullptr);
}

KrpcDataStreamSender::KrpcDataStreamSender(TDataSinkId sink_id, int sender_id,
    const KrpcDataStreamSenderConfig& sink_config, const TDataStreamSink& sink,
    const google::protobuf::RepeatedPtrField<PlanFragmentDestinationPB>& destinations,
    int per_channel_buffer_size, RuntimeState* state)
  : DataSink(sink_id, sink_config,
        Substitute("KrpcDataStreamSender (dst_id=$0)", sink.dest_node_id), state),
    sender_id_(sender_id),
    partition_type_(sink_config.partition_type_),
    per_channel_buffer_size_(per_channel_buffer_size),
    partition_exprs_(sink_config.partition_exprs_),
    dest_node_id_(sink.dest_node_id),
    next_unknown_partition_(0),
    exchange_hash_seed_(sink_config.exchange_hash_seed_),
    hash_and_add_rows_fn_(sink_config.hash_and_add_rows_fn_),
    filepath_to_hosts_(sink_config.filepath_to_hosts_) {
  DCHECK_GT(destinations.size(), 0);
  DCHECK(sink.output_partition.type == TPartitionType::UNPARTITIONED
      || sink.output_partition.type == TPartitionType::HASH_PARTITIONED
      || sink.output_partition.type == TPartitionType::RANDOM
      || sink.output_partition.type == TPartitionType::KUDU
      || sink.output_partition.type == TPartitionType::DIRECTED);

  string process_address =
      NetworkAddressPBToString(ExecEnv::GetInstance()->krpc_address());
  for (const auto& destination : destinations) {
    bool is_local =
        process_address == NetworkAddressPBToString(destination.krpc_backend());
    channels_.emplace_back(new Channel(this, row_desc_, destination.address().hostname(),
        destination.krpc_backend(), destination.fragment_instance_id(), sink.dest_node_id,
        per_channel_buffer_size, is_local));

    if (IsDirectedMode()) {
      DCHECK(host_to_channel_.find(destination.address()) == host_to_channel_.end());
      host_to_channel_[destination.address()] = channels_.back().get();
    }
  }

  if (partition_type_ == TPartitionType::UNPARTITIONED
      || partition_type_ == TPartitionType::RANDOM) {
    // Randomize the order we open/transmit to channels to avoid thundering herd problems.
    random_shuffle(channels_.begin(), channels_.end());
  }

  DCHECK(filepath_to_hosts_.empty() || partition_type_ == TPartitionType::DIRECTED) <<
      " TPartitionType: " << partition_type_ << " dest ID: " << dest_node_id_;
}

KrpcDataStreamSender::~KrpcDataStreamSender() {
  // TODO: check that sender was either already closed() or there was an error
  // on some channel
}

Status KrpcDataStreamSender::Prepare(
    RuntimeState* state, MemTracker* parent_mem_tracker) {
  RETURN_IF_ERROR(DataSink::Prepare(state, parent_mem_tracker));
  state_ = state;
  SCOPED_TIMER(profile_->total_time_counter());
  RETURN_IF_ERROR(ScalarExprEvaluator::Create(partition_exprs_, state, state->obj_pool(),
      expr_perm_pool_.get(), expr_results_pool_.get(), &partition_expr_evals_));
  serialize_batch_timer_ = ADD_TIMER(profile(), "SerializeBatchTime");
  rpc_retry_counter_ = ADD_COUNTER(profile(), "RpcRetry", TUnit::UNIT);
  rpc_failure_counter_ = ADD_COUNTER(profile(), "RpcFailure", TUnit::UNIT);
  bytes_sent_counter_ = ADD_COUNTER(profile(), "TotalBytesSent", TUnit::BYTES);
  state->AddBytesSentCounter(bytes_sent_counter_);
  bytes_sent_time_series_counter_ =
      ADD_TIME_SERIES_COUNTER(profile(), "BytesSent", bytes_sent_counter_);
  network_throughput_counter_ =
      ADD_SUMMARY_STATS_COUNTER(profile(), "NetworkThroughput", TUnit::BYTES_PER_SECOND);
  network_time_stats_ =
      ADD_SUMMARY_STATS_COUNTER(profile(), "RpcNetworkTime", TUnit::TIME_NS);
  recvr_time_stats_ =
      ADD_SUMMARY_STATS_COUNTER(profile(), "RpcRecvrTime", TUnit::TIME_NS);
  eos_sent_counter_ = ADD_COUNTER(profile(), "EosSent", TUnit::UNIT);
  uncompressed_bytes_counter_ =
      ADD_COUNTER(profile(), "UncompressedRowBatchSize", TUnit::BYTES);
  total_sent_rows_counter_ = ADD_COUNTER(profile(), "RowsSent", TUnit::UNIT);

  outbound_rb_mem_tracker_.reset(
      new MemTracker(-1, "RowBatchSerialization", mem_tracker_.get()));
  char_mem_tracker_allocator_.reset(
      new CharMemTrackerAllocator(outbound_rb_mem_tracker_));

  string process_address =
       NetworkAddressPBToString(ExecEnv::GetInstance()->krpc_address());

  serialization_batch_.reset(new OutboundRowBatch(char_mem_tracker_allocator_));
  if (partition_type_ == TPartitionType::UNPARTITIONED) {
    in_flight_batch_.reset(new OutboundRowBatch(char_mem_tracker_allocator_));
  }

  compression_scratch_.reset(new TrackedString(*char_mem_tracker_allocator_.get()));

  for (int i = 0; i < channels_.size(); ++i) {
    RETURN_IF_ERROR(channels_[i]->Init(state, char_mem_tracker_allocator_));
  }
  return Status::OK();
}

Status KrpcDataStreamSender::Open(RuntimeState* state) {
  SCOPED_TIMER(profile_->total_time_counter());
  RETURN_IF_ERROR(DataSink::Open(state));
  return ScalarExprEvaluator::Open(partition_expr_evals_, state);
}

// An example of generated code. Used the following query to generate it:
//   use functional_orc_def;
//   select a.outer_struct, b.small_struct
//   from complextypes_nested_structs a
//       full outer join complextypes_structs b
//           on b.small_struct.i = a.outer_struct.inner_struct2.i + 19091;
//
// define i64 @KrpcDataStreamSenderHashRow(%"class.impala::KrpcDataStreamSender"* %this,
//                                         %"class.impala::TupleRow"* %row,
//                                         i64 %seed) #49 {
// entry:
//   %0 = alloca i64
//   %1 = call %"class.impala::ScalarExprEvaluator"*
//       @_ZN6impala20KrpcDataStreamSender25GetPartitionExprEvaluatorEi(
//           %"class.impala::KrpcDataStreamSender"* %this, i32 0)
//   %partition_val = call { i8, i64 }
//       @"impala::Operators::Add_BigIntVal_BigIntValWrapper"(
//           %"class.impala::ScalarExprEvaluator"* %1, %"class.impala::TupleRow"* %row)
//   br label %entry1
//
// entry1:                                           ; preds = %entry
//   %2 = extractvalue { i8, i64 } %partition_val, 0
//   %is_null = trunc i8 %2 to i1
//   br i1 %is_null, label %null, label %non_null
//
// non_null:                                         ; preds = %entry1
//   %val = extractvalue { i8, i64 } %partition_val, 1
//   store i64 %val, i64* %0
//   %native_ptr = bitcast i64* %0 to i8*
//   br label %hash_val_block
//
// null:                                             ; preds = %entry1
//   br label %hash_val_block
//
// hash_val_block:                                   ; preds = %non_null, %null
//   %native_ptr_phi = phi i8* [ %native_ptr, %non_null ], [ null, %null ]
//   %hash_val = call i64
//       @_ZN6impala8RawValue20GetHashValueFastHashEPKvRKNS_10ColumnTypeEm(
//           i8* %native_ptr_phi, %"struct.impala::ColumnType"* @expr_type_arg, i64 %seed)
//   ret i64 %hash_val
// }
Status KrpcDataStreamSenderConfig::CodegenHashRow(
    LlvmCodeGen* codegen, llvm::Function** fn) {
  llvm::LLVMContext& context = codegen->context();
  LlvmBuilder builder(context);

  LlvmCodeGen::FnPrototype prototype(
      codegen, "KrpcDataStreamSenderHashRow", codegen->i64_type());
  prototype.AddArgument(LlvmCodeGen::NamedVariable(
      "this", codegen->GetNamedPtrType(KrpcDataStreamSender::LLVM_CLASS_NAME)));
  prototype.AddArgument(
      LlvmCodeGen::NamedVariable("row", codegen->GetStructPtrType<TupleRow>()));
  prototype.AddArgument(LlvmCodeGen::NamedVariable("seed", codegen->i64_type()));

  llvm::Value* args[3];
  llvm::Function* hash_row_fn = prototype.GeneratePrototype(&builder, args);
  llvm::Value* this_arg = args[0];
  llvm::Value* row_arg = args[1];

  // Store the initial seed to hash_val
  llvm::Value* hash_val = args[2];

  // Unroll the loop and codegen each of the partition expressions
  for (int i = 0; i < partition_exprs_.size(); ++i) {
    llvm::Function* compute_fn;
    RETURN_IF_ERROR(
        partition_exprs_[i]->GetCodegendComputeFn(codegen, false, &compute_fn));

    // Load the expression evaluator for the i-th partition expression
    llvm::Function* get_expr_eval_fn =
        codegen->GetFunction(IRFunction::KRPC_DSS_GET_PART_EXPR_EVAL, false);
    DCHECK(get_expr_eval_fn != nullptr);
    llvm::Value* expr_eval_arg =
        builder.CreateCall(get_expr_eval_fn, {this_arg, codegen->GetI32Constant(i)});

    // Compute the value against the i-th partition expression
    llvm::Value* compute_fn_args[] = {expr_eval_arg, row_arg};
    CodegenAnyVal partition_val = CodegenAnyVal::CreateCallWrapped(codegen, &builder,
        partition_exprs_[i]->type(), compute_fn, compute_fn_args, "partition_val");

    CodegenAnyValReadWriteInfo rwi = partition_val.ToReadWriteInfo();
    rwi.entry_block().BranchTo(&builder);

    llvm::BasicBlock* hash_val_block =
        llvm::BasicBlock::Create(context, "hash_val_block", hash_row_fn);

    // Set the pointer to NULL in case 'partition_val' evaluates to NULL
    builder.SetInsertPoint(rwi.null_block());
    llvm::Value* null_ptr = codegen->null_ptr_value();
    builder.CreateBr(hash_val_block);

    // Saves 'partition_val' on the stack and passes a pointer to it to the hash function
    builder.SetInsertPoint(rwi.non_null_block());
    llvm::Value* native_ptr = SlotDescriptor::CodegenStoreNonNullAnyValToNewAlloca(rwi);
    native_ptr = builder.CreatePointerCast(native_ptr, codegen->ptr_type(), "native_ptr");
    builder.CreateBr(hash_val_block);

    // Picks the input value to hash function
    builder.SetInsertPoint(hash_val_block);
    llvm::PHINode* val_ptr_phi = rwi.CodegenNullPhiNode(native_ptr, null_ptr);

    // Creates a global constant of the partition expression's ColumnType. It has to be a
    // constant for constant propagation and dead code elimination in 'get_hash_value_fn'
    llvm::Type* col_type = codegen->GetStructType<ColumnType>();
    llvm::Constant* expr_type_arg = codegen->ConstantToGVPtr(
        col_type, partition_exprs_[i]->type().ToIR(codegen), "expr_type_arg");

    // Update 'hash_val' with the new 'partition-val'
    llvm::Value* get_hash_value_args[] = {val_ptr_phi, expr_type_arg, hash_val};
    llvm::Function* get_hash_value_fn =
        codegen->GetFunction(IRFunction::RAW_VALUE_GET_HASH_VALUE_FAST_HASH, false);
    DCHECK(get_hash_value_fn != nullptr);
    hash_val = builder.CreateCall(get_hash_value_fn, get_hash_value_args, "hash_val");
  }

  builder.CreateRet(hash_val);
  *fn = codegen->FinalizeFunction(hash_row_fn);
  if (*fn == nullptr) {
    return Status("Codegen'd KrpcDataStreamSenderHashRow() fails verification. See log");
  }
  return Status::OK();
}

string KrpcDataStreamSenderConfig::PartitionTypeName() const {
  switch (partition_type_) {
  case TPartitionType::UNPARTITIONED:
    return "Unpartitioned";
  case TPartitionType::HASH_PARTITIONED:
    return "Hash Partitioned";
  case TPartitionType::RANDOM:
    return "Random Partitioned";
  case TPartitionType::KUDU:
    return "Kudu Partitioned";
  case TPartitionType::DIRECTED:
    return "Directed distribution mode";
  default:
    DCHECK(false) << partition_type_;
    return "";
  }
}

void KrpcDataStreamSenderConfig::Codegen(FragmentState* state) {
  LlvmCodeGen* codegen = state->codegen();
  DCHECK(codegen != nullptr);
  const string sender_name = PartitionTypeName() + " Sender";
  if (partition_type_ != TPartitionType::HASH_PARTITIONED) {
    const string& msg = Substitute("not $0",
        partition_type_ == TPartitionType::KUDU ? "supported" : "needed");
    codegen_status_msgs_.emplace_back(
        FragmentState::GenerateCodegenMsg(false, msg, sender_name));
    return;
  }

  llvm::Function* hash_row_fn;
  Status codegen_status = CodegenHashRow(codegen, &hash_row_fn);
  if (codegen_status.ok()) {
    llvm::Function* hash_and_add_rows_fn =
        codegen->GetFunction(IRFunction::KRPC_DSS_HASH_AND_ADD_ROWS, true);
    DCHECK(hash_and_add_rows_fn != nullptr);

    int num_replaced;
    // Replace GetNumChannels() with a constant.
    num_replaced = codegen->ReplaceCallSitesWithValue(hash_and_add_rows_fn,
        codegen->GetI32Constant(num_channels_), "GetNumChannels");
    DCHECK_EQ(num_replaced, 1);

    // Replace HashRow() with the handcrafted IR function.
    num_replaced = codegen->ReplaceCallSites(hash_and_add_rows_fn,
        hash_row_fn, KrpcDataStreamSender::HASH_ROW_SYMBOL);
    DCHECK_EQ(num_replaced, 1);

    hash_and_add_rows_fn = codegen->FinalizeFunction(hash_and_add_rows_fn);
    if (hash_and_add_rows_fn == nullptr) {
      codegen_status =
          Status("Codegen'd HashAndAddRows() failed verification. See log");
    } else {
      codegen->AddFunctionToJit(hash_and_add_rows_fn, &hash_and_add_rows_fn_);
    }
  }
  AddCodegenStatus(codegen_status, sender_name);
}

Status KrpcDataStreamSender::AddRowToChannel(const int channel_id, TupleRow* row) {
  return channels_[channel_id]->AddRow(row);
}

uint64_t KrpcDataStreamSender::HashRow(TupleRow* row, uint64_t seed) {
  uint64_t hash_val = seed;
  for (ScalarExprEvaluator* eval : partition_expr_evals_) {
    void* partition_val = eval->GetValue(row);
    // We can't use the crc hash function here because it does not result in
    // uncorrelated hashes with different seeds. Instead we use FastHash.
    // TODO: fix crc hash/GetHashValue()
    hash_val = RawValue::GetHashValueFastHash(
        partition_val, eval->root().type(), hash_val);
  }
  return hash_val;
}

Status KrpcDataStreamSender::Send(RuntimeState* state, RowBatch* batch) {
  SCOPED_TIMER(profile()->total_time_counter());
  DCHECK(!closed_);
  DCHECK(!flushed_);

  if (batch->num_rows() == 0) return Status::OK();
  if (partition_type_ == TPartitionType::UNPARTITIONED) {
    // Only skip compression if there is a single channel and destination is in the same
    // process. TODO: could be optimized to send the uncompressed buffer to the local
    // targets to avoid decompression cost at the receiver.
    bool is_local = channels_.size() == 1 && channels_[0]->IsLocal();
    RETURN_IF_ERROR(SerializeBatch(
        batch, serialization_batch_.get(), !is_local,  channels_.size()));
    // TransmitData() will block if there are still in-flight rpcs (and those will
    // reference the previously written in_flight_batch_).
    for (int i = 0; i < channels_.size(); ++i) {
      // Do not swap serialization_batch_ with the channel's outbound_batch_ to allow
      // multiple channels to use the data backing serialization_batch_ in parallel.
      RETURN_IF_ERROR(
          channels_[i]->TransmitData(&serialization_batch_, false /*swap_batch*/));
    }
    // At this point no RPCs can still refer to the old in_flight_batch_.
    in_flight_batch_.swap(serialization_batch_);
  } else if (partition_type_ == TPartitionType::RANDOM || channels_.size() == 1) {
    // Round-robin batches among channels. Wait for the current channel to finish its
    // rpc before overwriting its batch.
    Channel* current_channel = channels_[current_channel_idx_].get();
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
  } else if (partition_type_ == TPartitionType::DIRECTED) {
    const int num_rows = batch->num_rows();
    char* prev_filename_ptr = nullptr;
    vector<Channel*> prev_channels;
    bool skipped_prev_row = false;
    for (int row_idx = 0; row_idx < num_rows; ++row_idx) {
      DCHECK_EQ(batch->num_tuples_per_row(), 1);
      TupleRow* tuple_row = batch->GetRow(row_idx);
      Tuple* row = batch->GetRow(row_idx)->GetTuple(0);
      StringValue* filename_value = row->GetStringSlot(0);
      DCHECK(filename_value != nullptr);
      StringValue::SimpleString filename_value_ss = filename_value->ToSimpleString();
      if (filename_value_ss.ptr == prev_filename_ptr) {
        // If the filename pointer is the same as the previous one then we can instantly
        // send the row to the same channels as the previous row.
        DCHECK(skipped_prev_row || !prev_channels.empty() ||
            (filename_value_ss.len == 0 && prev_channels.empty()));
        for (Channel* ch : prev_channels) RETURN_IF_ERROR(ch->AddRow(tuple_row));
        continue;
      }
      prev_filename_ptr = filename_value_ss.ptr;
      string filename(filename_value_ss.ptr, filename_value_ss.len);

      const auto filepath_to_hosts_it = filepath_to_hosts_.find(filename);
      if (filepath_to_hosts_it == filepath_to_hosts_.end()) {
        // This can happen when e.g. compaction removed some data files from a snapshot
        // but a delete file referencing them remained because it references other data
        // files that remains in the new snapshot.
        // Another use-case is table sampling where we read only a subset of the data
        // files.
        // A third case is when the delete record is invalid.
        if (UNLIKELY(filename_value_ss.len == 0)) {
          state->LogError(
            ErrorMsg(TErrorCode::GENERAL, "NULL found as file_path in delete file"));
        } else {
          VLOG(3) << "Row from delete file refers to a non-existing data file: " <<
              filename;
        }
        skipped_prev_row = true;
        continue;
      }
      skipped_prev_row = false;

      prev_channels.clear();
      for (const NetworkAddressPB& host_addr : filepath_to_hosts_it->second) {
        const auto channel_map_it = host_to_channel_.find(host_addr);
        if (channel_map_it == host_to_channel_.end()) {
          DumpFilenameToHostsMapping();
          DumpDestinationHosts();

          stringstream ss;
          ss << "Failed to distribute Iceberg delete file content"
              " in DIRECTED distribution mode. Host not found " << host_addr <<
              ". Try 'SET DISABLE_OPTIMIZED_ICEBERG_V2_READ=1' as a workaround.";
          return Status(ss.str());
        }

        prev_channels.push_back(channel_map_it->second);
        RETURN_IF_ERROR(channel_map_it->second->AddRow(tuple_row));
      }
    }
  } else {
    DCHECK_EQ(partition_type_, TPartitionType::HASH_PARTITIONED);
    const KrpcDataStreamSenderConfig::HashAndAddRowsFn hash_and_add_rows_fn =
        hash_and_add_rows_fn_.load();
    if (hash_and_add_rows_fn != nullptr) {
      RETURN_IF_ERROR(hash_and_add_rows_fn(this, batch));
    } else {
      RETURN_IF_ERROR(HashAndAddRows(batch));
    }
  }
  COUNTER_ADD(total_sent_rows_counter_, batch->num_rows());
  expr_results_pool_->Clear();
  RETURN_IF_ERROR(state->CheckQueryState());
  return Status::OK();
}

void KrpcDataStreamSender::DumpFilenameToHostsMapping() const {
  VLOG(3) << "Dumping the contents of the filename to hosts mapping";
  if (filepath_to_hosts_.empty()) {
    VLOG(3) << "The mapping is empty";
    return;
  }
  for (const auto& file_to_hosts : filepath_to_hosts_) {
    for (const auto& host_addr : file_to_hosts.second) {
      VLOG(3) << "Filename: " << file_to_hosts.first << " host address: " << host_addr;
    }
  }
}

void KrpcDataStreamSender::DumpDestinationHosts() const {
  VLOG(3) << "Dumping the destination hosts";
  for (const auto& host : host_to_channel_) {
    VLOG(3) << "Network Address: " << host.first;
  }
}

Status KrpcDataStreamSender::FlushFinal(RuntimeState* state) {
  SCOPED_TIMER(profile()->total_time_counter());
  DCHECK(!flushed_);
  DCHECK(!closed_);
  flushed_ = true;

  // Send out the final row batches and EOS signals on all channels in parallel.
  // If we hit an error here, we can return without closing the remaining channels as
  // the error is propagated back to the coordinator, which in turn cancels the query,
  // which will cause the remaining open channels to be closed.
  for (unique_ptr<Channel>& channel : channels_) {
    RETURN_IF_ERROR(channel->FlushBatches());
  }
  for (unique_ptr<Channel>& channel : channels_) {
    RETURN_IF_ERROR(channel->WaitForRpc());
  }
  for (unique_ptr<Channel>& channel : channels_) {
    RETURN_IF_ERROR(channel->SendEosAsync());
  }
  for (unique_ptr<Channel>& channel : channels_) {
    RETURN_IF_ERROR(channel->WaitForRpc());
  }
  return Status::OK();
}

void KrpcDataStreamSender::Close(RuntimeState* state) {
  SCOPED_TIMER(profile()->total_time_counter());
  if (closed_) return;
  for (int i = 0; i < channels_.size(); ++i) {
    channels_[i]->Teardown(state);
  }

  serialization_batch_.reset(nullptr);
  in_flight_batch_.reset(nullptr);
  compression_scratch_.reset(nullptr);

  if (outbound_rb_mem_tracker_.get() != nullptr) {
    outbound_rb_mem_tracker_->Close();
  }

  ScalarExprEvaluator::Close(partition_expr_evals_, state);
  profile()->StopPeriodicCounters();
  DataSink::Close(state);
}

Status KrpcDataStreamSender::SerializeBatch(
    RowBatch* src, OutboundRowBatch* dest, bool compress, int num_receivers) {
  VLOG_ROW << "serializing " << src->num_rows() << " rows";
  {
    SCOPED_TIMER(serialize_batch_timer_);
    RETURN_IF_ERROR(
        src->Serialize(dest, compress ? compression_scratch_.get() : nullptr));
    int64_t uncompressed_bytes = RowBatch::GetDeserializedSize(*dest);
    COUNTER_ADD(uncompressed_bytes_counter_, uncompressed_bytes * num_receivers);
  }
  return Status::OK();
}

int64_t KrpcDataStreamSender::GetNumDataBytesSent() const {
  return bytes_sent_counter_->value();
}

} // namespace impala
