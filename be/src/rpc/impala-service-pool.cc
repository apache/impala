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

#include "rpc/impala-service-pool.h"

#include <boost/thread/mutex.hpp>
#include <glog/logging.h>
#include <memory>
#include <string>
#include <vector>

#include "exec/kudu-util.h"
#include "kudu/gutil/gscoped_ptr.h"
#include "kudu/gutil/ref_counted.h"
#include "kudu/gutil/strings/join.h"
#include "kudu/rpc/inbound_call.h"
#include "kudu/rpc/messenger.h"
#include "kudu/rpc/service_if.h"
#include "kudu/rpc/service_queue.h"
#include "kudu/util/metrics.h"
#include "kudu/util/status.h"
#include "kudu/util/trace.h"

#include "common/names.h"
#include "common/status.h"

METRIC_DEFINE_histogram(server, impala_unused,
    "RPC Queue Time",
    kudu::MetricUnit::kMicroseconds,
    "Number of microseconds incoming RPC requests spend in the worker queue",
    60000000LU, 3);

namespace impala {

ImpalaServicePool::ImpalaServicePool(std::unique_ptr<kudu::rpc::ServiceIf> service,
                         const scoped_refptr<kudu::MetricEntity>& entity,
                         size_t service_queue_length)
  : service_(std::move(service)),
    service_queue_(service_queue_length),
    unused_histogram_(METRIC_impala_unused.Instantiate(entity)) {

}

ImpalaServicePool::~ImpalaServicePool() {
  Shutdown();
}

Status ImpalaServicePool::Init(int num_threads) {
  for (int i = 0; i < num_threads; i++) {
    std::unique_ptr<Thread> new_thread;
    RETURN_IF_ERROR(Thread::Create("service pool", "rpc worker",
        &ImpalaServicePool::RunThread, this, &new_thread));
    threads_.push_back(std::move(new_thread));
  }
  return Status::OK();
}

void ImpalaServicePool::Shutdown() {
  service_queue_.Shutdown();

  lock_guard<mutex> lock(shutdown_lock_);
  if (closing_) return;
  closing_ = true;
  // TODO (from KRPC): Use a proper thread pool implementation.
  for (std::unique_ptr<Thread>& thread : threads_) {
    thread->Join();
  }

  // Now we must drain the service queue.
  kudu::Status status = kudu::Status::ServiceUnavailable("Service is shutting down");
  std::unique_ptr<kudu::rpc::InboundCall> incoming;
  while (service_queue_.BlockingGet(&incoming)) {
    incoming.release()->RespondFailure(
        kudu::rpc::ErrorStatusPB::FATAL_SERVER_SHUTTING_DOWN, status);
  }

  service_->Shutdown();
}

void ImpalaServicePool::RejectTooBusy(kudu::rpc::InboundCall* c) {
  string err_msg =
      Substitute("$0 request on $1 from $2 dropped due to backpressure. "
                 "The service queue is full; it has $3 items.",
                 c->remote_method().method_name(),
                 service_->service_name(),
                 c->remote_address().ToString(),
                 service_queue_.max_size());
  rpcs_queue_overflow_.Add(1);
  c->RespondFailure(kudu::rpc::ErrorStatusPB::ERROR_SERVER_TOO_BUSY,
                    kudu::Status::ServiceUnavailable(err_msg));
  VLOG(1) << err_msg << " Contents of service queue:\n"
          << service_queue_.ToString();
}

kudu::rpc::RpcMethodInfo* ImpalaServicePool::LookupMethod(
    const kudu::rpc::RemoteMethod& method) {
  return service_->LookupMethod(method);
}

kudu::Status ImpalaServicePool::QueueInboundCall(
    gscoped_ptr<kudu::rpc::InboundCall> call) {
  kudu::rpc::InboundCall* c = call.release();

  vector<uint32_t> unsupported_features;
  for (uint32_t feature : c->GetRequiredFeatures()) {
    if (!service_->SupportsFeature(feature)) {
      unsupported_features.push_back(feature);
    }
  }

  if (!unsupported_features.empty()) {
    c->RespondUnsupportedFeature(unsupported_features);
    return kudu::Status::NotSupported("call requires unsupported application feature flags",
                                JoinMapped(unsupported_features,
                                           [] (uint32_t flag) { return std::to_string(flag); },
                                           ", "));
  }

  TRACE_TO(c->trace(), "Inserting onto call queue"); // NOLINT(*)

  // Queue message on service queue
  boost::optional<kudu::rpc::InboundCall*> evicted;
  auto queue_status = service_queue_.Put(c, &evicted);
  if (queue_status == kudu::rpc::QueueStatus::QUEUE_FULL) {
    RejectTooBusy(c);
    return kudu::Status::OK();
  }

  if (PREDICT_FALSE(evicted != boost::none)) {
    RejectTooBusy(*evicted);
  }

  if (PREDICT_TRUE(queue_status == kudu::rpc::QueueStatus::QUEUE_SUCCESS)) {
    // NB: do not do anything with 'c' after it is successfully queued --
    // a service thread may have already dequeued it, processed it, and
    // responded by this point, in which case the pointer would be invalid.
    return kudu::Status::OK();
  }

  kudu::Status status = kudu::Status::OK();
  if (queue_status == kudu::rpc::QueueStatus::QUEUE_SHUTDOWN) {
    status = kudu::Status::ServiceUnavailable("Service is shutting down");
    c->RespondFailure(kudu::rpc::ErrorStatusPB::FATAL_SERVER_SHUTTING_DOWN, status);
  } else {
    status = kudu::Status::RuntimeError(
        Substitute("Unknown error from BlockingQueue: $0", queue_status));
    c->RespondFailure(kudu::rpc::ErrorStatusPB::FATAL_UNKNOWN, status);
  }
  return status;
}

void ImpalaServicePool::RunThread() {
  while (true) {
    std::unique_ptr<kudu::rpc::InboundCall> incoming;
    if (!service_queue_.BlockingGet(&incoming)) {
      VLOG(1) << "ImpalaServicePool: messenger shutting down.";
      return;
    }

    // We need to call RecordHandlingStarted() to update the InboundCall timing.
    incoming->RecordHandlingStarted(unused_histogram_);
    ADOPT_TRACE(incoming->trace());

    if (PREDICT_FALSE(incoming->ClientTimedOut())) {
      TRACE_TO(incoming->trace(), "Skipping call since client already timed out"); // NOLINT(*)
      rpcs_timed_out_in_queue_.Add(1);

      // Respond as a failure, even though the client will probably ignore
      // the response anyway.
      incoming->RespondFailure(
        kudu::rpc::ErrorStatusPB::ERROR_SERVER_TOO_BUSY,
        kudu::Status::TimedOut("Call waited in the queue past client deadline"));

      // Must release since RespondFailure above ends up taking ownership
      // of the object.
      ignore_result(incoming.release());
      continue;
    }

    TRACE_TO(incoming->trace(), "Handling call"); // NOLINT(*)

    // Release the InboundCall pointer -- when the call is responded to,
    // it will get deleted at that point.
    service_->Handle(incoming.release());
  }
}

const string ImpalaServicePool::service_name() const {
  return service_->service_name();
}

} // namespace impala
