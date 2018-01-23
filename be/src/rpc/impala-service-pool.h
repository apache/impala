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

#ifndef IMPALA_SERVICE_POOL_H
#define IMPALA_SERVICE_POOL_H

#include <string>
#include <vector>

#include "kudu/gutil/macros.h"
#include "kudu/gutil/gscoped_ptr.h"
#include "kudu/gutil/ref_counted.h"
#include "kudu/rpc/rpc_service.h"
#include "kudu/rpc/service_queue.h"
#include "kudu/util/status.h"
#include "util/histogram-metric.h"
#include "util/spinlock.h"
#include "util/thread.h"

namespace impala {
class MemTracker;

/// A pool of threads that handle new incoming RPC calls.
/// Also includes a queue that calls get pushed onto for handling by the pool.
class ImpalaServicePool : public kudu::rpc::RpcService {
 public:
  /// 'service_queue_length' is the maximum number of requests that may be queued for
  /// this service before clients begin to see rejection errors.
  ///
  /// 'service' contains an interface implementation that will handle RPCs.
  ///
  /// 'service_mem_tracker' is the MemTracker for tracking the memory usage of RPC
  /// payloads in the service queue.
  ImpalaServicePool(const scoped_refptr<kudu::MetricEntity>& entity,
      size_t service_queue_length, kudu::rpc::GeneratedServiceIf* service,
      MemTracker* service_mem_tracker);

  virtual ~ImpalaServicePool();

  /// Start up the thread pool.
  virtual Status Init(int num_threads);

  /// Shut down the queue and the thread pool.
  virtual void Shutdown();

  virtual kudu::rpc::RpcMethodInfo* LookupMethod(const kudu::rpc::RemoteMethod& method)
    override;

  virtual kudu::Status
      QueueInboundCall(gscoped_ptr<kudu::rpc::InboundCall> call) OVERRIDE;

  const std::string service_name() const;

  /// Expose the service pool metrics by storing them as JSON in 'value'.
  void ToJson(rapidjson::Value* value, rapidjson::Document* document);

 private:
  void RunThread();
  void RejectTooBusy(kudu::rpc::InboundCall* c);

  /// Respond with failure to the incoming call in 'call' with 'error_code' and 'status'
  /// and release the payload memory from 'mem_tracker_'. Takes ownership of 'call'.
  void FailAndReleaseRpc(const kudu::rpc::ErrorStatusPB::RpcErrorCodePB& error_code,
      const kudu::Status& status, kudu::rpc::InboundCall* call);

  /// Synchronizes accesses to 'service_mem_tracker_' to avoid over consumption.
  SpinLock mem_tracker_lock_;

  /// Tracks memory of inbound calls in 'service_queue_'.
  MemTracker* const service_mem_tracker_;

  /// Reference to the implementation of the RPC handlers. Not owned.
  kudu::rpc::GeneratedServiceIf* const service_;

  /// The set of service threads started to process incoming RPC calls.
  std::vector<std::unique_ptr<Thread>> threads_;

  /// The pending RPCs to be dequeued by the service threads.
  kudu::rpc::LifoServiceQueue service_queue_;

  /// Histogram to track time spent by requests in the krpc incoming requests queue.
  scoped_refptr<kudu::Histogram> incoming_queue_time_;

  /// Histogram for incoming request payload size for each method of this service.
  std::unordered_map<std::string, std::unique_ptr<HistogramMetric>>
      payload_size_histograms_;

  /// Number of RPCs that were rejected due to the queue being full. Not owned.
  IntCounter* rpcs_queue_overflow_= nullptr;

  /// Protects against concurrent Shutdown() operations.
  /// TODO: This seems implausible given our current usage pattern.
  /// Consider removing lock.
  boost::mutex shutdown_lock_;
  bool closing_ = false;

  DISALLOW_COPY_AND_ASSIGN(ImpalaServicePool);
};

} // namespace impala

#endif  // IMPALA_SERVICE_POOL_H
