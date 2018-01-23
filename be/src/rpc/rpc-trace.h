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

#ifndef IMPALA_RPC_RPC_TRACE_H
#define IMPALA_RPC_RPC_TRACE_H

#include "rpc/thrift-server.h"
#include "util/histogram-metric.h"
#include "util/internal-queue.h"

#include <thrift/TProcessor.h>
#include <boost/thread/thread.hpp>
#include <boost/unordered_map.hpp>

#include <rapidjson/document.h>

namespace impala {

class MetricGroup;
class RpcMgr;
class Webserver;

/// An RpcEventHandler is called every time an Rpc is started and completed. There is at
/// most one RpcEventHandler per ThriftServer. When an Rpc is started, getContext() creates
/// an InvocationContext recording the current time and other metadata for that invocation.
class RpcEventHandler : public apache::thrift::TProcessorEventHandler {
 public:
  RpcEventHandler(const std::string& server_name, MetricGroup* metrics);

  /// From TProcessorEventHandler, called initially when an Rpc is invoked. Returns an
  /// InvocationContext*. 'server_context' is a per-connection context object. For our
  /// Thrift servers, it is always a ThriftServer::ConnectionContext*.
  virtual void* getContext(const char* fn_name, void* server_context);

  /// From TProcessorEventHandler, called after all bytes were written to the calling
  /// client. 'ctx' is the context returned by getContext(), which is an
  /// InvocationContext*.
  virtual void postWrite(void* ctx, const char* fn_name, uint32_t bytes);

  /// Helper method to dump all per-method summaries to Json
  /// Json produced looks like:
  /// {
  ///   "name": "beeswax",
  ///   "methods": [
  ///   {
  ///     "name": "BeeswaxService.get_state",
  ///     "summary": " count: 1, last: 0, min: 0, max: 0, mean: 0, stddev: 0",
  ///     "in_flight": 0
  ///     },
  ///   {
  ///     "name": "BeeswaxService.query",
  ///     "summary": " count: 1, last: 293, min: 293, max: 293, mean: 293, stddev: 0",
  ///     "in_flight": 0
  ///     },
  ///   ]
  /// }
  void ToJson(rapidjson::Value* server, rapidjson::Document* document);

  /// Resets the statistics for a single method
  void Reset(const std::string& method_name);

  /// Resets the statistics for all methods
  void ResetAll();

  std::string server_name() const { return server_name_; }

 private:
  /// Per-method descriptor
  struct MethodDescriptor {
    /// Name of the method
    std::string name;

    /// Distribution of the time taken to process this RPC.
    HistogramMetric* processing_time_distribution;

    /// Number of invocations in flight
    AtomicInt32 num_in_flight;
  };

  /// Map from method name to descriptor
  typedef boost::unordered_map<std::string, MethodDescriptor*> MethodMap;

  /// Created per-Rpc invocation
  struct InvocationContext {
    /// Monotonic milliseconds (typically boot time) when the call started.
    const int64_t start_time_ms;

    /// Per-connection information, owned by ThriftServer. The lifetime of this struct is
    /// tied to the lifetime of the connection, which is guaranteed to be longer than the
    /// rpc lifetime.
    const ThriftServer::ConnectionContext* cnxn_ctx;

    /// Pointer to parent MethodDescriptor, to save a lookup on deletion
    MethodDescriptor* method_descriptor;

    InvocationContext(int64_t start_time, const ThriftServer::ConnectionContext* cnxn_ctx,
        MethodDescriptor* descriptor)
        : start_time_ms(start_time), cnxn_ctx(cnxn_ctx), method_descriptor(descriptor) { }
  };

  /// Protects method_map_ and rpc_counter_
  boost::mutex method_map_lock_;

  /// Map of all methods, populated lazily as they are invoked for the first time.
  MethodMap method_map_;

  /// Name of the server that we listen for events from.
  std::string server_name_;

  /// Metrics subsystem access
  MetricGroup* metrics_;

};

/// Initialises rpc event tracing, must be called before any RpcEventHandlers are created.
void InitRpcEventTracing(Webserver* webserver, RpcMgr* = nullptr);

}

#endif
