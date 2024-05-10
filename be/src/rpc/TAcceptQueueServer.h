/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
// This file was copied from apache::thrift::server::TThreadedServer.cpp v0.9.0, with the
// significant changes noted inline below.

#ifndef IMPALA_RPC_TACCEPTQUEUESERVER_H
#define IMPALA_RPC_TACCEPTQUEUESERVER_H

#include <thrift/concurrency/Monitor.h>
#include <thrift/concurrency/ThreadFactory.h>
#include <thrift/server/TServer.h>
#include <thrift/transport/TServerTransport.h>

#include <boost/shared_ptr.hpp>

#include "util/metrics-fwd.h"

namespace apache {
namespace thrift {
namespace server {

using apache::thrift::TProcessor;
using apache::thrift::transport::TServerTransport;
using apache::thrift::transport::TTransportFactory;
using apache::thrift::concurrency::Monitor;
using apache::thrift::concurrency::ThreadFactory;

struct TAcceptQueueEntry {
  std::shared_ptr<TTransport> client_;
  int64_t expiration_time_ = 0LL;
};

/**
 * In TAcceptQueueServer, the main server thread calls accept() and then immediately
 * places the returned TTransport on a queue to be processed by a separate thread,
 * asynchronously.
 *
 * This helps solve IMPALA-4135, where connections were timing out while waiting in the
 * OS accept queue, by ensuring that accept() is called as quickly as possible.
 */
class TAcceptQueueServer : public TServer {
 public:
  class Task;
  TAcceptQueueServer(const std::shared_ptr<TProcessor>& processor,
      const std::shared_ptr<TServerTransport>& serverTransport,
      const std::shared_ptr<TTransportFactory>& transportFactory,
      const std::shared_ptr<TProtocolFactory>& protocolFactory,
      const std::shared_ptr<ThreadFactory>& threadFactory,
      const std::string& name, int32_t maxTasks = 0,
      int64_t queue_timeout_ms = 0, int64_t idle_poll_period_ms = 0,
      bool is_external_facing = true);

  ~TAcceptQueueServer() override = default;

  void serve() override;

  void stop() override {
    stop_ = true;
    serverTransport_->interrupt();
  }

  // New - Adds a metric for the size of the queue of connections waiting to be setup to
  // the provided MetricGroup, prefixing its key with key_prefix.
  void InitMetrics(impala::MetricGroup* metrics, const std::string& key_prefix);

 protected:
  void init();

  // This is the work function for the thread pool, which does the work of setting up the
  // connection and starting a thread to handle it. Will block if there are currently
  // maxTasks_ connections and maxTasks_ is non-zero.
  void SetupConnection(std::shared_ptr<TAcceptQueueEntry> entry);

  // Helper function to close a client connection in case of server side errors.
  void CleanupAndClose(const std::string& error, std::shared_ptr<TTransport> io_transport,
      std::shared_ptr<TTransport> client);

  std::shared_ptr<ThreadFactory> threadFactory_;
  volatile bool stop_ = false;

  /// Name of the thrift server.
  const std::string name_;

  // Monitor protecting tasks_, notified on removal.
  Monitor tasksMonitor_;
  std::set<Task*> tasks_;

  // The maximum number of running tasks allowed at a time.
  const int32_t maxTasks_;

  /// True if metrics are enabled
  bool metrics_enabled_ = false;

  /// Number of connections that have been accepted and are waiting to be setup.
  impala::IntGauge* queue_size_metric_ = nullptr;

  /// Number of connections rejected due to timeout.
  impala::IntGauge* timedout_cnxns_metric_ = nullptr;

  /// Distribution of connection setup time in microseconds. This does not include the
  /// time spent waiting for a service thread to be available.
  impala::HistogramMetric* cnxns_setup_time_us_metric_ = nullptr;

  /// Distribution of wait time in microseconds for service threads to be available.
  impala::HistogramMetric* thread_wait_time_us_metric_ = nullptr;

  /// Amount of time in milliseconds after which a connection request will be timed out.
  /// Default value is 0, which means no timeout.
  int64_t queue_timeout_ms_;

  /// Amount of time in milliseconds of client's inactivity before the service thread
  /// wakes up to check if the connection should be closed due to inactivity. If 0, no
  /// polling happens.
  int64_t idle_poll_period_ms_;

  /// Whether this is interacting with external untrusted clients. If true, this
  /// uses ThriftExternalRpcMaxMessageSize(). If false, this uses the
  /// ThriftInternalRpcMaxMessageSize().
  bool is_external_facing_;
};

} // namespace server
} // namespace thrift
} // namespace apache

#endif // #ifndef IMPALA_RPC_TACCEPTQUEUESERVER_H
