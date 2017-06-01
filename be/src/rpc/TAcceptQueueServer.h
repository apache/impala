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
#include <thrift/concurrency/Thread.h>
#include <thrift/server/TServer.h>
#include <thrift/transport/TServerTransport.h>

#include <boost/shared_ptr.hpp>

#include "util/metrics.h"

namespace apache {
namespace thrift {
namespace server {

using apache::thrift::TProcessor;
using apache::thrift::transport::TServerTransport;
using apache::thrift::transport::TTransportFactory;
using apache::thrift::concurrency::Monitor;
using apache::thrift::concurrency::ThreadFactory;

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

  // TODO: Determine which c'tors are used and remove unused ones.
  template <typename ProcessorFactory>
  TAcceptQueueServer(const boost::shared_ptr<ProcessorFactory>& processorFactory,
      const boost::shared_ptr<TServerTransport>& serverTransport,
      const boost::shared_ptr<TTransportFactory>& transportFactory,
      const boost::shared_ptr<TProtocolFactory>& protocolFactory,
      int32_t maxTasks = 0,
      THRIFT_OVERLOAD_IF(ProcessorFactory, TProcessorFactory));

  template <typename ProcessorFactory>
  TAcceptQueueServer(const boost::shared_ptr<ProcessorFactory>& processorFactory,
      const boost::shared_ptr<TServerTransport>& serverTransport,
      const boost::shared_ptr<TTransportFactory>& transportFactory,
      const boost::shared_ptr<TProtocolFactory>& protocolFactory,
      const boost::shared_ptr<ThreadFactory>& threadFactory,
      int32_t maxTasks = 0,
      THRIFT_OVERLOAD_IF(ProcessorFactory, TProcessorFactory));

  template <typename Processor>
  TAcceptQueueServer(const boost::shared_ptr<Processor>& processor,
      const boost::shared_ptr<TServerTransport>& serverTransport,
      const boost::shared_ptr<TTransportFactory>& transportFactory,
      const boost::shared_ptr<TProtocolFactory>& protocolFactory,
      int32_t maxTasks = 0,
      THRIFT_OVERLOAD_IF(Processor, TProcessor));

  template <typename Processor>
  TAcceptQueueServer(const boost::shared_ptr<Processor>& processor,
      const boost::shared_ptr<TServerTransport>& serverTransport,
      const boost::shared_ptr<TTransportFactory>& transportFactory,
      const boost::shared_ptr<TProtocolFactory>& protocolFactory,
      const boost::shared_ptr<ThreadFactory>& threadFactory,
      int32_t maxTasks = 0,
      THRIFT_OVERLOAD_IF(Processor, TProcessor));

  virtual ~TAcceptQueueServer();

  virtual void serve();

  void stop() {
    stop_ = true;
    serverTransport_->interrupt();
  }

  // New - Adds a metric for the size of the queue of connections waiting to be setup to
  // the provided MetricGroup, prefixing its key with key_prefix.
  void InitMetrics(impala::MetricGroup* metrics, const string& key_prefix);

 protected:
  void init();

  // This is the work function for the thread pool, which does the work of setting up the
  // connection and starting a thread to handle it. Will block if there are currently
  // maxTasks_ connections and maxTasks_ is non-zero.
  void SetupConnection(boost::shared_ptr<TTransport> client);

  boost::shared_ptr<ThreadFactory> threadFactory_;
  volatile bool stop_;

  // Monitor protecting tasks_, notified on removal.
  Monitor tasksMonitor_;
  std::set<Task*> tasks_;

  // The maximum number of running tasks allowed at a time.
  const int32_t maxTasks_;

  /// New - True if metrics are enabled
  bool metrics_enabled_;

  /// New - Number of connections that have been accepted and are waiting to be setup.
  impala::IntGauge* queue_size_metric_;
};

template <typename ProcessorFactory>
TAcceptQueueServer::TAcceptQueueServer(
    const boost::shared_ptr<ProcessorFactory>& processorFactory,
    const boost::shared_ptr<TServerTransport>& serverTransport,
    const boost::shared_ptr<TTransportFactory>& transportFactory,
    const boost::shared_ptr<TProtocolFactory>& protocolFactory,
    int32_t maxTasks,
    THRIFT_OVERLOAD_IF_DEFN(ProcessorFactory, TProcessorFactory))
  : TServer(processorFactory, serverTransport, transportFactory, protocolFactory),
    maxTasks_(maxTasks) {
  init();
}

template <typename ProcessorFactory>
TAcceptQueueServer::TAcceptQueueServer(
    const boost::shared_ptr<ProcessorFactory>& processorFactory,
    const boost::shared_ptr<TServerTransport>& serverTransport,
    const boost::shared_ptr<TTransportFactory>& transportFactory,
    const boost::shared_ptr<TProtocolFactory>& protocolFactory,
    const boost::shared_ptr<ThreadFactory>& threadFactory,
    int32_t maxTasks,
    THRIFT_OVERLOAD_IF_DEFN(ProcessorFactory, TProcessorFactory))
  : TServer(processorFactory, serverTransport, transportFactory, protocolFactory),
    threadFactory_(threadFactory), maxTasks_(maxTasks) {
  init();
}

template <typename Processor>
TAcceptQueueServer::TAcceptQueueServer(const boost::shared_ptr<Processor>& processor,
    const boost::shared_ptr<TServerTransport>& serverTransport,
    const boost::shared_ptr<TTransportFactory>& transportFactory,
    const boost::shared_ptr<TProtocolFactory>& protocolFactory,
    int32_t maxTasks,
    THRIFT_OVERLOAD_IF_DEFN(Processor, TProcessor))
  : TServer(processor, serverTransport, transportFactory, protocolFactory),
    maxTasks_(maxTasks) {
  init();
}

template <typename Processor>
TAcceptQueueServer::TAcceptQueueServer(const boost::shared_ptr<Processor>& processor,
    const boost::shared_ptr<TServerTransport>& serverTransport,
    const boost::shared_ptr<TTransportFactory>& transportFactory,
    const boost::shared_ptr<TProtocolFactory>& protocolFactory,
    const boost::shared_ptr<ThreadFactory>& threadFactory,
    int32_t maxTasks,
    THRIFT_OVERLOAD_IF_DEFN(Processor, TProcessor))
  : TServer(processor, serverTransport, transportFactory, protocolFactory),
    threadFactory_(threadFactory), maxTasks_(maxTasks) {
  init();
}
} // namespace server
} // namespace thrift
} // namespace apache

#endif // #ifndef IMPALA_RPC_TACCEPTQUEUESERVER_H
