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

#include <algorithm>
#include "rpc/TAcceptQueueServer.h"

#include <thrift/concurrency/PlatformThreadFactory.h>
#include <thrift/transport/TSocket.h>

#include "util/metrics.h"
#include "rpc/thrift-util.h"
#include "rpc/thrift-server.h"
#include "util/thread-pool.h"

DEFINE_int32(accepted_cnxn_queue_depth, 10000,
    "(Advanced) The size of the post-accept, pre-setup connection queue in each thrift "
    "server set up to service Impala internal and external connections.");

DEFINE_int32(accepted_cnxn_setup_thread_pool_size, 2,
    "(Advanced) The size of the thread pool that is used to process the "
    "post-accept, pre-setup connection queue in each thrift server set up to service "
    "Impala internal and external connections.");

namespace apache {
namespace thrift {
namespace server {

using boost::shared_ptr;
using namespace std;
using namespace apache::thrift;
using namespace apache::thrift::protocol;
using namespace apache::thrift::transport;
using namespace apache::thrift::concurrency;
using namespace impala;

class TAcceptQueueServer::Task : public Runnable {
 public:
  Task(TAcceptQueueServer& server, shared_ptr<TProcessor> processor,
      shared_ptr<TProtocol> input, shared_ptr<TProtocol> output,
      shared_ptr<TTransport> transport)
    : server_(server),
      processor_(std::move(processor)),
      input_(std::move(input)),
      output_(std::move(output)),
      transport_(std::move(transport)) {}

  ~Task() override = default;

  void run() override {
    boost::shared_ptr<TServerEventHandler> eventHandler = server_.getEventHandler();
    void* connectionContext = nullptr;
    if (eventHandler != nullptr) {
      connectionContext = eventHandler->createContext(input_, output_);
    }
    try {
      for (;;) {
        if (eventHandler != nullptr) {
          eventHandler->processContext(connectionContext, transport_);
        }
        // Setting a socket timeout for process() may lead to false positive
        // and prematurely closes a slow client's connection.
        if (!processor_->process(input_, output_, connectionContext) ||
            !Peek(input_, connectionContext, eventHandler)) {
          break;
        }
      }
    } catch (const TTransportException& ttx) {
      if (ttx.getType() != TTransportException::END_OF_FILE) {
        string errStr = string("TAcceptQueueServer client died: ") + ttx.what();
        GlobalOutput(errStr.c_str());
      }
    } catch (const std::exception& x) {
      GlobalOutput.printf(
          "TAcceptQueueServer exception: %s: %s", typeid(x).name(), x.what());
    } catch (...) {
      GlobalOutput("TAcceptQueueServer uncaught exception.");
    }
    if (eventHandler != nullptr) {
      eventHandler->deleteContext(connectionContext, input_, output_);
    }

    try {
      input_->getTransport()->close();
    } catch (TTransportException& ttx) {
      string errStr = string("TAcceptQueueServer input close failed: ") + ttx.what();
      GlobalOutput(errStr.c_str());
    }
    try {
      output_->getTransport()->close();
    } catch (TTransportException& ttx) {
      string errStr = string("TAcceptQueueServer output close failed: ") + ttx.what();
      GlobalOutput(errStr.c_str());
    }

    // Remove this task from parent bookkeeping
    {
      Synchronized s(server_.tasksMonitor_);
      server_.tasks_.erase(this);
      server_.tasksMonitor_.notify();
    }
  }

 private:

  // This function blocks until some bytes show up from the client.
  // Returns true if some bytes are available from client;
  // Returns false upon reading EOF, in which case the connection
  // will be closed by the caller.
  //
  // If idle_poll_period_ms_ is not 0, this function will block up
  // to idle_poll_period_ms_ milliseconds before waking up to check
  // if the sessions associated with the connection have all expired
  // due to inactivity. If so, it will return false and the connection
  // will be closed by the caller.
  bool Peek(shared_ptr<TProtocol> input, void* connectionContext,
      boost::shared_ptr<TServerEventHandler> eventHandler) {
    // Set a timeout on input socket if idle_poll_period_ms_ is non-zero.
    TSocket* socket = static_cast<TSocket*>(transport_.get());
    if (server_.idle_poll_period_ms_ > 0) {
      socket->setRecvTimeout(server_.idle_poll_period_ms_);
    }

    // Block until some bytes show up or EOF or timeout.
    bool bytes_pending = true;
    for (;;) {
      try {
        bytes_pending = input_->getTransport()->peek();
        break;
      } catch (const TTransportException& ttx) {
        // Implementaion of the underlying transport's peek() may call either
        // read() or peek() of the socket.
        if (eventHandler != nullptr && server_.idle_poll_period_ms_ > 0 &&
            (IsReadTimeoutTException(ttx) || IsPeekTimeoutTException(ttx))) {
          ThriftServer::ThriftServerEventProcessor* thriftServerHandler =
              static_cast<ThriftServer::ThriftServerEventProcessor*>(eventHandler.get());
          if (thriftServerHandler->IsIdleContext(connectionContext)) {
            const string& client = socket->getSocketInfo();
            GlobalOutput.printf(
               "TAcceptQueueServer closing connection to idle client %s", client.c_str());
            bytes_pending = false;
            break;
          }
        } else {
          // Rethrow the exception to be handled by callers.
          throw;
        }
      }
    }
    // Unset the socket timeout.
    if (server_.idle_poll_period_ms_ > 0) socket->setRecvTimeout(0);
    return bytes_pending;
  }

  TAcceptQueueServer& server_;
  friend class TAcceptQueueServer;

  shared_ptr<TProcessor> processor_;
  shared_ptr<TProtocol> input_;
  shared_ptr<TProtocol> output_;
  shared_ptr<TTransport> transport_;
};

TAcceptQueueServer::TAcceptQueueServer(const boost::shared_ptr<TProcessor>& processor,
    const boost::shared_ptr<TServerTransport>& serverTransport,
    const boost::shared_ptr<TTransportFactory>& transportFactory,
    const boost::shared_ptr<TProtocolFactory>& protocolFactory,
    const boost::shared_ptr<ThreadFactory>& threadFactory, const string& name,
    int32_t maxTasks, int64_t queue_timeout_ms, int64_t idle_poll_period_ms)
    : TServer(processor, serverTransport, transportFactory, protocolFactory),
      threadFactory_(threadFactory), name_(name), maxTasks_(maxTasks),
      queue_timeout_ms_(queue_timeout_ms), idle_poll_period_ms_(idle_poll_period_ms) {
  init();
}

void TAcceptQueueServer::init() {
  stop_ = false;
  metrics_enabled_ = false;
  queue_size_metric_ = nullptr;

  if (!threadFactory_) {
    threadFactory_.reset(new PlatformThreadFactory);
  }
}

void TAcceptQueueServer::CleanupAndClose(const string& error,
    shared_ptr<TTransport> input, shared_ptr<TTransport> output,
    shared_ptr<TTransport> client) {
  if (input != nullptr) {
    input->close();
  }
  if (output != nullptr) {
    output->close();
  }
  if (client != nullptr) {
    client->close();
  }
  GlobalOutput(error.c_str());
}

// New.
void TAcceptQueueServer::SetupConnection(shared_ptr<TAcceptQueueEntry> entry) {
  if (metrics_enabled_) queue_size_metric_->Increment(-1);
  shared_ptr<TTransport> inputTransport;
  shared_ptr<TTransport> outputTransport;
  shared_ptr<TTransport> client = entry->client_;
  try {
    inputTransport = inputTransportFactory_->getTransport(client);
    outputTransport = outputTransportFactory_->getTransport(client);
    shared_ptr<TProtocol> inputProtocol =
        inputProtocolFactory_->getProtocol(inputTransport);
    shared_ptr<TProtocol> outputProtocol =
        outputProtocolFactory_->getProtocol(outputTransport);

    shared_ptr<TProcessor> processor =
        getProcessor(inputProtocol, outputProtocol, client);

    TAcceptQueueServer::Task* task = new TAcceptQueueServer::Task(
        *this, processor, inputProtocol, outputProtocol, client);

    // Create a task
    shared_ptr<Runnable> runnable = shared_ptr<Runnable>(task);

    // Create a thread for this task
    shared_ptr<Thread> thread = shared_ptr<Thread>(threadFactory_->newThread(runnable));

    // Insert thread into the set of threads
    {
      Synchronized s(tasksMonitor_);
      int64_t wait_time_ms = 0;

      while (maxTasks_ > 0 && tasks_.size() >= maxTasks_) {
        if (entry->expiration_time_ != 0) {
          // We don't want wait_time to 'accidentally' go non-positive,
          // so wait for at least 1ms.
          wait_time_ms = std::max(1L, entry->expiration_time_ - MonotonicMillis());
        }
        LOG_EVERY_N(INFO, 10) << name_ <<": All " << maxTasks_
                  << " server threads are in use. "
                  << "Waiting for " << wait_time_ms << " milliseconds.";
        int wait_result = tasksMonitor_.waitForTimeRelative(wait_time_ms);
        if (wait_result == THRIFT_ETIMEDOUT) {
          if (metrics_enabled_) timedout_cnxns_metric_->Increment(1);
          LOG(INFO) << name_ << ": Server busy. Timing out connection request.";
          string errStr = "TAcceptQueueServer: " + name_ + " server busy";
          CleanupAndClose(errStr, inputTransport, outputTransport, client);
          return;
        }
      }
      tasks_.insert(task);
    }

    // Start the thread!
    thread->start();
  } catch (TException& tx) {
    string errStr = string("TAcceptQueueServer: Caught TException: ") + tx.what();
    CleanupAndClose(errStr, inputTransport, outputTransport, client);
  } catch (string s) {
    string errStr = "TAcceptQueueServer: Unknown exception: " + s;
    CleanupAndClose(errStr, inputTransport, outputTransport, client);
  }
}

void TAcceptQueueServer::serve() {
  // Start the server listening
  serverTransport_->listen();

  // Run the preServe event
  if (eventHandler_ != nullptr) {
    eventHandler_->preServe();
  }

  if (FLAGS_accepted_cnxn_setup_thread_pool_size > 1) {
    LOG(INFO) << "connection_setup_thread_pool_size is set to "
              << FLAGS_accepted_cnxn_setup_thread_pool_size;
  }

  // New - this is the thread pool used to process the internal accept queue.
  ThreadPool<shared_ptr<TAcceptQueueEntry>> connection_setup_pool("setup-server",
      "setup-worker", FLAGS_accepted_cnxn_setup_thread_pool_size,
      FLAGS_accepted_cnxn_queue_depth,
      [this](int tid, const shared_ptr<TAcceptQueueEntry>& item) {
        this->SetupConnection(item);
      });
  // Initialize the thread pool
  Status status = connection_setup_pool.Init();
  if (!status.ok()) {
    status.AddDetail("TAcceptQueueServer: thread pool could not start.");
    string errStr = status.GetDetail();
    GlobalOutput(errStr.c_str());
    stop_ = true;
  }

  while (!stop_) {
    try {
      // Fetch client from server
      shared_ptr<TTransport> client = serverTransport_->accept();

      shared_ptr<TAcceptQueueEntry> entry{new TAcceptQueueEntry};
      entry->client_ = client;
      if (queue_timeout_ms_ > 0) {
        entry->expiration_time_ = MonotonicMillis() + queue_timeout_ms_;
      }

      // New - the work done to set up the connection has been moved to SetupConnection.
      // Note that we move() entry so it's owned by SetupConnection thread.
      if (!connection_setup_pool.Offer(std::move(entry))) {
        string errStr = string("TAcceptQueueServer: thread pool unexpectedly shut down.");
        GlobalOutput(errStr.c_str());
        stop_ = true;
        break;
      }
      if (metrics_enabled_) queue_size_metric_->Increment(1);
    } catch (TTransportException& ttx) {
      if (!stop_ || ttx.getType() != TTransportException::INTERRUPTED) {
        string errStr =
            string("TAcceptQueueServer: TServerTransport died on accept: ") + ttx.what();
        GlobalOutput(errStr.c_str());
      }
      continue;
    } catch (TException& tx) {
      string errStr = string("TAcceptQueueServer: Caught TException: ") + tx.what();
      GlobalOutput(errStr.c_str());
      continue;
    } catch (string s) {
      string errStr = "TAcceptQueueServer: Unknown exception: " + s;
      GlobalOutput(errStr.c_str());
      break;
    }
  }

  // If stopped manually, make sure to close server transport
  if (stop_) {
    try {
      serverTransport_->close();
      connection_setup_pool.Shutdown();
    } catch (TException& tx) {
      string errStr = string("TAcceptQueueServer: Exception shutting down: ") + tx.what();
      GlobalOutput(errStr.c_str());
    }
    try {
      Synchronized s(tasksMonitor_);
      while (!tasks_.empty()) {
        tasksMonitor_.wait();
      }
    } catch (TException& tx) {
      string errStr =
          string("TAcceptQueueServer: Exception joining workers: ") + tx.what();
      GlobalOutput(errStr.c_str());
    }
    stop_ = false;
  }
}

void TAcceptQueueServer::InitMetrics(MetricGroup* metrics, const string& key_prefix) {
  DCHECK(metrics != nullptr);
  stringstream queue_size_ss;
  queue_size_ss << key_prefix << ".connection-setup-queue-size";
  queue_size_metric_ = metrics->AddGauge(queue_size_ss.str(), 0);
  stringstream timedout_cnxns_ss;
  timedout_cnxns_ss << key_prefix << ".timedout-cnxn-requests";
  timedout_cnxns_metric_ = metrics->AddGauge(timedout_cnxns_ss.str(), 0);
  metrics_enabled_ = true;
}

} // namespace server
} // namespace thrift
} // namespace apache
