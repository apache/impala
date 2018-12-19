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

#include "rpc/TAcceptQueueServer.h"

#include <thrift/concurrency/PlatformThreadFactory.h>

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
        if (!processor_->process(input_, output_, connectionContext)
            || !input_->getTransport()->peek()) {
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
    const boost::shared_ptr<ThreadFactory>& threadFactory,
    int32_t maxTasks)
    : TServer(processor, serverTransport, transportFactory, protocolFactory),
      threadFactory_(threadFactory), maxTasks_(maxTasks) {
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

// New.
void TAcceptQueueServer::SetupConnection(boost::shared_ptr<TTransport> client) {
  if (metrics_enabled_) queue_size_metric_->Increment(-1);
  shared_ptr<TTransport> inputTransport;
  shared_ptr<TTransport> outputTransport;
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
      while (maxTasks_ > 0 && tasks_.size() >= maxTasks_) {
        tasksMonitor_.wait();
      }
      tasks_.insert(task);
    }

    // Start the thread!
    thread->start();
  } catch (TException& tx) {
    if (inputTransport != nullptr) {
      inputTransport->close();
    }
    if (outputTransport != nullptr) {
      outputTransport->close();
    }
    if (client != nullptr) {
      client->close();
    }
    string errStr = string("TAcceptQueueServer: Caught TException: ") + tx.what();
    GlobalOutput(errStr.c_str());
  } catch (string s) {
    if (inputTransport != nullptr) {
      inputTransport->close();
    }
    if (outputTransport != nullptr) {
      outputTransport->close();
    }
    if (client != nullptr) {
      client->close();
    }
    string errStr = "TAcceptQueueServer: Unknown exception: " + s;
    GlobalOutput(errStr.c_str());
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
  ThreadPool<shared_ptr<TTransport>> connection_setup_pool("setup-server", "setup-worker",
      FLAGS_accepted_cnxn_setup_thread_pool_size, FLAGS_accepted_cnxn_queue_depth,
      [this](int tid, const shared_ptr<TTransport>& item) {
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

      // New - the work done to setup the connection has been moved to SetupConnection.
      if (!connection_setup_pool.Offer(std::move(client))) {
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
  metrics_enabled_ = true;
}

} // namespace server
} // namespace thrift
} // namespace apache
