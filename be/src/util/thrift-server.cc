// Copyright (c) 2012 Cloudera, Inc. All rights reserved.

#include <boost/thread.hpp>
#include <boost/thread/mutex.hpp>
#include <boost/thread/condition_variable.hpp>
#include <concurrency/PosixThreadFactory.h>
#include <concurrency/Thread.h>
#include <concurrency/ThreadManager.h>
#include <protocol/TBinaryProtocol.h>
#include <server/TNonblockingServer.h>

#include "util/thrift-server.h"

#include <sstream>

using namespace std;
using namespace boost;
using namespace apache::thrift;
using namespace apache::thrift::concurrency;
using namespace apache::thrift::protocol;
using namespace apache::thrift::transport;
using namespace apache::thrift::server;

namespace impala {

// Helper class that starts a server in a separate thread, and handles
// the inter-thread communication to monitor whether it started
// correctly.
class ThriftServer::ThriftServerEventProcessor : public TServerEventHandler {
 public:
  ThriftServerEventProcessor(ThriftServer* thrift_server)
      : thrift_server_(thrift_server),
        signal_fired_(false) { }

  // Called by TNonBlockingServer when server has acquired its resources and is ready to
  // serve, and signals to StartAndWaitForServer that start-up is finished. 
  // From TServerEventHandler. 
  virtual void preServe();

  // Waits for a timeout of TIMEOUT_MS for a server to signal that it has started 
  // correctly.
  Status StartAndWaitForServer();

 private:
  // Lock used to ensure that there are no missed notifications between starting the
  // supervision thread and calling signal_cond_.timed_wait. Also used to ensure
  // thread-safe access to members of thrift_server_
  boost::mutex signal_lock_;

  // Condition variable that is notified by the supervision thread once either
  // a) all is well or b) an error occurred.
  boost::condition_variable signal_cond_;

  // The ThriftServer under management. This class is a friend of ThriftServer, and
  // reaches in to change member variables at will.
  ThriftServer* thrift_server_;

  // Guards against spurious condition variable wakeups
  bool signal_fired_;

  // The time, in milliseconds, to wait for a server to come up
  static const int TIMEOUT_MS = 2500;

  // Called in a separate thread; wraps TNonBlockingServer::serve in an exception handler
  void Supervise();
};

Status ThriftServer::ThriftServerEventProcessor::StartAndWaitForServer() {
  // Locking here protects against missed notifications if Supervise executes quickly
  unique_lock<mutex> lock(signal_lock_);
  thrift_server_->started_ = false;

  thrift_server_->server_thread_.reset(
      new thread(&ThriftServer::ThriftServerEventProcessor::Supervise, this));

  system_time deadline = get_system_time() +
      posix_time::milliseconds(ThriftServer::ThriftServerEventProcessor::TIMEOUT_MS);

  // Loop protects against spurious wakeup. Locks provide necessary fences to ensure
  // visibility.
  while (!signal_fired_) {
    // Yields lock and allows supervision thread to continue and signal
    if (!signal_cond_.timed_wait(lock, deadline)) {
      stringstream ss;
      ss << "ThriftServer '" << thrift_server_->name_ << "' (on port: "
         << thrift_server_->port_ << ") did not start within "
         << ThriftServer::ThriftServerEventProcessor::TIMEOUT_MS << "ms";
      LOG(ERROR) << ss.str();
      return Status(ss.str());
    }
  }

  // started_ == true only if preServe was called. May be false if there was an exception
  // after preServe that was caught by Supervise, causing it to reset the error condition.
  if (thrift_server_->started_ == false) {
    stringstream ss;
    ss << "ThriftServer '" << thrift_server_->name_ << "' (on port: "
       << thrift_server_->port_ << ") did not start correctly ";
    LOG(ERROR) << ss.str();
    return Status(ss.str());
  }
  return Status::OK;
}

void ThriftServer::ThriftServerEventProcessor::Supervise() {
  DCHECK(thrift_server_->server_.get() != NULL);
  try {
    thrift_server_->server_->serve();
  } catch (TException& e) {
    LOG(ERROR) << "ThriftServer '" << thrift_server_->name_ << "' (on port: "
               << thrift_server_->port_ << ") exited due to TException: " << e.what();

    {
      // signal_lock_ ensures mutual exclusion of access to thrift_server_
      lock_guard<mutex> lock(signal_lock_);
      thrift_server_->started_ = false;

      // There may not be anyone waiting on this signal (if the
      // exception occurs after startup). That's not a problem, this is
      // just to avoid waiting for the timeout in case of a bind
      // failure, for example.
      signal_fired_ = true;
    }
    signal_cond_.notify_all();
  }
}

void ThriftServer::ThriftServerEventProcessor::preServe() {
  // Acquire the signal lock to ensure that StartAndWaitForServer is
  // waiting on signal_cond_ when we notify.
  lock_guard<mutex> lock(signal_lock_);
  signal_fired_ = true;

  // This is the (only) success path - if this is not reached within TIMEOUT_MS,
  // StartAndWaitForServer will indicate failure.
  thrift_server_->started_ = true;

  // Should only be one thread waiting on signal_cond_, but wake all just in case.
  signal_cond_.notify_all();
}

ThriftServer::ThriftServer(const string& name, const shared_ptr<TProcessor>& processor,
    int port, int num_worker_threads)
    : started_(false),
      port_(port),
      num_worker_threads_(num_worker_threads),
      name_(name),
      server_thread_(NULL),
      server_(NULL),
      processor_(processor) {
}

Status ThriftServer::Start() {
  DCHECK(!started_);
  shared_ptr<TProtocolFactory> protocol_factory(new TBinaryProtocolFactory());
  shared_ptr<ThreadManager> thread_mgr(
      ThreadManager::newSimpleThreadManager(num_worker_threads_));
  shared_ptr<ThreadFactory> thread_factory(new PosixThreadFactory());

  thread_mgr->threadFactory(thread_factory);
  thread_mgr->start();

  server_.reset(new TNonblockingServer(processor_, protocol_factory, port_, thread_mgr));
  shared_ptr<ThriftServer::ThriftServerEventProcessor> event_processor(
      new ThriftServer::ThriftServerEventProcessor(this));
  server_->setServerEventHandler(event_processor);

  RETURN_IF_ERROR(event_processor->StartAndWaitForServer());

  LOG(INFO) << "ThriftServer '" << name_ << "' started on port: " << port_;

  DCHECK(started_);
  return Status::OK;
}

void ThriftServer::Join() {
  DCHECK(server_thread_ != NULL);
  DCHECK(started_);
  server_thread_->join();
}

}
