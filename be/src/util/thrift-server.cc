// Copyright (c) 2012 Cloudera, Inc. All rights reserved.

#include <boost/thread.hpp>
#include <concurrency/PosixThreadFactory.h>
#include <concurrency/Thread.h>
#include <concurrency/ThreadManager.h>
#include <protocol/TBinaryProtocol.h>
#include <server/TNonblockingServer.h>

#include "util/thrift-server.h"

using namespace boost;
using namespace apache::thrift;
using namespace apache::thrift::concurrency;
using namespace apache::thrift::protocol;
using namespace apache::thrift::transport;
using namespace apache::thrift::server;

namespace impala {

ThriftServer::ThriftServer(const shared_ptr<TProcessor>& processor, int port,
    int num_worker_threads)
    : started_(false),
      port_(port),
      num_worker_threads_(num_worker_threads),
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
  server_thread_.reset(new thread(&TNonblockingServer::serve, server_.get()));
  started_ = true;
  return Status::OK;
}

void ThriftServer::Join() {
  DCHECK(server_thread_ != NULL);
  DCHECK(started_);
  server_thread_->join();
}

}
