// Copyright (c) 2012 Cloudera, Inc. All rights reserved.

#ifndef IMPALA_UTIL_THRIFT_SERVER_H
#define IMPALA_UTIL_THRIFT_SERVER_H

#include <boost/scoped_ptr.hpp>
#include <boost/shared_ptr.hpp>
#include <server/TNonblockingServer.h>
#include <TProcessor.h>

#include "common/status.h"

namespace boost { class thread; }

namespace impala {

// Utility class for all Thrift servers. Runs a TNonblockingServer
// with, by default, 2 worker threads, that exposes the interface
// described by a user-supplied TProcessor object.
class ThriftServer {
 public:
  static const int DEFAULT_WORKER_THREADS = 2;

  // Creates, but does not start, a new server on the specified port
  // that exports the supplied interface.
  ThriftServer(const boost::shared_ptr<apache::thrift::TProcessor>& processor, int port,
      int num_worker_threads = DEFAULT_WORKER_THREADS);

  int port() const { return port_; }

  // Blocks until the server stops and exits its main thread.
  void Join();

  // Starts the main server thread. Once this call returns, clients
  // may connect to this server and issue RPCs. May not be called more
  // than once.
  Status Start();

 private:
  bool started_;
  int port_;
  int num_worker_threads_;
  boost::scoped_ptr<boost::thread> server_thread_;
  boost::scoped_ptr<apache::thrift::server::TNonblockingServer> server_;
  boost::shared_ptr<apache::thrift::TProcessor> processor_;
};

}

#endif
