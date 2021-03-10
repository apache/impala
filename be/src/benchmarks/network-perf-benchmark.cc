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

#include <iostream>

#include <thrift/protocol/TBinaryProtocol.h>
#include <thrift/protocol/TDebugProtocol.h>
#include <thrift/transport/TSocket.h>
#include <thrift/transport/TTransportUtils.h>

#include <boost/algorithm/string.hpp>
#include <boost/thread/thread.hpp>
#include "gen-cpp/NetworkTest_types.h"
#include "gen-cpp/NetworkTestService.h"

#include "common/init.h"
#include "common/logging.h"
#include "util/cpu-info.h"
#include "util/stopwatch.h"
#include "rpc/thrift-client.h"
#include "rpc/thrift-server.h"
#include "rpc/thrift-thread.h"

#include "common/names.h"

DEFINE_int32(port, 22222, "Port for NetworkTestService");
DEFINE_int64(send_batch_size, 0, "Batch size (in bytes).  Data is split up into batches");

// Simple client server network speed benchmark utility.  This compiles to
// a binary that runs as both the client and server.  The server can be started
// up by just running the binary.  After the server starts up, it will drop into
// the client 'shell' where benchmarks can be run.
// The supported benchmarks are:
//   'send <size in mb> <target ip>'
//   'broadcast <size in mb> <list of space separated target ips>
// The command can also be passed in via command line in the same format.  If
// run in this mode, the server does not start up.
// For broadcast, the data is sent in parallel to all nodes.
//
// The expected usage for measuring 'send' is to start up the server on one machine
// and issue the send from another.
// For 'broadcast', the server should be started on all the machines and then the
// broadcast is issued from one of them.

using boost::algorithm::is_any_of;
using boost::algorithm::token_compress_on;
using boost::algorithm::split;

using namespace apache::thrift;
using namespace apache::thrift::protocol;
using namespace apache::thrift::transport;
using namespace apache::thrift::server;
using namespace apache::thrift::concurrency;
using namespace impala;
using namespace impalatest;


class TestServer : public NetworkTestServiceIf {
 public:
  TestServer() {
  }

  virtual ~TestServer() {
  }

  virtual void Send(ThriftDataResult& result, const ThriftDataParams& params) {
    result.__set_bytes_received(params.data.size());
  }

  void Server(ThriftServer* server) {
    ABORT_IF_ERROR(server->Start());
    server->Join();
  }
};

// Send bytes to client respecting the batch size
// Returns the rate in mb/s to send the data.
double Send(ThriftClient<NetworkTestServiceClient>* client, int64_t bytes) {
  int64_t batch_size = FLAGS_send_batch_size;
  if (batch_size == 0) batch_size = bytes;
  int64_t total_sent = 0;

  MonotonicStopWatch timer;
  timer.Start();
  while (total_sent < bytes) {
    int64_t send_size = min(bytes - total_sent, batch_size);
    total_sent += send_size;

    ThriftDataParams data;
    ThriftDataResult result;
    data.data.resize(send_size);
    client->iface()->Send(result, data);

    if (result.bytes_received != send_size) {
      return -1;
    }
  }
  timer.Stop();

  double mb = bytes / (1024. * 1024.);
  double sec = timer.ElapsedTime() / (1000.) / (1000.) / (1000.);
  return mb/sec;
}

// Send tokens[1] megabytes to tokens[2]
void HandleSend(const vector<string>& tokens) {
  if (tokens.size() != 3) {
    return;
  }

  int64_t mbs = atoi(tokens[1].c_str());
  int64_t bytes = mbs * (1024L * 1024L);
  cout << "Sending " << mbs << " megabytes..." << endl;
  const string& ip = tokens[2];

  ThriftClient<NetworkTestServiceClient> client(ip, FLAGS_port);
  Status status = client.Open();
  if (!status.ok()) {
    cerr << "Could not connect to server" << endl;
    return;
  }

  double rate = Send(&client, bytes);
  if (rate < 0) {
    cerr << "Send failed";
    return;
  }
  cout << "Send rate: (MB/s): " << rate << endl;
}

// Broadcast tokens[1] megabytes to tokens[2...n] nodes in parallel.
void HandleBroadcast(const vector<string>& tokens) {
  if (tokens.size() <= 2) {
    return;
  }
  int64_t mbs = atoi(tokens[1].c_str());
  int64_t bytes = mbs * (1024L * 1024L);
  cout << "Broadcasting " << mbs << " megabytes..." << endl;

  vector<ThriftClient<NetworkTestServiceClient>* > clients;
  for (int i = 2; i < tokens.size(); ++i) {
    ThriftClient<NetworkTestServiceClient>* client =
        new ThriftClient<NetworkTestServiceClient>(tokens[i], FLAGS_port);
    Status status = client->Open();
    if (!status.ok()) {
      cerr << "Could not connect to server: " << tokens[i] << endl;
      return;
    }
    clients.push_back(client);
  }

  MonotonicStopWatch timer;
  timer.Start();
  thread_group threads;
  for (int i = 0; i < clients.size(); ++i) {
    threads.add_thread(new thread(Send, clients[i], bytes));
  }
  threads.join_all();
  timer.Stop();

  double mb = bytes / (1024 * 1024.);
  double sec = timer.ElapsedTime() / (1000.) / (1000.) / (1000.);

  cout << "Send rate per node: (MB/s) " << (mb/sec) << endl;
  cout << "Send rate cluster: (MB/s) " << (mb * clients.size() / sec) << endl;
}

void ConvertToLowerCase(vector<string>* tokens) {
  for (int i = 0; i < tokens->size(); ++i) {
    transform(
        (*tokens)[i].begin(), (*tokens)[i].end(), (*tokens)[i].begin(), ::tolower);
  }
}

bool ProcessCommand(const vector<string>& tokens) {
  if (tokens.empty()) return false;

  if (tokens[0] == "quit") return true;;

  if (tokens[0] == "send") {
    HandleSend(tokens);
  } else if (tokens[0] == "broadcast") {
    HandleBroadcast(tokens);
  } else {
    cerr << "Invalid command" << endl;
    return false;
  }
  return false;
}

int main(int argc, char** argv) {
  google::ParseCommandLineFlags(&argc, &argv, true);
  impala::InitCommonRuntime(argc, argv, false, impala::TestInfo::BE_TEST);

  if (argc != 1) {
    // Just run client from command line args
    vector<string> tokens;
    for (int i = 1; i < argc; ++i) {
      tokens.push_back(argv[i]);
    }
    ConvertToLowerCase(&tokens);
    ProcessCommand(tokens);
    return 0;
  }

  // Start up server and client shell
  std::shared_ptr<TestServer> handler(new TestServer);
  std::shared_ptr<ThreadFactory> thread_factory(
      new ThriftThreadFactory("test", "test"));
  std::shared_ptr<TProcessor> processor(new NetworkTestServiceProcessor(handler));
  ThriftServer* server;
  ABORT_IF_ERROR(ThriftServerBuilder("Network Test Server", processor, FLAGS_port)
                     .max_concurrent_connections(100)
                     .Build(&server));
  thread* server_thread = new thread(&TestServer::Server, handler.get(), server);

  string input;
  while (1) {
    vector<string> tokens;
    cout << "> ";
    cout.flush();

    getline(cin, input);
    if (cin.eof()) break;

    split(tokens, input, is_any_of(" "), token_compress_on);

    ConvertToLowerCase(&tokens);
    if (ProcessCommand(tokens)) break;
  }

  server->StopForTesting();
  server_thread->join();

  return 0;
}
