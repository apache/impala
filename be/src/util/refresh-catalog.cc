// Copyright (c) 2012 Cloudera, Inc. All rights reserved.

#include <protocol/TBinaryProtocol.h>
#include <protocol/TDebugProtocol.h>
#include <transport/TSocket.h>
#include <transport/TTransportUtils.h>
#include <gflags/gflags.h>

#include "gen-cpp/ImpalaPlanService.h"
#include "gen-cpp/ImpalaPlanService_types.h"
#include "gen-cpp/ImpalaBackendService.h"
#include "gen-cpp/ImpalaService.h"
#include "gen-cpp/Data_types.h"

#include <iostream>

DEFINE_string(host, "localhost", "Hostname of planservice");
DEFINE_int32(port, 20000, "Port number of planservice");
DEFINE_bool(impalad, false, "Refresh via impalad instead of planservice");

using namespace apache::thrift;
using namespace apache::thrift::protocol;
using namespace apache::thrift::transport;
using namespace boost;
using namespace impala;
using namespace std;

// Simple utility to force a planservice or impalad frontend to reload its catalog
int main(int argc, char** argv) {
  google::ParseCommandLineFlags(&argc, &argv, true);

  shared_ptr<TSocket> socket(new TSocket(FLAGS_host, FLAGS_port));
  shared_ptr<TBufferedTransport> transport(new TBufferedTransport(socket));
  shared_ptr<TBinaryProtocol> protocol(new TBinaryProtocol(transport));
  cout << "Connecting to " << FLAGS_host << ":" << FLAGS_port << endl;

  try {
    transport->open();
  } catch (TTransportException& e) {
    cout << "Could not open connection to impalad / planservice: " << e.what() << endl;
    exit(1);
  }
  cout << "Connected. Refreshing metadata." << endl;

  if (FLAGS_impalad) {
    ImpalaServiceClient client(protocol);

    TStatus status;
    client.ResetCatalog(status);
    transport->close();
    cout << "Done." << endl;
  } else {
    ImpalaPlanServiceClient client(protocol);

    client.RefreshMetadata();
    transport->close();
    cout << "Done." << endl;
  }
  return 0;
}
