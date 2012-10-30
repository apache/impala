// Copyright 2012 Cloudera Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include <protocol/TBinaryProtocol.h>
#include <protocol/TDebugProtocol.h>
#include <transport/TSocket.h>
#include <transport/TTransportUtils.h>

#include "gen-cpp/ImpalaPlanService.h"
#include "gen-cpp/ImpalaPlanService_types.h"
#include "gen-cpp/ImpalaInternalService.h"
#include "gen-cpp/ImpalaService.h"
#include "gen-cpp/Data_types.h"

#include "common/logging.h"
#include "util/cpu-info.h"
#include "util/thrift-client.h"

#include <iostream>

DEFINE_bool(impalad, false, "Refresh via impalad instead of planservice");
DECLARE_string(planservice_host);
DECLARE_int32(planservice_port);

using namespace apache::thrift;
using namespace apache::thrift::protocol;
using namespace apache::thrift::transport;
using namespace boost;
using namespace impala;
using namespace std;

// Simple utility to force a planservice or impalad frontend to reload its catalog
int main(int argc, char** argv) {
  google::ParseCommandLineFlags(&argc, &argv, true);
  CpuInfo::Init();

  cout << "Connecting to " << FLAGS_planservice_host << ":"
       << FLAGS_planservice_port << endl;

  if (FLAGS_impalad) {
    ThriftClient<ImpalaServiceClient> client(FLAGS_planservice_host,
        FLAGS_planservice_port);
    EXIT_IF_ERROR(client.Open());
    cout << "Connected. Refreshing metadata." << endl;
    TStatus status;
    client.iface()->ResetCatalog(status);
    cout << "Done." << endl;
  } else {
    shared_ptr<TSocket> socket(
        new TSocket(FLAGS_planservice_host, FLAGS_planservice_port));
    shared_ptr<TBufferedTransport> transport(new TBufferedTransport(socket));
    shared_ptr<TBinaryProtocol> protocol(new TBinaryProtocol(transport));

    try {
      transport->open();
    } catch (TTransportException& e) {
      cout << "Could not open connection to planservice: " << e.what() << endl;
      exit(1);
    }
    cout << "Connected. Refreshing metadata." << endl;

    ImpalaPlanServiceClient client(protocol);

    client.RefreshMetadata();
    transport->close();
    cout << "Done." << endl;
  }
  return 0;
}
