// Copyright (c) 2012 Cloudera, Inc. All rights reserved.
//
// This file contains the main() function for the state store process,
// which exports the Thrift service StateStoreService.

#include <glog/logging.h>
#include <gflags/gflags.h>
#include <iostream>

#include "common/status.h"
#include "sparrow/state-store-service.h"
#include "util/cpu-info.h"
#include "util/metrics.h"
#include "util/webserver.h"
#include "util/logging.h"
#include "util/default-path-handlers.h"

DECLARE_int32(state_store_port);
DECLARE_int32(webserver_port);
DECLARE_bool(enable_webserver);

using impala::Webserver;
using impala::Status;
using impala::Metrics;
using namespace std;
using namespace boost;

int main(int argc, char** argv) {
  // Override default for webserver port
  FLAGS_webserver_port = 9190;
  google::ParseCommandLineFlags(&argc, &argv, true);
  impala::InitGoogleLoggingSafe(argv[0]);
  impala::CpuInfo::Init();

  scoped_ptr<Webserver> webserver(new Webserver());

  if (FLAGS_enable_webserver) {
    impala::AddDefaultPathHandlers(webserver.get());
    EXIT_IF_ERROR(webserver->Start());
  } else {
    LOG(INFO) << "Not starting webserver";
  }

  scoped_ptr<Metrics> metrics(new Metrics());
  metrics->Init(FLAGS_enable_webserver ? webserver.get() : NULL);

  shared_ptr<sparrow::StateStore> state_store(
      new sparrow::StateStore(sparrow::StateStore::DEFAULT_UPDATE_FREQUENCY_MS, 
          metrics.get()));
  state_store->Start(FLAGS_state_store_port);
  state_store->WaitForServerToStop();
}
