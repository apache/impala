// Copyright (c) 2012 Cloudera, Inc. All rights reserved.
//
// This file contains the main() function for the state store process,
// which exports the Thrift service StateStoreService.

#include <glog/logging.h>
#include <gflags/gflags.h>

#include "sparrow/state-store-service.h"
#include "util/cpu-info.h"

DEFINE_int32(state_store_port, 24000, "port where StateStoreService is exported");

int main(int argc, char** argv) {
  google::InitGoogleLogging(argv[0]);
  google::ParseCommandLineFlags(&argc, &argv, true);
  impala::CpuInfo::Init();

  boost::shared_ptr<sparrow::StateStore> state_store(new sparrow::StateStore());
  state_store->Start(FLAGS_state_store_port);
  state_store->WaitForServerToStop();
}
