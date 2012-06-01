// Copyright (c) 2011 Cloudera, Inc. All rights reserved.

#include "testutil/test-exec-env.h"

#include <server/TServer.h>
#include <glog/logging.h>
#include <gflags/gflags.h>
#include <boost/thread/thread.hpp>

#include "common/status.h"
#include "service/backend-service.h"
#include "runtime/client-cache.h"
#include "runtime/data-stream-mgr.h"
#include "runtime/hdfs-fs-cache.h"
#include "scheduler/scheduler.h"
#include "gen-cpp/ImpalaBackendService.h"

using namespace boost;
using namespace std;
using namespace apache::thrift::server;

namespace impala {

struct TestExecEnv::BackendInfo {
  thread backend_thread;
  TServer* server;
  ExecEnv exec_env;

  BackendInfo(HdfsFsCache* fs_cache) : server(NULL), exec_env(fs_cache) {}
};

class TestExecEnv::TestScheduler : public Scheduler {
 public:
  TestScheduler(TestExecEnv* parent): parent_(parent) {}

  virtual Status GetHosts(
      const vector<string>& data_locations,
      vector<pair<string, int> >* hostports); 
 
 private:
  TestExecEnv* parent_;
};

Status TestExecEnv::TestScheduler::GetHosts(
    const vector<string>& data_locations, vector<pair<std::string, int> >* hostports) {
  hostports->clear();
  for (int i = 0; i < data_locations.size(); ++i) {
    hostports->push_back(make_pair(
        "localhost", parent_->start_port_ + (i % parent_->num_backends_)));
  }
  return Status::OK;
}

TestExecEnv::TestExecEnv(int num_backends, int start_port)
  : num_backends_(num_backends),
    start_port_(start_port),
    test_scheduler_(new TestScheduler(this)) {
  scheduler_ = test_scheduler_.get();
}

TestExecEnv::~TestExecEnv() {
  for (int i = 0; i < backend_info_.size(); ++i) {
    BackendInfo* info = backend_info_[i];
    // for some reason, this doesn't stop the thrift service loop anymore,
    // so that the subsequent join() hangs
    // TODO: investigate and fix
    info->server->stop();
    //info->backend_thread.join();
    // TODO: auto_ptr?
    delete info;
  }
}

Status TestExecEnv::StartBackends() {
  LOG(INFO) << "starting " << num_backends_ << " backends";
  int port = start_port_;
  for (int i = 0; i < num_backends_; ++i) {
    BackendInfo* info = new BackendInfo(fs_cache_);
    info->server = StartImpalaBackendService(&info->exec_env, port++);
    DCHECK(info->server != NULL);
    info->backend_thread = thread(&TestExecEnv::RunBackendServer, this, info->server);
    backend_info_.push_back(info);
  }
  return Status::OK;
}

void TestExecEnv::RunBackendServer(TServer* server) {
  VLOG(1) << "serve()";
  server->serve();
  VLOG(1) << "exiting service loop";
}

string TestExecEnv::DebugString() {
  return client_cache_->DebugString();
}

}
