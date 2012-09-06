// Copyright (c) 2011 Cloudera, Inc. All rights reserved.

#include "testutil/test-exec-env.h"

#include <glog/logging.h>
#include <gflags/gflags.h>
#include <boost/thread/thread.hpp>

#include "common/status.h"
#include "common/service-ids.h"
#include "service/impala-server.h"
#include "runtime/client-cache.h"
#include "runtime/data-stream-mgr.h"
#include "runtime/hdfs-fs-cache.h"
#include "sparrow/simple-scheduler.h"
#include "sparrow/state-store-subscriber-service.h"
#include "util/metrics.h"
#include "util/thrift-server.h"
#include "gen-cpp/ImpalaInternalService.h"

using namespace boost;
using namespace std;
using namespace apache::thrift::server;
using sparrow::SimpleScheduler;
using sparrow::SubscriptionManager;
using sparrow::StateStore;

namespace impala {

// ExecEnv for slave backends run as part of a test environment, with webserver disabled,
// no scheduler (since coordinator takes care of that) and a state store subscriber.
class BackendTestExecEnv : public ExecEnv {
 public:
  BackendTestExecEnv(int subscriber_port, int state_store_port);

  virtual Status StartServices();
};


struct TestExecEnv::BackendInfo {
  ThriftServer* server;
  BackendTestExecEnv exec_env;

  BackendInfo(int subscriber_port, int state_store_port)
      : server(NULL),
        exec_env(subscriber_port, state_store_port) {
  }
};


BackendTestExecEnv::BackendTestExecEnv(int subscriber_port, int state_store_port)
  : ExecEnv() {
  subscription_mgr_.reset(new SubscriptionManager("localhost", subscriber_port,
      "localhost", state_store_port));
  scheduler_.reset(NULL);
}

Status BackendTestExecEnv::StartServices() {
  // Don't start the scheduler, or the webserver
  RETURN_IF_ERROR(subscription_mgr_->Start());
  return Status::OK;
}

TestExecEnv::TestExecEnv(int num_backends, int start_port)
  : num_backends_(num_backends),
    start_port_(start_port),
    state_store_(new StateStore(500)) {
}

TestExecEnv::~TestExecEnv() {
  for (int i = 0; i < backend_info_.size(); ++i) {
    BackendInfo* info = backend_info_[i];
    // TODO: auto_ptr?
    delete info;
  }
}

Status TestExecEnv::StartBackends() {
  LOG(INFO) << "Starting " << num_backends_ << " backends";
  int next_free_port = start_port_;
  state_store_port_ = next_free_port++;
  LOG(INFO) << "Starting in-process state-store";
  state_store_->Start(state_store_port_);
  RETURN_IF_ERROR(WaitForServer("localhost", state_store_port_, 10, 100));

  for (int i = 0; i < num_backends_; ++i) {
    BackendInfo* info = new BackendInfo(next_free_port++, state_store_port_);
    int backend_port = next_free_port++;
    CreateImpalaServer(&info->exec_env, 0, backend_port, NULL, &info->server);
    DCHECK(info->server != NULL);
    backend_info_.push_back(info);
    info->exec_env.StartServices();
    info->server->Start();
    THostPort address;
    address.host = "localhost";
    address.port = backend_port;
    RETURN_IF_ERROR(
        info->exec_env.subscription_mgr()->RegisterService(
          IMPALA_SERVICE_ID, address));
  }

  // Coordinator exec env gets both a subscription manager and a scheduler.
  subscription_mgr_.reset(new SubscriptionManager("localhost", next_free_port++,
      "localhost", state_store_port_));
  scheduler_.reset(new SimpleScheduler(subscription_mgr_.get(), IMPALA_SERVICE_ID, NULL));
  subscription_mgr_->Start();
  scheduler_->Init();

  // Wait until we see all the backends registered, or timeout if 5s pass
  vector<pair<string, int> > host_ports;
  const int NUM_RETRIES = 100;
  const int POLL_INTERVAL_MS = 50;

  for (int i = 1; i <= NUM_RETRIES; ++i) {
    scheduler_->GetAllKnownHosts(&host_ports);

    if (host_ports.size() == num_backends_) {
      VLOG(1) << "Complete set of backends observed in under " 
              << i * POLL_INTERVAL_MS << "ms";
      break;
    } else if (i == NUM_RETRIES) {
      stringstream error_msg;
      error_msg << "Failed to see " << num_backends_
                << " backends, last membership size observed was: " << host_ports.size();
      return Status(error_msg.str());
    }
    usleep(POLL_INTERVAL_MS * 1000);
  };

  return Status::OK;
}

string TestExecEnv::DebugString() {
  return client_cache_->DebugString();
}

}
