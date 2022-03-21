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

#pragma once

#include "common/global-types.h"
#include "common/status.h"
#include "gen-cpp/common.pb.h"
#include "util/metrics.h"
#include "util/network-util.h"

namespace impala {

class AdmissionController;
class AdmissionControlService;
class ClusterMembershipMgr;
class ImpalaHttpHandler;
class MemTracker;
class MetricGroup;
class PoolMemTrackerRegistry;
class RequestPoolService;
class RpcMgr;
class Scheduler;
class StatestoreSubscriber;

/// Contains and initializes singleton objects needed for the admisison control service,
/// analogous to ExecEnv for impalads.
class AdmissiondEnv {
 public:
  static AdmissiondEnv* GetInstance() { return admissiond_env_; }

  AdmissiondEnv();
  ~AdmissiondEnv();

  Status Init() WARN_UNUSED_RESULT;
  Status StartServices() WARN_UNUSED_RESULT;

  AdmissionController* admission_controller() { return admission_controller_.get(); }
  AdmissionControlService* admission_control_service() {
    return admission_control_svc_.get();
  }
  ClusterMembershipMgr* cluster_membership_mgr() { return cluster_membership_mgr_.get(); }
  MemTracker* process_mem_tracker() { return mem_tracker_.get(); }
  MetricGroup* rpc_metrics() { return rpc_metrics_; }
  PoolMemTrackerRegistry* pool_mem_trackers() { return pool_mem_trackers_.get(); }
  RequestPoolService* request_pool_service() { return request_pool_service_.get(); }
  RpcMgr* rpc_mgr() { return rpc_mgr_.get(); }
  Scheduler* scheduler() { return scheduler_.get(); }
  StatestoreSubscriber* subscriber() { return statestore_subscriber_.get(); }

 private:
  static AdmissiondEnv* admissiond_env_;

  /// Address of the KRPC backend service: ip_address + krpc_port and UDS address.
  NetworkAddressPB krpc_address_;

  std::unique_ptr<AdmissionController> admission_controller_;
  std::unique_ptr<AdmissionControlService> admission_control_svc_;
  std::unique_ptr<ClusterMembershipMgr> cluster_membership_mgr_;
  std::unique_ptr<ImpalaHttpHandler> http_handler_;
  std::unique_ptr<MemTracker> mem_tracker_;
  std::unique_ptr<PoolMemTrackerRegistry> pool_mem_trackers_;
  std::unique_ptr<RequestPoolService> request_pool_service_;
  std::unique_ptr<RpcMgr> rpc_mgr_;
  std::unique_ptr<Scheduler> scheduler_;
  std::unique_ptr<StatestoreSubscriber> statestore_subscriber_;

  MetricGroup* rpc_metrics_ = nullptr;
};

} // namespace impala
