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

#include "scheduling/admissiond-env.h"

#include "common/daemon-env.h"
#include "rpc/rpc-mgr.h"
#include "runtime/mem-tracker.h"
#include "scheduling/admission-control-service.h"
#include "scheduling/admission-controller.h"
#include "scheduling/cluster-membership-mgr.h"
#include "scheduling/scheduler.h"
#include "service/impala-http-handler.h"
#include "util/default-path-handlers.h"
#include "util/mem-info.h"
#include "util/memory-metrics.h"
#include "util/metrics.h"
#include "util/uid-util.h"

#include "common/names.h"

DEFINE_int32(
    admission_service_port, 29500, "The port where the admission control service runs");

DECLARE_string(state_store_host);
DECLARE_int32(state_store_port);
DECLARE_string(state_store_2_host);
DECLARE_int32(state_store_2_port);
DECLARE_int32(state_store_subscriber_port);
DECLARE_string(hostname);

namespace impala {

AdmissiondEnv* AdmissiondEnv::admissiond_env_ = nullptr;

AdmissiondEnv::AdmissiondEnv()
  : pool_mem_trackers_(new PoolMemTrackerRegistry),
    request_pool_service_(new RequestPoolService(DaemonEnv::GetInstance()->metrics())),
    rpc_mgr_(new RpcMgr(IsInternalTlsConfigured())),
    rpc_metrics_(DaemonEnv::GetInstance()->metrics()->GetOrCreateChildGroup("rpc")) {
  MetricGroup* metrics = DaemonEnv::GetInstance()->metrics();

  TNetworkAddress admission_service_addr =
      MakeNetworkAddress(FLAGS_hostname, FLAGS_admission_service_port);
  TNetworkAddress subscriber_address =
      MakeNetworkAddress(FLAGS_hostname, FLAGS_state_store_subscriber_port);
  TNetworkAddress statestore_address =
      MakeNetworkAddress(FLAGS_state_store_host, FLAGS_state_store_port);
  TNetworkAddress statestore2_address =
      MakeNetworkAddress(FLAGS_state_store_2_host, FLAGS_state_store_2_port);
  statestore_subscriber_.reset(new StatestoreSubscriber(
      Substitute("admissiond@$0", TNetworkAddressToString(admission_service_addr)),
      subscriber_address, statestore_address, statestore2_address, metrics,
      TStatestoreSubscriberType::ADMISSIOND));

  scheduler_.reset(new Scheduler(metrics, request_pool_service()));
  cluster_membership_mgr_.reset(new ClusterMembershipMgr(
      PrintId(DaemonEnv::GetInstance()->backend_id()), subscriber(), metrics));
  admission_controller_.reset(new AdmissionController(cluster_membership_mgr(),
      subscriber(), request_pool_service(), metrics, scheduler(), pool_mem_trackers(),
      admission_service_addr));
  http_handler_.reset(ImpalaHttpHandler::CreateAdmissiondHandler(
      admission_controller_.get(), cluster_membership_mgr_.get()));

  admissiond_env_ = this;
}

AdmissiondEnv::~AdmissiondEnv() {
  if (rpc_mgr_ != nullptr) rpc_mgr_->Shutdown();
}

Status AdmissiondEnv::Init() {
  int64_t bytes_limit;
  RETURN_IF_ERROR(ChooseProcessMemLimit(&bytes_limit));
  mem_tracker_.reset(
      new MemTracker(AggregateMemoryMetrics::TOTAL_USED, bytes_limit, "Process"));
  mem_tracker_->RegisterMetrics(
      DaemonEnv::GetInstance()->metrics(), "mem-tracker.process");

  http_handler_->RegisterHandlers(DaemonEnv::GetInstance()->webserver());
  if (DaemonEnv::GetInstance()->metrics_webserver() != nullptr) {
    http_handler_->RegisterHandlers(
        DaemonEnv::GetInstance()->metrics_webserver(), /* metrics_only */ true);
  }

  IpAddr ip_address;
  RETURN_IF_ERROR(HostnameToIpAddr(FLAGS_hostname, &ip_address));
  // TODO: advertise BackendId of admissiond to coordinators via heartbeats.
  // Use admissiond's IP address as unique ID for UDS now.
  krpc_address_ = MakeNetworkAddressPB(
      ip_address, FLAGS_admission_service_port, UdsAddressUniqueIdPB::IP_ADDRESS);
  RETURN_IF_ERROR(rpc_mgr_->Init(krpc_address_));
  admission_control_svc_.reset(
      new AdmissionControlService(DaemonEnv::GetInstance()->metrics()));
  RETURN_IF_ERROR(admission_control_svc_->Init());

  RETURN_IF_ERROR(cluster_membership_mgr_->Init());
  cluster_membership_mgr_->RegisterUpdateCallbackFn(
      [&](ClusterMembershipMgr::SnapshotPtr snapshot) {
        std::unordered_set<BackendIdPB> current_backends;
        for (const auto& it : snapshot->current_backends) {
          current_backends.insert(it.second.backend_id());
        }
        admission_control_svc_->CancelQueriesOnFailedCoordinators(current_backends);
      });
  RETURN_IF_ERROR(admission_controller_->Init());
  return Status::OK();
}

Status AdmissiondEnv::StartServices() {
  LOG(INFO) << "Starting statestore subscriber service";
  // Must happen after all topic registrations / callbacks are done
  if (statestore_subscriber_.get() != nullptr) {
    Status status = statestore_subscriber_->Start();
    if (!status.ok()) {
      status.AddDetail("Statestore subscriber did not start up.");
      return status;
    }
  }

  LOG(INFO) << "Starting KRPC service.";
  RETURN_IF_ERROR(rpc_mgr_->StartServices());

  // Mark service as started.
  // Should be called only after the statestore subscriber service and KRPC service
  // has started.
  admission_control_svc_->service_started_ = true;
  return Status::OK();
}

} // namespace impala
