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
#include "util/gflag-validator-util.h"
#include "util/mem-info.h"
#include "util/memory-metrics.h"
#include "util/metrics.h"
#include "util/pretty-printer.h"
#include "util/uid-util.h"

#include "common/names.h"

DEFINE_int32(
    admission_service_port, 29500, "The port where the admission control service runs");
DEFINE_int32(cluster_membership_retained_removed_coords, 1000,
    "Max number of removed coordinators to track. Oldest entry is evicted when full.");
DEFINE_validator(
    cluster_membership_retained_removed_coords, [](const char* name, const int val) {
      if (val > 0 && val <= 1000) return true;
      LOG(ERROR) << "Flag '" << name
                 << "' must be greater than 0 and less than or equal to 1000.";
      return false;
    });
DEFINE_bool(enable_admission_service_mem_safeguard, true,
    "When true, enables a hard memory limit safeguard for the admission service. "
    "This rejects new queries if the in-use process memory from tcmalloc exceeds "
    "admission_service_mem_limit to prevent OOM.");

DECLARE_string(state_store_host);
DECLARE_int32(state_store_port);
DECLARE_string(state_store_2_host);
DECLARE_int32(state_store_2_port);
DECLARE_int32(state_store_subscriber_port);
DECLARE_string(hostname);
DECLARE_string(cluster_membership_topic_id);

namespace impala {

AdmissiondEnv* AdmissiondEnv::admissiond_env_ = nullptr;

AdmissiondEnv::AdmissiondEnv()
  : pool_mem_trackers_(new PoolMemTrackerRegistry),
    request_pool_service_(
        new RequestPoolService(DaemonEnv::GetInstance()->metrics(), true)),
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
  string subscriber_id = Substitute("admissiond@$0",
      TNetworkAddressToString(admission_service_addr));
  if (!FLAGS_cluster_membership_topic_id.empty()) {
    subscriber_id = FLAGS_cluster_membership_topic_id + '-' + subscriber_id;
  }
  statestore_subscriber_.reset(new StatestoreSubscriber(subscriber_id, subscriber_address,
      statestore_address, statestore2_address, metrics,
      TStatestoreSubscriberType::ADMISSIOND));

  scheduler_.reset(new Scheduler(metrics, request_pool_service()));
  cluster_membership_mgr_.reset(
      new ClusterMembershipMgr(PrintId(DaemonEnv::GetInstance()->backend_id()),
          subscriber(), metrics, true /* is_admissiond */));
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
  if (FLAGS_enable_admission_service_mem_safeguard) {
    admission_mem_limit_ = bytes_limit;
    LOG(INFO) << "Set admission service memory limit to "
              << PrettyPrinter::Print(admission_mem_limit_, TUnit::BYTES);
  }

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
      [&](const ClusterMembershipMgr::SnapshotPtr& snapshot) {
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
