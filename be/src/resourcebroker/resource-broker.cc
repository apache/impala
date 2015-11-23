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

#include "resourcebroker/resource-broker.h"

#include <boost/algorithm/string/join.hpp>
#include <boost/foreach.hpp>
#include <boost/uuid/uuid.hpp>
#include <boost/uuid/uuid_generators.hpp>
#include <boost/uuid/uuid_io.hpp>
#include <boost/lexical_cast.hpp>
#include <gutil/strings/substitute.h>
#include <thrift/Thrift.h>

#include "common/status.h"
#include "rpc/thrift-util.h"
#include "rpc/thrift-server.h"
#include "scheduling/query-resource-mgr.h"
#include "scheduling/scheduler.h"
#include "util/debug-util.h"
#include "util/uid-util.h"
#include "util/network-util.h"
#include "util/llama-util.h"
#include "util/time.h"
#include "gen-cpp/ResourceBrokerService.h"
#include "gen-cpp/Llama_types.h"

#include "common/names.h"

using boost::algorithm::join;
using boost::algorithm::to_lower;
using boost::uuids::random_generator;
using namespace ::apache::thrift::server;
using namespace ::apache::thrift;
using namespace impala;
using namespace strings;

DECLARE_int64(llama_registration_timeout_secs);
DECLARE_int64(llama_registration_wait_secs);
DECLARE_int64(llama_max_request_attempts);

DECLARE_int32(resource_broker_cnxn_attempts);
DECLARE_int32(resource_broker_cnxn_retry_interval_ms);
DECLARE_int32(resource_broker_send_timeout);
DECLARE_int32(resource_broker_recv_timeout);

static const string LLAMA_KERBEROS_SERVICE_NAME = "llama";

namespace impala {

// String to search for in Llama error messages to detect that Llama has restarted,
// and hence the resource broker must re-register.
const string LLAMA_RESTART_SEARCH_STRING = "unknown handle";

class LlamaNotificationThriftIf : public llama::LlamaNotificationServiceIf {
 public:
  LlamaNotificationThriftIf(ResourceBroker* resource_broker)
    : resource_broker_(resource_broker) {}

  virtual void AMNotification(llama::TLlamaAMNotificationResponse& response,
      const llama::TLlamaAMNotificationRequest& request) {
    resource_broker_->AMNotification(request, response);
  }

  virtual void NMNotification(llama::TLlamaNMNotificationResponse& response,
      const llama::TLlamaNMNotificationRequest& request) {
    LOG(WARNING) << "Ignoring node-manager notification. Handling not yet implemented.";
    response.status.__set_status_code(llama::TStatusCode::OK);
  }

  virtual ~LlamaNotificationThriftIf() {}

 private:
  ResourceBroker* resource_broker_;
};

ResourceBroker::ResourceBroker(const vector<TNetworkAddress>& llama_addresses,
    const TNetworkAddress& llama_callback_address, MetricGroup* metrics) :
    llama_addresses_(llama_addresses),
    active_llama_addr_idx_(-1),
    llama_callback_address_(llama_callback_address),
    metrics_(metrics),
    scheduler_(NULL),
    llama_callback_thrift_iface_(new LlamaNotificationThriftIf(this)),
    llama_client_cache_(new ClientCache<llama::LlamaAMServiceClient>(
        FLAGS_resource_broker_cnxn_attempts,
        FLAGS_resource_broker_cnxn_retry_interval_ms,
        FLAGS_resource_broker_send_timeout,
        FLAGS_resource_broker_recv_timeout,
        LLAMA_KERBEROS_SERVICE_NAME)) {
  DCHECK(metrics != NULL);
  llama_client_cache_->InitMetrics(metrics, "resource-broker");
  active_llama_metric_ = metrics->AddProperty<string>(
      "resource-broker.active-llama", "none");
  active_llama_handle_metric_ = metrics->AddProperty<string>(
      "resource-broker.active-llama-handle", "none");

  reservation_rpc_time_metric_ = StatsMetric<double>::CreateAndRegister(metrics,
      "resource-broker.reservation-request-rpc-time");
  reservation_response_time_metric_ = StatsMetric<double>::CreateAndRegister(metrics,
      "resource-broker.reservation-request-response-time");
  reservation_requests_total_metric_ = metrics->AddCounter<int64_t>(
      "resource-broker.reservation-requests-total", 0);
  reservation_requests_fulfilled_metric_ = metrics->AddCounter<int64_t>(
      "resource-broker.reservation-requests-fulfilled", 0);
  reservation_requests_failed_metric_ = metrics->AddCounter<int64_t>(
      "resource-broker.reservation-requests-failed", 0);
  reservation_requests_rejected_metric_ = metrics->AddCounter<int64_t>(
      "resource-broker.reservation-requests-rejected", 0);
  reservation_requests_timedout_metric_ = metrics->AddCounter<int64_t>(
      "resource-broker.reservation-requests-timedout", 0);

  expansion_rpc_time_metric_ = StatsMetric<double>::CreateAndRegister(metrics,
      "resource-broker.expansion-request-rpc-time");
  expansion_response_time_metric_ = StatsMetric<double>::CreateAndRegister(metrics,
      "resource-broker.expansion-request-response-time");
  expansion_requests_total_metric_ = metrics->AddCounter<int64_t>(
      "resource-broker.expansion-requests-total", 0);
  expansion_requests_fulfilled_metric_ = metrics->AddCounter<int64_t>(
      "resource-broker.expansion-requests-fulfilled", 0);
  expansion_requests_failed_metric_ = metrics->AddCounter<int64_t>(
      "resource-broker.expansion-requests-failed", 0);
  expansion_requests_rejected_metric_ = metrics->AddCounter<int64_t>(
      "resource-broker.expansion-requests-rejected", 0);
  expansion_requests_timedout_metric_ = metrics->AddCounter<int64_t>(
      "resource-broker.expansion-requests-timedout", 0);

  requests_released_metric_ = metrics->AddCounter<int64_t>(
      "resource-broker.requests-released", 0);
  allocated_memory_metric_ = metrics->AddGauge<uint64_t>(
      "resource-broker.memory-resources-in-use", 0L);
  allocated_vcpus_metric_ = metrics->AddGauge<uint64_t>(
      "resource-broker.vcpu-resources-in-use", 0);
}

Status ResourceBroker::Init() {
  // The scheduler must have been set before calling Init().
  DCHECK(scheduler_ != NULL);
  DCHECK(llama_callback_thrift_iface_ != NULL);
  if (llama_addresses_.size() == 0) {
    return Status("No Llama addresses configured (see --llama_addresses)");
  }

  shared_ptr<TProcessor> llama_callback_proc(
      new llama::LlamaNotificationServiceProcessor(llama_callback_thrift_iface_));
  llama_callback_server_.reset(new ThriftServer("llama-callback", llama_callback_proc,
      llama_callback_address_.port, NULL, metrics_, 5));
  RETURN_IF_ERROR(llama_callback_server_->Start());

  // Generate client id for registration with Llama, and register with LLama.
  random_generator uuid_generator;
  llama_client_id_ = uuid_generator();
  RETURN_IF_ERROR(RegisterWithLlama());
  RETURN_IF_ERROR(RefreshLlamaNodes());
  return Status::OK();
}

Status ResourceBroker::RegisterWithLlama() {
  // Remember the current llama_handle_ to detect if another thread has already
  // completed the registration successfully.
  llama::TUniqueId current_llama_handle = llama_handle_;

  // Start time that this thread attempted registration. Used to limit the time that a
  // query will wait for re-registration with the Llama to succeed.
  int64_t start = MonotonicSeconds();
  lock_guard<mutex> l(llama_registration_lock_);
  if (llama_handle_ != current_llama_handle) return Status::OK();

  active_llama_metric_->set_value("none");
  active_llama_handle_metric_->set_value("none");

  int llama_addr_idx = (active_llama_addr_idx_ + 1) % llama_addresses_.size();
  int64_t now = MonotonicSeconds();
  while (FLAGS_llama_registration_timeout_secs == -1 ||
      (now - start) < FLAGS_llama_registration_timeout_secs) {
    // Connect to the Llama at llama_address.
    const TNetworkAddress& llama_address = llama_addresses_[llama_addr_idx];
    // Client status will be ok if a Thrift connection could be successfully established
    // for the returned client at some point in the past. Hence, there is no guarantee
    // that the connection is still valid now and we must check for broken pipes, etc.
    Status client_status;
    ClientConnection<llama::LlamaAMServiceClient> llama_client(llama_client_cache_.get(),
        llama_address, &client_status);
    if (client_status.ok()) {
      // Register this resource broker with Llama.
      llama::TLlamaAMRegisterRequest request;
      request.__set_version(llama::TLlamaServiceVersion::V1);
      llama::TUniqueId llama_uuid;
      UUIDToTUniqueId(llama_client_id_, &llama_uuid);
      request.__set_client_id(llama_uuid);

      llama::TNetworkAddress callback_address;
      callback_address << llama_callback_address_;
      request.__set_notification_callback_service(callback_address);
      llama::TLlamaAMRegisterResponse response;
      LOG(INFO) << "Registering Resource Broker with Llama at " << llama_address;
      Status rpc_status =
          llama_client.DoRpc(&llama::LlamaAMServiceClient::Register, request, &response);
      if (rpc_status.ok()) {
        // TODO: Is there a period where an inactive Llama may respond to RPCs?
        // If so, then we need to keep cycling through Llamas here and not
        // return an error.
        RETURN_IF_ERROR(LlamaStatusToImpalaStatus(
            response.status, "Failed to register Resource Broker with Llama."));
        LOG(INFO) << "Received Llama client handle " << response.am_handle
                  << ((response.am_handle == llama_handle_) ? " (same as old)" : "");
        llama_handle_ = response.am_handle;
        break;
      }
    }
    // Cycle through the list of Llama addresses for Llama failover.
    llama_addr_idx = (llama_addr_idx + 1) % llama_addresses_.size();
    LOG(INFO) << "Failed to connect to Llama at " << llama_address << "." << endl
              << "Error: " << client_status.GetDetail() << endl
              << "Retrying to connect to Llama at "
              << llama_addresses_[llama_addr_idx] << " in "
              << FLAGS_llama_registration_wait_secs << "s.";
    // Sleep to give Llama time to recover/failover before the next attempt.
    SleepForMs(FLAGS_llama_registration_wait_secs * 1000);
    now = MonotonicSeconds();
  }
  DCHECK(FLAGS_llama_registration_timeout_secs != -1);
  if ((now - start) >= FLAGS_llama_registration_timeout_secs) {
    return Status("Failed to (re-)register Resource Broker with Llama.");
  }

  if (llama_addr_idx != active_llama_addr_idx_) {
    // TODO: We've switched to a different Llama (failover). Cancel all queries
    // coordinated by this Impalad to free up physical resources that are not
    // accounted for anymore by Yarn.
  }

  // If we reached this point, (re-)registration was successful.
  active_llama_addr_idx_ = llama_addr_idx;
  active_llama_metric_->set_value(lexical_cast<string>(llama_addresses_[llama_addr_idx]));
  active_llama_handle_metric_->set_value(lexical_cast<string>(llama_handle_));
  return Status::OK();
}

bool ResourceBroker::LlamaHasRestarted(const llama::TStatus& status) const {
  if (status.status_code == llama::TStatusCode::OK || !status.__isset.error_msgs) {
    return false;
  }
  // Check whether one of the error messages contains LLAMA_RESTART_SEARCH_STRING.
  for (int i = 0; i < status.error_msgs.size(); ++i) {
    string error_msg = status.error_msgs[i];
    to_lower(error_msg);
    if (error_msg.find(LLAMA_RESTART_SEARCH_STRING) != string::npos) {
      LOG(INFO) << "Assuming Llama restart from error message: " << status.error_msgs[i];
      return true;
    }
  }
  return false;
}

void ResourceBroker::Close() {
  // Close connections to all Llama addresses, not just the active one.
  BOOST_FOREACH(const TNetworkAddress& llama_address, llama_addresses_) {
    llama_client_cache_->CloseConnections(llama_address);
  }
  llama_callback_server_->Join();
}

void ResourceBroker::CreateLlamaReservationRequest(
    const TResourceBrokerReservationRequest& src,
    llama::TLlamaAMReservationRequest& dest) {
  dest.version = llama::TLlamaServiceVersion::V1;
  dest.am_handle = llama_handle_;
  dest.gang = src.gang;
  // Queue is optional, so must be explicitly set for all versions of Thrift to work
  // together.
  dest.__set_queue(src.queue);
  dest.user = src.user;
  dest.resources = src.resources;
  random_generator uuid_generator;
  llama::TUniqueId request_id;
  UUIDToTUniqueId(uuid_generator(), &request_id);
  dest.__set_reservation_id(request_id);
}

template <class F, typename LlamaReqType, typename LlamaRespType>
Status ResourceBroker::LlamaRpc(const F& f, LlamaReqType* request,
    LlamaRespType* response, StatsMetric<double>* rpc_time_metric) {
  int attempts = 0;
  MonotonicStopWatch sw;
  // Indicates whether to re-register with Llama before the next RPC attempt,
  // e.g. because Llama has restarted or become unavailable.
  bool register_with_llama = false;
  while (attempts < FLAGS_llama_max_request_attempts) {
    if (register_with_llama) {
      RETURN_IF_ERROR(ReRegisterWithLlama(*request, response));
      // Set the new Llama handle received from re-registering.
      request->__set_am_handle(llama_handle_);
      VLOG_RPC << "Retrying Llama RPC after re-registration: " << *request;
      register_with_llama = false;
    }
    ++attempts;
    Status rpc_status;
    ClientConnection<llama::LlamaAMServiceClient> llama_client(llama_client_cache_.get(),
        llama_addresses_[active_llama_addr_idx_], &rpc_status);
    if (!rpc_status.ok()) {
      register_with_llama = true;
      continue;
    }

    sw.Start();
    Status status = llama_client.DoRpc(f, *request, response);
    if (!status.ok()) {
      VLOG_RPC << "Error making Llama RPC: " << status.GetDetail();
      register_with_llama = status.code() == TErrorCode::RPC_CLIENT_CONNECT_FAILURE;
      continue;
    }
    if (rpc_time_metric != NULL) {
      rpc_time_metric->Update(sw.ElapsedTime() / (1000.0 * 1000.0 * 1000.0));
    }

    // Check whether Llama has been restarted. If so, re-register with it.
    // Break out of the loop here upon success of the RPC.
    if (!LlamaHasRestarted(response->status)) break;
    register_with_llama = true;
  }
  if (attempts >= FLAGS_llama_max_request_attempts) {
    return Status(Substitute(
        "Request aborted after $0 attempts due to connectivity issues with Llama.",
        FLAGS_llama_max_request_attempts));
  }
  return Status::OK();
}

template <typename LlamaReqType, typename LlamaRespType>
Status ResourceBroker::ReRegisterWithLlama(const LlamaReqType& request,
    LlamaRespType* response) {
  RETURN_IF_ERROR(RegisterWithLlama());
  return RefreshLlamaNodes();
}

template <>
Status ResourceBroker::ReRegisterWithLlama(const llama::TLlamaAMGetNodesRequest& request,
    llama::TLlamaAMGetNodesResponse* response) {
  return RegisterWithLlama();
}

void ResourceBroker::PendingRequest::GetResources(ResourceMap* resources) {
  resources->clear();
  BOOST_FOREACH(const llama::TAllocatedResource& resource, allocated_resources_) {
    TNetworkAddress host = MakeNetworkAddress(resource.location);
    (*resources)[host] = resource;
    VLOG_QUERY << "Getting allocated resource for reservation id "
               << reservation_id_ << " and location " << host;
  }
}

void ResourceBroker::PendingRequest::SetResources(
    const vector<llama::TAllocatedResource>& resources) {
  // TODO: Llama returns a dump of all resources that we need to manually group by
  // reservation id. Can Llama do the grouping for us?
  BOOST_FOREACH(const llama::TAllocatedResource& resource, resources) {
    // Ignore resources that don't belong to the given reservation id.
    if (resource.reservation_id == request_id()) {
      allocated_resources_.push_back(resource);
    }
  }
}

bool ResourceBroker::WaitForNotification(int64_t timeout, ResourceMap* resources,
    bool* timed_out, PendingRequest* pending_request) {
  bool request_granted;
  if (timeout <= 0) {
    *timed_out = false;
    request_granted = pending_request->promise()->Get();
  } else {
    request_granted = pending_request->promise()->Get(timeout, timed_out);
  }

  // Remove the promise from the pending-requests map.
  const llama::TUniqueId& res_id = pending_request->reservation_id();
  {
    lock_guard<mutex> l(pending_requests_lock_);
    pending_requests_.erase(pending_request->request_id());
    if (pending_request->is_expansion()) {
      PendingExpansionIdsMap::iterator it = pending_expansion_ids_.find(res_id);
      if (it == pending_expansion_ids_.end()) {
        // If the AMNotification was received as the reservation was being cleaned up,
        // it's possible that the pending/allocated request structures were updated
        // before this thread was able to acquire the lock.
        VLOG_RPC << "Didn't find reservation=" << res_id << " in pending requests";
        return false;
      }
      it->second.erase(pending_request->request_id());
    }
  }

  if (request_granted && !*timed_out) {
    pending_request->GetResources(resources);
    int64_t total_memory_mb = 0L;
    int32_t total_vcpus = 0;
    BOOST_FOREACH(const ResourceMap::value_type& resource, *resources) {
      total_memory_mb += resource.second.memory_mb;
      total_vcpus += resource.second.v_cpu_cores;
    }
    {
      lock_guard<mutex> l(allocated_requests_lock_);
      AllocatedRequestMap::iterator it = allocated_requests_.find(res_id);
      if (it == allocated_requests_.end()) {
        // The reservation may have already been cleaned up. See above.
        VLOG_RPC << "Didn't find reservation=" << res_id << " in allocated requests";
        return false;
      }
      it->second.push_back(AllocatedRequest(res_id, total_memory_mb, total_vcpus,
          pending_request->is_expansion()));
      allocated_memory_metric_->Increment(total_memory_mb * 1024L * 1024L);
      allocated_vcpus_metric_->Increment(total_vcpus);
    }
  }

  return request_granted;
}

Status ResourceBroker::Expand(const TUniqueId& reservation_id,
    const llama::TResource& resource, int64_t timeout_ms, llama::TUniqueId* expansion_id,
    llama::TAllocatedResource* allocated_resource) {
  llama::TLlamaAMReservationExpansionRequest ll_request;
  llama::TLlamaAMReservationExpansionResponse ll_response;

  ll_request.version = llama::TLlamaServiceVersion::V1;
  ll_request.am_handle = llama_handle_;
  ll_request.expansion_of << reservation_id;
  random_generator uuid_generator;
  llama::TUniqueId request_id;
  UUIDToTUniqueId(uuid_generator(), &request_id);
  ll_request.__set_expansion_id(request_id);
  ll_request.resource = resource;
  VLOG_RPC << "Sending expansion request for reservation_id=" << reservation_id
           << " expansion_id=" << request_id
           << " resource=" << resource;

  PendingRequest* pending_request;
  {
    lock_guard<mutex> l(pending_requests_lock_);
    PendingExpansionIdsMap::iterator it =
        pending_expansion_ids_.find(ll_request.expansion_of);
    // If pending_expansion_ids_ doesn't contain the reservation id then the
    // QueryResourceMgr has already been unregistered and the reservation has been
    // released.
    if (it == pending_expansion_ids_.end()) {
      return Status(Substitute("Resource expansion request (expansion id=$0, "
          "reservation id=$1) made after reservation released.",
          PrintId(ll_request.expansion_id), PrintId(reservation_id)));
    }
    it->second.insert(request_id);
    pending_request = new PendingRequest(ll_request.expansion_of, request_id, true);
    pending_requests_.insert(make_pair(request_id, pending_request));
  }

  MonotonicStopWatch sw;
  sw.Start();
  Status status = LlamaRpc(&llama::LlamaAMServiceClient::Expand, &ll_request,
      &ll_response, expansion_rpc_time_metric_);
  // Check the status of the response.
  if (!status.ok()) {
    expansion_requests_failed_metric_->Increment(1);
    return status;
  }

  Status request_status = LlamaStatusToImpalaStatus(ll_response.status);
  if (!request_status.ok()) {
    expansion_requests_failed_metric_->Increment(1);
    return request_status;
  }

  ResourceMap allocated_resources;
  bool timed_out = false;
  bool request_granted = WaitForNotification(timeout_ms,
      &allocated_resources, &timed_out, pending_request);

  if (timed_out) {
    expansion_requests_timedout_metric_->Increment(1);
    Status release_status = ReleaseRequest(request_id);
    if (!release_status.ok()) {
      VLOG_QUERY << "Error releasing timed out expansion request, expansion_id="
                 << request_id << " status: " << release_status.GetDetail();
    }
    return Status(Substitute("Resource expansion request (expansion id=$0, "
        "reservation id=$1) exceeded timeout of $2.",
        PrintId(ll_request.expansion_id),
        PrintId(reservation_id),
        PrettyPrinter::Print(timeout_ms * 1000L * 1000L, TUnit::TIME_NS)));
  }
  expansion_response_time_metric_->Update(
      sw.ElapsedTime() / (1000.0 * 1000.0 * 1000.0));

  if (!request_granted) {
    if (pending_request->is_cancelled()) {
      return Status(Substitute("Resource expansion request (expansion id=$0, "
          "reservation id=$1) was cancelled.", PrintId(ll_request.expansion_id),
          PrintId(reservation_id)));
    }
    expansion_requests_rejected_metric_->Increment(1);
    return Status(Substitute("Resource expansion request (expansion id=$0, "
        "reservation id=$1) was rejected.", PrintId(ll_request.expansion_id),
        PrintId(reservation_id)));
  }

  DCHECK_EQ(allocated_resources.size(), 1);
  *allocated_resource = allocated_resources.begin()->second;
  *expansion_id = request_id;

  VLOG_QUERY << "Fulfilled expansion for id=" << ll_response.expansion_id
             << " resource=" << *allocated_resource;
  expansion_requests_fulfilled_metric_->Increment(1);
  return Status::OK();
}

Status ResourceBroker::Reserve(const TResourceBrokerReservationRequest& request,
    TResourceBrokerReservationResponse* response) {
  VLOG_QUERY << "Sending reservation request: " << request;
  reservation_requests_total_metric_->Increment(1);

  llama::TLlamaAMReservationRequest ll_request;
  llama::TLlamaAMReservationResponse ll_response;
  CreateLlamaReservationRequest(request, ll_request);
  const llama::TUniqueId& res_id = ll_request.reservation_id;

  PendingRequest* pending_request;
  {
    pending_request = new PendingRequest(res_id, res_id, false);
    lock_guard<mutex> l(pending_requests_lock_);
    pending_requests_.insert(make_pair(pending_request->request_id(), pending_request));
  }
  {
    lock_guard<mutex> l(allocated_requests_lock_);
    DCHECK(allocated_requests_.find(res_id) == allocated_requests_.end());
    allocated_requests_[res_id] = vector<AllocatedRequest>();
  }

  MonotonicStopWatch sw;
  sw.Start();
  Status status = LlamaRpc(&llama::LlamaAMServiceClient::Reserve, &ll_request,
      &ll_response, reservation_rpc_time_metric_);
  // Check the status of the response.
  if (!status.ok()) {
    reservation_requests_failed_metric_->Increment(1);
    return status;
  }
  Status request_status = LlamaStatusToImpalaStatus(ll_response.status);
  if (!request_status.ok()) {
    reservation_requests_failed_metric_->Increment(1);
    return request_status;
  }
  VLOG_RPC << "Received reservation response, waiting for notification on: " << res_id;

  bool timed_out = false;
  bool request_granted = WaitForNotification(request.request_timeout,
      &response->allocated_resources, &timed_out, pending_request);

  if (request_granted || timed_out) {
    // Set the reservation_id to make sure it eventually gets released - even if when
    // timed out, since the response may arrive later.
    response->__set_reservation_id(CastTUniqueId<llama::TUniqueId, TUniqueId>(res_id));
  }

  if (timed_out) {
    reservation_requests_timedout_metric_->Increment(1);
    return Status(Substitute(
        "Resource reservation request (id=$0) exceeded timeout of $1.",
        PrintId(res_id),
        PrettyPrinter::Print(request.request_timeout * 1000L * 1000L,
        TUnit::TIME_NS)));
  }
  reservation_response_time_metric_->Update(
      sw.ElapsedTime() / (1000.0 * 1000.0 * 1000.0));

  if (!request_granted) {
    reservation_requests_rejected_metric_->Increment(1);
    return Status(Substitute("Resource reservation request (id=$0) was rejected.",
        PrintId(res_id)));
  }

  response->__set_reservation_id(CastTUniqueId<llama::TUniqueId, TUniqueId>(res_id));
  VLOG_QUERY << "Fulfilled reservation with id: " << res_id;
  reservation_requests_fulfilled_metric_->Increment(1);
  return Status::OK();
}

void ResourceBroker::ClearRequests(const TUniqueId& reservation_id) {
  int64_t total_memory_bytes = 0L;
  int32_t total_vcpus = 0L;
  llama::TUniqueId llama_id = CastTUniqueId<TUniqueId, llama::TUniqueId>(reservation_id);
  {
    lock_guard<mutex> l(pending_requests_lock_);
    PendingExpansionIdsMap::iterator it = pending_expansion_ids_.find(llama_id);
    if (it != pending_expansion_ids_.end()) {
      BOOST_FOREACH(const llama::TUniqueId& id, it->second) {
        PendingRequestMap::iterator request_it = pending_requests_.find(id);
        DCHECK(request_it != pending_requests_.end());
        if (request_it == pending_requests_.end()) continue;
        // It is possible that the AMNotification thread set the promise and the thread
        // waiting on the promise hasn't had a chance to acquire the
        // pending_requests_lock_ yet to remove it from pending_requests_. We don't need
        // to do anything because it will be released with the reservation anyway.
        if (request_it->second->promise()->IsSet()) continue;
        request_it->second->SetCancelled();
        request_it->second->promise()->Set(false);
      }
      it->second.clear();
      pending_expansion_ids_.erase(it);
    }
  }
  {
    lock_guard<mutex> l(allocated_requests_lock_);
    AllocatedRequestMap::iterator it = allocated_requests_.find(llama_id);
    if (it == allocated_requests_.end()) return;
    BOOST_FOREACH(AllocatedRequest& allocated_req, it->second) {
      DCHECK(allocated_req.reservation_id() == llama_id);
      total_memory_bytes += (allocated_req.memory_mb() * 1024L * 1024L);
      total_vcpus += allocated_req.vcpus();
    }
    it->second.clear();
    allocated_requests_.erase(it);
    allocated_memory_metric_->Increment(-total_memory_bytes);
    allocated_vcpus_metric_->Increment(-total_vcpus);
  }

  VLOG_QUERY << "Releasing "
             << PrettyPrinter::Print(total_memory_bytes, TUnit::BYTES)
             << " and " << total_vcpus << " cores for " << llama_id;
}

Status ResourceBroker::ReleaseRequest(const llama::TUniqueId& request_id) {
  llama::TLlamaAMReleaseRequest llama_request;
  llama::TLlamaAMReleaseResponse llama_response;
  llama_request.version = llama::TLlamaServiceVersion::V1;
  llama_request.am_handle = llama_handle_;
  llama_request.reservation_id = request_id;

  RETURN_IF_ERROR(LlamaRpc(&llama::LlamaAMServiceClient::Release,
          &llama_request, &llama_response,reservation_rpc_time_metric_));
  RETURN_IF_ERROR(LlamaStatusToImpalaStatus(llama_response.status));
  return Status::OK();
}

Status ResourceBroker::ReleaseReservation(const impala::TUniqueId& reservation_id) {
  VLOG_QUERY << "Releasing all resources for reservation: " << reservation_id;
  llama::TUniqueId llama_id = CastTUniqueId<TUniqueId, llama::TUniqueId>(reservation_id);

  ClearRequests(reservation_id);
  RETURN_IF_ERROR(ReleaseRequest(llama_id));
  requests_released_metric_->Increment(1);
  return Status::OK();
}

void ResourceBroker::AMNotification(const llama::TLlamaAMNotificationRequest& request,
    llama::TLlamaAMNotificationResponse& response) {
  {
    // This Impalad may have restarted, so it is possible Llama is sending notifications
    // while this Impalad is registering with Llama.
    lock_guard<mutex> l(llama_registration_lock_);
    if (request.am_handle != llama_handle_) {
      VLOG_QUERY << "Ignoring Llama AM notification with mismatched AM handle. "
                 << "Known handle: " << llama_handle_ << ". Received handle: "
                 << request.am_handle;
      // Ignore all notifications with mismatched handles.
      return;
    }
  }
  // Nothing to be done for heartbeats.
  if (request.heartbeat) return;
  VLOG_QUERY << "Received non-heartbeat AM notification";

  lock_guard<mutex> l(pending_requests_lock_);

  // Process granted allocations.
  BOOST_FOREACH(const llama::TUniqueId& res_id, request.allocated_reservation_ids) {
    PendingRequestMap::iterator it = pending_requests_.find(res_id);
    if (it == pending_requests_.end()) {
      VLOG_RPC << "Allocation for " << res_id << " arrived after timeout or cleanup";
      continue;
    }
    if (it->second->promise()->IsSet()) {
      // The promise should not have been set unless it was already cancelled.
      DCHECK(it->second->is_cancelled());
      continue;
    }
    LOG(INFO) << "Received allocated resource for reservation id: " << res_id;
    it->second->SetResources(request.allocated_resources);
    it->second->promise()->Set(true);
  }

  // Process rejected allocations.
  BOOST_FOREACH(const llama::TUniqueId& res_id, request.rejected_reservation_ids) {
    PendingRequestMap::iterator it = pending_requests_.find(res_id);
    if (it == pending_requests_.end()) {
      VLOG_RPC << "Rejection for " << res_id << " arrived after timeout";
      continue;
    }
    if (it->second->promise()->IsSet()) {
      DCHECK(it->second->is_cancelled());
      continue;
    }
    it->second->promise()->Set(false);
  }

  // TODO: We maybe want a thread pool for handling preemptions to avoid
  // blocking this function on query cancellations.
  // Process preempted reservations.
  BOOST_FOREACH(const llama::TUniqueId& res_id, request.preempted_reservation_ids) {
    TUniqueId impala_res_id;
    impala_res_id << res_id;
    scheduler_->HandlePreemptedReservation(impala_res_id);
  }

  // Process preempted client resources.
  BOOST_FOREACH(const llama::TUniqueId& res_id, request.preempted_client_resource_ids) {
    TUniqueId impala_res_id;
    impala_res_id << res_id;
    scheduler_->HandlePreemptedResource(impala_res_id);
  }

  // Process lost client resources.
  BOOST_FOREACH(const llama::TUniqueId& res_id, request.lost_client_resource_ids) {
    TUniqueId impala_res_id;
    impala_res_id << res_id;
    scheduler_->HandlePreemptedResource(impala_res_id);
  }

  response.status.__set_status_code(llama::TStatusCode::OK);
}

void ResourceBroker::NMNotification(const llama::TLlamaNMNotificationRequest& request,
    llama::TLlamaNMNotificationResponse& response) {
}

Status ResourceBroker::RefreshLlamaNodes() {
  llama::TLlamaAMGetNodesRequest llama_request;
  llama_request.__set_am_handle(llama_handle_);
  llama_request.__set_version(llama::TLlamaServiceVersion::V1);
  llama::TLlamaAMGetNodesResponse llama_response;

  RETURN_IF_ERROR(LlamaRpc(&llama::LlamaAMServiceClient::GetNodes, &llama_request,
      &llama_response, NULL));
  RETURN_IF_ERROR(LlamaStatusToImpalaStatus(llama_response.status));
  llama_nodes_ = llama_response.nodes;
  LOG(INFO) << "Llama Nodes [" << join(llama_nodes_, ", ") << "]";
  return Status::OK();
}

bool ResourceBroker::GetQueryResourceMgr(const TUniqueId& query_id,
    const TUniqueId& reservation_id, const TNetworkAddress& local_resource_address,
    QueryResourceMgr** mgr) {
  lock_guard<mutex> l(query_resource_mgrs_lock_);
  pair<int32_t, QueryResourceMgr*>* entry = &query_resource_mgrs_[query_id];
  if (entry->second == NULL) {
    entry->second =
        new QueryResourceMgr(reservation_id, local_resource_address, query_id);
    DCHECK_EQ(entry->first, 0);
    // Also create the per-query entries in the allocated_resources_ and
    // pending_expansion_ids_ map.
    llama::TUniqueId llama_id = CastTUniqueId<TUniqueId, llama::TUniqueId>(reservation_id);
    {
      lock_guard<mutex> pending_lock(pending_requests_lock_);
      DCHECK(pending_expansion_ids_.find(llama_id) == pending_expansion_ids_.end());
      pending_expansion_ids_[llama_id] = boost::unordered_set<llama::TUniqueId>();
    }
    {
      lock_guard<mutex> allocated_lock(allocated_requests_lock_);
      if (allocated_requests_.find(llama_id) == allocated_requests_.end()) {
        allocated_requests_[llama_id] = vector<AllocatedRequest>();
      }
    }
  }
  *mgr = entry->second;
  // Return true if this is the first reference to this resource mgr.
  return ++entry->first == 1L;
}

void ResourceBroker::UnregisterQueryResourceMgr(const TUniqueId& query_id) {
  lock_guard<mutex> l(query_resource_mgrs_lock_);
  QueryResourceMgrsMap::iterator it = query_resource_mgrs_.find(query_id);
  DCHECK(it != query_resource_mgrs_.end())
      << "UnregisterQueryResourceMgr() without corresponding GetQueryResourceMgr()";
  if (--it->second.first == 0) {
    it->second.second->Shutdown();
    ClearRequests(it->second.second->reservation_id());
    delete it->second.second;
    query_resource_mgrs_.erase(it);
  }
}

ostream& operator<<(ostream& os,
    const map<TNetworkAddress, llama::TAllocatedResource>& resources) {
  typedef map<TNetworkAddress, llama::TAllocatedResource> ResourceMap;
  int count = 0;
  BOOST_FOREACH(const ResourceMap::value_type& resource, resources) {
    os << "(" << resource.first << "," << resource.second << ")";
    if (++count != resources.size()) os << ",";
  }
  return os;
}

ostream& operator<<(ostream& os, const TResourceBrokerReservationRequest& request) {
  os << "Reservation Request("
     << "queue=" << request.queue << " "
     << "user=" << request.user << " "
     << "gang=" << request.gang << " "
     << "request_timeout=" << request.request_timeout << " "
     << "resources=[";
  for (int i = 0; i < request.resources.size(); ++i) {
    os << request.resources[i];
    if (i + 1 != request.resources.size()) os << ",";
  }
  os << "])";
  return os;
}

ostream& operator<<(ostream& os, const TResourceBrokerReservationResponse& reservation) {
  os << "Granted Reservation("
     << "reservation id=" << reservation.reservation_id << " "
     << "resources=[" << reservation.allocated_resources << "])";
  return os;
}

ostream& operator<<(ostream& os, const TResourceBrokerExpansionRequest& request) {
  os << "Expansion Request("
     << "reservation id=" << request.reservation_id << " "
     << "resource=" << request.resource << " "
     << "request_timeout=" << request.request_timeout << ")";
  return os;
}

ostream& operator<<(ostream& os, const TResourceBrokerExpansionResponse& expansion) {
  os << "Expansion Response("
     << "reservation id=" << expansion.reservation_id << " "
     << "resources=[" << expansion.allocated_resources << "])";
  return os;
}

}
