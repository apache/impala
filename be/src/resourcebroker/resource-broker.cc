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
#include <thrift/Thrift.h>

#include "common/status.h"
#include "rpc/thrift-util.h"
#include "rpc/thrift-server.h"
#include "statestore/query-resource-mgr.h"
#include "statestore/scheduler.h"
#include "util/debug-util.h"
#include "util/stopwatch.h"
#include "util/uid-util.h"
#include "util/network-util.h"
#include "util/llama-util.h"
#include "util/time.h"
#include "gen-cpp/ResourceBrokerService.h"
#include "gen-cpp/Llama_types.h"

using namespace std;
using namespace impala;
using namespace boost;
using namespace boost::algorithm;
using namespace boost::uuids;

using namespace ::apache::thrift::server;
using namespace ::apache::thrift;

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
    const TNetworkAddress& llama_callback_address, Metrics* metrics) :
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
  DCHECK_GT(llama_addresses_.size(), 0);
  DCHECK(metrics != NULL);
  active_llama_metric_ = metrics->CreateAndRegisterPrimitiveMetric<string>(
      "resource-broker.active-llama", "none");
  active_llama_handle_metric_ = metrics->CreateAndRegisterPrimitiveMetric<string>(
      "resource-broker.active-llama-handle", "none");

  reservation_rpc_time_metric_ =
      metrics->RegisterMetric(
          new StatsMetric<double>("resource-broker.reservation-request-rpc-time"));
  reservation_response_time_metric_ =
      metrics->RegisterMetric(
          new StatsMetric<double>("resource-broker.reservation-request-response-time"));
  reservation_requests_total_metric_ =
      metrics->CreateAndRegisterPrimitiveMetric<int64_t>(
          "resource-broker.reservation-requests-total", 0);
  reservation_requests_fulfilled_metric_ =
      metrics->CreateAndRegisterPrimitiveMetric<int64_t>(
          "resource-broker.reservation-requests-fulfilled", 0);
  reservation_requests_failed_metric_ =
      metrics->CreateAndRegisterPrimitiveMetric<int64_t>(
          "resource-broker.reservation-requests-failed", 0);
  reservation_requests_rejected_metric_ =
      metrics->CreateAndRegisterPrimitiveMetric<int64_t>(
          "resource-broker.reservation-requests-rejected", 0);
  reservation_requests_timedout_metric_ =
      metrics->CreateAndRegisterPrimitiveMetric<int64_t>(
          "resource-broker.reservation-requests-timedout", 0);

  expansion_rpc_time_metric_ =
      metrics->RegisterMetric(
          new StatsMetric<double>("resource-broker.expansion-request-rpc-time"));
  expansion_response_time_metric_ =
      metrics->RegisterMetric(
          new StatsMetric<double>("resource-broker.expansion-request-response-time"));
  expansion_requests_total_metric_ =
      metrics->CreateAndRegisterPrimitiveMetric<int64_t>(
          "resource-broker.expansion-requests-total", 0);
  expansion_requests_fulfilled_metric_ =
      metrics->CreateAndRegisterPrimitiveMetric<int64_t>(
          "resource-broker.expansion-requests-fulfilled", 0);
  expansion_requests_failed_metric_ =
      metrics->CreateAndRegisterPrimitiveMetric<int64_t>(
          "resource-broker.expansion-requests-failed", 0);
  expansion_requests_rejected_metric_ =
      metrics->CreateAndRegisterPrimitiveMetric<int64_t>(
          "resource-broker.expansion-requests-rejected", 0);
  expansion_requests_timedout_metric_ =
      metrics->CreateAndRegisterPrimitiveMetric<int64_t>(
          "resource-broker.expansion-requests-timedout", 0);

  allocated_memory_metric_ =
      metrics->RegisterMetric(new Metrics::BytesMetric(
          "resource-broker.memory-resources-in-use", 0L));

  allocated_vcpus_metric_ =
      metrics->CreateAndRegisterPrimitiveMetric<int64_t>(
          "resource-broker.vcpu-resources-in-use", 0);

  requests_released_metric_ =
      metrics->CreateAndRegisterPrimitiveMetric<int64_t>(
          "resource-broker.requests-released", 0);
}

Status ResourceBroker::Init() {
  // The scheduler must have been set before calling Init().
  DCHECK(scheduler_ != NULL);
  DCHECK(llama_callback_thrift_iface_ != NULL);
  shared_ptr<TProcessor> llama_callback_proc(
      new llama::LlamaNotificationServiceProcessor(llama_callback_thrift_iface_));
  llama_callback_server_.reset(new ThriftServer("llama-callback", llama_callback_proc,
      llama_callback_address_.port, NULL, metrics_, 5));
  RETURN_IF_ERROR(llama_callback_server_->Start());

  // Generate client id for registration with Llama, and register with LLama.
  random_generator uuid_generator;
  llama_client_id_= uuid_generator();
  RETURN_IF_ERROR(RegisterWithLlama());
  RETURN_IF_ERROR(RefreshLlamaNodes());
  return Status::OK;
}

Status ResourceBroker::RegisterWithLlama() {
  // Remember the current llama_handle_ to detect if another thread has already
  // completed the registration successfully.
  llama::TUniqueId current_llama_handle = llama_handle_;

  // Start time that this thread attempted registration. Used to limit the time that a
  // query will wait for re-registration with the Llama to succeed.
  int64_t start = TimestampValue::local_time_micros().time_of_day().total_seconds();
  lock_guard<mutex> l(llama_registration_lock_);
  if (llama_handle_ != current_llama_handle) return Status::OK;

  active_llama_metric_->Update("none");
  active_llama_handle_metric_->Update("none");

  int llama_addr_idx = (active_llama_addr_idx_ + 1) % llama_addresses_.size();
  int64_t now = TimestampValue::local_time_micros().time_of_day().total_seconds();
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
      bool rpc_success = false;
      try {
        llama_client->Register(response, request);
        rpc_success = true;
      } catch (const TException& e) {
        client_status = llama_client.Reopen();
        if (client_status.ok()) {
          llama_client->Register(response, request);
          rpc_success = true;
        }
      }
      if (rpc_success) {
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
              << "Error: " << client_status.GetErrorMsg() << endl
              << "Retrying to connect to Llama at "
              << llama_addresses_[llama_addr_idx] << " in "
              << FLAGS_llama_registration_wait_secs << "s.";
    // Sleep to give Llama time to recover/failover before the next attempt.
    SleepForMs(FLAGS_llama_registration_wait_secs * 1000);
    now = TimestampValue::local_time_micros().time_of_day().total_seconds();
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
  stringstream hostport_ss;
  hostport_ss << llama_addresses_[llama_addr_idx];
  active_llama_metric_->Update(hostport_ss.str());
  stringstream handle_ss;
  handle_ss << llama_handle_;
  active_llama_handle_metric_->Update(handle_ss.str());
  return Status::OK;
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
}

// Creates a Llama release request from a resource broker release request.
void ResourceBroker::CreateLlamaReleaseRequest(const TResourceBrokerReleaseRequest& src,
    llama::TLlamaAMReleaseRequest& dest) {
  dest.version = llama::TLlamaServiceVersion::V1;
  dest.am_handle = llama_handle_;
  dest.reservation_id << src.reservation_id;
}

template <typename LlamaReqType, typename LlamaRespType>
Status ResourceBroker::LlamaRpc(LlamaReqType* request, LlamaRespType* response,
    StatsMetric<double>* rpc_time_metric) {
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
    try {
      SendLlamaRpc(&llama_client, *request, response);
    } catch (const TException& e) {
      VLOG_RPC << "Reopening Llama client due to: " << e.what();
      rpc_status = llama_client.Reopen();
      if (!rpc_status.ok()) {
        register_with_llama = true;
        continue;
      }
      VLOG_RPC << "Retrying Llama RPC: " << *request;
      SendLlamaRpc(&llama_client, *request, response);
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
    stringstream err;
    err << "Request aborted after " << FLAGS_llama_max_request_attempts
        << " attempts due to connectivity issues with Llama.";
    return Status(err.str());
  }
  return Status::OK;
}

template <typename LlamaReqType, typename LlamaRespType>
void ResourceBroker::SendLlamaRpc(
    ClientConnection<llama::LlamaAMServiceClient>* llama_client,
    const LlamaReqType& request, LlamaRespType* response) {
  DCHECK(false) << "SendLlamaRpc template function must be specialized.";
}

// Template specialization for the Llama GetNodes() RPC.
template <>
void ResourceBroker::SendLlamaRpc(
    ClientConnection<llama::LlamaAMServiceClient>* llama_client,
    const llama::TLlamaAMGetNodesRequest& request,
    llama::TLlamaAMGetNodesResponse* response) {
  DCHECK(response != NULL);
  (*llama_client)->GetNodes(*response, request);
}

// Template specialization for the Llama Reserve() RPC.
template <>
void ResourceBroker::SendLlamaRpc(
    ClientConnection<llama::LlamaAMServiceClient>* llama_client,
    const llama::TLlamaAMReservationRequest& request,
    llama::TLlamaAMReservationResponse* response) {
  DCHECK(response != NULL);
  (*llama_client)->Reserve(*response, request);
}

// Template specialization for the Llama Expand() RPC.
template <>
void ResourceBroker::SendLlamaRpc(
    ClientConnection<llama::LlamaAMServiceClient>* llama_client,
    const llama::TLlamaAMReservationExpansionRequest& request,
    llama::TLlamaAMReservationExpansionResponse* response) {
  DCHECK(response != NULL);
  (*llama_client)->Expand(*response, request);
}

// Template specialization for the Llama Release() RPC.
template <>
void ResourceBroker::SendLlamaRpc(
    ClientConnection<llama::LlamaAMServiceClient>* llama_client,
    const llama::TLlamaAMReleaseRequest& request,
    llama::TLlamaAMReleaseResponse* response) {
  DCHECK(response != NULL);
  (*llama_client)->Release(*response, request);
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

void ResourceBroker::ReservationFulfillment::GetResources(ResourceMap* resources,
    TUniqueId* reservation_id) {
  resources->clear();
  BOOST_FOREACH(const llama::TAllocatedResource& resource, allocated_resources_) {
    TNetworkAddress host = MakeNetworkAddress(resource.location);
    (*resources)[host] = resource;
    VLOG_QUERY << "Getting allocated resource for reservation id "
               << reservation_id_ << " and location " << host;
  }
  (*reservation_id) << reservation_id_;
}

void ResourceBroker::ReservationFulfillment::SetResources(
    const vector<llama::TAllocatedResource>& resources,
    const llama::TUniqueId& reservation_id) {
  // TODO: Llama returns a dump of all resources that we need to manually group by
  // reservation id. Can Llama do the grouping for us?
  reservation_id_ = reservation_id;
  BOOST_FOREACH(const llama::TAllocatedResource& resource, resources) {
    // Ignore resources that don't belong to the given reservation id.
    if (resource.reservation_id == reservation_id_) {
      allocated_resources_.push_back(resource);
    }
  }
}


bool ResourceBroker::WaitForNotification(const llama::TUniqueId& request_id,
    int64_t timeout, TUniqueId* reservation_id, ResourceMap* resources, bool* timed_out) {
  shared_ptr<ReservationFulfillment> fulfillment;
  {
    lock_guard<mutex> l(requests_lock_);
    // It's possible that the AM notification arrived before we called into this method
    // and took the lock. If so, AMNotification() should have placed a
    // ReservationFulfillment in the pending_requests_ map already.
    FulfillmentMap::iterator it = pending_requests_.find(request_id);
    if (it == pending_requests_.end()) {
      // We got here first, so add the ReservationFulfillment object implicitly and set
      // the promise field so that we get signalled when AMNotification() gets an update.
      fulfillment.reset(new ReservationFulfillment());
      pending_requests_.insert(make_pair(request_id, fulfillment));
    } else {
      fulfillment = it->second;
    }
  }

  // Need to give up requests_lock_ before waiting
  bool request_granted = fulfillment->promise()->Get(timeout, timed_out);
  if (request_granted && !*timed_out) {
    fulfillment->GetResources(resources, reservation_id);
    BOOST_FOREACH(const ResourceMap::value_type& resource, *resources) {
      allocated_memory_metric_->Increment(resource.second.memory_mb * 1024L * 1024L);
      allocated_vcpus_metric_->Increment(resource.second.v_cpu_cores);
    }
  }

  // Remove the promise from the pending-requests map.
  {
    lock_guard<mutex> l(requests_lock_);
    pending_requests_.erase(request_id);
    if (request_granted && !*timed_out) allocated_requests_[request_id] = fulfillment;
  }

  return request_granted;
}

Status ResourceBroker::Expand(const TResourceBrokerExpansionRequest& request,
    TResourceBrokerExpansionResponse* response) {
  VLOG_RPC << "Sending expansion request: " << request;
  llama::TLlamaAMReservationExpansionRequest ll_request;
  llama::TLlamaAMReservationExpansionResponse ll_response;

  ll_request.version = llama::TLlamaServiceVersion::V1;
  ll_request.am_handle = llama_handle_;
  ll_request.expansion_of << request.reservation_id;
  ll_request.resource = request.resource;

  MonotonicStopWatch sw;
  sw.Start();
  Status status = LlamaRpc(&ll_request, &ll_response, expansion_rpc_time_metric_);
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

  TUniqueId reservation_id;
  bool timed_out = false;
  bool request_granted = WaitForNotification(ll_response.reservation_id,
      request.request_timeout, &reservation_id, &response->allocated_resources,
      &timed_out);

  if (timed_out) {
    expansion_requests_timedout_metric_->Increment(1);
    stringstream error_msg;
    error_msg << "Resource expansion request exceeded timeout of "
        << request.request_timeout << "ms.";
    return Status(error_msg.str());
  }
  expansion_response_time_metric_->Update(
      sw.ElapsedTime() / (1000.0 * 1000.0 * 1000.0));

  if (!request_granted) {
    expansion_requests_rejected_metric_->Increment(1);
    return Status("Resource expansion request was rejected.");
  }

  response->__set_reservation_id(reservation_id);
  VLOG_QUERY << "Fulfilled expansion for id: " << ll_response.reservation_id;
  expansion_requests_fulfilled_metric_->Increment(1);
  return Status::OK;
}

Status ResourceBroker::Reserve(const TResourceBrokerReservationRequest& request,
    TResourceBrokerReservationResponse* response) {
  VLOG_QUERY << "Sending reservation request: " << request;
  reservation_requests_total_metric_->Increment(1);

  llama::TLlamaAMReservationRequest llama_request;
  llama::TLlamaAMReservationResponse llama_response;
  CreateLlamaReservationRequest(request, llama_request);

  MonotonicStopWatch sw;
  sw.Start();
  Status status = LlamaRpc(&llama_request, &llama_response, reservation_rpc_time_metric_);
  // Check the status of the response.
  if (!status.ok()) {
    reservation_requests_failed_metric_->Increment(1);
    return status;
  }
  Status request_status = LlamaStatusToImpalaStatus(llama_response.status);
  if (!request_status.ok()) {
    reservation_requests_failed_metric_->Increment(1);
    return request_status;
  }

  VLOG_RPC << "Received reservation response from Llama, waiting for notification on: "
           << llama_response.reservation_id;

  TUniqueId reservation_id;
  bool timed_out = false;
  bool request_granted = WaitForNotification(llama_response.reservation_id,
      request.request_timeout, &reservation_id, &response->allocated_resources,
      &timed_out);

  if (timed_out) {
    // Set the reservation_id to release it from Llama.
    // WaitForNotification() does not set reservation_id if the request timed out.
    reservation_id << llama_response.reservation_id;
    response->__set_reservation_id(reservation_id);
    reservation_requests_timedout_metric_->Increment(1);
    stringstream error_msg;
    error_msg << "Resource reservation request exceeded timeout of "
        << request.request_timeout << "ms.";
    return Status(error_msg.str());
  }
  reservation_response_time_metric_->Update(
      sw.ElapsedTime() / (1000.0 * 1000.0 * 1000.0));

  if (!request_granted) {
    reservation_requests_rejected_metric_->Increment(1);
    return Status("Resource reservation request was rejected.");
  }

  DCHECK_EQ(reservation_id, llama_response.reservation_id);
  response->__set_reservation_id(reservation_id);
  VLOG_QUERY << "Fulfilled reservation with id: " << reservation_id;
  reservation_requests_fulfilled_metric_->Increment(1);
  return Status::OK;
}

Status ResourceBroker::Release(const TResourceBrokerReleaseRequest& request,
    TResourceBrokerReleaseResponse* response) {
  VLOG_QUERY << "Releasing reservation with id " << request.reservation_id;

  llama::TLlamaAMReleaseRequest llama_request;
  llama::TLlamaAMReleaseResponse llama_response;
  CreateLlamaReleaseRequest(request, llama_request);

  RETURN_IF_ERROR(LlamaRpc(
      &llama_request, &llama_response,reservation_rpc_time_metric_));
  RETURN_IF_ERROR(LlamaStatusToImpalaStatus(llama_response.status));
  requests_released_metric_->Increment(1);
  {
    lock_guard<mutex> l(requests_lock_);
    FulfillmentMap::iterator it = allocated_requests_.find(llama_request.reservation_id);
    // There may be no allocated resources when releasing a request that timed out.
    if (it != allocated_requests_.end()) {
      BOOST_FOREACH(const llama::TAllocatedResource& resource, it->second->resources()) {
        DCHECK(resource.reservation_id == llama_request.reservation_id);
        VLOG_QUERY << "Releasing "
                   << PrettyPrinter::Print(resource.memory_mb * 1024L * 1024L,
                                           TCounterType::BYTES)
                   << " and " << resource.v_cpu_cores << " cores for "
                   << llama_request.reservation_id;
        allocated_memory_metric_->Increment(-resource.memory_mb * 1024L * 1024L);
        allocated_vcpus_metric_->Increment(-resource.v_cpu_cores);
      }
      allocated_requests_.erase(it);
    }
  }

  VLOG_QUERY << "Released reservation with id " << request.reservation_id;
  return Status::OK;
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

  lock_guard<mutex> l(requests_lock_);

  // Process granted allocations.
  BOOST_FOREACH(const llama::TUniqueId& res_id, request.allocated_reservation_ids) {
    // TODO: Garbage collect fulfillments that live for a long time, since they probably
    // don't correspond to any query.
    FulfillmentMap::iterator it = pending_requests_.find(res_id);
    shared_ptr<ReservationFulfillment> fulfillment;
    if (it == pending_requests_.end()) {
      fulfillment.reset(new ReservationFulfillment());
      pending_requests_.insert(make_pair(res_id, fulfillment));
    } else {
      fulfillment = it->second;
    }
    LOG(INFO) << "Received allocated resource for reservation id: " << res_id;
    fulfillment->SetResources(request.allocated_resources, res_id);
    fulfillment->promise()->Set(true);
  }

  // Process rejected allocations.
  BOOST_FOREACH(const llama::TUniqueId& res_id, request.rejected_reservation_ids) {
    FulfillmentMap::iterator it = pending_requests_.find(res_id);
    shared_ptr<ReservationFulfillment> fulfillment;
    if (it == pending_requests_.end()) {
      fulfillment.reset(new ReservationFulfillment());
      pending_requests_.insert(make_pair(res_id, fulfillment));
    } else {
      fulfillment = it->second;
    }

    fulfillment->promise()->Set(false);
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

  RETURN_IF_ERROR(LlamaRpc(&llama_request, &llama_response, NULL));
  RETURN_IF_ERROR(LlamaStatusToImpalaStatus(llama_response.status));
  llama_nodes_ = llama_response.nodes;
  LOG(INFO) << "Llama Nodes [" << join(llama_nodes_, ", ") << "]";
  return Status::OK;
}

bool ResourceBroker::GetQueryResourceMgr(const TUniqueId& query_id,
    const TUniqueId& reservation_id, const TNetworkAddress& local_resource_address,
    QueryResourceMgr** mgr) {
  lock_guard<mutex> l(query_resource_mgrs_lock_);
  pair<int32_t, QueryResourceMgr*>* entry = &query_resource_mgrs_[query_id];
  if (entry->second == NULL) {
    entry->second =
        new QueryResourceMgr(reservation_id, local_resource_address, query_id);
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
  it->second.second->Shutdown();
  if (--it->second.first == 0) {
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
