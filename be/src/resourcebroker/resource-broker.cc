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
#include "gen-cpp/ResourceBrokerService.h"
#include "gen-cpp/Llama_types.h"

using namespace std;
using namespace impala;
using namespace boost;
using namespace boost::algorithm;
using namespace boost::uuids;

using namespace ::apache::thrift::server;
using namespace ::apache::thrift;

DECLARE_int32(resource_broker_cnxn_attempts);
DECLARE_int32(resource_broker_cnxn_retry_interval_ms);
DECLARE_int32(resource_broker_send_timeout);
DECLARE_int32(resource_broker_recv_timeout);

static const string LLAMA_KERBEROS_SERVICE_NAME = "llama";

// TODO: Refactor the Llama restart and thrift connect/reconnect/reopen logic into
// a common place.

namespace impala {

// String to search for in Llama error messages to detect that Llama has restarted,
// and hence the resource broker must re-register.
const string LLAMA_RESTART_SEARCH_STRING = "Unknown handle";

// Number of seconds to wait between Llama registration attempts.
const int64_t LLAMA_REGISTRATION_WAIT_SECS = 3;

// Maximum number of seconds that a any query will wait for (re-)registration with the
// Llama before failing the query with an error.
const int64_t LLAMA_REGISTRATION_TIMEOUT_SECS = 30;

// Maximum number of times a reserve/release against the Llama is retried
// if the Llama restarted while processing the request. If this maximum number of
// attempts is exceeded, then the originating query fails with an error.
const int64_t LLAMA_MAX_REQUEST_ATTEMPTS = 5;

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

ResourceBroker::ResourceBroker(const TNetworkAddress& llama_address,
    const TNetworkAddress& llama_callback_address, Metrics* metrics) :
    llama_address_(llama_address),
    llama_callback_address_(llama_callback_address),
    metrics_(metrics),
    scheduler_(NULL),
    llama_callback_thrift_iface_(new LlamaNotificationThriftIf(this)),
    llama_client_cache_(new ClientCache<llama::LlamaAMServiceClient>(
        FLAGS_resource_broker_cnxn_attempts,
        FLAGS_resource_broker_cnxn_retry_interval_ms,
        FLAGS_resource_broker_send_timeout,
        FLAGS_resource_broker_recv_timeout,
        LLAMA_KERBEROS_SERVICE_NAME)),
    is_mini_llama_(false) {
  DCHECK(metrics != NULL);
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
  RETURN_IF_ERROR(RegisterAndRefreshLlama());
  return Status::OK;
}

Status ResourceBroker::RegisterAndRefreshLlama() {
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

  bool needs_reopen = false;
  int64_t now = TimestampValue::local_time_micros().time_of_day().total_seconds();
  while((now - start) < LLAMA_REGISTRATION_TIMEOUT_SECS) {
    // Connect to the Llama.
    Status status;
    ClientConnection<llama::LlamaAMServiceClient> llama_client(llama_client_cache_.get(),
        llama_address_, &status);
    if (needs_reopen) {
      status = llama_client.Reopen();
      needs_reopen = false;
    }
    if (status.ok()) {
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
      LOG(INFO) << "Registering Resource Broker with Llama at " << llama_address_;
      try {
        llama_client->Register(response, request);
        RETURN_IF_ERROR(LlamaStatusToImpalaStatus(
            response.status, "Failed to register Resource Broker with Llama."));
        llama_handle_ = response.am_handle;
        LOG(INFO) << "Received Llama client handle " << llama_handle_;
        break;
      } catch (const TException& e) {
        needs_reopen = true;
      }
    }
    LOG(INFO) << "Failed to connect to Llama at " << llama_address_ << ".\n"
              << "Error: " << status.GetErrorMsg() << "\n"
              << "Retrying to connect in "
              << LLAMA_REGISTRATION_WAIT_SECS << "s.";
    // Sleep even if we just need to Reopen() the client to stagger re-registrations
    // of multiple Impalads with the Llama in case the Llama went down and came back up.
    usleep(LLAMA_REGISTRATION_WAIT_SECS * 1000 * 1000);
    now = TimestampValue::local_time_micros().time_of_day().total_seconds();
  }
  if ((now - start) >= LLAMA_REGISTRATION_TIMEOUT_SECS) {
    return Status("Failed to (re-)register Resource Broker with Llama.");
  }
  return Status::OK;
}

bool ResourceBroker::LlamaHasRestarted(const llama::TStatus& status) const {
  if (status.status_code == llama::TStatusCode::OK || !status.__isset.error_msgs) {
    return false;
  }
  // Check whether one of the error messages contains LLAMA_RESTART_SEARCH_STRING.
  for (int i = 0; i < status.error_msgs.size(); ++i) {
    if (status.error_msgs[i].find(LLAMA_RESTART_SEARCH_STRING) != string::npos) {
      LOG(INFO) << "Assuming Llama restart from error message: " << status.error_msgs[i];
      return true;
    }
  }
  return false;
}

void ResourceBroker::Close() {
  llama_client_cache_->CloseConnections(llama_address_);
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

void ResourceBroker::ReservationFulfillment::GetResources(ResourceMap* resources,
    TUniqueId* reservation_id) {
  resources->clear();
  // TODO: Llama returns a dump of all resources that we need to manually
  // group by reservation id. Can Llama do the grouping for us?
  BOOST_FOREACH(const llama::TAllocatedResource& resource, allocated_resources_) {
    // Ignore resources that don't belong to the given reservation id.
    if (resource.reservation_id != reservation_id_) continue;
    TNetworkAddress host = MakeNetworkAddress(resource.location);
    (*resources)[host] = resource;
    VLOG_QUERY << "Getting allocated resource for reservation id "
               << reservation_id_ << " and location " << host;
  }
  (*reservation_id) << reservation_id_;
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
  fulfillment->GetResources(resources, reservation_id);

  // Remove the promise from the pending-requests map.
  {
    lock_guard<mutex> l(requests_lock_);
    pending_requests_.erase(request_id);
  }

  return request_granted;
}

Status ResourceBroker::Expand(const TResourceBrokerExpansionRequest& request,
    TResourceBrokerExpansionResponse* response) {
  llama::TLlamaAMReservationExpansionRequest ll_request;
  llama::TLlamaAMReservationExpansionResponse ll_response;

  ll_request.version = llama::TLlamaServiceVersion::V1;
  ll_request.am_handle = llama_handle_;
  ll_request.expansion_of << request.reservation_id;
  ll_request.resource = request.resource;

  int attempts = 0;
  MonotonicStopWatch sw;
  while (attempts < LLAMA_MAX_REQUEST_ATTEMPTS) {
    ++attempts;
    Status status;
    ClientConnection<llama::LlamaAMServiceClient>
        llama_client(llama_client_cache_.get(), llama_address_, &status);
    RETURN_IF_ERROR(status);

    sw.Start();
    try {
      llama_client->Expand(ll_response, ll_request);
    } catch (TTransportException& e) {
      VLOG_RPC << "Retrying Expand: " << e.what();
      status = llama_client.Reopen();
      if (!status.ok()) return status;
      llama_client->Expand(ll_response, ll_request);
    }

    expansion_rpc_time_metric_->Update(sw.ElapsedTime() / (1000.0 * 1000.0 * 1000.0));

    // Check whether Llama has been restarted. If so, re-register with it.
    if (LlamaHasRestarted(ll_response.status)) {
      RETURN_IF_ERROR(RegisterAndRefreshLlama());
      // Set the new Llama handle received from re-registering.
      ll_request.__set_am_handle(llama_handle_);
      LOG(INFO) << "Retrying expansion request: " << request;
      continue;
    }
    break;
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

  if (!timed_out) {
    expansion_response_time_metric_->Update(
        sw.ElapsedTime() / (1000.0 * 1000.0 * 1000.0));
  } else {
    expansion_requests_timedout_metric_->Increment(1);
    stringstream error_msg;
    error_msg << "Resource expansion request exceeded timeout of "
              << request.request_timeout << "ms.";
    return Status(error_msg.str());
  }

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

  int attempts = 0;
  MonotonicStopWatch sw;
  while (attempts < LLAMA_MAX_REQUEST_ATTEMPTS) {
    ++attempts;
    Status status;
    ClientConnection<llama::LlamaAMServiceClient> llama_client(llama_client_cache_.get(),
        llama_address_, &status);
    RETURN_IF_ERROR(status);

    sw.Start();
    try {
      llama_client->Reserve(llama_response, llama_request);
    } catch (const TException& e) {
      VLOG_RPC << "Retrying Reserve: " << e.what();
      status = llama_client.Reopen();
      if (!status.ok()) continue;
      llama_client->Reserve(llama_response, llama_request);
    }
    reservation_rpc_time_metric_->Update(sw.ElapsedTime() / (1000.0 * 1000.0 * 1000.0));

    // Check whether Llama has been restarted. If so, re-register with it.
    if (LlamaHasRestarted(llama_response.status)) {
      RETURN_IF_ERROR(RegisterAndRefreshLlama());
      // Set the new Llama handle received from re-registering.
      llama_request.__set_am_handle(llama_handle_);
      LOG(INFO) << "Retrying reservation request: " << request;
      continue;
    }
    break;
  }
  if (attempts >= LLAMA_MAX_REQUEST_ATTEMPTS) {
    reservation_requests_failed_metric_->Increment(1);
    return Status("Reservation request aborted due to connectivity issues with Llama.");
  }

  // Check the status of the response.
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

  if (!timed_out) {
    reservation_response_time_metric_->Update(
        sw.ElapsedTime() / (1000.0 * 1000.0 * 1000.0));
  } else {
    // Set the reservation_id to release it from Llama.
    response->__set_reservation_id(reservation_id);
    reservation_requests_timedout_metric_->Increment(1);
    stringstream error_msg;
    error_msg << "Resource reservation request exceeded timeout of "
              << request.request_timeout << "ms.";
    return Status(error_msg.str());
  }
  if (!request_granted) {
    reservation_requests_rejected_metric_->Increment(1);
    return Status("Resource reservation request was rejected.");
  }

  response->__set_reservation_id(reservation_id);

  VLOG_QUERY << "Fulfilled reservation with id: " << llama_response.reservation_id;
  reservation_requests_fulfilled_metric_->Increment(1);
  return Status::OK;
}

Status ResourceBroker::Release(const TResourceBrokerReleaseRequest& request,
    TResourceBrokerReleaseResponse* response) {
  VLOG_QUERY << "Releasing reservation with id " << request.reservation_id;

  llama::TLlamaAMReleaseRequest llama_request;
  llama::TLlamaAMReleaseResponse llama_response;
  CreateLlamaReleaseRequest(request, llama_request);

  int attempts = 0;
  while (attempts < LLAMA_MAX_REQUEST_ATTEMPTS) {
    ++attempts;
    Status status;
    ClientConnection<llama::LlamaAMServiceClient> llama_client(llama_client_cache_.get(),
        llama_address_, &status);
    RETURN_IF_ERROR(status);
    try {
      llama_client->Release(llama_response, llama_request);
    } catch (const TException& e) {
      VLOG_RPC << "Retrying Release: " << e.what();
      status = llama_client.Reopen();
      if (!status.ok()) continue;
      llama_client->Release(llama_response, llama_request);
    }

    // Check whether Llama has been restarted. If so, re-register with it.
    if (LlamaHasRestarted(llama_response.status)) {
      RETURN_IF_ERROR(RegisterAndRefreshLlama());
      // Set the new Llama handle received from re-registering.
      llama_request.__set_am_handle(llama_handle_);
      LOG(INFO) << "Retrying release of reservation with id "
                << request.reservation_id;
      continue;
    }
    break;
  }
  if (attempts >= LLAMA_MAX_REQUEST_ATTEMPTS) {
    return Status("Reservation release aborted due to connectivity issues with Llama.");
  }

  RETURN_IF_ERROR(LlamaStatusToImpalaStatus(llama_response.status));
  requests_released_metric_->Increment(1);

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

  int attempts = 0;
  while (attempts < LLAMA_MAX_REQUEST_ATTEMPTS) {
    ++attempts;
    Status status;
    ClientConnection<llama::LlamaAMServiceClient> llama_client(llama_client_cache_.get(),
        llama_address_, &status);
    RETURN_IF_ERROR(status);
    try {
      llama_client->GetNodes(llama_response, llama_request);
    } catch (const TException& e) {
      VLOG_RPC << "Retrying GetNodes: " << e.what();
      status = llama_client.Reopen();
      if (!status.ok()) continue;
      llama_client->GetNodes(llama_response, llama_request);
    }
    // Check whether Llama has been restarted. If so, re-register with it.
    if (LlamaHasRestarted(llama_response.status)) {
      RETURN_IF_ERROR(RegisterWithLlama());
      // Set the new Llama handle received from re-registering.
      llama_request.__set_am_handle(llama_handle_);
      LOG(INFO) << "Retrying GetNodes";
      continue;
    }
    break;
  }
  if (attempts >= LLAMA_MAX_REQUEST_ATTEMPTS) {
    return Status("GetNodes request aborted due to connectivity issues with Llama.");
  }

  RETURN_IF_ERROR(LlamaStatusToImpalaStatus(llama_response.status));
  llama_nodes_ = llama_response.nodes;
  LOG(INFO) << "Llama Nodes [" << join(llama_nodes_, ", ") << "]";

  // The Llama is a Mini Llama if all nodes know to it are on 127.0.0.1.
  is_mini_llama_ = true;
  for (int i = 0; i < llama_nodes_.size(); ++i) {
    TNetworkAddress hostport = MakeNetworkAddress(llama_nodes_[i]);
    if (hostport.hostname != "127.0.0.1") {
      is_mini_llama_ = false;
      break;
    }
  }
  LOG(INFO) << "Resource broker is using Mini LLama: " << is_mini_llama_;
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
