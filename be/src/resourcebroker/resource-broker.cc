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

#include "common/status.h"
#include "rpc/thrift-util.h"
#include "rpc/thrift-server.h"
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
using namespace ::apache::thrift::transport;

DECLARE_int32(resource_broker_cnxn_attempts);
DECLARE_int32(resource_broker_cnxn_retry_interval_ms);
DECLARE_int32(resource_broker_send_timeout);
DECLARE_int32(resource_broker_recv_timeout);

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
        FLAGS_resource_broker_recv_timeout)),
    is_mini_llama_(false) {
  DCHECK(metrics != NULL);
  request_rpc_time_metric_ =
      metrics->RegisterMetric(
          new StatsMetric<double>("resource-broker.request-rpc-time"));
  request_response_time_metric_ =
      metrics->RegisterMetric(
          new StatsMetric<double>("resource-broker.request-response-time"));
  requests_total_metric_ =
      metrics->CreateAndRegisterPrimitiveMetric<int64_t>(
          "resource-broker.requests-total", 0);
  requests_fulfilled_metric_ =
      metrics->CreateAndRegisterPrimitiveMetric<int64_t>(
          "resource-broker.requests-fulfilled", 0);
  requests_failed_metric_ =
      metrics->CreateAndRegisterPrimitiveMetric<int64_t>(
          "resource-broker.requests-failed", 0);
  requests_rejected_metric_ =
      metrics->CreateAndRegisterPrimitiveMetric<int64_t>(
          "resource-broker.requests-rejected", 0);
  requests_timedout_metric_ =
      metrics->CreateAndRegisterPrimitiveMetric<int64_t>(
          "resource-broker.requests-timedout", 0);
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
      } catch (TTransportException& e) {
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
  dest.queue = src.queue;
  dest.resources = src.resources;
}

// Creates a Llama release request from a resource broker release request.
void ResourceBroker::CreateLlamaReleaseRequest(const TResourceBrokerReleaseRequest& src,
    llama::TLlamaAMReleaseRequest& dest) {
  dest.version = llama::TLlamaServiceVersion::V1;
  dest.am_handle = llama_handle_;
  dest.reservation_id << src.reservation_id;
}

void ResourceBroker::ReservationPromise::FillReservation(
    const llama::TUniqueId& reservation_id,
    const vector<llama::TAllocatedResource>& allocated_resources) {
  reservation_->allocated_resources.clear();
  // TODO: Llama returns a dump of all resources that we need to manually
  // group by reservation id. Can Llama do the grouping for us?
  BOOST_FOREACH(const llama::TAllocatedResource& resource, allocated_resources) {
    // Ignore resources that don't belong to the given reservation id.
    if (resource.reservation_id != reservation_id) continue;
    TNetworkAddress host = MakeNetworkAddress(resource.location);
    reservation_->allocated_resources[host] = resource;
    VLOG_QUERY << "Getting allocated resource for reservation id "
               << reservation_id << " and location " << host;
  }
  TUniqueId id;
  id << reservation_id;
  reservation_->__set_reservation_id(id);
}

Status ResourceBroker::Reserve(const TResourceBrokerReservationRequest& request,
    TResourceBrokerReservationResponse* response) {
  VLOG_QUERY << "Sending reservation request: " << request;
  requests_total_metric_->Increment(1);

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
    } catch (TTransportException& e) {
      VLOG_RPC << "Retrying Reserve: " << e.what();
      status = llama_client.Reopen();
      if (!status.ok()) continue;
      llama_client->Reserve(llama_response, llama_request);
    }
    request_rpc_time_metric_->Update(sw.ElapsedTime() / (1000.0 * 1000.0 * 1000.0));

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
    requests_failed_metric_->Increment(1);
    return Status("Reservation request aborted due to connectivity issues with Llama.");
  }

  // Check the status of the response.
  Status request_status = LlamaStatusToImpalaStatus(llama_response.status);
  if (!request_status.ok()) {
    requests_failed_metric_->Increment(1);
    return request_status;
  }

  // Add a promise to the pending-requests map and wait for it to be fulfilled by
  // the Llama via an async call into LlamaNotificationThriftIf::AMNotification().
  ReservationPromise reservation_promise(response);
  {
    lock_guard<mutex> l(requests_lock_);
    pending_requests_[llama_response.reservation_id] = &reservation_promise;
  }

  DCHECK(request.__isset.request_timeout);
  bool timed_out = false;
  bool request_granted = reservation_promise.Get(request.request_timeout, &timed_out);

  // Remove the promise from the pending-requests map.
  {
    lock_guard<mutex> l(requests_lock_);
    pending_requests_.erase(llama_response.reservation_id);
  }

  if (!timed_out) {
    request_response_time_metric_->Update(sw.ElapsedTime() / (1000.0 * 1000.0 * 1000.0));
  }
  if (timed_out) {
    // Set the reservation_id to release it from Llama.
    TUniqueId reservation_id;
    reservation_id << llama_response.reservation_id;
    response->__set_reservation_id(reservation_id);
    requests_timedout_metric_->Increment(1);
    stringstream error_msg;
    error_msg << "Resource reservation request exceeded timeout of "
              << request.request_timeout << "ms.";
    return Status(error_msg.str());
  }
  if (!request_granted) {
    requests_rejected_metric_->Increment(1);
    return Status("Resource reservation request was rejected.");
  }

  VLOG_QUERY << "Fulfilled reservation with id: " << llama_response.reservation_id;
  requests_fulfilled_metric_->Increment(1);
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
    } catch (TTransportException& e) {
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
    ReservationPromise* reservation_promise = pending_requests_[res_id];
    if (reservation_promise == NULL) {
      LOG(WARNING) << "Ignoring unknown allocated reservation id " << res_id;
      continue;
    }
    reservation_promise->FillReservation(res_id, request.allocated_resources);
    reservation_promise->Set(true);
  }

  // Process rejected allocations.
  BOOST_FOREACH(const llama::TUniqueId& res_id, request.rejected_reservation_ids) {
    ReservationPromise* reservation_promise = pending_requests_[res_id];
    if (reservation_promise == NULL) {
      LOG(WARNING) << "Ignoring unknown rejected reservation id " << res_id;
      continue;
    }
    reservation_promise->Set(false);
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
    } catch (TTransportException& e) {
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

ostream& operator<<(ostream& os, const TResourceBrokerReservationRequest& request) {
  os << "Reservation Request("
     << "queue=" << request.queue << " "
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
     << "resources=[";
  int count = 0;
  map<TNetworkAddress, llama::TAllocatedResource>::const_iterator alloc_resource;
  for (alloc_resource = reservation.allocated_resources.begin();
      alloc_resource != reservation.allocated_resources.end();
      ++alloc_resource) {
    os << alloc_resource->second;
    ++count;
    if (count != reservation.allocated_resources.size()) os << ",";
  }
  os << "])";
  return os;
}

}
