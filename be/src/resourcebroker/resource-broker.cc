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

using namespace std;
using namespace impala;
using namespace boost;
using namespace boost::algorithm;
using namespace boost::uuids;

using namespace ::apache::thrift::server;
using namespace ::apache::thrift::transport;

namespace impala {

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
    llama_client_cache_(new ClientCache<llama::LlamaAMServiceClient>()) {
  DCHECK(metrics != NULL);
  request_rpc_time_metric_ =
      metrics->RegisterMetric(
          new StatsMetric<double>("resource-broker-request-rpc-time"));
  request_response_time_metric_ =
      metrics->RegisterMetric(
          new StatsMetric<double>("resource-broker-request-response-time"));
  requests_total_metric_ =
      metrics->CreateAndRegisterPrimitiveMetric<int64_t>(
          "resource-broker-requests-total", 0);
  requests_fulfilled_metric_ =
      metrics->CreateAndRegisterPrimitiveMetric<int64_t>(
          "resource-broker-requests-fulfilled", 0);
  requests_failed_metric_ =
      metrics->CreateAndRegisterPrimitiveMetric<int64_t>(
          "resource-broker-requests-failed", 0);
  requests_rejected_metric_ =
      metrics->CreateAndRegisterPrimitiveMetric<int64_t>(
          "resource-broker-requests-rejected", 0);
  requests_timedout_metric_ =
      metrics->CreateAndRegisterPrimitiveMetric<int64_t>(
          "resource-broker-requests-timedout", 0);
  requests_released_metric_ =
      metrics->CreateAndRegisterPrimitiveMetric<int64_t>(
          "resource-broker-requests-released", 0);
}

Status ResourceBroker::Init() {
  // The scheduler must have been set before calling Init().
  DCHECK(scheduler_ != NULL);
  DCHECK(llama_callback_thrift_iface_ != NULL);
  shared_ptr<TProcessor> llama_callback_proc(
      new llama::LlamaNotificationServiceProcessor(llama_callback_thrift_iface_));
  llama_callback_server_.reset(new ThriftServer("llama-callback", llama_callback_proc,
      llama_callback_address_.port, metrics_, 5));
  RETURN_IF_ERROR(llama_callback_server_->Start());

  // Generate client id for registration with Llama.
  DCHECK(llama_client_id_.empty());
  random_generator uuid_generator;
  llama_client_id_= lexical_cast<string>(uuid_generator());

  // Register this resource broker with Llama.
  Status status;
  ClientConnection<llama::LlamaAMServiceClient> llama_client(llama_client_cache_.get(),
      llama_address_, &status);
  RETURN_IF_ERROR(status);
  llama::TLlamaAMRegisterRequest request;
  request.__set_version(llama::TLlamaServiceVersion::V1);
  request.__set_client_id(llama_client_id_);
  llama::TNetworkAddress callback_address;
  callback_address << llama_callback_address_;
  request.__set_notification_callback_service(callback_address);

  llama::TLlamaAMRegisterResponse response;
  LOG(INFO) << "Registering Resource Broker with Llama at " << llama_address_;
  llama_client->Register(response, request);
  RETURN_IF_ERROR(LlamaStatusToImpalaStatus(
      response.status, "Failed to register Resource Broker with Llama."));
  llama_handle_ = response.am_handle;
  LOG(INFO) << "Received Llama client handle " << llama_handle_;
  RETURN_IF_ERROR(RefreshLlamaNodes());
  LOG(INFO) << "Llama Nodes [" << join(llama_nodes_, ", ") << "]";
  return Status::OK;
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
    VLOG_QUERY << "Getting allocated resource for reservation id "
               << reservation_id << " and location " << resource.location;
    TNetworkAddress host = MakeNetworkAddress(resource.location);
    reservation_->allocated_resources[host] = resource;
  }
  reservation_->reservation_id << reservation_id;
}

Status ResourceBroker::Reserve(const TResourceBrokerReservationRequest& request,
    TResourceBrokerReservationResponse* response) {
  VLOG_QUERY << "Sending reservation request: " << request;

  llama::TLlamaAMReservationRequest llama_request;
  llama::TLlamaAMReservationResponse llama_response;
  CreateLlamaReservationRequest(request, llama_request);

  Status status;
  ClientConnection<llama::LlamaAMServiceClient> llama_client(llama_client_cache_.get(),
      llama_address_, &status);
  RETURN_IF_ERROR(status);

  requests_total_metric_->Increment(1);
  MonotonicStopWatch sw;
  sw.Start();
  try {
    llama_client->Reserve(llama_response, llama_request);
  } catch (TTransportException& e) {
    VLOG_RPC << "Retrying Reserve: " << e.what();
    status = llama_client.Reopen();
    if (!status.ok()) {
      return status;
    }
    llama_client->Reserve(llama_response, llama_request);
  }
  request_rpc_time_metric_->Update(sw.ElapsedTime() / (1000.0 * 1000.0 * 1000.0));

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

  int64_t timeout = DEFAULT_REQUEST_TIMEOUT;
  if (request.__isset.request_timeout) timeout = request.request_timeout;
  bool timed_out = false;
  bool request_granted = reservation_promise.Get(timeout, &timed_out);

  // Remove the promise from the pending-requests map.
  {
    lock_guard<mutex> l(requests_lock_);
    pending_requests_.erase(llama_response.reservation_id);
  }

  if (!timed_out) {
    request_response_time_metric_->Update(sw.ElapsedTime() / (1000.0 * 1000.0 * 1000.0));
  }
  if (timed_out) {
    requests_timedout_metric_->Increment(1);
    return Status("Resource reservation request timed out.");
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

  Status status;
  ClientConnection<llama::LlamaAMServiceClient> llama_client(llama_client_cache_.get(),
      llama_address_, &status);
  RETURN_IF_ERROR(status);
  try {
    llama_client->Release(llama_response, llama_request);
  } catch (TTransportException& e) {
    VLOG_RPC << "Retrying Release: " << e.what();
    status = llama_client.Reopen();
    if (!status.ok()) {
      return status;
    }
    llama_client->Release(llama_response, llama_request);
  }
  RETURN_IF_ERROR(LlamaStatusToImpalaStatus(llama_response.status));
  requests_released_metric_->Increment(1);

  VLOG_QUERY << "Released reservation with id " << request.reservation_id;
  return Status::OK;
}

void ResourceBroker::AMNotification(const llama::TLlamaAMNotificationRequest& request,
    llama::TLlamaAMNotificationResponse& response) {
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

  response.status.__set_status_code(llama::TStatusCode::OK);
}

void ResourceBroker::NMNotification(const llama::TLlamaNMNotificationRequest& request,
    llama::TLlamaNMNotificationResponse& response) {
}

Status ResourceBroker::RefreshLlamaNodes() {
  Status status;
  ClientConnection<llama::LlamaAMServiceClient> llama_client(llama_client_cache_.get(),
      llama_address_, &status);
  RETURN_IF_ERROR(status);
  llama::TLlamaAMGetNodesRequest request;
  request.__set_am_handle(llama_handle_);
  request.__set_version(llama::TLlamaServiceVersion::V1);
  llama::TLlamaAMGetNodesResponse response;
  llama_client->GetNodes(response, request);
  RETURN_IF_ERROR(LlamaStatusToImpalaStatus(response.status));
  llama_nodes_ = response.nodes;

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

}

