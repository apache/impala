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

#ifndef RESOURCE_BROKER_H_
#define RESOURCE_BROKER_H_

#include <boost/unordered_map.hpp>
#include <boost/scoped_ptr.hpp>

#include "runtime/client-cache.h"
#include "util/non-primitive-metrics.h"
#include "util/promise.h"
#include "gen-cpp/LlamaAMService.h"
#include "gen-cpp/LlamaNotificationService.h"
#include "gen-cpp/ResourceBrokerService_types.h"

namespace impala {

class Status;
class Metrics;
class Scheduler;
class ResourceBrokerNotificationServiceClient;

// Mediates resource-reservation requests between Impala and Yarn via the Llama service.
// The resource broker requests resources via the Llama's thrift interface and exposes
// a thrift server for the Llama to notify it of granted/denied/preempted resource
// reservations. The reserve/release API of the resource broker is blocking.
// TODO: Implement NM notification service.
class ResourceBroker {
 public:
  ResourceBroker(const TNetworkAddress& llama_address,
      const TNetworkAddress& llama_callback_address, Metrics* metrics);

  // Register this resource broker with LLama and starts the Llama callback service.
  // Returns a non-OK status if the callback service failed to start (e.g., port in use)
  // or if registration with the Llama failed (e.g., connection to Llama failed).
  Status Init();

  // Closes the llama_client_cache_ and joins the llama_callback_server_.
  void Close();

  // Requests resources from Llama. Blocks until the request has been granted or denied.
  Status Reserve(const TResourceBrokerReservationRequest& request,
      TResourceBrokerReservationResponse* response);

  // Releases resources from Llama.
  Status Release(const TResourceBrokerReleaseRequest& request,
      TResourceBrokerReleaseResponse* response);

  // Handles asynchronous Llama Application Master (AM) notifications including
  // granted/denied/preempted reservations and resources.
  void AMNotification(const llama::TLlamaAMNotificationRequest& request,
      llama::TLlamaAMNotificationResponse& response);

  // Handles asynchronous notifications from the Llama Node Manager (NM)
  // auxiliary service, in particular, incoming Yarn container allocations
  // that are going to claim resources.
  // TODO: Implement once NM service is fully functional.
  void NMNotification(const llama::TLlamaNMNotificationRequest& request,
      llama::TLlamaNMNotificationResponse& response);

  const std::vector<std::string>& llama_nodes() { return llama_nodes_; }

  // Retrieves the nodes known to Llama and stores them in llama_nodes_.
  Status RefreshLlamaNodes();

  void set_scheduler(Scheduler* scheduler) { scheduler_ = scheduler; };

 private:
  // Creates a Llama reservation request from a resource broker reservation request.
  void CreateLlamaReservationRequest(const TResourceBrokerReservationRequest& src,
      llama::TLlamaAMReservationRequest& dest);

  // Creates a Llama release request from a resource broker release request.
  void CreateLlamaReleaseRequest(const TResourceBrokerReleaseRequest& src,
      llama::TLlamaAMReleaseRequest& dest);

  // Address where the Llama service is running on.
  TNetworkAddress llama_address_;

  // Address of thrift server started in this resource broker to handle
  // Llama notifications.
  TNetworkAddress llama_callback_address_;

  Metrics* metrics_;

  Scheduler* scheduler_;

  // Accumulated statistics on the time taken to RPC a reservation request and receive
  // an acknowledgement from Llama.
  StatsMetric<double>* request_rpc_time_metric_;

  // Accumulated statistics on the time taken to complete a reservation request
  // (granted or denied). The time includes the request RPC to Llama and the time
  // the requesting thread waits on the pending_requests_'s promise.
  // The metric does not include requests that timed out.
  StatsMetric<double>* request_response_time_metric_;

  // Total number of reservation requests.
  Metrics::PrimitiveMetric<int64_t>* requests_total_metric_;

  // Number of fulfilled reservation requests.
  Metrics::PrimitiveMetric<int64_t>* requests_fulfilled_metric_;

  // Reservation requests that failed due to a malformed request or an internal
  // error in Llama.
  Metrics::PrimitiveMetric<int64_t>* requests_failed_metric_;

  // Number of well-formed reservation requests rejected by the central scheduler.
  Metrics::PrimitiveMetric<int64_t>* requests_rejected_metric_;

  // Number of well-formed reservation requests that did not get fulfilled within
  // the timeout period.
  Metrics::PrimitiveMetric<int64_t>* requests_timedout_metric_;

  // Total number of fulfilled reservation requests that have been released.
  Metrics::PrimitiveMetric<int64_t>* requests_released_metric_;

  // Client id used to register with Llama. Set in Init().
  std::string llama_client_id_;

  // Handle received from Llama during registration. Set in Init().
  llama::TUniqueId llama_handle_;

  // Thrift API implementation which proxies Llama notifications onto this ResourceBroker.
  boost::shared_ptr<llama::LlamaNotificationServiceIf> llama_callback_thrift_iface_;
  boost::scoped_ptr<ThriftServer> llama_callback_server_;

  // Cache of Llama client connections.
  boost::scoped_ptr<ClientCache<llama::LlamaAMServiceClient> > llama_client_cache_;

  // List of nodes registered with Llama. Set in Init() after registering with Llama.
  // Mostly for debugging now.
  std::vector<std::string> llama_nodes_;

  // Used to implement a blocking resource-reservation interface on top of Llama's
  // non-blocking request interface (the Llama asynchronously notifies the resource
  // broker of granted/denied reservations). Callers of Reserve() block on a reservation
  // promise until the Llama has issued a corresponding response via the resource
  // broker's callback service (or until a timeout has been reached).
  class ReservationPromise {
   public:
    ReservationPromise(TResourceBrokerReservationResponse* reservation)
      : reservation_(reservation) {
      DCHECK(reservation != NULL);
    }

    // Returns true if the reservation request was granted, false if the request
    // was denied by the central resource manager (Yarn).
    bool Get(int64_t timeout_millis, bool* timed_out) {
      return promise_.Get(timeout_millis, timed_out);
    }
    void Set(bool val) { promise_.Set(val); }

    // Populates reservation_ based on the allocated_resources.
    void FillReservation(const llama::TUniqueId& reservation_id,
        const std::vector<llama::TAllocatedResource>& allocated_resources);

   private:
    TResourceBrokerReservationResponse* reservation_;
    Promise<bool> promise_;
  };

  // Protects pending_requests_;
  boost::mutex requests_lock_;

  // Maps from the unique reservation id received from Llama as response to a
  // reservation request to a promise that will be set once that request has been
  // fulfilled or rejected by the Llama. The original resource requester (the scheduler)
  // blocks on the promise.
  boost::unordered_map<llama::TUniqueId, ReservationPromise*> pending_requests_;

  // Max time in milliseconds to wait for a resource request to be fulfilled by Llama.
  static const int64_t DEFAULT_REQUEST_TIMEOUT = 60000;
};

std::ostream& operator<<(std::ostream& os,
    const TResourceBrokerReservationRequest& request);

}

#endif
