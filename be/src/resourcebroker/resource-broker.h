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
#include <boost/uuid/uuid.hpp>

#include "runtime/client-cache.h"
#include "util/non-primitive-metrics.h"
#include "util/promise.h"
#include "gen-cpp/LlamaAMService.h"
#include "gen-cpp/LlamaNotificationService.h"
#include "gen-cpp/ResourceBrokerService_types.h"

namespace impala {

class QueryResourceMgr;
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

  // Requests more resources from Llama for an existing reservation. Blocks until the
  // request has been granted or denied.
  Status Expand(const TResourceBrokerExpansionRequest& request,
      TResourceBrokerExpansionResponse* response);

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

  // Retrieves or creates a new QueryResourceMgr for the given query ID. Returns true if
  // this is the first 'checkout' of this QueryResourceMgr, false otherwise. The other
  // parameters are passed to the QueryResourceMgr constructor.
  bool GetQueryResourceMgr(const TUniqueId& query_id,
      const TUniqueId& reservation_id, const TNetworkAddress& local_resource_address,
      QueryResourceMgr** res_mgr);

  // Decrements the reference count for a particular QueryResourceMgr. If this is the last
  // reference (i.e. the ref count goes to 0), the QueryResourceMgr is deleted. It's an
  // error to call this with a query_id that does not have a registered QueryResourceMgr.
  void UnregisterQueryResourceMgr(const TUniqueId& query_id);

 private:
  typedef std::map<TNetworkAddress, llama::TAllocatedResource> ResourceMap;

  // Registers this resource broker with the Llama. Returns a non-OK status if the
  // registration failed.
  Status RegisterWithLlama();

  // Registers this resource broker with the Llama and refreshes the Llama nodes
  // after a successful registration. Returns a non-OK status if the registration
  // or the node refresh failed.
  Status RegisterAndRefreshLlama();

  // TODO: Add comments before pushing changes once we agree on the high-level approach.
  template <typename LlamaReqType, typename LlamaRespType>
  Status LlamaRpc(LlamaReqType* request, LlamaRespType* response,
      StatsMetric<double>* rpc_time_metric);

  template <typename LlamaReqType, typename LlamaRespType>
  void SendLlamaRpc(ClientConnection<llama::LlamaAMServiceClient>* llama_client,
      const LlamaReqType& request, LlamaRespType* response);

  template <typename LlamaReqType, typename LlamaRespType>
  Status HandleLlamaRestart(const LlamaReqType& request, LlamaRespType* response);

  // Detects Llama restarts from the given return status of a Llama RPC.
  bool LlamaHasRestarted(const llama::TStatus& status) const;

  // Creates a Llama reservation request from a resource broker reservation request.
  void CreateLlamaReservationRequest(const TResourceBrokerReservationRequest& src,
      llama::TLlamaAMReservationRequest& dest);

  // Creates a Llama release request from a resource broker release request.
  void CreateLlamaReleaseRequest(const TResourceBrokerReleaseRequest& src,
      llama::TLlamaAMReleaseRequest& dest);

  // Wait for a reservation or expansion request to be fulfilled by the Llama via an async
  // call into LlamaNotificationThriftIf::AMNotification(), or for a timeout to occur (in
  // which case *timed_out is set to true). If the request is fulfilled, resources and
  // reservation_id are populated.
  bool WaitForNotification(const llama::TUniqueId& request_id, int64_t timeout,
      TUniqueId* reservation_id, ResourceMap* resources, bool* timed_out);

  // Address where the Llama service is running on.
  TNetworkAddress llama_address_;

  // Address of thrift server started in this resource broker to handle
  // Llama notifications.
  TNetworkAddress llama_callback_address_;

  Metrics* metrics_;

  Scheduler* scheduler_;

  // Accumulated statistics on the time taken to RPC a reservation request and receive
  // an acknowledgement from Llama.
  StatsMetric<double>* reservation_rpc_time_metric_;

  // Accumulated statistics on the time taken to complete a reservation request
  // (granted or denied). The time includes the request RPC to Llama and the time
  // the requesting thread waits on the pending_requests_'s promise.
  // The metric does not include requests that timed out.
  StatsMetric<double>* reservation_response_time_metric_;

  // Total number of reservation requests.
  Metrics::PrimitiveMetric<int64_t>* reservation_requests_total_metric_;

  // Number of fulfilled reservation requests.
  Metrics::PrimitiveMetric<int64_t>* reservation_requests_fulfilled_metric_;

  // Reservation requests that failed due to a malformed request or an internal
  // error in Llama.
  Metrics::PrimitiveMetric<int64_t>* reservation_requests_failed_metric_;

  // Number of well-formed reservation requests rejected by the central scheduler.
  Metrics::PrimitiveMetric<int64_t>* reservation_requests_rejected_metric_;

  // Number of well-formed reservation requests that did not get fulfilled within
  // the timeout period.
  Metrics::PrimitiveMetric<int64_t>* reservation_requests_timedout_metric_;

  // Accumulated statistics on the time taken to RPC an expansion request and receive an
  // acknowledgement from Llama.
  StatsMetric<double>* expansion_rpc_time_metric_;

  // Accumulated statistics on the time taken to complete an expansion request
  // (granted or denied). The time includes the request RPC to Llama and the time
  // the requesting thread waits on the pending_requests_'s promise.
  // The metric does not include requests that timed out.
  StatsMetric<double>* expansion_response_time_metric_;

  // Total number of expansion requests.
  Metrics::PrimitiveMetric<int64_t>* expansion_requests_total_metric_;

  // Number of fulfilled expansion requests.
  Metrics::PrimitiveMetric<int64_t>* expansion_requests_fulfilled_metric_;

  // Expansion requests that failed due to a malformed request or an internal
  // error in Llama.
  Metrics::PrimitiveMetric<int64_t>* expansion_requests_failed_metric_;

  // Number of well-formed expansion requests rejected by the central scheduler.
  Metrics::PrimitiveMetric<int64_t>* expansion_requests_rejected_metric_;

  // Number of well-formed expansion requests that did not get fulfilled within
  // the timeout period.
  Metrics::PrimitiveMetric<int64_t>* expansion_requests_timedout_metric_;


  // Total number of fulfilled reservation requests that have been released.
  Metrics::PrimitiveMetric<int64_t>* requests_released_metric_;

  // Client id used to register with Llama. Set in Init().
  boost::uuids::uuid llama_client_id_;

  // Thrift API implementation which proxies Llama notifications onto this ResourceBroker.
  boost::shared_ptr<llama::LlamaNotificationServiceIf> llama_callback_thrift_iface_;
  boost::scoped_ptr<ThriftServer> llama_callback_server_;

  // Cache of Llama client connections.
  boost::scoped_ptr<ClientCache<llama::LlamaAMServiceClient> > llama_client_cache_;

  // Lock to ensure that only a single registration with Llama is sent, e.g.,
  // when multiple concurrent requests realize that Llama has restarted.
  boost::mutex llama_registration_lock_;

  // Handle received from Llama during registration. Set in RegisterWithLlama().
  llama::TUniqueId llama_handle_;

  // List of nodes registered with Llama. Set in RefreshLlamaNodes().
  std::vector<std::string> llama_nodes_;

  // Used to coordinate between AMNotification() and WaitForNotification(). Either method
  // might create this object (if it's the first to look up a reservation in
  // pending_requests_). Callers of Reserve() and Expand() are blocked (via
  // WaitForNotification()), on a ReservationFulfillment's promise().
  class ReservationFulfillment {
   public:
    // Promise is set to true if the reservation or expansion request was granted, false
    // if it was rejected by Yarn.
    Promise<bool>* promise() { return &promise_; }

    // Called by WaitForNotification() to populate a map of resources once the
    // corresponding request has returned successfully (and promise() therefore has
    // returned true).
    // TODO: Can we remove reservation_id?
    void GetResources(ResourceMap* resources, TUniqueId* reservation_id);

    void SetResources(const std::vector<llama::TAllocatedResource>& resources,
        const llama::TUniqueId& id) {
      allocated_resources_ = resources;
      reservation_id_ = id;
    }

   private:
    // Promise object that WaitForNotification() waits on and AMNotification() signals.
    Promise<bool> promise_;

    // Filled in by AMNotification(), so that WaitForNotification() can read the set of
    // allocated_resources without AMNotification() having to wait (hence the copy is
    // deliberate, since the original copy may go out of scope).
    std::vector<llama::TAllocatedResource> allocated_resources_;

    // The ID for this reservation
    llama::TUniqueId reservation_id_;
  };

  // Protects pending_requests_;
  boost::mutex requests_lock_;

  // Maps from the unique reservation id received from Llama as response to a reservation
  // request to a structure that contains the results of the reservation request.
  typedef boost::unordered_map<llama::TUniqueId,
                               boost::shared_ptr<ReservationFulfillment> > FulfillmentMap;
  FulfillmentMap pending_requests_;

  // Protects query_resource_mgrs_
  boost::mutex query_resource_mgrs_lock_;
  typedef boost::unordered_map<TUniqueId, std::pair<int32_t, QueryResourceMgr*> >
        QueryResourceMgrsMap;

  // Map from query ID to a (ref_count, QueryResourceMgr*) pair, i.e. one QueryResourceMgr
  // per query. The refererence count is always non-zero - once it hits zero the entry in
  // the map is removed and the QueryResourceMgr is deleted.
  QueryResourceMgrsMap query_resource_mgrs_;
};

std::ostream& operator<<(std::ostream& os,
    const TResourceBrokerReservationRequest& request);

std::ostream& operator<<(std::ostream& os,
    const TResourceBrokerReservationResponse& reservation);

std::ostream& operator<<(std::ostream& os,
    const TResourceBrokerExpansionRequest& request);

std::ostream& operator<<(std::ostream& os,
    const TResourceBrokerExpansionResponse& expansion);

}

#endif
