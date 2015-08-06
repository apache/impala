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
#include "util/collection-metrics.h"
#include "util/promise.h"
#include "gen-cpp/LlamaAMService.h"
#include "gen-cpp/LlamaNotificationService.h"
#include "gen-cpp/ResourceBrokerService_types.h"

namespace impala {

class QueryResourceMgr;
class Status;
class MetricGroup;
class Scheduler;
class ResourceBrokerNotificationServiceClient;

/// Mediates resource-reservation requests between Impala and Yarn via the Llama service.
/// The resource broker requests resources via the Llama's thrift interface and exposes
/// a thrift server for the Llama to notify it of granted/denied/preempted resource
/// reservations. The reserve/release API of the resource broker is blocking.
/// The resource broker is configured with a list of Llama addresses that
/// are cycled through for failover.
/// TODO: Implement NM notification service.
class ResourceBroker {
 public:
  ResourceBroker(const std::vector<TNetworkAddress>& llama_addresses,
      const TNetworkAddress& llama_callback_address, MetricGroup* metrics);

  /// Register this resource broker with LLama and starts the Llama callback service.
  /// Returns a non-OK status if the callback service failed to start (e.g., port in use)
  /// or if registration with the Llama failed (e.g., connection to Llama failed).
  Status Init();

  /// Closes the llama_client_cache_ and joins the llama_callback_server_.
  void Close();

  /// Requests resources from Llama. Blocks until the request has been granted or denied.
  Status Reserve(const TResourceBrokerReservationRequest& request,
      TResourceBrokerReservationResponse* response);

  /// Requests more resources from Llama for an existing reservation. Blocks until the
  /// request has been granted or denied.
  Status Expand(const TResourceBrokerExpansionRequest& request,
      TResourceBrokerExpansionResponse* response);

  /// Removes the record of all resource requests associated with this reservationID
  /// (except the reservation request itself, if include_reservation is false) so that the
  /// per-node accounting is correct when plan fragments finish. Does not communicate this
  /// to Llama (i.e. only updates the local node's accounting), so the coordinator should
  /// always call Release() to make sure that Llama knows the resources have gone.
  void ClearRequests(const TUniqueId& reservation_id, bool include_reservation);

  /// Releases resources acquired from Llama for this reservation and all associated
  /// expansion requests across _all_ nodes. Should therefore only be called once per
  /// query.
  Status Release(const TResourceBrokerReleaseRequest& request,
      TResourceBrokerReleaseResponse* response);

  /// Handles asynchronous Llama Application Master (AM) notifications including
  /// granted/denied/preempted reservations and resources.
  void AMNotification(const llama::TLlamaAMNotificationRequest& request,
      llama::TLlamaAMNotificationResponse& response);

  /// Handles asynchronous notifications from the Llama Node Manager (NM)
  /// auxiliary service, in particular, incoming Yarn container allocations
  /// that are going to claim resources.
  /// TODO: Implement once NM service is fully functional.
  void NMNotification(const llama::TLlamaNMNotificationRequest& request,
      llama::TLlamaNMNotificationResponse& response);

  const std::vector<std::string>& llama_nodes() { return llama_nodes_; }

  /// Retrieves the nodes known to Llama and stores them in llama_nodes_.
  Status RefreshLlamaNodes();

  void set_scheduler(Scheduler* scheduler) { scheduler_ = scheduler; };

  /// Retrieves or creates a new QueryResourceMgr for the given query ID. Returns true if
  /// this is the first 'checkout' of this QueryResourceMgr, false otherwise. The other
  /// parameters are passed to the QueryResourceMgr constructor.
  bool GetQueryResourceMgr(const TUniqueId& query_id,
      const TUniqueId& reservation_id, const TNetworkAddress& local_resource_address,
      QueryResourceMgr** res_mgr);

  /// Decrements the reference count for a particular QueryResourceMgr. If this is the last
  /// reference (i.e. the ref count goes to 0), the QueryResourceMgr is deleted. It's an
  /// error to call this with a query_id that does not have a registered QueryResourceMgr.
  void UnregisterQueryResourceMgr(const TUniqueId& query_id);

 private:
  typedef std::map<TNetworkAddress, llama::TAllocatedResource> ResourceMap;

  bool has_standby_llama() { return llama_addresses_.size() > 1; }

  /// Registers this resource broker with the Llama. Cycles through the list of
  /// Llama addresses to find the active Llama which is accepting requests (if any).
  /// Returns a non-OK status if registration with any of the Llama's did not succeed
  /// within FLAGS_llama_registration_timeout_s seconds.
  /// Registration with the Llama is idempotent with respect to the llama_client_id_
  /// (see comment on llama_client_id_ for details).
  Status RegisterWithLlama();

  /// Issues the Llama RPC f where F is a thrift call taking LlamaReqType and returning
  /// LlamaRespType. If failures occur, this function handles re-registering with Llama
  /// if necessary and re-trying multiple times. If rpc_time_metric is non-NULL, the
  /// metric is updated upon success of the RPC. Returns a non-OK status if the RPC
  /// failed due to connectivity issues with the Llama. Returns OK if the RPC succeeded.
  template <class F, typename LlamaReqType, typename LlamaRespType>
  Status LlamaRpc(const F& f, LlamaReqType* request, LlamaRespType* response,
      StatsMetric<double>* rpc_time_metric);

  /// Re-registers with Llama to recover from the Llama being unreachable. Handles both
  /// Llama restart and failover. This function is a template to allow specialization on
  /// the Llama request/response type.
  template <typename LlamaReqType, typename LlamaRespType>
  Status ReRegisterWithLlama(const LlamaReqType& request, LlamaRespType* response);

  /// Detects Llama restarts from the given return status of a Llama RPC.
  bool LlamaHasRestarted(const llama::TStatus& status) const;

  /// Creates a Llama reservation request from a resource broker reservation request.
  void CreateLlamaReservationRequest(const TResourceBrokerReservationRequest& src,
      llama::TLlamaAMReservationRequest& dest);

  /// Creates a Llama release request from a resource broker release request.
  void CreateLlamaReleaseRequest(const TResourceBrokerReleaseRequest& src,
      llama::TLlamaAMReleaseRequest& dest);

  class PendingRequest;
  /// Wait for a reservation or expansion request to be fulfilled by the Llama via an async
  /// call into LlamaNotificationThriftIf::AMNotification(), or for a timeout to occur (in
  /// which case *timed_out is set to true). If the request is fulfilled, resources and
  /// reservation_id are populated.
  bool WaitForNotification(int64_t timeout, ResourceMap* resources, bool* timed_out,
      PendingRequest* reservation);

  /// Llama availability group.
  std::vector<TNetworkAddress> llama_addresses_;

  /// Indexes into llama_addresses_ indicating the currently active Llama.
  /// Protected by llama_registration_lock_.
  int active_llama_addr_idx_;

  /// Address of thrift server started in this resource broker to handle
  /// Llama notifications.
  TNetworkAddress llama_callback_address_;

  MetricGroup* metrics_;

  Scheduler* scheduler_;

  /// Address of the active Llama. A Llama is considered active once we have successfully
  /// registered with it. Set to "none" while registering with the Llama.
  StringProperty* active_llama_metric_;

  /// Llama handle received from the active Llama upon registration.
  /// Set to "none" while not registered with Llama.
  StringProperty* active_llama_handle_metric_;

  /// Accumulated statistics on the time taken to RPC a reservation request and receive
  /// an acknowledgement from Llama.
  StatsMetric<double>* reservation_rpc_time_metric_;

  /// Accumulated statistics on the time taken to complete a reservation request
  /// (granted or denied). The time includes the request RPC to Llama and the time
  /// the requesting thread waits on the pending_requests_'s promise.
  /// The metric does not include requests that timed out.
  StatsMetric<double>* reservation_response_time_metric_;

  /// Total number of reservation requests.
  IntCounter* reservation_requests_total_metric_;

  /// Number of fulfilled reservation requests.
  IntCounter* reservation_requests_fulfilled_metric_;

  /// Reservation requests that failed due to a malformed request or an internal
  /// error in Llama.
  IntCounter* reservation_requests_failed_metric_;

  /// Number of well-formed reservation requests rejected by the central scheduler.
  IntCounter* reservation_requests_rejected_metric_;

  /// Number of well-formed reservation requests that did not get fulfilled within
  /// the timeout period.
  IntCounter* reservation_requests_timedout_metric_;

  /// Accumulated statistics on the time taken to RPC an expansion request and receive an
  /// acknowledgement from Llama.
  StatsMetric<double>* expansion_rpc_time_metric_;

  /// Accumulated statistics on the time taken to complete an expansion request
  /// (granted or denied). The time includes the request RPC to Llama and the time
  /// the requesting thread waits on the pending_requests_'s promise.
  /// The metric does not include requests that timed out.
  StatsMetric<double>* expansion_response_time_metric_;

  /// Total number of expansion requests.
  IntCounter* expansion_requests_total_metric_;

  /// Number of fulfilled expansion requests.
  IntCounter* expansion_requests_fulfilled_metric_;

  /// Expansion requests that failed due to a malformed request or an internal
  /// error in Llama.
  IntCounter* expansion_requests_failed_metric_;

  /// Number of well-formed expansion requests rejected by the central scheduler.
  IntCounter* expansion_requests_rejected_metric_;

  /// Number of well-formed expansion requests that did not get fulfilled within
  /// the timeout period.
  IntCounter* expansion_requests_timedout_metric_;

  /// Total amount of memory currently allocated by Llama to this node
  UIntGauge* allocated_memory_metric_;

  /// Total number of vcpu cores currently allocated by Llama to this node
  UIntGauge* allocated_vcpus_metric_;

  /// Total number of fulfilled reservation requests that have been released.
  IntCounter* requests_released_metric_;

  /// Client id used to register with Llama. Set in Init(). Used to communicate to Llama
  /// whether this Impalad has restarted. Registration with Llama is idempotent if the
  /// same llama_client_id_ is passed, i.e., the same Llama handle is returned and
  /// resource allocations are preserved. From Llama's perspective an unknown
  /// llama_client_id_ indicates a new registration and all resources allocated by this
  /// Impalad under a different llama_client_id_ are consider lost and will be released.
  boost::uuids::uuid llama_client_id_;

  /// Thrift API implementation which proxies Llama notifications onto this ResourceBroker.
  boost::shared_ptr<llama::LlamaNotificationServiceIf> llama_callback_thrift_iface_;
  boost::scoped_ptr<ThriftServer> llama_callback_server_;

  /// Cache of Llama client connections.
  boost::scoped_ptr<ClientCache<llama::LlamaAMServiceClient> > llama_client_cache_;

  /// Lock to ensure that only a single registration with Llama is sent, e.g.,
  /// when multiple concurrent requests realize that Llama has restarted.
  boost::mutex llama_registration_lock_;

  /// Handle received from Llama during registration. Set in RegisterWithLlama().
  llama::TUniqueId llama_handle_;

  /// List of nodes registered with Llama. Set in RefreshLlamaNodes().
  std::vector<std::string> llama_nodes_;

  /// A PendingRequest tracks a single reservation or expansion request that is in flight
  /// to Llama. A new PendingRequest is created in either Expand() or Reserve(), and its
  /// promise() is blocked on there until a response is received for that request from
  /// Llama via AMNotification(), or until a timeout occurs.
  //
  /// Every request has a unique request_id which is assigned by the resource broker. Each
  /// request is also associated with exactly one reservation, via reservation_id(). This
  /// allows us to track which resources belong to which reservation, and to make sure that
  /// all are correctly accounted for when the reservation is released. Each reservation ID
  /// will belong to exactly one reservation request, and 0 or more expansion requests.
  class PendingRequest {
   public:
    PendingRequest(const llama::TUniqueId& reservation_id,
        const llama::TUniqueId& request_id, bool is_expansion)
        : reservation_id_(reservation_id), request_id_(request_id),
          is_expansion_(is_expansion) {
      DCHECK(is_expansion || reservation_id == request_id);
    }

    /// Promise is set to true if the reservation or expansion request was granted, false
    /// if it was rejected by Yarn. When promise()->Get() returns true,
    /// allocated_resources_ will be populated and it will be safe to call GetResources().
    Promise<bool>* promise() { return &promise_; }

    /// Called by WaitForNotification() to populate a map of resources once the
    /// corresponding request has returned successfully (and promise() therefore has
    /// returned true).
    void GetResources(ResourceMap* resources);

    /// Populates allocated_resources_ from all members of resources that match the given
    /// reservation id. Called in AMNotification().
    void SetResources(const std::vector<llama::TAllocatedResource>& resources);

    const llama::TUniqueId& request_id() const { return request_id_; }
    const llama::TUniqueId& reservation_id() const { return reservation_id_; }

    bool is_expansion() const { return is_expansion_; }

   private:
    /// Promise object that WaitForNotification() waits on and AMNotification() signals.
    Promise<bool> promise_;

    /// Filled in by AMNotification(), so that WaitForNotification() can read the set of
    /// allocated_resources without AMNotification() having to wait (hence the copy is
    /// deliberate, since the original copy may go out of scope).
    std::vector<llama::TAllocatedResource> allocated_resources_;

    /// The ID for the reservation associated with this request. There is always exactly
    /// one reservation associated with every request.
    llama::TUniqueId reservation_id_;

    /// The unique ID for this request. If this is a reservation request, request_id_ ==
    /// reservation_id_, otherwise this is generated during Expand().
    llama::TUniqueId request_id_;

    /// True if this is an expansion request, false if it is a reservation request
    bool is_expansion_;
  };

  /// Protects pending_requests_
  boost::mutex pending_requests_lock_;

  /// Map from unique request ID provided to Llama (for both reservation and expansion
  /// requests) to PendingRequest object used to coordinate when a response is received
  /// from Llama.
  typedef boost::unordered_map<llama::TUniqueId, PendingRequest*> PendingRequestMap;
  PendingRequestMap pending_requests_;

  /// An AllocatedRequest tracks resources allocated in response to one reservation or
  /// expansion request.
  class AllocatedRequest {
   public:
    AllocatedRequest(const llama::TUniqueId& reservation_id,
        uint64_t memory_mb, uint32_t vcpus, bool is_expansion)
        : reservation_id_(reservation_id), memory_mb_(memory_mb), vcpus_(vcpus),
          is_expansion_(is_expansion) { }

    const llama::TUniqueId reservation_id() const { return reservation_id_; }
    uint64_t memory_mb() const { return memory_mb_; }
    uint32_t vcpus() const { return vcpus_; }
    bool is_expansion() const { return is_expansion_; }

   private:
    /// The reservation ID for this request. Expansions all share the same reservation ID.
    llama::TUniqueId reservation_id_;

    /// The total memory allocated to this request
    uint64_t memory_mb_;

    /// The number of VCPUs allocated to this request
    uint32_t vcpus_;

    /// True if this is an expansion request, false if it is a reservation request
    bool is_expansion_;
  };

  /// Protectes allocated_requests_
  boost::mutex allocated_requests_lock_;

  /// Map from reservation ID to all satisfied requests - reservation and expansion -
  /// associated with that reservation. Used only for bookkeeping so that Impala can report
  /// on the current resource usage.
  typedef boost::unordered_map<llama::TUniqueId, std::vector<AllocatedRequest> >
      AllocatedRequestMap;
  AllocatedRequestMap allocated_requests_;

  /// Protects query_resource_mgrs_
  boost::mutex query_resource_mgrs_lock_;
  typedef boost::unordered_map<TUniqueId, std::pair<int32_t, QueryResourceMgr*> >
      QueryResourceMgrsMap;

  /// Map from query ID to a (ref_count, QueryResourceMgr*) pair, i.e. one QueryResourceMgr
  /// per query. The refererence count is always non-zero - once it hits zero the entry in
  /// the map is removed and the QueryResourceMgr is deleted.
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
