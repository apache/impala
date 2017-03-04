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

#ifndef IMPALA_RUNTIME_QUERY_STATE_H
#define IMPALA_RUNTIME_QUERY_STATE_H

#include <memory>
#include <unordered_map>
#include <boost/scoped_ptr.hpp>

#include "common/atomic.h"
#include "common/object-pool.h"
#include "gen-cpp/ImpalaInternalService_types.h"
#include "gen-cpp/Types_types.h"
#include "runtime/tmp-file-mgr.h"
#include "util/spinlock.h"
#include "util/uid-util.h"

namespace kudu { namespace client { class KuduClient; } }

namespace impala {

class FragmentInstanceState;
class MemTracker;
class ReservationTracker;

/// Central class for all backend execution state (example: the FragmentInstanceStates
/// of the individual fragment instances) created for a particular query.
/// This class contains or makes accessible state that is shared across fragment
/// instances; in contrast, fragment instance-specific state is collected in
/// FragmentInstanceState.
///
/// The lifetime of an instance of this class is dictated by a reference count.
/// Any thread that executes on behalf of a query, and accesses any of its state,
/// must obtain a reference to the corresponding QueryState and hold it for at least the
/// duration of that access. The reference is obtained and released via
/// QueryExecMgr::Get-/ReleaseQueryState() or via QueryState::ScopedRef (the latter
/// for references limited to the scope of a single function or block).
/// As long as the reference count is greater than 0, all query state (contained
/// either in this class or accessible through this class, such as the
/// FragmentInstanceStates) is guaranteed to be alive.
///
/// Thread-safe, unless noted otherwise.
class QueryState {
 public:
  /// Use this class to obtain a QueryState for the duration of a function/block,
  /// rather than manually via QueryExecMgr::Get-/ReleaseQueryState().
  /// Pattern:
  /// {
  ///   QueryState::ScopedRef qs(qid);
  ///   if (qs->query_state() == nullptr) <do something, such as return>
  ///   ...
  /// }
  class ScopedRef {
   public:
    /// Looks up the query state with GetQueryState(). The query state is non-NULL if
    /// the query was already registered.
    ScopedRef(const TUniqueId& query_id);
    ~ScopedRef();

    /// may return nullptr
    QueryState* get() const { return query_state_; }
    QueryState* operator->() const { return query_state_; }

   private:
    QueryState* query_state_;
    DISALLOW_COPY_AND_ASSIGN(ScopedRef);
  };

  /// a shared pool for all objects that have query lifetime
  ObjectPool* obj_pool() { return &obj_pool_; }

  /// This TQueryCtx was copied from the first fragment instance which led to the
  /// creation of this QueryState. For all subsequently arriving fragment instances the
  /// desc_tbl in this context will be incorrect, therefore query_ctx().desc_tbl should
  /// not be used. This restriction will go away with the switch to a per-query exec
  /// rpc.
  const TQueryCtx& query_ctx() const { return query_ctx_; }

  const TUniqueId& query_id() const { return query_ctx_.query_id; }

  const TQueryOptions& query_options() const {
    return query_ctx_.client_request.query_options;
  }

  MemTracker* query_mem_tracker() const { return query_mem_tracker_; }
  ReservationTracker* buffer_reservation() const { return buffer_reservation_; }
  TmpFileMgr::FileGroup* file_group() const { return file_group_; }

  /// Sets up state required for fragment execution: memory reservations, etc. Fails
  /// if resources could not be acquired. Safe to call concurrently and idempotent:
  /// the first thread to call this does the setup work.
  Status Prepare();

  /// Registers a new FInstanceState.
  void RegisterFInstance(FragmentInstanceState* fis);

  /// Returns the instance state or nullptr if the instance id has not previously
  /// been registered. The returned FIS is valid for the duration of the QueryState.
  FragmentInstanceState* GetFInstanceState(const TUniqueId& instance_id);

  /// Called once the query is complete to release any resources.
  /// Must be called before destroying the QueryState.
  void ReleaseResources();

  /// Gets a KuduClient for this list of master addresses. It will lookup and share
  /// an existing KuduClient if possible. Otherwise, it will create a new KuduClient
  /// internally and return a pointer to it. All KuduClients accessed through this
  /// interface are owned by the QueryState. Thread safe.
  Status GetKuduClient(const std::vector<std::string>& master_addrs,
                       kudu::client::KuduClient** client);

  ~QueryState();

 private:
  friend class QueryExecMgr;

  static const int DEFAULT_BATCH_SIZE = 1024;

  TQueryCtx query_ctx_;

  ObjectPool obj_pool_;
  AtomicInt32 refcnt_;

  /// Held for duration of Prepare(). Protects 'prepared_',
  /// 'prepare_status_' and the members initialized in Prepare().
  SpinLock prepare_lock_;

  /// Non-OK if Prepare() failed the first time it was called.
  /// All subsequent calls to Prepare() return this status.
  Status prepare_status_;

  /// True if Prepare() executed and finished successfully.
  bool prepared_;

  /// True if and only if ReleaseResources() has been called.
  bool released_resources_;

  SpinLock fis_map_lock_; // protects fis_map_

  /// map from instance id to its state (owned by obj_pool_)
  std::unordered_map<TUniqueId, FragmentInstanceState*> fis_map_;

  /// The top-level MemTracker for this query (owned by obj_pool_).
  MemTracker* query_mem_tracker_;

  /// Buffer reservation for this query (owned by obj_pool_)
  /// Only non-null in backend tests that explicitly enabled the new buffer pool
  /// TODO: this will always be non-null once IMPALA-3200 is done
  ReservationTracker* buffer_reservation_;

  /// Temporary files for this query (owned by obj_pool_)
  /// Only non-null in backend tests the explicitly enabled the new buffer pool
  /// TODO: this will always be non-null once IMPALA-3200 is done
  TmpFileMgr::FileGroup* file_group_;

  SpinLock kudu_client_map_lock_; // protects kudu_client_map_

  /// Opaque type for storing the pointer to the KuduClient. This allows us
  /// to avoid including Kudu header files.
  struct KuduClientPtr;

  /// Map from the master addresses string for a Kudu table to the KuduClientPtr for
  /// accessing that table. The master address string is constructed by joining
  /// the master address list entries with a comma separator.
  typedef std::unordered_map<std::string, std::unique_ptr<KuduClientPtr>> KuduClientMap;

  /// Map for sharing KuduClients between fragment instances. Each Kudu table has
  /// a list of master addresses stored in the Hive Metastore. This map requires
  /// that the master address lists be identical in order to share a KuduClient.
  KuduClientMap kudu_client_map_;

  /// Create QueryState w/ copy of query_ctx and refcnt of 0.
  /// The query is associated with the resource pool named 'pool'
  QueryState(const TQueryCtx& query_ctx, const std::string& pool);

  /// Called from Prepare() to initialize MemTrackers.
  void InitMemTrackers(const std::string& pool);

  /// Called from PrepareForExecution() to setup buffer reservations and the
  /// file group. Fails if required resources are not available.
  Status InitBufferPoolState();
};
}

#endif
