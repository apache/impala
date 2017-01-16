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


#ifndef IMPALA_RUNTIME_QUERY_EXEC_MGR_H
#define IMPALA_RUNTIME_QUERY_EXEC_MGR_H

#include <boost/thread/mutex.hpp>
#include <unordered_map>

#include "common/status.h"
#include "util/uid-util.h"
#include "gen-cpp/Types_types.h"

namespace impala {

class QueryState;
class Thread;
class TExecPlanFragmentParams;
class TQueryCtx;
class TUniqueId;
class FragmentInstanceState;

/// A daemon-wide registry and manager of QueryStates. This is the central
/// entry point for gaining refcounted access to a QueryState. It also initiates
/// fragment instance execution.
/// Thread-safe.
///
/// TODO: as part of Impala-2550 (per-query exec rpc)
/// replace Start-/CancelFInstance() with StartQuery()/CancelQuery()
class QueryExecMgr {
 public:
  /// Initiates execution of this fragment instance in a newly created thread.
  /// Also creates a QueryState for this query, if none exists.
  /// In both cases it increases the refcount prior to instance execution and decreases
  /// it after execution finishes.
  ///
  /// Returns an error if there was some unrecoverable problem before the fragment
  /// was started (like low memory). In that case, no QueryState is created or has its
  /// refcount incremented. After this call returns, it is legal to call
  /// FragmentInstanceState::Cancel() on this fragment instance, regardless of the
  /// return value of this function.
  Status StartFInstance(const TExecPlanFragmentParams& params);

  /// Creates a QueryState for the given query with the provided parameters. Only valid
  /// to call if the QueryState does not already exist. The caller must call
  /// ReleaseQueryState() with the returned QueryState to decrement the refcount.
  QueryState* CreateQueryState(
      const TQueryCtx& query_ctx, const std::string& request_pool);

  /// If a QueryState for the given query exists, increments that refcount and returns
  /// the QueryState, otherwise returns nullptr.
  QueryState* GetQueryState(const TUniqueId& query_id);

  /// Decrements the refcount for the given QueryState.
  void ReleaseQueryState(QueryState* qs);

 private:
  /// protects qs_map_
  boost::mutex qs_map_lock_;

  /// map from query id to QueryState (owned by us)
  std::unordered_map<TUniqueId, QueryState*> qs_map_;

  /// Gets the existing QueryState or creates a new one if not present.
  /// 'created' is set to true if it was created, false otherwise.
  QueryState* GetOrCreateQueryState(
      const TQueryCtx& query_ctx, const std::string& request_pool, bool* created);

  /// Execute instance.
  void ExecFInstance(FragmentInstanceState* fis);
};
}

#endif
