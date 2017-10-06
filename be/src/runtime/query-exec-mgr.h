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
#include "gen-cpp/Types_types.h"
#include "util/sharded-query-map-util.h"

namespace impala {

class QueryState;
class Thread;
class TExecPlanFragmentParams;
class TQueryCtx;
class TUniqueId;
class FragmentInstanceState;

/// A daemon-wide registry and manager of QueryStates. This is the central
/// entry point for gaining refcounted access to a QueryState. It also initiates
/// query execution.
/// Thread-safe.
class QueryExecMgr : public CacheLineAligned {
 public:
  /// Creates QueryState if it doesn't exist and initiates execution of all fragment
  /// instance for this query. All fragment instances hold a reference to their
  /// QueryState for the duration of their execution.
  ///
  /// Returns an error if there was some unrecoverable problem before any instance
  /// was started (like low memory). In that case, no QueryState is created.
  /// After this function returns, it is legal to call QueryState::Cancel(), regardless of
  /// the return value of this function.
  Status StartQuery(const TExecQueryFInstancesParams& params);

  /// Creates a QueryState for the given query with the provided parameters. Only valid
  /// to call if the QueryState does not already exist. The caller must call
  /// ReleaseQueryState() with the returned QueryState to decrement the refcount.
  QueryState* CreateQueryState(const TQueryCtx& query_ctx);

  /// If a QueryState for the given query exists, increments that refcount and returns
  /// the QueryState, otherwise returns nullptr.
  QueryState* GetQueryState(const TUniqueId& query_id);

  /// Decrements the refcount for the given QueryState.
  void ReleaseQueryState(QueryState* qs);

 private:

  typedef ShardedQueryMap<QueryState*> QueryStateMap;
  QueryStateMap qs_map_;

  /// Gets the existing QueryState or creates a new one if not present.
  /// 'created' is set to true if it was created, false otherwise.
  /// Increments the refcount.
  QueryState* GetOrCreateQueryState(const TQueryCtx& query_ctx, bool* created);

  /// Execute instances and decrement refcount (acquire ownership of qs).
  void StartQueryHelper(QueryState* qs);
};
}

#endif
