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

#pragma once

#include <unordered_map>

#include "common/logging.h"
#include "gen-cpp/Frontend_types.h"
#include "gen-cpp/common.pb.h"

namespace impala {

class QuerySchedulePB;
class TQueryExecRequest;
class TQueryOptions;
class TPlanFragment;

/// This class is a wrapper around references to the parameters that the Coordinator needs
/// for query execution - the TExecRequest which is owned by the QueryDriver and the
/// QuerySchedulePB which is owned by the ClientRequestState - along with some convenience
/// functions for accessing them.
class QueryExecParams {
 public:
  QueryExecParams(
      const TExecRequest& exec_request, const QuerySchedulePB& query_schedule);

  const QuerySchedulePB& query_schedule() const { return query_schedule_; }

  const UniqueIdPB& query_id() const { return query_id_; }
  const TQueryExecRequest& query_exec_request() const {
    return exec_request_.query_exec_request;
  }
  const TQueryOptions& query_options() const { return exec_request_.query_options; }

  int32_t num_fragments() const { return fragments_.size(); }

  const std::vector<const TPlanFragment*>& GetFragments() const { return fragments_; }

  /// Return the coordinator fragment, or nullptr if there isn't one.
  const TPlanFragment* GetCoordFragment() const;

  /// Returns true if the the query has a result sink
  bool HasResultSink() const;

  /// Return the total number of instances across all fragments.
  int GetNumFragmentInstances() const;

 private:
  friend class CoordinatorBackendStateTest;

  UniqueIdPB query_id_;

  /// Owned by the QueryDriver and has query lifetime.
  const TExecRequest& exec_request_;

  /// List of pointers to fragments in 'exec_request_', indexed by fragment idx.
  std::vector<const TPlanFragment*> fragments_;

  /// Owned by the ClientRwquestState and has query lifetime.
  const QuerySchedulePB& query_schedule_;

  const TPlanFragment* GetCoordFragmentImpl() const;

  bool HasResultSink(const TPlanFragment* fragment) const;

  /// Constructor used for testing only. Does not initialize 'fragments_'.
  QueryExecParams(const UniqueIdPB& query_id, const TExecRequest& exec_request,
      const QuerySchedulePB& query_schedule);
};
} // namespace impala
