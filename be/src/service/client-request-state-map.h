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

#include "common/status.h"
#include "util/sharded-query-map-util.h"
#include "util/unique-id-hash.h"

namespace impala {

class ClientRequestState;

/// A ShardedQueryMap for ClientRequestStates. Maps a query_id to its corresponding
/// ClientRequestState. Provides helper methods to easily add and delete
/// ClientRequestStates from a ShardedQueryMap. The ClientRequestStates are non-const, so
/// users of this class need to synchronize access to the ClientRequestStates either by
/// creating a ScopedShardedMapRef or by locking on the ClientRequestState::lock().
class ClientRequestStateMap
    : public ShardedQueryMap<std::shared_ptr<ClientRequestState>> {
 public:
  /// Adds the given (query_id, request_state) pair to the map. Returns an error Status
  /// if the query id already exists in the map.
  Status AddClientRequestState(
      const TUniqueId& query_id, std::shared_ptr<ClientRequestState> request_state);

  /// Deletes the specified (query_id, request_state) pair from the map. Returns an error
  /// Status if the query_id cannot be found in the map.
  Status DeleteClientRequestState(const TUniqueId& query_id);
};
}
