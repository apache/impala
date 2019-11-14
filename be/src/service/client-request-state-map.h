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

#include "gutil/strings/substitute.h"
#include "util/sharded-query-map-util.h"

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
      const TUniqueId& query_id, std::shared_ptr<ClientRequestState> request_state) {
    ScopedShardedMapRef<std::shared_ptr<ClientRequestState>> map_ref(query_id, this);
    DCHECK(map_ref.get() != nullptr);

    auto entry = map_ref->find(query_id);
    if (entry != map_ref->end()) {
      // There shouldn't be an active query with that same id.
      // (query_id is globally unique)
      return Status(ErrorMsg(TErrorCode::INTERNAL_ERROR,
          strings::Substitute("query id $0 already exists", PrintId(query_id))));
    }
    map_ref->insert(make_pair(query_id, request_state));
    return Status::OK();
  }

  /// Deletes the specified (query_id, request_state) pair from the map and sets the given
  /// request_state pointer to the ClientRequestState associated with the given query_id.
  /// If request_state == nullptr, it is not set. Returns an error Status if the query_id
  /// cannot be found in the map.
  Status DeleteClientRequestState(const TUniqueId& query_id,
      std::shared_ptr<ClientRequestState>* request_state = nullptr) {
    ScopedShardedMapRef<std::shared_ptr<ClientRequestState>> map_ref(query_id, this);
    DCHECK(map_ref.get() != nullptr);
    auto entry = map_ref->find(query_id);
    if (entry == map_ref->end()) {
      string error_msg =
          strings::Substitute("Invalid or unknown query handle $0", PrintId(query_id));
      VLOG(1) << error_msg;
      return Status::Expected(error_msg);
    } else if (request_state != nullptr) {
      *request_state = entry->second;
    }
    map_ref->erase(entry);
    return Status::OK();
  }
};
}
