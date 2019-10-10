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

class QueryDriver;

/// A ShardedQueryMap for QueryDrivers. Maps a query_id to its corresponding
/// QueryDriver. Provides helper methods to easily add and delete
/// QueryDrivers from a ShardedQueryMap. The QueryDrivers are non-const, so
/// users of this class can synchronize access to the QueryDrivers by creating a
/// ScopedShardedMapRef.
class QueryDriverMap : public ShardedQueryMap<std::shared_ptr<QueryDriver>> {
 public:
  /// Adds the given (query_id, query_driver) pair to the map. Returns an error Status
  /// if the query id already exists in the map.
  Status AddQueryDriver(
      const TUniqueId& query_id, std::shared_ptr<QueryDriver> request_state);

  /// Deletes the specified (query_id, query_driver) pair from the map. Returns an error
  /// Status if the query_id cannot be found in the map.
  Status DeleteQueryDriver(const TUniqueId& query_id);
};
}
