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

#include "util/sharded-query-map-util.h"

#include "runtime/query-driver.h"
#include "scheduling/admission-control-service.h"
#include "util/debug-util.h"

namespace impala {

template <typename K, typename V>
Status GenericShardedQueryMap<K, V>::Add(const K& query_id, const V& obj) {
  GenericScopedShardedMapRef<K, V> map_ref(query_id, this);
  DCHECK(map_ref.get() != nullptr);

  auto entry = map_ref->find(query_id);
  if (entry != map_ref->end()) {
    // There shouldn't be an active query with that same id.
    // (query_id is globally unique)
    return Status(ErrorMsg(TErrorCode::INTERNAL_ERROR,
        strings::Substitute("query id $0 already exists", PrintId(query_id))));
  }
  map_ref->insert(make_pair(query_id, obj));
  return Status::OK();
}

template <typename K, typename V>
Status GenericShardedQueryMap<K, V>::Get(const K& query_id, V* obj) {
  GenericScopedShardedMapRef<K, V> map_ref(query_id, this);
  DCHECK(map_ref.get() != nullptr);

  auto entry = map_ref->find(query_id);
  if (entry == map_ref->end()) {
    Status err = Status::Expected(TErrorCode::INVALID_QUERY_HANDLE, PrintId(query_id));
    VLOG(1) << err.GetDetail();
    return err;
  }
  *obj = entry->second;
  return Status::OK();
}

template <typename K, typename V>
Status GenericShardedQueryMap<K, V>::Delete(const K& query_id) {
  GenericScopedShardedMapRef<K, V> map_ref(query_id, this);
  DCHECK(map_ref.get() != nullptr);
  auto entry = map_ref->find(query_id);
  if (entry == map_ref->end()) {
    Status err = Status::Expected(TErrorCode::INVALID_QUERY_HANDLE, PrintId(query_id));
    VLOG(1) << err.GetDetail();
    return err;
  }
  map_ref->erase(entry);
  return Status::OK();
}

// Needed by ImpalaServer
template Status GenericShardedQueryMap<TUniqueId, std::shared_ptr<QueryDriver>>::Add(
    TUniqueId const&, const std::shared_ptr<QueryDriver>&);
template Status GenericShardedQueryMap<TUniqueId, std::shared_ptr<QueryDriver>>::Get(
    TUniqueId const&, std::shared_ptr<QueryDriver>*);
template Status GenericShardedQueryMap<TUniqueId, std::shared_ptr<QueryDriver>>::Delete(
    TUniqueId const&);

// Needed by AdmissionControlService
template Status GenericShardedQueryMap<UniqueIdPB,
    std::shared_ptr<AdmissionControlService::AdmissionState>>::Add(UniqueIdPB const&,
    const std::shared_ptr<AdmissionControlService::AdmissionState>&);
template Status GenericShardedQueryMap<UniqueIdPB,
    std::shared_ptr<AdmissionControlService::AdmissionState>>::Get(UniqueIdPB const&,
    std::shared_ptr<AdmissionControlService::AdmissionState>*);
template Status GenericShardedQueryMap<UniqueIdPB,
    std::shared_ptr<AdmissionControlService::AdmissionState>>::Delete(UniqueIdPB const&);

} // namespace impala
