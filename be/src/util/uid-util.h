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

#include <string.h>
#include <cstdint>
#include <string>

#include <boost/uuid/uuid.hpp>

#include "common/logging.h"
#include "gen-cpp/Types_types.h"
#include "gen-cpp/common.pb.h"

namespace impala {

inline void UUIDToTUniqueId(const boost::uuids::uuid& uuid, TUniqueId* unique_id) {
  memcpy(&(unique_id->hi), &uuid.data[0], 8);
  memcpy(&(unique_id->lo), &uuid.data[8], 8);
}

inline void UUIDToUniqueIdPB(const boost::uuids::uuid& uuid, UniqueIdPB* unique_id) {
  uint64_t hi, lo;
  memcpy(&hi, &uuid.data[0], 8);
  memcpy(&lo, &uuid.data[8], 8);
  unique_id->set_hi(hi);
  unique_id->set_lo(lo);
}

inline void TUniqueIdToUniqueIdPB(
    const TUniqueId& t_unique_id, UniqueIdPB* unique_id_pb) {
  unique_id_pb->set_lo(t_unique_id.lo);
  unique_id_pb->set_hi(t_unique_id.hi);
}

inline void UniqueIdPBToTUniqueId(
    const UniqueIdPB& unique_id_pb, TUniqueId* t_unique_id) {
  t_unique_id->__set_lo(unique_id_pb.lo());
  t_unique_id->__set_hi(unique_id_pb.hi());
}

/// Query id: uuid with bottom 4 bytes set to 0
/// Fragment instance id: query id with instance index stored in the bottom 4 bytes

constexpr int64_t FRAGMENT_IDX_MASK = (1L << 32) - 1;

inline TUniqueId UuidToQueryId(const boost::uuids::uuid& uuid) {
  TUniqueId result;
  memcpy(&result.hi, &uuid.data[0], 8);
  memcpy(&result.lo, &uuid.data[8], 8);
  result.lo &= ~FRAGMENT_IDX_MASK;  // zero out bottom 4 bytes
  return result;
}

inline TUniqueId ProtoToQueryId(const UniqueIdPB& uid_pb) {
  DCHECK(uid_pb.IsInitialized());
  TUniqueId result;
  result.hi = uid_pb.hi();
  result.lo = uid_pb.lo();
  return result;
}

inline TUniqueId GetQueryId(const TUniqueId& fragment_instance_id) {
  TUniqueId result = fragment_instance_id;
  result.lo &= ~FRAGMENT_IDX_MASK;  // zero out bottom 4 bytes
  return result;
}

inline int32_t GetInstanceIdx(const TUniqueId& fragment_instance_id) {
  return fragment_instance_id.lo & FRAGMENT_IDX_MASK;
}

inline int32_t GetInstanceIdx(const UniqueIdPB& fragment_instance_id) {
  return fragment_instance_id.lo() & FRAGMENT_IDX_MASK;
}

inline bool IsValidFInstanceId(const TUniqueId& fragment_instance_id) {
  return fragment_instance_id.hi != 0L;
}

inline TUniqueId CreateInstanceId(
    const TUniqueId& query_id, int32_t instance_idx) {
  DCHECK_EQ(GetInstanceIdx(query_id), 0);  // well-formed query id
  DCHECK_GE(instance_idx, 0);
  TUniqueId result = query_id;
  result.lo += instance_idx;
  return result;
}

inline UniqueIdPB CreateInstanceId(const UniqueIdPB& query_id, int32_t instance_idx) {
  DCHECK_EQ(GetInstanceIdx(query_id), 0); // well-formed query id
  DCHECK_GE(instance_idx, 0);
  UniqueIdPB result = query_id;
  result.set_lo(result.lo() + instance_idx);
  return result;
}

template <typename F, typename T>
inline T CastTUniqueId(const F& from) {
  T to;
  to.hi = from.hi;
  to.lo = from.lo;
  return to;
}

/// generates a 16 byte UUID
std::string GenerateUUIDString();

/// generates a 16 byte UUID
inline TUniqueId GenerateUUID() {
  const std::string& u = GenerateUUIDString();
  TUniqueId uid;
  memcpy(&uid.hi, u.data(), sizeof(int64_t));
  memcpy(&uid.lo, u.data() + sizeof(int64_t), sizeof(int64_t));
  return uid;
}

/// Determines if a query id is empty.
inline bool UUIDEmpty(const TUniqueId& id) {
  return id.hi == 0 && id.lo == 0;
}
} // namespace impala
