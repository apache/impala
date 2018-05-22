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


#ifndef IMPALA_UTIL_UID_UTIL_H
#define IMPALA_UTIL_UID_UTIL_H

#include <boost/functional/hash.hpp>
#include <boost/uuid/uuid.hpp>
#include <boost/uuid/uuid_generators.hpp>

#include "gen-cpp/Types_types.h"  // for TUniqueId
#include "gen-cpp/control_service.pb.h"
#include "util/debug-util.h"

namespace impala {

inline std::size_t hash_value(const TUniqueId& id) {
  std::size_t seed = 0;
  boost::hash_combine(seed, id.lo);
  boost::hash_combine(seed, id.hi);
  return seed;
}

}

/// Hash function for std:: containers
namespace std {

template<> struct hash<impala::TUniqueId> {
  std::size_t operator()(const impala::TUniqueId& id) const {
    return impala::hash_value(id);
  }
};

}

namespace impala {

inline void UUIDToTUniqueId(const boost::uuids::uuid& uuid, TUniqueId* unique_id) {
  memcpy(&(unique_id->hi), &uuid.data[0], 8);
  memcpy(&(unique_id->lo), &uuid.data[8], 8);
}

inline void TUniqueIdToUniqueIdPB(
    const TUniqueId& t_unique_id, UniqueIdPB* unique_id_pb) {
  unique_id_pb->set_lo(t_unique_id.lo);
  unique_id_pb->set_hi(t_unique_id.hi);
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

template <typename F, typename T>
inline T CastTUniqueId(const F& from) {
  T to;
  to.hi = from.hi;
  to.lo = from.lo;
  return to;
}

/// generates a 16 byte UUID
inline string GenerateUUIDString() {
  boost::uuids::basic_random_generator<boost::mt19937> gen;
  boost::uuids::uuid u = gen();
  string uuid(u.begin(), u.end());
  return uuid;
}

/// generates a 16 byte UUID
inline TUniqueId GenerateUUID() {
  const string& u = GenerateUUIDString();
  TUniqueId uid;
  memcpy(&uid.hi, u.data(), sizeof(int64_t));
  memcpy(&uid.lo, u.data() + sizeof(int64_t), sizeof(int64_t));
  return uid;
}

} // namespace impala
#endif
