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


#ifndef IMPALA_UTIL_CONTAINER_UTIL_H
#define IMPALA_UTIL_CONTAINER_UTIL_H

#include <map>
#include <unordered_map>
#include <boost/unordered_map.hpp>
#include <boost/functional/hash.hpp>

#include "util/hash-util.h"

#include "gen-cpp/ErrorCodes_types.h"
#include "gen-cpp/Frontend_types.h"
#include "gen-cpp/Status_types.h"
#include "gen-cpp/Types_types.h"
#include "gen-cpp/StatestoreService_types.h"
#include "gen-cpp/common.pb.h"

/// Comparators for types that we commonly use in containers.
namespace impala {

// This function and the following macro are used to assert that the size of the type T
// does not change unexpectedly. This helps to ensure that the operators take all fields
// of a struct into consideration. The benefit of this solution over a simple static
// assert is that it includes the expected and actual struct sizes in the compile time
// error message.
template <typename T, int64_t Expected, int64_t Actual = sizeof(T)>
constexpr void static_assert_size() {
  static_assert(Expected == Actual, "Type has unexpected size");
}
#define STATIC_ASSERT_SIZE(type, expected) \
  inline void static_assert_size_##type() { static_assert_size<type, expected>(); }

// TUniqueId
STATIC_ASSERT_SIZE(TUniqueId, 24);

inline bool operator==(const TUniqueId& lhs, const TUniqueId& rhs) {
  return std::tie(lhs.hi, lhs.lo) == std::tie(rhs.hi, rhs.lo);
}

inline bool operator!=(const TUniqueId& lhs, const TUniqueId& rhs) {
  return !(lhs == rhs);
}

inline bool operator<(const TUniqueId& lhs, const TUniqueId& rhs) {
  return std::tie(lhs.hi, lhs.lo) < std::tie(rhs.hi, rhs.lo);
}

// UniqueIdPB
STATIC_ASSERT_SIZE(UniqueIdPB, 40);

inline bool operator==(const UniqueIdPB& lhs, const UniqueIdPB& rhs) {
  return lhs.hi() == rhs.hi() && lhs.lo() == rhs.lo();
}

inline bool operator!=(const UniqueIdPB& lhs, const UniqueIdPB& rhs) {
  return !(lhs == rhs);
}

inline bool operator<(const UniqueIdPB& lhs, const UniqueIdPB& rhs) {
  return lhs.hi() < rhs.hi() || (lhs.hi() == rhs.hi() && lhs.lo() < rhs.lo());
}

// TNetworkAddress
STATIC_ASSERT_SIZE(TNetworkAddress, 88);

inline bool operator==(const TNetworkAddress& lhs, const TNetworkAddress& rhs) {
  return std::tie(lhs.hostname, lhs.port) == std::tie(rhs.hostname, rhs.port);
}

inline bool operator!=(const TNetworkAddress& lhs, const TNetworkAddress& rhs) {
  return !(lhs == rhs);
}

// NetworkAddressPB
STATIC_ASSERT_SIZE(NetworkAddressPB, 48);

inline bool operator==(const NetworkAddressPB& lhs, const NetworkAddressPB& rhs) {
  return lhs.hostname() == rhs.hostname() && lhs.port() == rhs.port();
}

inline bool operator!=(const NetworkAddressPB& lhs, const NetworkAddressPB& rhs) {
  return !(lhs == rhs);
}

// TStatus
STATIC_ASSERT_SIZE(TStatus, 48);

inline bool operator==(const TStatus& lhs, const TStatus& rhs) {
  //static_assert_size<TStatus, 48>();
  return std::tie(lhs.status_code, lhs.error_msgs)
      == std::tie(rhs.status_code, rhs.error_msgs);
}

// TCounter
STATIC_ASSERT_SIZE(TCounter, 56);

inline bool operator==(const TCounter& lhs, const TCounter& rhs) {
  return std::tie(lhs.name, lhs.unit, lhs.value)
      == std::tie(rhs.name, rhs.unit, rhs.value);
}

// THeavyMemoryQuery
STATIC_ASSERT_SIZE(THeavyMemoryQuery, 40);

inline bool operator>(const THeavyMemoryQuery& lhs, const THeavyMemoryQuery& rhs) {
  return std::tie(lhs.memory_consumed, lhs.queryId)
      > std::tie(rhs.memory_consumed, rhs.queryId);
}

/// Hash function for TNetworkAddress. This function must be called hash_value to be picked
/// up properly by boost.
inline std::size_t hash_value(const TNetworkAddress& host_port) {
  uint32_t hash =
      HashUtil::Hash(host_port.hostname.c_str(), host_port.hostname.length(), 0);
  return HashUtil::Hash(&host_port.port, sizeof(host_port.port), hash);
}

/// Hash function for NetworkAddressPB. This function must be called hash_value to be
/// picked up properly by boost.
inline std::size_t hash_value(const NetworkAddressPB& host_port) {
  uint32_t hash =
      HashUtil::Hash(host_port.hostname().c_str(), host_port.hostname().length(), 0);
  int32_t port = host_port.port();
  return HashUtil::Hash(&port, sizeof(port), hash);
}

} // end namespace impala

/// Hash function for std:: containers
namespace std {

template<> struct hash<impala::TNetworkAddress> {
  std::size_t operator()(const impala::TNetworkAddress& host_port) const {
    return impala::hash_value(host_port);
  }
};

template <>
struct hash<impala::NetworkAddressPB> {
  std::size_t operator()(const impala::NetworkAddressPB& host_port) const {
    return impala::hash_value(host_port);
  }
};

} // end namespace std

namespace impala {

struct HashTNetworkAddressPtr : public std::unary_function<TNetworkAddress*, size_t> {
  size_t operator()(const TNetworkAddress* const& p) const { return hash_value(*p); }
};

struct TNetworkAddressPtrEquals : public std::unary_function<TNetworkAddress*, bool> {
  bool operator()(const TNetworkAddress* const& p1,
                  const TNetworkAddress* const& p2) const {
    return p1->hostname == p2->hostname && p1->port == p2->port;
  }
};


struct pair_hash {
  template <class T1, class T2>
  std::size_t operator () (const std::pair<T1, T2> &p) const {
    size_t seed = 0;
    boost::hash_combine(seed, std::hash<T1>{}(p.first));
    boost::hash_combine(seed, std::hash<T2>{}(p.second));
    return seed;
  }
};

/// FindOrInsert(): if the key is present, return the value; if the key is not present,
/// create a new entry (key, default_val) and return default_val.
/// TODO: replace with single template which takes a template param

template <typename K, typename V>
V* FindOrInsert(std::map<K,V>* m, const K& key, const V& default_val) {
  typename std::map<K,V>::iterator it = m->find(key);
  if (it == m->end()) {
    it = m->insert(std::make_pair(key, default_val)).first;
  }
  return &it->second;
}

template <typename K, typename V>
V* FindOrInsert(std::unordered_map<K,V>* m, const K& key, const V& default_val) {
  typename std::unordered_map<K,V>::iterator it = m->find(key);
  if (it == m->end()) {
    it = m->insert(std::make_pair(key, default_val)).first;
  }
  return &it->second;
}

template <typename K, typename V>
V* FindOrInsert(boost::unordered_map<K,V>* m, const K& key, const V& default_val) {
  typename boost::unordered_map<K,V>::iterator it = m->find(key);
  if (it == m->end()) {
    it = m->insert(std::make_pair(key, default_val)).first;
  }
  return &it->second;
}


/// FindWithDefault: if the key is present, return the corresponding value; if the key
/// is not present, return the supplied default value

template <typename K, typename V>
const V& FindWithDefault(const std::map<K, V>& m, const K& key, const V& default_val) {
  typename std::map<K,V>::const_iterator it = m.find(key);
  if (it == m.end()) return default_val;
  return it->second;
}

template <typename K, typename V>
const V& FindWithDefault(const boost::unordered_map<K, V>& m, const K& key,
                         const V& default_val) {
  typename boost::unordered_map<K,V>::const_iterator it = m.find(key);
  if (it == m.end()) return default_val;
  return it->second;
}

template <typename K, typename V>
const V& FindWithDefault(const google::protobuf::Map<K, V>& m, const K& key,
                         const V& default_val) {
  typename google::protobuf::Map<K,V>::const_iterator it = m.find(key);
  if (it == m.end()) return default_val;
  return it->second;
}

/// Merges (by summing) the values from two maps of values. The values must be
/// native types or support operator +=.
template<typename MAP_TYPE>
void MergeMapValues(const MAP_TYPE& src, MAP_TYPE* dst) {
  for (typename MAP_TYPE::const_iterator src_it = src.begin();
      src_it != src.end(); ++src_it) {
    typename MAP_TYPE::iterator dst_it = dst->find(src_it->first);
    if (dst_it == dst->end()) {
      (*dst)[src_it->first] = src_it->second;
    } else {
      dst_it->second += src_it->second;
    }
  }
}

} // end namespace impala

#endif
